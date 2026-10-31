package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.planner.SqlTextUtil;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.Split;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.ScanReader;

import java.util.ArrayList;
import java.util.List;

/**
 * 单切片扫描适配算子 —— 把连接器的切片读取（{@link DojaoConnector#openSplit}）包成拉取模型
 * 算子（{@code open / next / close}），供任意下游算子链消费（分布式 Producer 切片扫描、分布式
 * 扫描任务的本片内排序尾等）。切片已由 Master 侧切片规划生成（自完备、带范围谓词 + 可选私有
 * 载荷），这里只做机械读取，不再二次分区。
 *
 * <p><b>游标归属</b>：{@code doOpen} 从连接器开一个 {@link ScanReader}，此后「读到哪儿了」属于
 * 本算子而非连接器对象 —— 于是同一连接器实例可被多个扫描线程共享，不再依赖引擎复制连接器。</p>
 *
 * <p>页生命周期：{@code next()} 返回的数据页所有权移交调用方（下游算子），EOF 页原样透传；
 * {@code doClose} 关闭游标，幂等。</p>
 *
 * @author jiaxin
 * @version 1.1
 * @date 2026/9/6
 */
public final class ConnectorScanOperator extends AbstractMeasurableOperator {

    private final DojaoConnector conn;
    private final ReadRequest request;
    private final Split split;

    /** doOpen 打开的切片游标（doClose 释放）。 */
    private ScanReader reader;
    /** 已读到 EOF：之后的 next() 直接返回 EOF，不再打扰连接器（防御连接器实现不一致）。 */
    private boolean done;

    /** 整表切片（无私有载荷）—— 谓词型连接器与单切片扫描用。 */
    public ConnectorScanOperator(DojaoConnector conn, ReadRequest request) {
        this(conn, request, Split.WHOLE);
    }

    public ConnectorScanOperator(DojaoConnector conn, ReadRequest request, Split split) {
        super("Scan", "Scan", 1);
        this.conn = conn;
        this.request = request;
        this.split = split == null ? Split.WHOLE : split;
    }

    /**
     * 构造一次「切片扫描 + 谓词就地执行」的算子链 —— <b>谓词永远在数据所在地执行</b>。
     *
     * <p>{@link ReadRequest#filterExpression()} 的语义是「本次扫描必须应用的谓词<b>全集</b>」，
     * 由本方法按连接器能力拆成两半：</p>
     * <ul>
     *   <li><b>连接器兑现下推</b>（{@link DojaoConnector#supportsPredicatePushdown()}）→ 数据源
     *       认识的合取项（列比较 / 数据源内建函数）留在请求里交给数据源在扫描时过滤；<b>引擎注册
     *       标量函数谓词</b>（如 {@code is_high(rating) = 1}）数据源不认识，从请求里<b>摘掉</b>，
     *       改由本节点的 {@link FilterOperator} 求值。摘掉这一步是必须的 —— 原样发给数据源会
     *       报「不认识该函数」或静默忽略（多返回行）。</li>
     *   <li><b>连接器不兑现</b>（如 Hive：只按切片读文件、不求值 {@code filterExpression}）→ 整串
     *       都留给本节点的 {@link FilterOperator}（它支持引擎标量函数：{@code Form.FUNC_COMPARE} /
     *       {@code FUNC_IS_NULL}）。</li>
     * </ul>
     *
     * <p><b>为什么必须在扫描节点补而不是 Master 侧</b>：放到 Master 过滤等于把整表拉回来，大表直接
     * 打爆 Master 堆（实测 ~880MB → OOM），且 {@code extractBaseTable} 遇 Filter 节点即放弃
     * 分布式聚合/窗口。</p>
     *
     * <p>拆解口径与规划期 {@code Planner.splitScanPredicate} / {@code engineFilterColumns} <b>完全同一份</b>
     * （{@link SqlTextUtil#splitByConnectorCapability}）：规划期据此决定「残留谓词列必须并进投影」，执行期
     * 据此决定「哪几项要补 Filter」——两边分叉就会出现「本以为数据源会过滤、实际没人过滤」的静默多返回行。
     * 逐项包裹而不整体一条串：{@code FilterOperator} 只编译单个简单谓词，整条合取串会落 UNKNOWN 静默放行全部行。</p>
     *
     * <p><b>在此之上再做一层「只减不加」的削减</b>：连接器声明已被分区裁剪兑现的合取项
     * （{@link DojaoConnector#partitionEnforcedConjuncts}）从待补 Filter 的清单里剔除。方向安全 ——
     * 削减只会让<b>引擎少留几列</b>需求，而规划期的投影是「所需的超集」，超集永远够用；且被剔除的项
     * 由连接器按「保住的分区内每一行都必然成立」担保，无需引擎复算。</p>
     */
    public static Operator openWithEngineFilter(DojaoConnector conn, ReadRequest request, Split split) {
        ReadRequest connRequest = request;
        List<String> engineConds = List.of();
        if (conn != null && request != null) {
            String expr = request.filterExpression().orElse(null);
            if (expr != null && !expr.isBlank()) {
                SqlTextUtil.PredicateSplit sp = SqlTextUtil.splitByConnectorCapability(expr);
                if (conn.supportsPredicatePushdown()) {
                    engineConds = sp.residualLeaves();          // 只有引擎函数谓词留在引擎侧
                    connRequest = request.withFilter(sp.pushdown());   // 数据源只拿它认识的
                } else {
                    engineConds = SqlTextUtil.splitConjuncts(expr);    // 整串引擎求值
                }
                // ★分区裁剪已兑现的合取项不必再求值：连接器按它裁掉了不匹配的分区之后，保留下来的每个
                //   分区内每一行都满足它（由连接器担保，见 DojaoConnector#partitionEnforcedConjuncts）。
                //   省掉的就是扫描之上那一层逐行重物化整页的重复 Filter。
                engineConds = dropPartitionEnforced(conn, request, engineConds);
            }
        }
        Operator chain = new ConnectorScanOperator(conn, connRequest, split);
        for (String cond : engineConds) {
            chain = new FilterOperator(chain, cond);
        }
        return chain;
    }

    /**
     * 减去连接器声明「已由分区裁剪兑现」的合取项（{@link DojaoConnector#partitionEnforcedConjuncts}）。
     * 连接器不声明 / 没兑现任何项 / 判定过程出错 → 原样返回（既有行为不变，只是少省一点）。
     *
     * <p><b>错误一律吞回退</b>：兑现判定只是一层优化，连接器侧（如 Hive 查 HMS）偶发失败不该让查询挂掉
     * —— 退回「引擎照旧自行求值」，结果仍然正确。</p>
     */
    private static List<String> dropPartitionEnforced(DojaoConnector conn, ReadRequest request,
                                                      List<String> engineConds) {
        if (conn == null || engineConds.isEmpty()) return engineConds;
        List<String> enforced;
        try {
            enforced = conn.partitionEnforcedConjuncts(request, engineConds);
        } catch (RuntimeException e) {
            return engineConds;
        }
        if (enforced == null || enforced.isEmpty()) return engineConds;
        List<String> out = new ArrayList<>(engineConds);
        out.removeAll(enforced);
        return out;
    }

    @Override
    protected void doOpen() {
        reader = conn.openSplit(request, split);
    }

    @Override
    protected Page doNext() throws Exception {
        if (done) return Page.eof();
        // Worker 级背压：节点堆外用量吃紧时放缓读取 —— 从<b>数据源头</b>降低摄入速率，让下游溢写
        // 追上来。这是「内存不够 → 磁盘来凑」的入口闸门：与其把节点撑爆，不如读得慢一点。
        NodeMemory.shared().throttle();
        Page p = reader.next();
        if (p == null || p.isEOF()) {
            done = true;
            return Page.eof();
        }
        return p;
    }

    @Override
    protected void doClose() {
        if (reader != null) {
            try {
                reader.close();
            } catch (Throwable ignored) {
            }
        } else {
            try {
                conn.closeRead();
            } catch (Throwable ignored) {
            }
        }
    }
}
