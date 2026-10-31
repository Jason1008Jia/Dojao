package com.dojao.server.test;

import com.dojao.common.data.Page;
import com.dojao.core.operator.FilterOperator;
import com.dojao.core.operator.Operator;
import com.dojao.core.planner.PhysicalNode;
import com.dojao.core.planner.PhysicalPlan;
import com.dojao.core.planner.PlanningContext;
import com.dojao.core.planner.Planner;
import com.dojao.core.runtime.master.WorkerRegistry;
import com.dojao.core.runtime.worker.ResourceSummary;
import com.dojao.core.runtime.worker.WorkerNode;
import com.dojao.core.runtime.worker.WorkerState;
import com.dojao.grammar.analyzer.plan.LogicalPlan;
import com.dojao.metastore.MetaStore;
import com.dojao.metastore.MetaStoreType;
import com.dojao.server.master.SqlValidator;
import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.ColumnStats;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.Schema;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.bean.Table;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.bean.WriteRequest;
import com.dojao.spi.service.DojaoConnector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * <b>HAVING 引用「SELECT 已投影但没起别名」的聚合</b>时过滤完全失效的缺陷探针。
 *
 * <p>用户线上 SQL：
 * <pre>
 *   SELECT spu_id, count(*) FROM aliyun002_mysql.dojao.spu
 *   GROUP BY spu_id HAVING count(*) &gt; 1
 * </pre>
 * 结果返回了大量 {@code count = 1} 的分组 —— HAVING 被静默丢弃。</p>
 *
 * <p><b>根因</b>：无别名聚合的输出列名就是归一化文本（{@code count(*)}）。{@code buildHaving}
 * 想下推到 {@link com.dojao.core.operator.HashAggregationOperator} 时用错了列清单（取的是聚合的
 * <b>子节点</b>列，永远查不到 {@code count(*)}）⇒ 下推失败 ⇒ 退回
 * {@link FilterOperator}。而 {@code FilterOperator.compile} 里 {@code count(*)} 既不是已注册标量
 * 函数、也不是裸列名 ⇒ 两侧都被当成字面量 ⇒ 落 {@code Form.UNKNOWN} = <b>逐行放行</b>。
 * 分布式聚合（{@code DistributedAggregation}）上游更是必然走这条 Filter 路径。</p>
 *
 * <p><b>不只 count：所有聚合函数同病</b>。{@code sum/avg/min/max/count(distinct)} 的无别名输出列
 * 同样是归一化文本（{@code sum(amount)}、{@code count(distinctamount)}…），落 {@code Form.UNKNOWN}
 * 的方式与计数完全一致，因此 H4/H6/H8–H20 全部覆盖，且包含复合谓词（AND/OR/NOT）、算术、
 * BETWEEN / IN / IS [NOT] NULL、限定列（{@code count(t.amount)}）、未投影聚合、NULL 聚合结果与
 * 派生表内层 HAVING。</p>
 *
 * <p><b>额外发现的第二条静默路径（H25）</b>：派生表内层 HAVING 走分布式计划时，谓词串随
 * {@code filterAfterAggregate} 下发到 Worker，Worker 侧<b>没有改名通道</b>，只能直接构造
 * {@link FilterOperator} ⇒ 无别名聚合列同样编译失败 = 逐行放行。现在 {@code Planner.planDerived}
 * 用 {@code filterableOnColumns} 把关：编译不出列就返回 {@code null} 回退单机计划，由
 * {@code buildHaving} 的改名方案正确过滤。</p>
 *
 * <p><b>聚合「参与表达式」再比较</b>（H26–H41、H43–H45）：{@code sum(x)+1 > 60}、
 * {@code sum(x)/count(*) > 20}、{@code round(sum(x)/count(*),1) > 20}、{@code max(x)-min(x) > 10}、
 * {@code abs(sum(x)-60) < 20}、{@code CAST(...) ... BETWEEN}、聚合参数本身是表达式
 * （{@code sum(amount+1)}）以及 NULL 结果经表达式传播 —— 全部必须真的过滤。这些形态由
 * {@code tryBuildGeneralHaving}（ScalarProject 求值布尔列 + Filter）承接。</p>
 *
 * <p><b>顺带修的列裁剪缺陷（H42）</b>：派生表内层 HAVING 用「聚合表达式」时，外层只按名要分组键，
 * 列裁剪会把 ScalarProject 仍要<b>透传</b>的无别名聚合列（{@code sum(amount)}）从子节点裁掉 ⇒
 * 运行期 {@code 标量投影透传列不存在: sum(amount)}。修：{@code computeChildRequirements} 对
 * {@link com.dojao.core.operator.ScalarProjectOperator} 追加它的全部透传列（计算列除外）。</p>
 *
 * <p><b>判别力</b>：修复前 H1/H4/H5 与分布式 H6 FAIL；恢复旧逻辑立刻 FAIL。H25 在去掉
 * {@code planDerived} 的闸门后会出现 {@code Filter[count(*) > 1]}（引用列为空）。H42 在去掉
 * 上面的透传列保留后逐字复现「标量投影透传列不存在」。</p>
 */
public class HavingProjectedAggNoAliasProbeTest {

    private static final String CATALOG = "mem";
    private static final String SCHEMA = "mem";
    private static final String T = "spu";

    private static int pass = 0;
    private static int fail = 0;
    private static BufferAllocator ALLOC;

    public static void main(String[] args) throws Exception {
        System.err.println("========== HAVING 引用「已投影无别名聚合」探针 ==========");
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ALLOC = allocator;
        WorkerRegistry registry = WorkerRegistry.get();
        try {
            // 数据：spu 1 → 2 行，spu 2 → 3 行，spu 3/4 → 各 1 行
            rows("H0 基线：无 HAVING 的分组计数",
                    "SELECT spu_id, count(*) FROM " + t() + " GROUP BY spu_id",
                    List.of(List.of(1L, 2L), List.of(2L, 3L), List.of(3L, 1L), List.of(4L, 1L)));

            rows("H1 用户原形：SELECT spu_id, count(*) ... HAVING count(*) > 1",
                    "SELECT spu_id, count(*) FROM " + t() + " GROUP BY spu_id HAVING count(*) > 1",
                    List.of(List.of(1L, 2L), List.of(2L, 3L)));

            rows("H2 对照：聚合带别名（HAVING c > 1）",
                    "SELECT spu_id, count(*) AS c FROM " + t() + " GROUP BY spu_id HAVING c > 1",
                    List.of(List.of(1L, 2L), List.of(2L, 3L)));

            rows("H3 对照：聚合未投影（内部列 __having_agg_0）",
                    "SELECT spu_id FROM " + t() + " GROUP BY spu_id HAVING count(*) > 1",
                    List.of(List.of(1L), List.of(2L)));

            rows("H4 无别名 min()：HAVING min(amount) > 20",
                    "SELECT spu_id, min(amount) FROM " + t() + " GROUP BY spu_id HAVING min(amount) > 20",
                    List.of(List.of(2L, 25L), List.of(3L, 55L), List.of(4L, 65L)));

            rows("H5 复合谓词：HAVING count(*) > 1 AND spu_id < 2",
                    "SELECT spu_id, count(*) FROM " + t() + " GROUP BY spu_id"
                            + " HAVING count(*) > 1 AND spu_id < 2",
                    List.of(List.of(1L, 2L)));

            rows("H6 无别名 sum()：HAVING sum(amount) > 50",
                    "SELECT spu_id, sum(amount) FROM " + t() + " GROUP BY spu_id HAVING sum(amount) > 50",
                    List.of(List.of(2L, 105L), List.of(3L, 55L), List.of(4L, 65L)));

            // ---- 其余聚合函数：结果同样必须被过滤（不能只有 count 走通） ----
            rows("H8 无别名 avg()：HAVING avg(amount) > 20",
                    "SELECT spu_id, avg(amount) FROM " + t() + " GROUP BY spu_id HAVING avg(amount) > 20",
                    List.of(List.of(2L, 35.0), List.of(3L, 55.0), List.of(4L, 65.0)));

            rows("H9 无别名 max(字符串)：HAVING max(name) > 'm'",
                    "SELECT spu_id, max(name) FROM " + t() + " GROUP BY spu_id HAVING max(name) > 'm'",
                    List.of(List.of(3L, "z")));

            // spu1 的 amount 为 5/15（去重 2 个）⇒ 也满足 > 1
            rows("H10 无别名 count(distinct)：HAVING count(distinct amount) > 1",
                    "SELECT spu_id, count(distinct amount) FROM " + t()
                            + " GROUP BY spu_id HAVING count(distinct amount) > 1",
                    List.of(List.of(1L, 2L), List.of(2L, 3L)));

            rows("H11 无别名 sum() BETWEEN",
                    "SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING sum(amount) BETWEEN 50 AND 110",
                    List.of(List.of(2L, 105L), List.of(3L, 55L), List.of(4L, 65L)));

            rows("H12 无别名 count(*) NOT(...)",
                    "SELECT spu_id, count(*) FROM " + t() + " GROUP BY spu_id HAVING NOT (count(*) > 1)",
                    List.of(List.of(3L, 1L), List.of(4L, 1L)));

            rows("H13 无别名 count(*) 算术：HAVING count(*) * 2 >= 4",
                    "SELECT spu_id, count(*) FROM " + t() + " GROUP BY spu_id HAVING count(*) * 2 >= 4",
                    List.of(List.of(1L, 2L), List.of(2L, 3L)));

            rows("H14 无别名 min() OR 分组键",
                    "SELECT spu_id, min(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING min(amount) > 20 OR spu_id = 1",
                    List.of(List.of(1L, 5L), List.of(2L, 25L), List.of(3L, 55L), List.of(4L, 65L)));

            rows("H15 未投影 avg()（__having_agg_N 内部列）",
                    "SELECT spu_id FROM " + t() + " GROUP BY spu_id HAVING avg(amount) > 30",
                    List.of(List.of(2L), List.of(3L), List.of(4L)));

            rows("H16 别名 avg() 小数比较：HAVING a > 27.5",
                    "SELECT spu_id, avg(amount) AS a FROM " + t() + " GROUP BY spu_id HAVING a > 27.5",
                    List.of(List.of(2L, 35.0), List.of(3L, 55.0), List.of(4L, 65.0)));

            rows("H17 无别名 count(限定列)：HAVING count(t.amount) > 1",
                    "SELECT t.spu_id, count(t.amount) FROM " + CATALOG + "." + SCHEMA + "." + T + " t"
                            + " GROUP BY t.spu_id HAVING count(t.amount) > 1",
                    List.of(List.of(1L, 2L), List.of(2L, 3L)));

            rows("H18 无别名 count(*) IN (2,3)",
                    "SELECT spu_id, count(*) FROM " + t() + " GROUP BY spu_id HAVING count(*) IN (2, 3)",
                    List.of(List.of(1L, 2L), List.of(2L, 3L)));

            rows("H19 无别名 count(*) IS NOT NULL（全保留）",
                    "SELECT spu_id, count(*) FROM " + t() + " GROUP BY spu_id HAVING count(*) IS NOT NULL",
                    List.of(List.of(1L, 2L), List.of(2L, 3L), List.of(3L, 1L), List.of(4L, 1L)));

            rows("H20 无别名 count(*) = 1",
                    "SELECT spu_id, count(*) FROM " + t() + " GROUP BY spu_id HAVING count(*) = 1",
                    List.of(List.of(3L, 1L), List.of(4L, 1L)));

            // NULL 聚合结果：SQL 三值逻辑（NULL 比较不成立、IS NULL 成立）
            rows("H21 无别名 sum() IS NULL（无 NULL 组 → 空结果）",
                    "SELECT spu_id, sum(amount) FROM " + t() + " GROUP BY spu_id HAVING sum(amount) IS NULL",
                    List.of());

            rows("H22 NULL 组：sum(amount) IS NULL 保留",
                    "SELECT spu_id, sum(amount) FROM " + CATALOG + "." + SCHEMA + ".spu_null"
                            + " GROUP BY spu_id HAVING sum(amount) IS NULL",
                    List.of(java.util.Arrays.asList(1L, null)));

            rows("H23 NULL 组：min(amount) IS NULL 保留",
                    "SELECT spu_id, min(amount) FROM " + CATALOG + "." + SCHEMA + ".spu_null"
                            + " GROUP BY spu_id HAVING min(amount) IS NULL",
                    List.of(java.util.Arrays.asList(1L, null)));

            // ---- 聚合「参与表达式」后再比较（HAVING 里不是裸聚合调用） ----
            rows("H26 聚合 + 算术：HAVING sum(amount) + 1 > 60",
                    "SELECT spu_id, sum(amount) FROM " + t() + " GROUP BY spu_id HAVING sum(amount) + 1 > 60",
                    List.of(List.of(2L, 105L), List.of(4L, 65L)));

            rows("H27 聚合 * 字面量：HAVING sum(amount) * 2 >= 100",
                    "SELECT spu_id, sum(amount) FROM " + t() + " GROUP BY spu_id HAVING sum(amount) * 2 >= 100",
                    List.of(List.of(2L, 105L), List.of(3L, 55L), List.of(4L, 65L)));

            rows("H28 聚合相除（count 未投影）：HAVING sum(amount) / count(*) > 20",
                    "SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING sum(amount) / count(*) > 20",
                    List.of(List.of(2L, 105L), List.of(3L, 55L), List.of(4L, 65L)));

            rows("H29 聚合套函数：HAVING round(sum(amount) / count(*), 1) > 20",
                    "SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING round(sum(amount) / count(*), 1) > 20",
                    List.of(List.of(2L, 105L), List.of(3L, 55L), List.of(4L, 65L)));

            rows("H30 两聚合相减（min 未投影）：HAVING max(amount) - min(amount) > 10",
                    "SELECT spu_id, max(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING max(amount) - min(amount) > 10",
                    List.of(List.of(2L, 45L)));

            rows("H31 聚合未投影 + 算术：SELECT spu_id ... HAVING sum(amount) + 5 > 60",
                    "SELECT spu_id FROM " + t() + " GROUP BY spu_id HAVING sum(amount) + 5 > 60",
                    List.of(List.of(2L), List.of(4L)));

            rows("H32 聚合套 abs：HAVING abs(sum(amount) - 60) < 20",
                    "SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING abs(sum(amount) - 60) < 20",
                    List.of(List.of(3L, 55L), List.of(4L, 65L)));

            rows("H33 两聚合相加（count 未投影）：HAVING sum(amount) + count(*) > 100",
                    "SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING sum(amount) + count(*) > 100",
                    List.of(List.of(2L, 105L)));

            rows("H34 聚合套 CAST + BETWEEN：CAST(sum(amount) AS DOUBLE) / count(*) BETWEEN 30 AND 40",
                    "SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING CAST(sum(amount) AS DOUBLE) / count(*) BETWEEN 30 AND 40",
                    List.of(List.of(2L, 105L)));

            rows("H35 聚合 * 小数：HAVING count(*) * 1.5 > 3",
                    "SELECT spu_id, count(*) FROM " + t() + " GROUP BY spu_id HAVING count(*) * 1.5 > 3",
                    List.of(List.of(2L, 3L)));

            rows("H36 字符串聚合进表达式（min 未投影）：HAVING min(name) < 'c'",
                    "SELECT spu_id, max(name) FROM " + t()
                            + " GROUP BY spu_id HAVING min(name) < 'c'",
                    List.of(List.of(1L, "beta"), List.of(2L, "d")));

            rows("H37 计算表达式起别名：HAVING avg_amt > 30",
                    "SELECT spu_id, sum(amount)/count(*) AS avg_amt FROM " + t()
                            + " GROUP BY spu_id HAVING avg_amt > 30",
                    List.of(List.of(2L, 35.0), List.of(3L, 55.0), List.of(4L, 65.0)));

            rows("H38 聚合 vs 聚合表达式：HAVING sum(amount) > count(*) * 10",
                    "SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING sum(amount) > count(*) * 10",
                    List.of(List.of(2L, 105L), List.of(3L, 55L), List.of(4L, 65L)));

            rows("H39 聚合表达式加括号：HAVING (sum(amount) - 50) * 2 > 0",
                    "SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING (sum(amount) - 50) * 2 > 0",
                    List.of(List.of(2L, 105L), List.of(3L, 55L), List.of(4L, 65L)));

            rows("H40 聚合的参数是表达式：sum(amount + 1)",
                    "SELECT spu_id, sum(amount + 1) FROM " + t()
                            + " GROUP BY spu_id HAVING sum(amount + 1) > 60",
                    List.of(List.of(2L, 108L), List.of(4L, 66L)));

            rows("H41 NULL 聚合经表达式传播（NULL + 1 仍不成立）",
                    "SELECT spu_id, sum(amount) FROM " + CATALOG + "." + SCHEMA + ".spu_null"
                            + " GROUP BY spu_id HAVING sum(amount) + 1 > 0",
                    List.of(List.of(2L, 5L)));

            rows("H42 派生表内层 HAVING（聚合表达式）+ JOIN（本地）",
                    "SELECT d.spu_id, t.amount FROM (SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING sum(amount) / count(*) > 20) d JOIN " + t()
                            + " t ON t.spu_id = d.spu_id",
                    List.of(List.of(2L, 25L), List.of(2L, 35L), List.of(2L, 45L),
                            List.of(3L, 55L), List.of(4L, 65L)));

            // 派生表内层 HAVING（无别名聚合）：本地执行必须真的过滤
            rows("H24 派生表内层 HAVING + JOIN（本地）",
                    "SELECT d.spu_id, t.amount FROM (SELECT spu_id, count(*) FROM " + t()
                            + " GROUP BY spu_id HAVING count(*) > 1) d JOIN " + t()
                            + " t ON t.spu_id = d.spu_id",
                    List.of(List.of(1L, 5L), List.of(1L, 15L),
                            List.of(2L, 25L), List.of(2L, 35L), List.of(2L, 45L)));

            distributedPlan(registry,
                    "SELECT spu_id, count(*) FROM " + t() + " GROUP BY spu_id HAVING count(*) > 1",
                    "H7 count(*)");
            distributedPlan(registry,
                    "SELECT spu_id, avg(amount) FROM " + t() + " GROUP BY spu_id HAVING avg(amount) > 20",
                    "H7b avg()");
            distributedPlan(registry,
                    "SELECT spu_id, count(distinct amount) FROM " + t()
                            + " GROUP BY spu_id HAVING count(distinct amount) > 1",
                    "H7c count(distinct)");

            distributedPlan(registry,
                    "SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING sum(amount) + 1 > 60",
                    "H43 聚合表达式 sum()+1");
            distributedPlan(registry,
                    "SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING sum(amount) / count(*) > 20",
                    "H44 聚合表达式 sum()/count()");
            distributedPlan(registry,
                    "SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING round(sum(amount) / count(*), 1) > 20",
                    "H45 聚合表达式 round(sum()/count())");

            // 派生表内层 HAVING（无别名聚合）的分布式计划：绝不能留下 UNKNOWN 过滤器（逐行放行）
            distributedNoUnknownFilter(registry,
                    "SELECT d.spu_id, t.amount FROM (SELECT spu_id, count(*) FROM " + t()
                            + " GROUP BY spu_id HAVING count(*) > 1) d JOIN " + t()
                            + " t ON t.spu_id = d.spu_id",
                    "H25 派生内层 HAVING 分布式计划不得有 UNKNOWN 过滤器");
            distributedNoUnknownFilter(registry,
                    "SELECT d.spu_id, t.amount FROM (SELECT spu_id, sum(amount) FROM " + t()
                            + " GROUP BY spu_id HAVING sum(amount) / count(*) > 20) d JOIN " + t()
                            + " t ON t.spu_id = d.spu_id",
                    "H46 派生内层 HAVING（聚合表达式）分布式计划不得有不可编译 HAVING");
        } finally {
            registry.unregister("hp-w0");
            registry.unregister("hp-w1");
            try {
                allocator.close();
            } catch (IllegalStateException leak) {
                System.err.println("    (allocator leak on close, ignored: " + leak.getMessage() + ")");
            }
        }
        System.err.printf("========== 结果: %d PASS / %d FAIL ==========%n", pass, fail);
        if (fail > 0) System.exit(1);
    }

    // ==================== 分布式计划形态 ====================

    /**
     * 分布式聚合位于 Master 本地 Filter 之下时，Filter 必须<b>真的能编译出列</b>
     * （{@code referencedColumns} 非空）。落 {@code Form.UNKNOWN} 就是逐行放行 —— 即线上
     * 「HAVING 完全失效」的签名。
     */
    private static void registerWorkers(WorkerRegistry registry) {
        long gb = 1024L * 1024 * 1024;
        for (int i = 0; i < 2; i++) {
            String id = "hp-w" + i;
            registry.register(new WorkerNode(id, "localhost", 0, "probe", System.nanoTime()));
            registry.updateHeartbeat(id, WorkerState.IDLE,
                    new ResourceSummary(4, 10, 8 * gb, 2 * gb, 0L, 0L, 4, System.nanoTime(), 0L, 0L, 0));
        }
    }

    private static void distributedPlan(WorkerRegistry registry, String sql, String label) {
        System.err.println("---------- " + label + " 分布式计划：HAVING 过滤器必须可编译（非 UNKNOWN） ----------");
        registerWorkers(registry);
        PhysicalPlan plan = plan(sql, true);
        if (plan == null) {
            check(label + " 分布式计划可生成", false);
            return;
        }
        System.err.println(indent(plan.explainText()));
        FilterOperator filter = findFilter(plan.root());
        check(label + " 计划里出现 HAVING 过滤器", filter != null);
        if (filter == null) return;
        System.err.println("    过滤谓词 = " + filter.filterExpression());
        System.err.println("    引用列 = " + filter.referencedColumns());
        check(label + " 过滤器可编译出真实的列引用（非 UNKNOWN = 逐行放行）",
                !filter.referencedColumns().isEmpty());
    }

    /**
     * 计划里<b>任何</b> {@link FilterOperator} 都不得落 {@code Form.UNKNOWN}（引用列为空 = 逐行放行）。
     * 用于派生表内层 HAVING 的分布式路径：Worker 侧只收谓词串、没有改名通道 —— 编译不出列的
     * 谓词必须让规划回退单机，而不是静默不过滤。
     */
    private static void distributedNoUnknownFilter(WorkerRegistry registry, String sql, String label) {
        System.err.println("---------- " + label + " ----------");
        registerWorkers(registry);
        PhysicalPlan plan = plan(sql, true);
        if (plan == null) {
            check(label + " 计划可生成", false);
            return;
        }
        System.err.println(indent(plan.explainText()));
        List<String> unknown = new ArrayList<>();
        collectUnknownFilters(plan.root(), unknown);
        check(label + "（发现 " + unknown.size() + " 个逐行放行的过滤器）", unknown.isEmpty());
        // 派生内层的 HAVING 不在计划树里当 FilterOperator 节点，而是随片段下发（having=…）——
        // 同样必须能编译出列引用，否则 Worker 侧照样逐行放行。
        List<String> uncompilableHaving = new ArrayList<>();
        collectUncompilableHaving(plan.root(), uncompilableHaving);
        check(label + "（发现 " + uncompilableHaving.size() + " 个不可编译的下发 HAVING" + uncompilableHaving + "）",
                uncompilableHaving.isEmpty());
    }

    private static void collectUnknownFilters(PhysicalNode node, List<String> out) {
        if (node == null) return;
        if (node.operator() instanceof FilterOperator f && f.referencedColumns().isEmpty()) {
            out.add(f.filterExpression());
        }
        for (PhysicalNode c : node.children()) collectUnknownFilters(c, out);
    }

    private static void collectUncompilableHaving(PhysicalNode node, List<String> out) {
        if (node == null) return;
        String name = node.name();
        int at = name.indexOf("having=");
        if (at >= 0) {
            // 节点名：…, having=<谓词>, <derivedKeys>=<peerKeys>, algo=…
            // ⇒ 谓词结束于「主键对清单」之前的最后一个 ", ["（谓词内部可能自带括号）
            int end = name.indexOf(", algo=", at);
            if (end < 0) end = name.lastIndexOf(']');
            if (end < 0) end = name.length();
            int keysAt = name.lastIndexOf(", [", end);
            if (keysAt > at) end = keysAt;
            String predicate = name.substring(at + "having=".length(), end);
            // 与 Planner.filterableOnColumns 同口径：既要编译出列引用，且这些引用必须真的是该节点的
            // 输出列。`count(*) > 1` 会被解析出 `count`（不是任何输出列）—— 送到 Worker 就是逐行放行。
            List<String> refs = FilterOperator.referencedColumnsOf(predicate);
            boolean ok = !refs.isEmpty() && refs.stream().allMatch(node::hasColumn);
            if (!ok) out.add(predicate + " → refs=" + refs);
        }
        for (PhysicalNode c : node.children()) collectUncompilableHaving(c, out);
    }

    // ==================== 执行 ====================

    private static void rows(String what, String sql, List<List<Object>> expected) {
        List<List<Object>> actual = run(sql);
        if (actual == null) {
            check(what + "（查询失败）", false);
            return;
        }
        // 结果顺序不保证（分组/join 输出顺序），两侧都按整行排序后再比
        List<List<Object>> want = new ArrayList<>(expected);
        want.sort(HavingProjectedAggNoAliasProbeTest::compareRow);
        boolean ok = actual.size() == want.size();
        if (ok) {
            for (int i = 0; i < want.size(); i++) {
                if (!rowEquals(want.get(i), actual.get(i))) { ok = false; break; }
            }
        }
        System.err.println("    actual = " + actual + " / expected = " + want);
        check(what, ok);
    }

    private static int compareRow(List<Object> a, List<Object> b) {
        int n = Math.min(a.size(), b.size());
        for (int i = 0; i < n; i++) {
            Object x = a.get(i);
            Object y = b.get(i);
            int c;
            if (x instanceof Number xn && y instanceof Number yn) {
                c = Double.compare(xn.doubleValue(), yn.doubleValue());
            } else if (x == null || y == null) {
                c = x == null ? (y == null ? 0 : -1) : 1;
            } else {
                c = x.toString().compareTo(y.toString());
            }
            if (c != 0) return c;
        }
        return Integer.compare(a.size(), b.size());
    }

    private static boolean rowEquals(List<Object> expected, List<Object> actual) {
        if (expected.size() != actual.size()) return false;
        for (int i = 0; i < expected.size(); i++) {
            Object e = expected.get(i);
            Object a = actual.get(i);
            if (e instanceof Number en && a instanceof Number an) {
                if (en.longValue() != an.longValue()) return false;
            } else if (e == null ? a != null : !e.toString().equals(String.valueOf(a))) {
                return false;
            }
        }
        return true;
    }

    private static List<List<Object>> run(String sql) {
        PhysicalPlan plan = plan(sql, false);
        if (plan == null) return null;
        if (System.getenv("PROBE_EXPLAIN") != null) {
            System.err.println("---- EXPLAIN: " + sql + "\n" + plan.explainText());
        }
        List<List<Object>> rows = new ArrayList<>();
        Operator op = plan.operator();
        try {
            op.open();
            Page p;
            while (!(p = op.next()).isEOF()) {
                VectorSchemaRoot root = p.getRoot();
                if (root != null) {
                    for (int r = 0; r < root.getRowCount(); r++) {
                        List<Object> row = new ArrayList<>(root.getFieldVectors().size());
                        for (FieldVector v : root.getFieldVectors()) {
                            row.add(v.isNull(r) ? null : v.getObject(r));
                        }
                        rows.add(row);
                    }
                }
                p.close();
            }
        } catch (Exception e) {
            System.err.println("    [" + sql + "] 本地执行失败: " + firstLine(e));
            return null;
        } finally {
            try { op.close(); } catch (Throwable ignored) { }
        }
        rows.sort(HavingProjectedAggNoAliasProbeTest::compareRow);
        return rows;
    }

    private static PhysicalPlan plan(String sql, boolean distributed) {
        Map<String, List<Object[]>> data = data();
        MetaStore metaStore = new MetaStore(MetaStoreType.LOCAL);
        metaStore.registerConnector(CATALOG, new InMemoryConnector(CATALOG, data, ALLOC));
        LogicalPlan logical;
        try {
            logical = new SqlValidator(metaStore).validate(sql);
        } catch (Exception e) {
            System.err.println("    [" + sql + "] 语义分析失败: " + firstLine(e));
            return null;
        }
        PlanningContext context = PlanningContext.builder(
                        catalog -> new InMemoryConnector(CATALOG, data, ALLOC))
                .distributedExecution(distributed)
                .memoryLimit(64L * 1024 * 1024)
                .parallelism(2)
                .build();
        try {
            return new Planner(context).plan(logical);
        } catch (Exception e) {
            System.err.println("    [" + sql + "] 物理规划失败: " + firstLine(e));
            return null;
        }
    }

    private static String t() {
        return CATALOG + "." + SCHEMA + "." + T;
    }

    private static Map<String, List<Object[]>> data() {
        Map<String, List<Object[]>> data = new LinkedHashMap<>();
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{1L, 5L, "alpha"});
        rows.add(new Object[]{1L, 15L, "beta"});
        rows.add(new Object[]{2L, 25L, "b"});
        rows.add(new Object[]{2L, 35L, "c"});
        rows.add(new Object[]{2L, 45L, "d"});
        rows.add(new Object[]{3L, 55L, "z"});
        rows.add(new Object[]{4L, 65L, "m"});
        data.put(T, rows);
        // 聚合输入全 NULL 的分组：sum/min 结果为 NULL（验证 HAVING 的三值逻辑）
        List<Object[]> nulls = new ArrayList<>();
        nulls.add(new Object[]{1L, null, null});
        nulls.add(new Object[]{1L, null, null});
        nulls.add(new Object[]{2L, 5L, "x"});
        data.put("spu_null", nulls);
        return data;
    }

    private static FilterOperator findFilter(PhysicalNode node) {
        if (node == null) return null;
        if (node.operator() instanceof FilterOperator f) return f;
        for (PhysicalNode c : node.children()) {
            FilterOperator hit = findFilter(c);
            if (hit != null) return hit;
        }
        return null;
    }

    private static String indent(String text) {
        StringBuilder sb = new StringBuilder();
        for (String line : text.split("\n")) sb.append("       | ").append(line).append('\n');
        return sb.toString();
    }

    private static String firstLine(Throwable t) {
        String s = String.valueOf(t);
        int i = s.indexOf('\n');
        String line = i < 0 ? s : s.substring(0, i);
        Throwable c = t.getCause();
        while (c != null) {
            line += " <- " + c.getClass().getSimpleName() + ": " + String.valueOf(c.getMessage());
            c = c.getCause();
        }
        return line;
    }

    private static void check(String what, boolean ok) {
        if (ok) { pass++; System.err.println("[PASS] " + what); }
        else { fail++; System.err.println("[FAIL] " + what); }
    }

    // ==================== 内存连接器 ====================

    private static final class InMemoryConnector implements DojaoConnector {

        private final String catalog;
        private final Map<String, List<Object[]>> data;
        private final BufferAllocator allocator;
        private static final int READ_BATCH = 4;

        private String cursorKey;
        private int cursorIdx;

        InMemoryConnector(String catalog, Map<String, List<Object[]>> data, BufferAllocator allocator) {
            this.catalog = catalog;
            this.data = data;
            this.allocator = allocator;
        }

        @Override
        public List<Field> sourceFields(String catalog, String schema, String table, List<String> columns) {
            List<Field> full = schemaFor().getFields();
            if (columns == null || columns.isEmpty()) return full;
            List<Field> out = new ArrayList<>();
            for (String name : columns) {
                String bare = name.replace("`", "");
                for (Field f : full) {
                    if (f.getName().equalsIgnoreCase(bare)) { out.add(f); break; }
                }
            }
            return out;
        }

        @Override public boolean supportsPredicatePushdown() { return false; }

        @Override
        public List<ScanSlice> planSplits(ReadRequest request, int parallelism) {
            return List.of(ScanSlice.whole(request));
        }

        @Override
        public Page read(ReadRequest request) {
            List<Object[]> rows = data.getOrDefault(request.table(), List.of());
            List<String> cols = request.columns().orElse(null);
            List<Object[]> projected = projectRows(rows, cols);

            String key = request.table() + "|" + (cols == null ? "*" : String.join(",", cols));
            if (!key.equals(cursorKey)) {
                cursorKey = key;
                cursorIdx = 0;
            }
            if (cursorIdx >= projected.size()) return Page.eof();

            List<Object[]> batch = new ArrayList<>();
            while (cursorIdx < projected.size() && batch.size() < READ_BATCH) {
                batch.add(projected.get(cursorIdx++));
            }

            List<Field> fields = sourceFields(catalog, request.schema(), request.table(), cols);
            VectorSchemaRoot root = VectorSchemaRoot.create(
                    new org.apache.arrow.vector.types.pojo.Schema(fields), allocator);
            root.allocateNew();
            for (int r = 0; r < batch.size(); r++) {
                Object[] row = batch.get(r);
                for (int c = 0; c < fields.size(); c++) setValue(root.getVector(c), r, row[c]);
            }
            root.setRowCount(batch.size());
            return Page.fromRoot(root);
        }

        private static void setValue(FieldVector v, int row, Object value) {
            if (value == null) { v.setNull(row); return; }
            switch (v) {
                case BigIntVector b -> b.setSafe(row, ((Number) value).longValue());
                case VarCharVector s -> s.setSafe(row, ((String) value).getBytes(StandardCharsets.UTF_8));
                default -> throw new IllegalStateException("unsupported vector: " + v.getClass());
            }
        }

        private List<Object[]> projectRows(List<Object[]> rows, List<String> cols) {
            if (cols == null || cols.isEmpty()) return rows;
            List<Field> full = schemaFor().getFields();
            int[] idx = new int[cols.size()];
            for (int i = 0; i < cols.size(); i++) {
                String bare = cols.get(i).replace("`", "");
                idx[i] = -1;
                for (int f = 0; f < full.size(); f++) {
                    if (full.get(f).getName().equalsIgnoreCase(bare)) { idx[i] = f; break; }
                }
            }
            List<Object[]> out = new ArrayList<>(rows.size());
            for (Object[] row : rows) {
                Object[] p = new Object[cols.size()];
                for (int i = 0; i < cols.size(); i++) {
                    p[i] = idx[i] < 0 || idx[i] >= row.length ? null : row[idx[i]];
                }
                out.add(p);
            }
            return out;
        }

        @Override public Optional<String> getPartitionKey(String schema, String table) { return Optional.empty(); }

        @Override
        public long getExactRowCount(String schema, String table) {
            List<Object[]> rows = data.get(table);
            return rows == null ? 0 : rows.size();
        }

        @Override
        public TableStats getTableStats(String catalog, String schema, String table) {
            long rows = getExactRowCount(schema, table);
            return new TableStats(catalog, schema, table, rows, Math.max(rows, 1) * 32L, List.of());
        }

        @Override public List<ColumnStats> getColumnStats(String catalog, String schema, String table) { return List.of(); }
        @Override public void write(WriteRequest request) { }
        @Override public boolean catalogExists(String catalog) { return this.catalog.equals(catalog); }
        @Override public Optional<Catalog> getCatalog(String catalog) { return Optional.empty(); }
        @Override public List<String> listCatalogs() { return List.of(catalog); }
        @Override public boolean schemaExists(String catalog, String schema) { return catalogExists(catalog); }
        @Override public Optional<Schema> getSchema(String catalog, String schema) { return Optional.empty(); }
        @Override public List<String> listSchemas(String catalog) { return catalogExists(catalog) ? List.of(SCHEMA) : List.of(); }
        @Override public boolean tableExists(String catalog, String schema, String table) { return data.containsKey(table); }
        @Override public Optional<Table> getTable(String catalog, String schema, String table) { return Optional.empty(); }

        @Override
        public List<Column> getTableColumns(String catalog, String schema, String table) {
            List<Column> out = new ArrayList<>();
            for (Field f : schemaFor().getFields()) {
                out.add(new Column(f.getName(), typeNameOf(f), null, null));
            }
            return out;
        }

        @Override public List<String> listTables(String catalog, String schema) {
            return catalogExists(catalog) ? new ArrayList<>(data.keySet()) : List.of();
        }

        @Override public boolean columnExists(String catalog, String schema, String table, String column) {
            return schemaFor().getFields().stream().anyMatch(f -> f.getName().equals(column));
        }

        @Override public Optional<Column> getColumn(String catalog, String schema, String table, String column) {
            return Optional.empty();
        }

        private static String typeNameOf(Field f) {
            if (f.getType() instanceof ArrowType.Utf8) return "varchar";
            return "bigint";
        }

        private static org.apache.arrow.vector.types.pojo.Schema schemaFor() {
            return new org.apache.arrow.vector.types.pojo.Schema(List.of(
                    new Field("spu_id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                    new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null),
                    new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        }
    }
}
