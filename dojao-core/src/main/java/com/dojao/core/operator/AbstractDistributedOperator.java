package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.core.config.ResourceConfig;
import com.dojao.core.operator.expr.ExprNode;
import com.dojao.core.operator.expr.ExprParser;
import com.dojao.core.operator.expr.ExprTypeInference;
import com.dojao.core.runtime.master.StageScheduler;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.worker.FragmentEvent;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.DojaoType;
import com.dojao.spi.service.DojaoConnector;
import java.util.function.Function;
import com.dojao.spi.util.DojaoTypeMapper;
import com.dojao.spi.util.SqlIdentifiers;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 分布式算子基类（Master 侧）—— 抽出 {@link DistributedJoinOperator} 共有的
 * "切分 → 派发 → 收集" 生命周期，供分布式聚合 / 窗口复用：
 *
 * <pre>
 *   doOpen（final）：
 *     dispatchStage()            子类：StageScheduler 切分 + 先 Consumer 后 Producer 派发
 *     numConsumers = Consumer 片段数
 *   doNext（final）：从结果队列拉 Worker 回传的数据页；
 *     全部 Consumer 完成 → EOF；任一失败 → fail-fast
 *   doClose（final）：反向取消所有片段任务，排空队列释放页，closeProbes() 释放探针连接器
 * </pre>
 *
 * <p>结果队列有界 → 回传即背压 → 消费慢时 Worker 侧算子阻塞 → 分区队列 → 发送端逐级阻塞。</p>
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/8/20
 */
public abstract class AbstractDistributedOperator extends AbstractMeasurableOperator
        implements ScanColumnAware {

    /** 结果队列容量（有界，回传即背压）。 */
    protected static final int RESULT_QUEUE_CAPACITY = 32;

    /**
     * 结果回传看门狗：从最后一次「接触」起，超过该毫秒仍无任何回传
     * → 判定结果回传链路故障，fail-fast 抛错替代无限挂起。
     *
     * <p><b>「接触」= 数据面回传（数据页 / 完成）<u>或</u>控制面存活心跳</b>（见
     * {@link #markAlive(String)}）。只看数据面会把「合法的长时间沉默」误杀：终端 Consumer
     * （如 {@code DistributedJoinAggregation}）要等<b>全部</b> Producer 把 shuffle 灌完才可能吐
     * 第一页，大表 join/聚合的这段沉默天然超过 60s —— 而 Worker 侧一直在正常干活。</p>
     *
     * <p>取值走配置 {@code query.result.watchdogMs}（conf/dojao.conf 或 {@code -Dquery.result.watchdogMs}
     * 覆盖），默认 <b>10 分钟</b> —— 长归并 / 长 build 常远超旧默认 60s，旧值会把合法长计算误杀，
     * 且报错文案（「疑似 result 回传链路故障」）指错方向（见 docs/deploy-linux.md）。</p>
     */
    private static final long WATCHDOG_IDLE_MS = ResourceConfig.resultWatchdogMs();

    protected final String queryId;
    protected final int numPartitions;
    protected final WorkerGateway gateway;
    protected final List<WorkerRegistration> workers;

    /** Worker 回传的<b>数据页</b>队列（有界：回传即背压）。 */
    protected final BlockingQueue<FragmentEvent> resultQueue = new LinkedBlockingQueue<>(RESULT_QUEUE_CAPACITY);
    /**
     * Worker 回传的<b>控制事件</b>队列（FINISHED / FAILED / PROGRESS，无界）。
     *
     * <p>为什么必须与控制面分开：消费方可能<b>合法地</b>长时间不读本算子（典型 = {@code UnionAllOperator}
     * 逐腿预取首屏：腿 0 要等 60s shuffle，腿 1..n 的算子已 open 但没人调 {@code next()}）。
     * 有界队列会被数据页 / 心跳灌满，而终止事件若也走它，就只能 5s 超时后丢弃 —— 算子永远等不到
     * 该片段完成，只能等 10 分钟看门狗（线上 ROLLUP：部分腿「投递片段完成事件失败」后整条查询挂住）。</p>
     *
     * <p>顺序安全：Worker 侧同一片段的数据页先入 {@link #resultQueue}，再改入本队列；因此 FINISHED
     * 到达时该片段的数据页必已入队。EOF 条件额外要求数据队列排空（见 {@link #doNext()}），
     * 控制事件先到也不会让数据丢失。</p>
     */
    protected final BlockingQueue<FragmentEvent> controlQueue = new LinkedBlockingQueue<>();
    /** 已完成的 Consumer 数，到齐 numConsumers 则 EOF。 */
    protected final AtomicInteger finishedCount = new AtomicInteger();
    /** 各 Consumer 片段回传的行数（可观测：验证数据确实分布在多个 Worker）。 */
    protected final Map<String, AtomicLong> rowsByFragment = new ConcurrentHashMap<>();
    /** 已派发片段：taskId → workerId（doClose 反向取消用）。 */
    protected final Map<String, String> taskOwner = new ConcurrentHashMap<>();
    /** 分布式写目标（可空）：非 null → 终端 Consumer 就地写走、不回传数据页。 */
    protected volatile FragmentSpec.SinkStep sink;
    /** 片内 top-N 截断尾（可空，Planner 命中下推 gate 时设置）：非 null → 终端 Consumer 本地
     *  按 ORDER BY 键排序 + 截断到 offset+limit 行后再回传候选集。 */
    protected volatile FragmentSpec.SortLimitStep tail;
    /** 终端二级聚合（可空，外层全局聚合下沉）：非 null → 终端窗口片段窗口输出后再本地聚合，
     *  每分区只回传 1 行 partial。 */
    protected volatile FragmentSpec.AggregateStep terminalPartialAggregate;
    /** join 之上的行级谓词（可空，join → Filter → 外层聚合下沉）：非 null → 终端 join 片段
     *  在本地 join 之后先过滤再聚合。 */
    protected volatile String terminalJoinFilter;
    /** 分布式写总行数：各分区 FragmentFinished.rowCount 求和（响应 rows_inserted）。 */
    protected final AtomicLong totalOutputRows = new AtomicLong();
    /** 片段任务跟踪：taskId → 运行态（监控用；生命周期 RUNNING → FINISHED / FAILED）。 */
    private final Map<String, FragmentTaskState> taskStates = new ConcurrentHashMap<>();
    /**
     * 最后一次控制面存活心跳时刻（{@link #markAlive(String)} 刷新，Worker 经 query_telemetry 上抛）。
     * 看门狗取「数据面事件时刻」与它的<b>较晚者</b>作判据 —— 二者任一说明查询在推进。
     */
    private volatile long lastHeartbeatNanos;

    // ---- INSERT OVERWRITE 生命周期（Feature B）：全成或全不成 ----
    // prepare    doOpen 派发前运行（权限探针 + 建 staging + setSink 换写到 staging）；可空
    // success    全部 N 分区 FragmentFinished 后恰好执行一次（commitOverwrite 原子切换）
    // abort      doClose 时未 success 则执行（abortOverwrite 删 staging，原表保持原样）
    private Runnable prepare;
    private volatile Runnable successFinalizer;
    private volatile Runnable abortFinalizer;
    private final AtomicBoolean finalized = new AtomicBoolean();

    protected volatile int numConsumers;
    protected volatile boolean eofReturned;
    protected volatile Throwable failure;

    // ---- CTAS：IF NOT EXISTS 且目标表已存在 → prepare 决定跳过派发（返回 rows=0，不触碰既有表） ----
    private volatile boolean skipDispatch;

    /** 让 doOpen 跳过派发（prepare 闭包里目标表已存在且 IF NOT EXISTS 时调用）→ doNext 立即 EOF。 */
    public void skipDispatch() {
        this.skipDispatch = true;
    }

    protected AbstractDistributedOperator(String name, String queryId, int numPartitions,
                                          WorkerGateway gateway, List<WorkerRegistration> workers) {
        super(name, name, Math.max(1, numPartitions));
        this.queryId = queryId;
        this.numPartitions = Math.max(1, numPartitions);
        this.gateway = gateway;
        this.workers = List.copyOf(workers);
    }

    /**
     * 切分并派发全部片段（先 Consumer 后 Producer，同步注册接收端），返回 Consumer 片段数。
     * 抛出的异常由 doOpen 向上传播 → 查询失败。
     */
    protected abstract int dispatchStage() throws Exception;

    /**
     * 本算子派发的全部基表扫描列集（{@link ScanColumnAware} 契约）—— 供
     * {@code ColumnPruningAudit} 在规划期审计「列裁剪是否随路径一路带下来」。
     * 子类必须如实自述；无基表扫描返回空表。
     */
    @Override
    public abstract List<ScanColumn> scanColumnSets();

    /** doClose 时释放探针连接器（无则空实现）。 */
    protected void closeProbes() {
    }

    /**
     * 结果页回传给下游前的最后一次修饰（默认原样返回）。
     *
     * <p>子类用它把 Worker 回传页裁剪到<b>算子声明的输出列</b>：为让 Worker 侧引擎 Filter 能求值
     * 谓词，扫描投影里会带上「只作过滤用、不在输出契约里」的列（如 Hive 不兑现下推时的分区列
     * {@code dt}）—— 该列必须随行数据流到 Filter，却绝不能出现在算子的输出页上（否则会漏进
     * {@code SELECT *} 结果、CTAS 建表语句）。</p>
     *
     * <p>只在 {@code doNext} 的数据页分支调用（EOF / 失败事件不经此）。裁剪须自行关闭入参页
     * （页所有权转移给返回页，与 {@code ProjectOperator} 一致）。</p>
     *
     * <p>回传页的分区号是 <b>Worker 内部 shuffle 桶布局</b>，与 Master 侧父算子自身的分区方案
     * 无关。父算子若是按 {@link Page#getPartitionId()} 分槽收集的本地 spillable 算子（如
     * {@code WindowSortOperator}），同一分组键会因分布在不同 shuffle 桶而被拆到不同槽、输出
     * 不再连续 ⇒ 下游窗口按分片重算（如 {@code sum(...) OVER (PARTITION BY ...)} 得每片小计）。
     * 故统一归一到 0 号分区，把回传页视为单一逻辑数据流（与 {@code UnionAllOperator} 同款不变量）。</p>
     */
    protected Page finalizeResultPage(Page page) {
        if (page == null || page.getRoot() == null || page.isEOF()) return page;
        // 已是 0 号分区或无分区号（下游 accumulatePage 按 orElse(0) 归 0）→ 原样
        if (page.getPartitionId().map(id -> id == 0).orElse(true)) return page;
        // 浅包装共享同一 root，源页丢弃不 close（root 由新页独占）
        return page.withPartitionId(0);
    }

    // ==================== CTAS 输出列推导（Master 派发前建目标表） ====================
    // 分布式写数据不回流，Master 须在派发前知道终端 Consumer 输出页的列（名 + 类型），
    // 才能按此先 createTable —— 各子类按其 worker 端终端算子拼 schema 的同一套规则复刻。

    /**
     * 本算子终端输出页的列清单（顺序 = 页 schema 顺序）。
     * 供 {@code Planner.buildCreateTableAsSelect} 在派发前建目标表。
     */
    public abstract List<Field> outputFields();

    /**
     * 读一张表扫描若干列会产出的 Arrow Field（保序）—— 源列类型是连接器的类型映射职责。
     * 优先走 {@link DojaoConnector#sourceFields}（连接器精确实现，如 MySQL 经 JDBC 元数据保
     * int 位宽 / decimal(p,s)）；连接器未实现 / 探针失败时回退 getColumnTypes → 宽安全默认。
     *
     * <p><b>列名先剥方言引号</b>：{@code ReadRequest.columns()} 里的是「可直接拼进 SELECT 的
     * 片段」（MySQL / ClickHouse 反引号、PostgreSQL 双引号，见连接器 {@code pruneColumns}），
     * 直接拿去查元数据永远查不到 → 类型静默退化成兜底口径（decimal 聚合被当 DOUBLE 等）。
     * 见 {@link SqlIdentifiers}。</p>
     *
     * @param columns 需要的列（保序）；null/空 = 整表（扫描读全表列时的形态，如 join 基表）
     */
    public static List<Field> scanFields(DojaoConnector probe, String catalog, String schema,
                                         String table, List<String> columns) {
        List<String> cols = (columns == null || columns.isEmpty())
                ? null : SqlIdentifiers.unquoteAll(columns);
        List<Field> exact = null;
        try {
            exact = probe.sourceFields(catalog, schema, table, cols);
        } catch (Throwable ignored) {
            // 连接器不认 sourceFields / 元数据读失败 → 回退 DojaoType 映射
        }
        if (cols == null) {
            if (exact != null && !exact.isEmpty()) {
                return exact;
            }
            return fallbackTableFields(probe, catalog, schema, table);
        }
        List<Field> out = new ArrayList<>();
        Map<String, Field> byName = new LinkedHashMap<>();
        if (exact != null) {
            for (Field f : exact) {
                byName.putIfAbsent(f.getName(), f);
            }
        }
        for (String n : cols) {
            Field f = byName.get(n);
            if (f == null) {
                // 连接器返回的列名大小写与请求不一致（MySQL 列名大小写不敏感）→ 忽略大小写再找一次，
                // 避免「查到了却按兜底类型」的静默降级。
                for (Map.Entry<String, Field> e : byName.entrySet()) {
                    if (e.getKey().equalsIgnoreCase(n)) {
                        f = e.getValue();
                        break;
                    }
                }
            }
            out.add(f != null ? f : fallbackColumnField(probe, schema, table, n));
        }
        return out;
    }

    /** 整表列兜底：getTableColumns 保序 + getColumnTypes（DojaoType）映射。 */
    private static List<Field> fallbackTableFields(DojaoConnector probe, String catalog,
                                                   String schema, String table) {
        List<Field> out = new ArrayList<>();
        try {
            Map<String, DojaoType> types = probe.getColumnTypes(schema, table);
            for (Column c : probe.getTableColumns(catalog, schema, table)) {
                DojaoType t = types.get(c.name());
                out.add(DojaoTypeMapper.toField(c.name(), t == null ? DojaoType.STRING : t));
            }
        } catch (Throwable ignored) {
            // 无元数据 → 空清单（调用方据缺列决定是否拒绝）
        }
        return out;
    }

    private static Field fallbackColumnField(DojaoConnector probe, String schema, String table,
                                             String name) {
        try {
            DojaoType t = probe.getColumnTypes(schema, table).get(name);
            return DojaoTypeMapper.toField(name, t);
        } catch (Throwable ignored) {
            return DojaoTypeMapper.toField(name, DojaoType.STRING);
        }
    }

    /**
     * 复刻 {@code HashJoinOperator.ensureOutputSchema} 的列取舍：左列全保留 + 右列（仅 INNER 剔除
     * 「右 join key 且左列已有同名」者）—— Worker join 输出页正是此顺序，CTAS 建表须一致。
     *
     * <p><b>外连接必须保留右键列</b>：未匹配行右键为 NULL、左键非 NULL，两者语义不同。丢掉右键列会让
     * 外层 {@code b.key} 按裸列名静默绑到左键列（恒非 NULL），{@code b.key IS NULL} 判定失效。</p>
     */
    protected static List<Field> joinOutputFields(JoinType joinType, List<Field> leftFields,
                                                  List<Field> rightFields,
                                                  List<String> leftKeys, List<String> rightKeys) {
        List<Field> out = new ArrayList<>(leftFields);
        boolean dedup = joinType == JoinType.INNER;
        for (Field rf : rightFields) {
            if (dedup && rightKeys.contains(rf.getName()) && leftKeys.contains(rf.getName())) {
                continue;
            }
            out.add(rf);
        }
        return out;
    }

    /** 聚合结果列 Field（复刻 AggregationHashTable 输出：outColumn + outputArrowType）。 */
    protected static Field aggregationOutputField(AggregationSpec a) {
        return aggregationOutputField(a, (Field) null);
    }

    /**
     * 聚合结果列 Field —— 按输入列类型推导（MIN/MAX 与输入同型；见
     * {@link AggregationSpec#outputArrowType(Field)}）。
     *
     * @param inputFields 聚合输入所在的列清单（按名查找 {@code a.inputColumn()}），null/无匹配时回退静态口径
     */
    public static Field aggregationOutputField(AggregationSpec a, List<Field> inputFields) {
        Field input = (inputFields == null || a.inputColumn() == null)
                ? null : fieldOf(inputFields, a.inputColumn());
        return aggregationOutputField(a, input);
    }

    private static Field aggregationOutputField(AggregationSpec a, Field inputField) {
        return new Field(a.outputColumn(), FieldType.nullable(a.outputArrowType(inputField)), null);
    }

    /**
     * 把 CASE / IF / 表达式派生列（聚合参数改写产生的 {@code __case_N} 等）的<b>静态类型</b>
     * 补进聚合输入列清单。
     *
     * <p>没有这一步时 {@code aggregationOutputField(a, inputFields)} 按名查不到派生列 ⇒ 退化为
     * 静态口径（DOUBLE），而 Worker 实际可能产出 DECIMAL（{@code sum(CAST(x AS DECIMAL(20,2)))}）
     * ⇒ 声明列与实际页 schema 不一致，下游（尤其 Join）按声明宽度解码会静默错值。</p>
     */
    public static List<Field> withDerivedFields(List<Field> inputFields,
                                                List<CaseProjection> projections) {
        if (projections == null || projections.isEmpty()) return inputFields;
        Map<String, Field> byName = new LinkedHashMap<>();
        if (inputFields != null) for (Field f : inputFields) byName.putIfAbsent(f.getName(), f);
        List<Field> out = inputFields == null ? new ArrayList<>() : new ArrayList<>(inputFields);
        for (CaseProjection p : projections) {
            if (p == null || p.derivedColumn() == null || byName.containsKey(p.derivedColumn())) continue;
            ArrowType t = ExprTypeInference.inferProjection(p, byName);
            if (t == null) continue;
            Field f = new Field(p.derivedColumn(), FieldType.nullable(t), null);
            out.add(f);
            byName.putIfAbsent(f.getName(), f);
        }
        return out;
    }

    /**
     * 窗口链输出列 Field（复刻 Worker 终端页 schema，供 CTAS 建表）。
     *
     * <p>逐步按 {@link FragmentSpec.WindowStep#outputColumns()} 的<b>前缀</b>解析输入列再追加
     * 本组窗口函数列。前缀里可能是窗口之间被吸收的投影列（K 形状：窗口 → 投影 → 窗口，
     * 如 {@code uid ← user_id}、{@code amt2 ← amount*2}）—— 它们不在扫描 / join 的 wire 里，
     * 由该步 {@code computed} 的表达式经 {@link ExprTypeInference} 静态推断类型；推断不出
     * （函数调用等）→ 响亮报错，建表不静默错型。</p>
     *
     * @param wire  链输入列（扫描 / join / 聚合的产出）
     * @param steps 窗口链（至少 1 组）
     */
    protected static List<Field> windowChainOutputFields(List<Field> wire,
                                                         List<FragmentSpec.WindowStep> steps) {
        List<Field> out = wire;
        if (steps == null) return out;
        for (FragmentSpec.WindowStep step : steps) {
            List<String> outCols = step.outputColumns();
            if (outCols == null || outCols.isEmpty()) {
                // 旧构造（未声明输出列）：直接在输入后追加函数列
                final List<Field> in = out;
                List<Field> next = new ArrayList<>(in);
                for (WindowFunctionSpec spec : step.functions()) {
                    next.add(windowFunctionField(spec, name -> fieldOf(in, name)));
                }
                out = next;
                continue;
            }
            int baseCount = Math.max(0, outCols.size() - step.functions().size());
            List<Field> base = new ArrayList<>(baseCount);
            for (String name : outCols.subList(0, baseCount)) {
                Field f = fieldOf(out, name);
                if (f == null) {
                    f = derivedFieldOf(step, name, out);
                }
                base.add(f);
            }
            List<Field> next = new ArrayList<>(base);
            for (WindowFunctionSpec spec : step.functions()) {
                next.add(windowFunctionField(spec, name -> fieldOf(base, name)));
            }
            out = next;
        }
        return out;
    }

    /** 派生投影列（链步 {@code computed} 中按 outputColumns 前缀命中的项）→ 静态推断的 Field。 */
    private static Field derivedFieldOf(FragmentSpec.WindowStep step, String name, List<Field> input) {
        GroupKeyExpression target = null;
        for (GroupKeyExpression c : step.computed()) {
            if (c.derivedColumn().equalsIgnoreCase(name)) { target = c; break; }
        }
        if (target == null) {
            throw new UnsupportedOperationException("CTAS 输出列推导：窗口链输出列 '" + name
                    + "' 既不在上游列 " + fieldNames(input) + " 中，也没有对应的投影表达式");
        }
        Map<String, Field> byName = new LinkedHashMap<>();
        for (Field f : input) {
            byName.putIfAbsent(f.getName(), f);
            byName.putIfAbsent(f.getName().toLowerCase(java.util.Locale.ROOT), f);
        }
        ArrowType type;
        try {
            ExprNode node = ExprParser.parse(target.exprText());
            type = ExprTypeInference.infer(node, byName);
        } catch (RuntimeException e) {
            type = null;
        }
        if (type == null) {
            throw new UnsupportedOperationException("CTAS 输出列推导：窗口链投影列 '" + name
                    + "' = " + target.exprText() + " 无法静态推断类型");
        }
        return new Field(name, FieldType.nullable(type), null);
    }

    private static List<String> fieldNames(List<Field> fields) {
        List<String> out = new ArrayList<>(fields.size());
        for (Field f : fields) out.add(f.getName());
        return out;
    }

    /** 窗口函数结果列 Field（复刻 WindowFunctionOperator.buildOutputSchema；LAG/LEAD 镜像输入列）。 */
    protected static Field windowFunctionField(WindowFunctionSpec spec,
                                               Function<String, Field> inputFieldOf) {
        FieldType ft = switch (spec.type()) {
            case ROW_NUMBER, RANK, DENSE_RANK, COUNT ->
                    FieldType.nullable(new ArrowType.Int(64, true));
            case SUM, AVG, MIN, MAX, AGGREGATE ->
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            case LAG, LEAD -> {
                Field src = spec.inputColumn() == null ? null : inputFieldOf.apply(spec.inputColumn());
                yield src != null
                        ? FieldType.nullable(src.getType())
                        : FieldType.nullable(ArrowType.Utf8.INSTANCE);
            }
        };
        return new Field(spec.outputColumn(), ft, null);
    }

    /** 传输 DTO（只带聚合名）的输出类型：count / count_distinct → BIGINT，其余 → DOUBLE（与内置一致）。 */
    protected static ArrowType dtoAggregateType(String aggregateName) {
        return dtoAggregateType(aggregateName, null);
    }

    /**
     * 传输 DTO 的输出类型 —— MIN/MAX 按输入列类型（时间戳 / 日期 / 字符串 / 布尔 / Decimal），
     * count / count_distinct → BIGINT，其余 → DOUBLE。
     */
    protected static ArrowType dtoAggregateType(String aggregateName, Field inputField) {
        if (inputField != null && aggregateName != null) {
            ArrowType t = inputField.getType();
            if (aggregateName.equalsIgnoreCase("min") || aggregateName.equalsIgnoreCase("max")) {
                if (!(t instanceof ArrowType.Int) && !(t instanceof ArrowType.FloatingPoint)) {
                    return t;
                }
            }
            // DECIMAL 的 SUM/AVG 输出 DECIMAL（精度放宽），保证跨 Worker 部分聚合仍精确
            if ((aggregateName.equalsIgnoreCase("sum") || aggregateName.equalsIgnoreCase("avg"))
                    && t instanceof ArrowType.Decimal d) {
                return AggregationSpec.widenDecimal(d);
            }
        }
        boolean bigint = aggregateName != null
                && (aggregateName.equalsIgnoreCase("count")
                        || aggregateName.equalsIgnoreCase("count_distinct"));
        return bigint
                ? new ArrowType.Int(64, true)
                : new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    }

    /** 传输 DTO 聚合的输出列 Field（名 = 别名；count→BIGINT、其余→DOUBLE）。 */
    protected static Field dtoAggregationField(String outputColumn, String aggregateName) {
        return new Field(outputColumn, FieldType.nullable(dtoAggregateType(aggregateName)), null);
    }

    /** 传输 DTO 聚合的输出列 Field —— 输入列在 {@code wire} 中按名取类型（MIN/MAX 与输入同型）。 */
    protected static Field dtoAggregationField(String outputColumn, String aggregateName,
                                               List<Field> wire, String inputColumn) {
        Field input = (wire == null || inputColumn == null) ? null : fieldOf(wire, inputColumn);
        return new Field(outputColumn, FieldType.nullable(dtoAggregateType(aggregateName, input)), null);
    }

    /** 在既有字段清单中按名（忽略大小写）取列；找不到返回 null。 */
    protected static Field fieldOf(List<Field> fields, String name) {
        for (Field f : fields) {
            if (f.getName().equalsIgnoreCase(name)) return f;
        }
        return null;
    }

    /** 按名从既有字段清单取列（保序）；任一名缺失 → 抛错（无法静态推导分组键类型）。 */
    protected static List<Field> fieldsByNames(List<Field> fields, List<String> names) {
        List<Field> out = new ArrayList<>(names.size());
        for (String n : names) {
            Field f = fieldOf(fields, n);
            if (f == null) {
                StringBuilder avail = new StringBuilder();
                for (Field c : fields) avail.append(avail.isEmpty() ? "" : ", ").append(c.getName());
                throw new UnsupportedOperationException("CTAS 输出列推导：分组键 '" + n
                        + "' 不在 join 输出列中（可用列: " + avail + "）—— 无法静态确定列类型");
            }
            out.add(f);
        }
        return out;
    }

    // ==================== 分布式写 ====================

    /** 设置分布式写目标（Planner 在 doOpen 前调用；null = SELECT 回传形态）。 */
    public void setSink(FragmentSpec.SinkStep s) {
        this.sink = s;
    }

    /**
     * 装配 INSERT OVERWRITE 生命周期三钩子（Planner 在 doOpen 前调用；三个均可 null）：
     *
     * <ul>
     *   <li>{@code prepare}：doOpen 派发前运行 —— 权限探针 + {@code beginOverwrite} 建 staging
     *       + {@code setSink(staging)}，保证 EXPLAIN（不 open 算子）零副作用、执行期才触碰数据源；</li>
     *   <li>{@code success}：doNext 内全部 N 分区完成、返回 EOF 前恰好一次 —— {@code commitOverwrite}
     *       原子切换 staging → 目标表；</li>
     *   <li>{@code abort}：doClose 时若尚未 success 则执行 —— {@code abortOverwrite} 删 staging，
     *       目标表从未被触碰（失败/取消语义）。</li>
     * </ul>
     *
     * <p>finalize 恰好一次：EOF 分支单调只命中一次；{@link #finalized} CAS 在 success 前先置位，
     * 防 doClose 的 abort 误删已 commit 的 staging。</p>
     */
    public void setOverwriteLifecycle(Runnable prepare, Runnable success, Runnable abort) {
        this.prepare = prepare;
        this.successFinalizer = success;
        this.abortFinalizer = abort;
    }

    /**
     * 有写目标时给终端 Consumer 片段附加 {@code sink}（Producer / 中间层原样返回），
     * 无写目标直接原样返回 —— 由各 {@code dispatchStage} 对派发片段统一应用。
     */
    protected List<FragmentSpec> withSink(List<FragmentSpec> frags) {
        return sink == null ? frags : StageScheduler.attachSink(frags, sink);
    }

    /**
     * 给终端 Consumer 片段统一应用所有终端修饰符：{@code sink}（分布式写）后叠加
     * {@code tail}（片内 top-N 截断）。非终端片段（Producer / Middle*）原样返回。
     * 各 {@code dispatchStage} 派发前调用本方法替代裸 {@link #withSink}。
     */
    protected List<FragmentSpec> withTerminal(List<FragmentSpec> frags) {
        List<FragmentSpec> out = withSink(frags);
        if (tail != null) out = StageScheduler.attachTail(out, tail);
        if (terminalJoinFilter != null && !terminalJoinFilter.isBlank()) {
            out = StageScheduler.attachJoinFilter(out, terminalJoinFilter);
        }
        if (terminalPartialAggregate != null) {
            out = StageScheduler.attachPartialAggregate(out, terminalPartialAggregate);
        }
        return out;
    }

    /** 设置片内 top-N 截断尾（Planner 命中下推 gate 时在 doOpen 前调用；null = 不下推）。 */
    public void setTail(FragmentSpec.SortLimitStep t) {
        this.tail = t;
    }

    /**
     * 终端<b>二级聚合</b>（外层全局聚合下沉；null = 无）：窗口链末端每分区再跑一次本地全局聚合，
     * 只回传 1 行 partial。由 {@link #withTerminal} 统一挂到终端 {@code WindowConsumer} 上 ——
     * 所有窗口链载体（扫描 / join / 派生 join / 聚合→窗口）共用这一条挂载路径，无需各自实现。
     */
    public void setTerminalPartialAggregate(FragmentSpec.AggregateStep step) {
        this.terminalPartialAggregate = step;
    }

    /** 已设置的终端二级聚合（可空；审计 / EXPLAIN 只读）。 */
    public FragmentSpec.AggregateStep terminalPartialAggregate() {
        return terminalPartialAggregate;
    }

    /**
     * 设置 join 之上的行级谓词（join → Filter → 外层聚合下沉；null = 不下推）。
     * 由 {@link #withTerminal} 统一挂到终端 join 片段（本地 join 之后、二级聚合之前）。
     */
    public void setTerminalJoinFilter(String filter) {
        this.terminalJoinFilter = filter;
    }

    /** 已设置的终端 join 过滤（可空；审计 / EXPLAIN 只读）。 */
    public String terminalJoinFilter() {
        return terminalJoinFilter;
    }

    /**
     * 终端片段输出的<b>哈希分区键</b>（空 = 未知 / 不保证协分区）。外层按分组键下沉
     * 「终态聚合」时必须满足 {@code 分组键 ⊇ 分区键} —— 同一分组才能整组落在同一分区。
     * 各载体按自己的 join / 窗口键覆写；未知即拒绝下沉（保守正确）。
     */
    public List<String> terminalPartitionKeys() {
        return List.of();
    }

    /** 哈希分区数（供 Planner 构建片段 DTO；只读）。 */
    public int partitionCount() {
        return numPartitions;
    }

    /** 分布式写总行数（各分区回传行数之和；SELECT 回传形态恒 0）。 */
    public long totalOutputRows() {
        return totalOutputRows.get();
    }

    // ==================== 片段派发 ====================

    /**
     * 统一片段派发入口（各 {@code dispatchStage} 不再裸调 {@code gateway.dispatchFragment}）：
     * 先登记任务生命周期（RUNNING）再记 owner 后派发 —— 监控读 {@link #fragmentTasks()} 时
     * 每个片段在派发瞬间即可见；派发失败的片段保持 RUNNING 由 doClose 失败路径统一转 FAILED。
     */
    protected final void dispatch(FragmentSpec spec) {
        taskStates.put(spec.fragmentId(), new FragmentTaskState(spec));
        taskOwner.put(spec.fragmentId(), spec.selfWorkerId());
        // 归属登记：派发出去的 jobId 必须归属到当前执行的查询，否则 Master 的僵尸片段回收会把
        // 主计划树之外（物化 CTE / 物化派生体查询体）的片段误判成残留。见 DispatchScope。
        com.dojao.core.runtime.master.DispatchScope.register(queryId);
        gateway.dispatchFragment(spec.selfWorkerId(), queryId, spec.fragmentId(), spec,
                resultQueue, controlQueue);
    }

    // ==================== 生命周期 ====================

    @Override
    protected final void doOpen() throws Exception {
        // INSERT OVERWRITE / CTAS：先 prepare（探针 / 建 staging / 建目标表 + 换 sink）再切分派发 ——
        // dispatchStage 的 withSink 读到的就是 staging/目标表。prepare 抛异常 → doOpen 传播 → 查询失败，
        // 未派发。CTAS 的 prepare 里表已存在且 IF NOT EXISTS 时可 skipDispatch() → 零派发、doNext 立即 EOF。
        if (prepare != null) prepare.run();
        if (skipDispatch) {
            numConsumers = 0;
            eofReturned = true;
            System.err.printf("[%s] query=%s 目标表已存在（IF NOT EXISTS）→ 跳过派发 rows=0%n",
                    getClass().getSimpleName(), queryId);
            return;
        }
        numConsumers = dispatchStage();
        // 完成契约：派发了片段就必须有终端片段回传 FragmentFinished。
        // 0 = 算子把「会回传的片段数」算错了（历史上多组窗口链曾按 consumers.size() 数进中间层，
        // 导致 Master 永远等不满）；返回 0 时 doNext 会立刻 EOF → 静默丢光全部结果。
        if (numConsumers <= 0) {
            throw new IllegalStateException(getClass().getSimpleName()
                    + " dispatchStage 返回终端完成数 " + numConsumers + "（query=" + queryId
                    + "）：已派发片段但无终端回传，查询会静默丢行");
        }
        System.err.printf("[%s] query=%s workers=%d partitions=%d consumers=%d%n",
                getClass().getSimpleName(), queryId, workers.size(), numPartitions, numConsumers);
    }

    @Override
    protected final Page doNext() throws Exception {
        long lastProgress = System.nanoTime();
        while (!eofReturned) {
            checkFailure();
            // 1) 控制面优先：FINISHED / FAILED / PROGRESS 走无界队列，消费者慢也丢不了。
            //    ★只处理事件、不在这里返回 EOF —— 数据页可能仍在 resultQueue 里排队
            //      （控制事件先到），必须先把数据交付完。
            FragmentEvent ce;
            while ((ce = controlQueue.poll()) != null) {
                lastProgress = System.nanoTime();
                handleControlEvent(ce);
            }
            checkFailure();
            // 2) 全部片段完成且数据队列排空 → EOF。数据排空条件不可省：FINISHED 可能先于
            //    消费方读取排队中的数据页被处理（控制/数据两个队列），提前 EOF 会静默丢行。
            if (finishedCount.get() >= numConsumers && resultQueue.isEmpty()) {
                eofReturned = true;
                // 只剩 Producer / 中间层 Consumer 未收完成事件（它们不回传 Master，靠下游交换
                // complete 协议收尾）→ 全部转 FINISHED，保证任务监控状态无悬挂 RUNNING
                for (FragmentTaskState running : taskStates.values()) {
                    if (DistributedTask.RUNNING.equals(running.state)) running.finish();
                }
                // INSERT OVERWRITE：全部 N 分区成功 → commitOverwrite（finalized CAS 先置位，
                // success 即使抛异常 doClose 也不再 abort —— commit 后 staging 已改名，语义正确）
                if (finalized.compareAndSet(false, true) && successFinalizer != null) {
                    successFinalizer.run();
                }
                return Page.eof();
            }
            // 3) 数据面
            FragmentEvent ev = resultQueue.poll(100, TimeUnit.MILLISECONDS);
            if (ev == null) {
                // 数据面（最近一个结果事件）与控制面（存活心跳）取较晚者：大表 join / 聚合在
                // 「全部 Producer 灌完 shuffle」之前一个结果页都回不来（终端 Consumer 要等收齐
                // 才能建表 / 出结果），这段沉默里 Worker 仍在正常扫表 / 打散 / 建表 —— 只要心跳
                // 还在到，就不是链路故障。真失联（Worker 死 / 片段线程亡）心跳随之中断 → 照常 fail-fast。
                long last = Math.max(lastProgress, lastHeartbeatNanos);
                long idleMs = (System.nanoTime() - last) / 1_000_000;
                if (idleMs > WATCHDOG_IDLE_MS) {
                    // 看门狗：既无数据页、又无存活心跳 → 链路故障，fail-fast 替代挂起。
                    // 报错带上仍未收尾的片段（类别@Worker#id），便于直接定位是哪个 Consumer 卡住。
                    throw new RuntimeException(getClass().getSimpleName()
                            + " 挂起 " + idleMs + "ms 无 Worker 结果回传（已收 " + finishedCount.get()
                            + "/" + numConsumers + " 个完成；仍未完成: " + runningFragmentSummary()
                            + "），疑似 doExchange result/{taskId} 回传链路故障");
                }
                continue;
            }
            lastProgress = System.nanoTime();
            if (ev instanceof FragmentEvent.FragmentData d) {
                rowsByFragment.computeIfAbsent(d.taskId(), k -> new AtomicLong())
                        .addAndGet(d.page().getRowCount());
                FragmentTaskState st = taskStates.get(d.taskId());
                if (st != null) st.rowsOut.addAndGet(d.page().getRowCount());
                // 终端框的「Input Bytes」：本算子从 Worker 收到的页大小之和。本类不走基类的
                // recordInput（输入不来自上游算子而是结果队列），这里只补字节计数 —— 行数与
                // 内存跟踪各有既定口径，不在此改动。
                if (d.page() != null && !d.page().isEOF()) {
                    getStats().recordInputBytes(d.page().getSizeInBytes());
                }
                return finalizeResultPage(d.page());
            }
            // 单队列网关（LocalWorkerGateway / 测试桩，未覆盖 6 参派发）把终止/心跳也投进
            // resultQueue —— 这里兜住，否则 FINISHED 被当数据页丢弃、finishedCount 永不到齐。
            lastProgress = System.nanoTime();
            handleControlEvent(ev);
        }
        return Page.eof();
    }

    /** 处理一条<b>控制事件</b>（来自 controlQueue，或单队列网关投进 resultQueue 的兜底）。 */
    private void handleControlEvent(FragmentEvent ce) {
        if (ce instanceof FragmentEvent.FragmentFinished f) {
            int done = finishedCount.incrementAndGet();
            totalOutputRows.addAndGet(f.rowCount());
            FragmentTaskState st = taskStates.get(f.taskId());
            if (st != null) st.finish();
            System.err.printf("[%s] 收到 FragmentFinished taskId=%s rows=%d %d/%d%n",
                    getClass().getSimpleName(), f.taskId(), f.rowCount(), done, numConsumers);
        } else if (ce instanceof FragmentEvent.FragmentFailed f) {
            FragmentTaskState st = taskStates.get(f.taskId());
            if (st != null) st.fail(f.cause());
            // 完整堆栈直接打 stderr：方便定位根因（FragmentFailed 的 cause 已带 cause 链）
            System.err.println("[" + getClass().getSimpleName() + "] fragment " + f.taskId()
                    + " 失败:\n" + f.cause());
            recordFailure(new RuntimeException("fragment " + f.taskId() + " 失败: " + f.cause()));
        }
        // PROGRESS：仅用于刷新看门狗（调用方已更新 lastProgress）
    }

    @Override
    protected final void doClose() throws Exception {
        // 0. INSERT OVERWRITE：未 success（失败/取消/提前关闭）→ abort 删 staging，原表保持原样
        //    （abort 闭包自判 staging 是否存在，未 begin 则 no-op；清理失败吞掉，不掩盖主错误）
        if (!finalized.get() && abortFinalizer != null) {
            try { abortFinalizer.run(); } catch (Throwable ignored) { }
        }
        // 0.5 失败路径：仍 RUNNING 的片段（被失败连坐/未及回传）→ 转 FAILED（监控状态不悬挂）
        if (failure != null) {
            for (FragmentTaskState running : taskStates.values()) {
                if (DistributedTask.RUNNING.equals(running.state)) {
                    running.fail("query failed (master)");
                }
            }
        }
        // 1. 反向取消所有已派发片段（Worker 侧中断执行线程）
        for (Map.Entry<String, String> e : taskOwner.entrySet()) {
            try {
                gateway.cancelTask(e.getValue(), e.getKey());
            } catch (Throwable ignored) {
            }
        }
        // 2. 排空结果队列，关闭残余数据页；控制事件无资源可放
        FragmentEvent ev;
        while ((ev = resultQueue.poll()) != null) {
            if (ev instanceof FragmentEvent.FragmentData d) {
                try { d.page().close(); } catch (Throwable ignored) { }
            }
        }
        controlQueue.clear();
        // 3. 释放探针连接器
        closeProbes();
    }

    // ==================== 失败传播 ====================

    protected synchronized void recordFailure(Throwable t) {
        if (failure == null) failure = t;
    }

    /** 任一片段失败即抛出（fail-fast，绝不把残缺数据当成功返回）。 */
    private void checkFailure() {
        Throwable f = failure;
        if (f != null) {
            if (f instanceof RuntimeException re) throw re;
            if (f instanceof Error err) throw err;
            throw new RuntimeException(getClass().getSimpleName() + " failed", f);
        }
    }

    // ==================== 可观测 ====================

    /** 各 Consumer 片段回传的行数（只读快照；验证结果确实分散在多个 Worker）。 */
    public Map<String, Long> rowsByFragment() {
        Map<String, Long> out = new ConcurrentHashMap<>();
        rowsByFragment.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    /**
     * 全部已派发片段任务的只读快照（按派发先后排序）。
     *
     * <p>片段任务 = Master 把一个分布式算子切出来的一个可独立执行的 Worker 任务
     * （Producer 扫切片推送 / Consumer 接桶本地算子回传 / 中间层接桶本地算子转发）。
     * 任务生命周期由 doNext 收到的事件推进：终端 Consumer 收 FragmentFinished/FragmentFailed；
     * 全部终端完成（EOF）时余下的 Producer / 中间层按完成处理；失败路径余下 RUNNING 转 FAILED。</p>
     */
    public List<DistributedTask> fragmentTasks() {
        List<DistributedTask> out = new ArrayList<>(taskStates.size());
        for (FragmentTaskState st : taskStates.values()) {
            // 链式片段 → 拆成 Join + Window/Aggregate 两个阶段任务（同一片段生命周期，行数各走各的
            // 遥测通道）。监控据此呈现「Join ⇒ Window」两级，而不是一坨 fused 框。
            if (st.chained()) {
                out.add(st.joinStageSnapshot());
            }
            out.add(st.snapshot());
        }
        out.sort((a, b) -> Long.compare(a.dispatchNanos(), b.dispatchNanos()));
        return out;
    }

    public String queryId() { return queryId; }
    public int numConsumers() { return numConsumers; }

    /**
     * Worker 遥测确认某个<b>非终端</b>片段已真正跑完（{@code done=true}）：把仍 RUNNING 的该片段任务
     * 置 FINISHED —— Producer / 中间层不回传 Master，若不在此处置位，它们要等整条查询收尾才被统一
     * 扫尾，监控上会长时间显示「RUNNING 且 0 行」（短命片段甚至一个采样帧都发不出，恒 0 行）。
     *
     * <p>仅 RUNNING 才置位：FAILED / LOST 不被掩盖（失败经控制面 fail-fast，晚到的 done 覆盖不了）。
     * 终值仅 best-effort，丢帧时查询收尾的扫尾逻辑仍是兜底。命中该 taskId 返回 true。</p>
     */
    public boolean markTaskFinished(String taskId) {
        if (taskId == null) {
            return false;
        }
        FragmentTaskState st = taskStates.get(taskId);
        if (st == null || !DistributedTask.RUNNING.equals(st.state)) {
            return false;
        }
        st.finish();
        return true;
    }

    /**
     * <b>存活心跳</b>：Worker 遥测每拍上抛的非终端片段 tick（{@code query_telemetry} →
     * {@code QueryManager.acceptFragmentTelemetry} → 本方法）—— 只表达「这个片段还活着」，
     * 命中已知 taskId 即刷新看门狗接触时刻。
     *
     * <p>与 {@link DistributedScanOperator#markAlive(String)} 同源同义，区别只是本类覆盖
     * <b>所有</b>分布式算子（聚合 / join / 窗口 / 派生 join / 写…）：终端 Consumer 必须等全部
     * Producer 把 shuffle 灌完才可能出第一行，大表查询这段沉默远超看门狗阈值，只看「是否吐结果页」
     * 必然误杀（2026-09-23 实测：users LEFT JOIN 91M 行 Hive 表 + GROUP BY，60s 判死）。
     * 独立的 {@code DistributedScanOperator} 是另一条类层次（不继承本类），故各自实现一份。</p>
     *
     * <p><b>只吞不刷新已结束任务</b>（对齐 {@code markTaskFinished} 的守卫）：片段收尾时 Worker 注销
     * 采样器、心跳随之中止，若这里还替 FINISHED / FAILED 片段续命，真挂起就再也没人兜住了。</p>
     *
     * @return 该 taskId 是否属于本算子（true = 这帧已被消费，不再按普通遥测处理）
     */
    public boolean markAlive(String taskId) {
        if (taskId == null) {
            return false;
        }
        FragmentTaskState st = taskStates.get(taskId);
        if (st == null) {
            return false;
        }
        if (DistributedTask.RUNNING.equals(st.state)) {
            lastHeartbeatNanos = System.nanoTime();
        }
        return true;
    }

    // ==================== 片段任务可观测（监控） ====================

    /**
     * 仍未收尾的片段摘要（看门狗报错用）：{@code 类别@workerId前8位#taskId前8位}，逗号分隔。
     * 看门狗触发时「哪个 Consumer 没回来」是排障的第一信息，只有完成计数时无从定位。
     */
    private String runningFragmentSummary() {
        StringBuilder sb = new StringBuilder();
        taskStates.forEach((id, st) -> {
            if (!DistributedTask.RUNNING.equals(st.state)) {
                return;
            }
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(st.kind).append('@').append(shortId(st.workerId)).append('#').append(shortId(id));
        });
        return sb.length() == 0 ? "无（片段均已收尾，完成事件可能丢失）" : sb.toString();
    }

    private static String shortId(String id) {
        return id == null ? "?" : id.substring(0, Math.min(8, id.length()));
    }

    /** 片段任务运行态（可变；监控读并发安全 —— 状态字段 volatile、行数 AtomicLong）。 */
    private static final class FragmentTaskState {
        final FragmentSpec spec;
        final String taskId, workerId, kind;
        final boolean producer, terminal;
        /** Producer 扫描基表（schema.table），非扫描片段为 null —— 监控据此按来源分组 Scan 叶子。 */
        final String source;
        final long dispatchNanos;
        final AtomicLong rowsOut = new AtomicLong();
        volatile String state = DistributedTask.RUNNING;
        volatile long finishNanos;
        volatile String failure;

        FragmentTaskState(FragmentSpec spec) {
            this.spec = spec;
            this.taskId = spec.fragmentId();
            this.workerId = spec.selfWorkerId();
            this.kind = kindOf(spec);
            this.producer = spec.isProducer();
            this.terminal = isTerminal(spec);
            this.source = (spec instanceof FragmentSpec.Producer p)
                    ? p.scan().request().schema() + "." + p.scan().request().table()
                    : null;
            this.dispatchNanos = System.nanoTime();
        }

        DistributedTask snapshot() {
            return new DistributedTask(taskId, workerId, kind, producer, terminal, state, rowsOut.get(),
                    dispatchNanos, finishNanos, failure, source);
        }

        /**
         * 链式片段（join → 窗口 / 聚合压在同一个 Worker 片段）的 <b>join 阶段</b>任务。
         *
         * <p>它与 {@link #snapshot()} 的窗口 / 聚合阶段共享同一片段生命周期（状态 / 完成时间跟随），
         * 行数由 Worker 遥测按 {@link DistributedTask#joinStageTaskId(String)} 上报（join 输出行数）——
         * 监控因此能把「Join」与「Window」拆成两个框，看出当前推进到哪一段。</p>
         */
        DistributedTask joinStageSnapshot() {
            return new DistributedTask(DistributedTask.joinStageTaskId(taskId), workerId, "Join",
                    false, false, state, 0L, dispatchNanos, finishNanos, failure, null);
        }

        boolean chained() {
            return spec instanceof FragmentSpec.ChainedJoinConsumer;
        }

        void finish() {
            state = DistributedTask.FINISHED;
            finishNanos = System.nanoTime();
        }

        void fail(String cause) {
            state = DistributedTask.FAILED;
            failure = cause;
            finishNanos = System.nanoTime();
        }
    }

    /**
     * 片段的人类可读任务类别（Worker 侧要干的活，供监控任务列表 / 阶段框展示）。
     *
     * <p>链式片段（{@code ChainedJoinConsumer}）这里只返回<b>后半段</b>的类别（Window / Aggregate），
     * 前半段 join 由 {@link FragmentTaskState#joinStageSnapshot()} 以独立的 "Join" 任务呈现 ——
     * 监控不再把 join 和窗口糊成一个框。</p>
     */
    private static String kindOf(FragmentSpec spec) {
        if (spec instanceof FragmentSpec.Producer) return "Scan→Shuffle";
        if (spec instanceof FragmentSpec.Consumer) return "Join";
        if (spec instanceof FragmentSpec.AggregateConsumer) return "Aggregate";
        if (spec instanceof FragmentSpec.WindowConsumer w) {
            return "Window#" + windowGroup(w.in().exchangeId());
        }
        if (spec instanceof FragmentSpec.ChainedJoinConsumer c) {
            // join 段已由 <fragmentId>#join 独立呈现，这里只描述紧随其后的计算段（链上恒为第 0 组）
            String stage = c.followUp() instanceof FragmentSpec.WindowStep ? "Window#0" : "Aggregate#0";
            return c.terminal() ? stage : stage + "→Shuffle";
        }
        if (spec instanceof FragmentSpec.MiddleJoinConsumer) return "Join→Shuffle";
        if (spec instanceof FragmentSpec.MiddleAggregateConsumer m) {
            return "Aggregate#" + exchangeGroup(m.in().exchangeId(), "AW") + "→Shuffle";
        }
        if (spec instanceof FragmentSpec.MiddleWindowConsumer m) {
            return "Window#" + windowGroup(m.in().exchangeId()) + "→Shuffle";
        }
        return "Fragment";
    }

    /** 窗口链交换名 {@code ...-JW<i>} / 聚合→窗口链 {@code ...-AW<i>} → 窗口组序号（无编号=0）。 */
    private static String windowGroup(String exchangeId) {
        if (exchangeId == null) {
            return "0";
        }
        int jw = exchangeId.lastIndexOf("-JW");
        int aw = exchangeId.lastIndexOf("-AW");
        int i = Math.max(jw, aw);
        if (i < 0) {
            return "0";
        }
        String tail = exchangeId.substring(i + 3);
        return tail.isEmpty() ? "0" : tail;
    }

    /** 从交换名取阶段序号（聚合链 {@code -AW<i>}；单组无编号视作 0）。 */
    private static String exchangeGroup(String exchangeId, String tag) {
        if (exchangeId == null) {
            return "0";
        }
        int i = exchangeId.lastIndexOf('-' + tag);
        if (i < 0) {
            return "0";
        }
        String tail = exchangeId.substring(i + tag.length() + 1);
        return tail.isEmpty() ? "0" : tail;
    }

    /** 片段是否把结果回传 Master（终端段）：Producer / 各类 MiddleConsumer 都是中间层。 */
    private static boolean isTerminal(FragmentSpec spec) {
        if (spec instanceof FragmentSpec.ChainedJoinConsumer c) {
            return c.terminal();
        }
        if (spec instanceof FragmentSpec.Producer) return false;
        if (spec instanceof FragmentSpec.MiddleJoinConsumer
                || spec instanceof FragmentSpec.MiddleAggregateConsumer
                || spec instanceof FragmentSpec.MiddleWindowConsumer) {
            return false;
        }
        return true;
    }
}
