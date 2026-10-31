package com.dojao.core.runtime.master;

import com.dojao.core.operator.AbstractDistributedOperator;
import com.dojao.core.operator.AggregationSpec;
import com.dojao.core.operator.CaseProjection;
import com.dojao.core.operator.FragmentSpec;
import com.dojao.core.operator.GroupKeyExpression;
import com.dojao.core.operator.JoinType;
import com.dojao.core.operator.WindowFilterConfig;
import com.dojao.core.operator.WindowFunctionSpec;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.service.DojaoConnector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.List;

/**
 * 分布式算子的舞台编排 —— 把一个算子（join / 聚合 / 窗口）切分为两类片段（Producer / Consumer），
 * 生成 round-robin owner 映射（co-partitioning 不变量，见 {@link PartitionLayout}）。
 *
 * <p>输入：表信息 + key + 参与 Worker 列表；输出：
 * <ul>
 *   <li><b>owner 映射</b>：{@code owner[pid] = workers[pid % numWorkers]} —— 同一查询内所有
 *       shuffle 共用同一映射，保证同 key 的 build / probe 行落同一 Worker；</li>
 *   <li><b>Producer 片段</b>：按连接器分区规划切成 {@code parallelism} 个数据切片，
 *       切片 i 派给 {@code workers[i % numWorkers]}，每个切片一个 Producer（扫切片 → 分区 → 推送）；</li>
 *   <li><b>Consumer 片段</b>：每个 Worker 一个，own 它 round-robin 分到的桶，本地执行算子后回传 Master。</li>
 * </ul>
 *
 * <p>每个交换的 {@code numSenders} = 该侧 Producer 片段数（每个 Producer 都会对全部桶发
 * complete()），Consumer 侧据此判定"收齐全部发送方"。</p>
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/8/20
 */
public final class StageScheduler {

    /** 舞台规划结果：owner 映射 + 消费者片段（先派发）+ 生产者片段（后派发）。 */
    public record DistributedStage(String[] ownerWorkerIds,
                                   List<FragmentSpec> consumers,
                                   List<FragmentSpec> producers) {
    }

    /**
     * 多 join 链舞台规划结果：全部片段已按<b>反死锁派发顺序</b>排好（消费者先、生产者后，
     * 消费者内又按 join 层逆序，生产者内 build(右) 先于 probe(左)），算子逐条派发即可。
     *
     * @param fragments          按派发顺序排列的全部片段（最终 Consumer → 中间层 → 基表 Producers）
     * @param finalConsumerCount 回传 Master 的最终 Consumer 数（= worker 数，Master 据此判 EOF）
     * @param ownerWorkerIds     round-robin owner 映射（全查询共享，co-partitioning 不变量）
     */
    public record JoinChainStage(List<FragmentSpec> fragments, int finalConsumerCount, String[] ownerWorkerIds) {
    }

    /**
     * UNION distinct 去重舞台：<b>无 Producer 片段</b> —— 发送方恒为 Master（Master 端
     * UnionAllOperator 输出按全列哈希直推 Worker）。只含每分区一个 AggregateConsumer 片段
     * （空聚合 = 全列 DISTINCT）+ 每分区的独立交换标识（Master 泵据此把桶路由到对应接收端）。
     */
    public record UnionDistinctStage(List<FragmentSpec> fragments, int finalConsumerCount,
                                     String[] ownerWorkerIds, List<String> perPartitionExchanges) {
    }

    /**
     * 多 join 链的<b>一个 join 环节</b>：本层 join 的键 + 右表（build 侧）的读取描述。
     * 左表（probe 侧）来自上一层的 join 输出（第 1 层 = 首个基表 {@code firstLeftKeys}）。
     *
     * @param rightFilter 右表（build 侧）单表 WHERE 下推谓词（已去限定符，可 null）
     * @param algorithm   本层 join 算法（{@code JoinDecider} 的结论，随 JoinStep 下发 Worker）
     * @param rightColumns 右表（build 侧）<b>实际扫描的列集</b>（顺序即页内列序，已含 join 键）；
     *                    null/空 = 读全列。<b>必须与扫描用的一致</b> —— 它是本层声明的 build 侧
     *                    schema（{@code JoinStep.declaredRightFields}）与实际右表页的唯一来源，
     *                    两处不一致会让空桶兜底时分区的列数与其他分区不同（下游按首张页定宽 → 越界）。
     */
    public record JoinLink(List<String> leftKeys, List<String> rightKeys, JoinType joinType,
                           String rightCatalog, String rightSchema, String rightTable, DojaoConnector rightProbe,
                           String rightFilter, FragmentSpec.JoinAlgorithm algorithm,
                           List<String> rightColumns, List<String> leftPartitionKeys) {
        /** 未指定算法 → 哈希 join（既有行为）。 */
        public JoinLink(List<String> leftKeys, List<String> rightKeys, JoinType joinType,
                        String rightCatalog, String rightSchema, String rightTable, DojaoConnector rightProbe,
                        String rightFilter) {
            this(leftKeys, rightKeys, joinType, rightCatalog, rightSchema, rightTable, rightProbe, rightFilter,
                    FragmentSpec.JoinAlgorithm.HASH, null, null);
        }

        /** 兼容旧签名（无下推谓词）。 */
        public JoinLink(List<String> leftKeys, List<String> rightKeys, JoinType joinType,
                        String rightCatalog, String rightSchema, String rightTable, DojaoConnector rightProbe) {
            this(leftKeys, rightKeys, joinType, rightCatalog, rightSchema, rightTable, rightProbe, null,
                    FragmentSpec.JoinAlgorithm.HASH, null, null);
        }

        /** 兼容旧签名（无列裁剪）。 */
        public JoinLink(List<String> leftKeys, List<String> rightKeys, JoinType joinType,
                        String rightCatalog, String rightSchema, String rightTable, DojaoConnector rightProbe,
                        String rightFilter, FragmentSpec.JoinAlgorithm algorithm) {
            this(leftKeys, rightKeys, joinType, rightCatalog, rightSchema, rightTable, rightProbe, rightFilter,
                    algorithm, null, null);
        }

        /** 兼容旧签名（无左分区键）—— 普通哈希 join 用 leftKeys 作分区键（共分区不变量）。 */
        public JoinLink(List<String> leftKeys, List<String> rightKeys, JoinType joinType,
                        String rightCatalog, String rightSchema, String rightTable, DojaoConnector rightProbe,
                        String rightFilter, FragmentSpec.JoinAlgorithm algorithm,
                        List<String> rightColumns) {
            this(leftKeys, rightKeys, joinType, rightCatalog, rightSchema, rightTable, rightProbe, rightFilter,
                    algorithm, rightColumns, null);
        }

        /**
         * 本层<b>左（probe）侧的分区键</b>。普通哈希 join 必须 = {@code leftKeys}（共分区不变量：
         * 同 join 键落同桶，build 侧也按同一键分区）。<b>交叉连接</b>（{@code joinType == CROSS}，
         * 无 join 键）时 build 侧被整份复制到全部桶，左表按任意确定函数分发都正确 ⇒ 用左表全列
         * 打散（{@code leftKeys} 为空，拿它分区会把左表全塞进 0 号桶，退化单机）。
         */
        public List<String> leftPartitionKeys() {
            return (leftPartitionKeys == null || leftPartitionKeys.isEmpty()) ? leftKeys : leftPartitionKeys;
        }
    }

    /**
     * 链上<b>一个关系的来源</b>：派生表（子查询 / CTE，展平不了的那种）。
     *
     * <p>该关系的数据流不再是「直扫基表 → 分区进本层交换」，而是
     * 「扫派生基表（列裁剪 + 内层 WHERE）→ 按分组键分区 → 每分区本地聚合（HAVING）→
     * 按本层 join 键重哈希进本层交换」的迷你流水线（{@link FragmentSpec.MiddleAggregateConsumer}
     * + 派生基表 {@link FragmentSpec.Producer}）。与两表派生 join（{@link #planDerivedJoin}）
     * 是同一套机制，只是这里把它接到多表链的某一层上。</p>
     *
     * <p>为 {@code null} 的层（{@link #planJoinChain} 的 sources 列表元素）＝ 普通基表扫描。</p>
     */
    public record DerivedSource(DojaoConnector innerProbe, ReadRequest innerRequest,
                                List<String> innerKeys, List<AggregationSpec> aggregations,
                                List<CaseProjection> caseProjections, String havingFilter,
                                List<DerivedProjection> projections, DerivedWindow window) {

        /** 聚合型派生源（无投影列）—— 既有调用点原样可用。 */
        public DerivedSource(DojaoConnector innerProbe, ReadRequest innerRequest,
                             List<String> innerKeys, List<AggregationSpec> aggregations,
                             List<CaseProjection> caseProjections, String havingFilter) {
            this(innerProbe, innerRequest, innerKeys, aggregations, caseProjections, havingFilter, null, null);
        }

        /** 投影型派生源（无聚合，逐行表达式投影）。 */
        public DerivedSource(DojaoConnector innerProbe, ReadRequest innerRequest,
                             List<String> innerKeys, List<AggregationSpec> aggregations,
                             List<CaseProjection> caseProjections, String havingFilter,
                             List<DerivedProjection> projections) {
            this(innerProbe, innerRequest, innerKeys, aggregations, caseProjections, havingFilter, projections, null);
        }

        /** true = 投影型派生源（无聚合，逐行表达式投影）。 */
        public boolean isProjection() {
            return projections != null && !projections.isEmpty();
        }

        /** true = 窗口型派生源（无聚合，单窗口组窗口函数）。 */
        public boolean isWindow() {
            return window != null;
        }
    }

    /**
     * 派生源的<b>窗口体</b> —— {@code (SELECT cols, win() OVER (PARTITION BY k ORDER BY o) FROM t WHERE p)} 里的窗口段。
     *
     * <p>与聚合 / 投影型并列的第三种派生体：不做分组，按 {@code partitionBy} 哈希分区后每分区本地
     * 排序 + 逐行计算（{@link FragmentSpec.MiddleWindowConsumer}）。</p>
     *
     * @param partitionBy 窗口 PARTITION BY 列（= 派生基表分区键）
     * @param orderBy     窗口 ORDER BY 列
     * @param ascending   各 ORDER BY 列方向
     * @param functions   窗口函数（{@code outputColumn} = 派生体对外可见的窗口输出列名）
     * @param columns     窗口输出列名（按外层 SELECT 项序；仅用于校验外层引用，不决定物理页序）
     */
    public record DerivedWindow(List<String> partitionBy, List<String> orderBy, List<Boolean> ascending,
                                List<WindowFunctionSpec> functions, List<String> columns) {
    }

    /**
     * 派生源的<b>投影列</b> —— {@code (SELECT a, b*2 AS d FROM t WHERE p)} 里 {@code b*2 → d} 这一项。
     *
     * <p>与聚合型派生源（{@code innerKeys + aggregations}）并列的另一种派生体：不做分组，
     * 只在 Worker <b>Producer 侧、哈希分区之前</b>把表达式物化成派生列
     * （复用 {@link FragmentSpec.Producer#groupKeyExpressions()} + {@code ExpressionProjectOperator}），
     * 再按该层 join 键分区进交换 —— 零新算子、零新片段类型。</p>
     *
     * @param column   派生列名（派生体对外可见名 = 外层引用名）
     * @param exprText 逐行求值的 SQL 表达式文本
     * @param field    该列的 Arrow 声明类型 —— 规划期静态推断（连 {@code DerivedProjection} 的
     *                 「声明列」用途），供 build 侧空桶退回声明值时使用
     */
    public record DerivedProjection(String column, String exprText, Field field) {
    }

    private StageScheduler() {
    }

    /**
     * 右表（build 侧）<b>声明列</b> —— 随 {@code JoinStep} 下发 Worker，让每个 join 分区（哪怕
     * build 侧一个页都没收到）都能算出同一份输出 schema。详见 {@code ArrowSchemaIpc}。
     *
     * <p>连接器元数据读不到 / 无连接器（派生表右通道）→ {@code null}（不声明，Worker 退回旧行为）。
     */
    private static List<Field> rightFieldsOf(DojaoConnector probe, String catalog,
                                             String schema, String table, List<String> columns) {
        if (probe == null || table == null) {
            return null;
        }
        try {
            List<Field> fields = AbstractDistributedOperator.scanFields(probe, catalog, schema, table, columns);
            return (fields == null || fields.isEmpty()) ? null : fields;
        } catch (Throwable t) {
            return null;
        }
    }

    /** {@link JoinLink} 右表声明列的便捷取法（列集取该层的 {@code rightColumns}）。 */
    private static List<Field> rightFieldsOf(JoinLink link) {
        return rightFieldsOf(link.rightProbe(), link.rightCatalog(), link.rightSchema(), link.rightTable(),
                link.rightColumns());
    }

    /**
     * 多 join 链第 {@code layer} 层（1 起）probe（左）侧的<b>声明列</b> = T0..T(layer-1) 的 join 输出列。
     *
     * <p>与 Worker 侧 join 算子的输出拼法保持一致：左列 + 右列；{@code INNER} 且哈希类 join
     * （HASH / BROADCAST，见 {@code HashJoinOperator.ensureOutputSchema}）会剔除与左键同名的右键，
     * SortMergeJoin 不剔除。任一层的声明列取不到 → 返回 null（该层退回旧行为，不猜）。</p>
     *
     * <p>用途：RIGHT / FULL join 的某分区左表一页未收时，输出「右行 + 左列全 NULL」的孤儿行
     * 需要左列类型（详见 {@code SortMergeJoinOperator#requireSchema} / {@code HashJoinOperator}）。</p>
     */
    private static List<Field> leftFieldsForChain(int layer, String firstCatalog, String firstSchema,
                                                  String firstTable, List<String> firstColumns,
                                                  DojaoConnector firstProbe,
                                                  List<JoinLink> links, List<DerivedSource> sources) {
        DerivedSource ds0 = sourceAt(sources, 0);
        List<Field> first = ds0 != null ? derivedFields(ds0)
                : rightFieldsOf(firstProbe, firstCatalog, firstSchema, firstTable, firstColumns);
        if (first == null) return null;
        List<Field> out = new ArrayList<>(first);
        for (int j = 1; j < layer; j++) {
            JoinLink lj = links.get(j - 1);
            DerivedSource ds = sourceAt(sources, j);
            List<Field> rightFields = ds != null ? derivedFields(ds) : rightFieldsOf(lj);
            if (rightFields == null) return null;
            boolean hashLike = lj.algorithm() == FragmentSpec.JoinAlgorithm.HASH
                    || lj.algorithm() == FragmentSpec.JoinAlgorithm.BROADCAST;
            for (Field f : rightFields) {
                if (hashLike && lj.joinType() == JoinType.INNER
                        && lj.rightKeys().contains(f.getName())
                        && lj.leftKeys().contains(f.getName())) {
                    continue;   // HashJoinOperator 对 INNER 剔除同名右键
                }
                out.add(f);
            }
        }
        return out;
    }

    /**
     * 基表扫描请求：{@code columns} 非空即做列裁剪（顺序即输出列序），null/空 = 读全列。
     *
     * <p><b>为什么列裁剪必须从规划期就带上</b>：join 片段里的扫描请求由本类构造，而非单表扫描路径
     * （{@code DistributedScanOperator} 自带 {@code baseRequest}）。此前 join 两侧一律
     * {@code ReadRequest.of(catalog, schema, table)} ⇒ 连接器读<b>全部列</b>（Hive 宽表 23 列时
     * 实测 37ms/万行 vs 投影后的 3.6ms/万行，10× I/O 白烧），且与 {@code outputFields()} 用全列
     * 复算的 schema 一致 —— 不裁剪是「一致的浪费」。列集来自语法层 {@code ColumnPruning} 已经算好的
     * {@code ResolvedRelation.columns()}（join 键必在其中，故无需再去重/补齐键列）。</p>
     */
    private static ReadRequest readRequest(DojaoConnector probe, String catalog, String schema,
                                           String table, List<String> columns) {
        ReadRequest base = ReadRequest.of(catalog, schema, table);
        return (probe == null || columns == null || columns.isEmpty())
                ? base : probe.pruneColumns(base, columns);
    }

    /**
     * 派生表一侧（{@code MiddleAggregateConsumer} 输出流）的<b>声明列</b> = 分组键列 + 聚合结果列，
     * 拼法与 {@code DistributedDerivedJoinOperator.outputFields()} 一字不差 —— 派生流没有连接器元数据，
     * 只能这样反推；这正是「{@code users LEFT JOIN (hive 派生表)}」中 build 侧（派生流）的列来源。
     */
    private static List<Field> derivedSideFields(DojaoConnector innerProbe, ReadRequest innerRequest,
                                                 List<String> innerKeys, List<AggregationSpec> aggregations,
                                                 List<CaseProjection> caseProjections) {
        List<Field> out = new ArrayList<>();
        // 无分组键（如派生体是 select count(*) from t）→ 聚合输出只有聚合列；此处不能调 scanFields
        // （空列清单在 scanFields 里意为「整表」，会把基表全列误当分组键列拼进来）。
        if (innerKeys != null && !innerKeys.isEmpty()) {
            out.addAll(AbstractDistributedOperator.scanFields(innerProbe,
                    innerRequest.catalog(), innerRequest.schema(), innerRequest.table(), innerKeys));
        }
        List<Field> inputFields = AbstractDistributedOperator.withDerivedFields(
                AbstractDistributedOperator.scanFields(innerProbe,
                        innerRequest.catalog(), innerRequest.schema(), innerRequest.table(),
                        innerRequest.columns().orElse(null)),
                caseProjections);
        for (AggregationSpec a : aggregations) {
            out.add(AbstractDistributedOperator.aggregationOutputField(a, inputFields));
        }
        return out.isEmpty() ? null : out;
    }

    /**
     * {@link DerivedSource} 的<b>声明列</b>（= 派生体输出页 schema：分组键列 + 聚合结果列）。
     *
     * <p>多表链里派生源占的那一层，其「右表列」不能从连接器元数据取（派生体不是表）——
     * 只能按 {@link #derivedSideFields} 反推，与派生 join 路径同源。</p>
     */
    public static List<Field> derivedFields(DerivedSource ds) {
        if (ds.isWindow()) {
            return windowSideFields(ds.innerProbe(), ds.innerRequest(), ds.window());
        }
        if (ds.isProjection()) {
            return projectionSideFields(ds.innerProbe(), ds.innerRequest(), ds.projections());
        }
        return derivedSideFields(ds.innerProbe(), ds.innerRequest(), ds.innerKeys(), ds.aggregations(),
                ds.caseProjections());
    }

    /**
     * 窗口型派生源的<b>声明列</b>：页 = 基表扫描列（扫描请求列序）+ 窗口函数结果列
     * （{@code WindowFunctionOperator} 把窗口列<b>追加</b>在末尾）—— 声明列须与实收页逐列
     * 同名同型同序，否则 build 侧空桶退回声明值时该分区列数与其他分区不同 → 下游越界。
     */
    public static List<Field> windowSideFields(DojaoConnector innerProbe, ReadRequest innerRequest,
                                               DerivedWindow w) {
        List<Field> out = new ArrayList<>(AbstractDistributedOperator.scanFields(innerProbe,
                innerRequest.catalog(), innerRequest.schema(), innerRequest.table(),
                innerRequest.columns().orElse(null)));
        for (WindowFunctionSpec spec : w.functions()) {
            out.add(windowResultField(spec, out));
        }
        return out.isEmpty() ? null : out;
    }

    /** 单个窗口函数结果列的 Arrow 声明类型 —— 与 {@code WindowFunctionOperator.buildOutputSchema} 同源。 */
    private static Field windowResultField(WindowFunctionSpec spec, List<Field> inputFields) {
        return switch (spec.type()) {
            case ROW_NUMBER, RANK, DENSE_RANK, COUNT ->
                    new Field(spec.outputColumn(), FieldType.nullable(new ArrowType.Int(64, true)), null);
            case SUM, AVG, MIN, MAX, AGGREGATE ->
                    new Field(spec.outputColumn(), FieldType.nullable(
                            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            case LAG, LEAD -> {
                Field input = null;
                for (Field f : inputFields) {
                    if (f.getName().equalsIgnoreCase(spec.inputColumn())) {
                        input = f;
                        break;
                    }
                }
                yield new Field(spec.outputColumn(),
                        FieldType.nullable(input != null ? input.getType() : new ArrowType.Utf8()), null);
            }
        };
    }

    /**
     * 投影型派生源的<b>声明列</b>：页 = 基表扫描列（扫描请求列序）+ 逐行投影派生列
     * （{@code ExpressionProjectOperator} 把派生列<b>追加</b>在末尾）—— 声明列须与实收页逐列
     * 同名同型同序，否则 build 侧空桶退回声明值时该分区列数与其他分区不同 → 下游越界。
     */
    private static List<Field> projectionSideFields(DojaoConnector innerProbe, ReadRequest innerRequest,
                                                    List<DerivedProjection> projections) {
        List<Field> out = new ArrayList<>();
        // null / 空列集 = 读全列（scanFields 自身语义）—— 与扫描页实际列序一致
        out.addAll(AbstractDistributedOperator.scanFields(innerProbe,
                innerRequest.catalog(), innerRequest.schema(), innerRequest.table(),
                innerRequest.columns().orElse(null)));
        for (DerivedProjection p : projections) {
            out.add(p.field());
        }
        return out.isEmpty() ? null : out;
    }

    /**
     * 规划一个单 join 的分布式执行舞台。
     *
     * @param queryId       所属查询（exchangeId 前缀，保证查询间隔离）
     * @param workers       参与 Worker（健康节点，非空）
     * @param numPartitions 哈希分区数（应 ≥ worker 数，保证每台至少一个桶）
     */
    public static DistributedStage plan(String queryId, List<WorkerRegistration> workers, int numPartitions,
                                        String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                        String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                        JoinType joinType, long memoryLimit,
                                        DojaoConnector leftProbe, DojaoConnector rightProbe,
                                        String leftFilter, String rightFilter) {
        return planJoin(queryId, workers, numPartitions,
                leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys,
                joinType, memoryLimit, leftProbe, rightProbe, null, leftFilter, rightFilter,
                null, null);
    }

    /** {@link #plan} 的列裁剪重载（{@code leftColumns} / {@code rightColumns} 空 = 读全列）。 */
    public static DistributedStage plan(String queryId, List<WorkerRegistration> workers, int numPartitions,
                                        String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                        String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                        JoinType joinType, long memoryLimit,
                                        DojaoConnector leftProbe, DojaoConnector rightProbe,
                                        String leftFilter, String rightFilter,
                                        List<String> leftColumns, List<String> rightColumns) {
        return planJoin(queryId, workers, numPartitions,
                leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys,
                joinType, memoryLimit, leftProbe, rightProbe, null, leftFilter, rightFilter,
                leftColumns, rightColumns);
    }

    /**
     * 规划一个 <b>join → 同 key 后续计算</b> 的链式分布式舞台（Phase 2，零二次交换）：两侧扫描 →
     * 按 join key 哈希分区 → 归属 Worker 本地 join → <b>直接在本地 join 输出上执行窗口 / 聚合</b> →
     * 回传 Master。owner 映射与同查询其他 shuffle 完全一致。
     *
     * <p>前置条件：后续计算的 key 与 join key <b>完全相同</b>（由 Planner 校验）。join 输出在
     * 归属 Worker 上已按 key 分布，故后续计算无需再交换，worker 内顺序执行即可（cowork without master）。</p>
     */
    public static DistributedStage planChainedJoin(String queryId, List<WorkerRegistration> workers, int numPartitions,
                                                   String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                                   String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                                   JoinType joinType, long memoryLimit,
                                                   DojaoConnector leftProbe, DojaoConnector rightProbe,
                                                   FragmentSpec.FollowUp followUp,
                                                   String leftFilter, String rightFilter) {
        return planJoin(queryId, workers, numPartitions,
                leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys,
                joinType, memoryLimit, leftProbe, rightProbe, followUp, leftFilter, rightFilter,
                null, null);
    }

    /** {@link #planChainedJoin} 的列裁剪重载（{@code leftColumns} / {@code rightColumns} 空 = 读全列）。 */
    public static DistributedStage planChainedJoin(String queryId, List<WorkerRegistration> workers, int numPartitions,
                                                   String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                                   String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                                   JoinType joinType, long memoryLimit,
                                                   DojaoConnector leftProbe, DojaoConnector rightProbe,
                                                   FragmentSpec.FollowUp followUp,
                                                   String leftFilter, String rightFilter,
                                                   List<String> leftColumns, List<String> rightColumns) {
        return planJoin(queryId, workers, numPartitions,
                leftCatalog, leftSchema, leftTable, leftKeys,
                rightCatalog, rightSchema, rightTable, rightKeys,
                joinType, memoryLimit, leftProbe, rightProbe, followUp, leftFilter, rightFilter,
                leftColumns, rightColumns);
    }

    /**
     * join 舞台核心：Producer 完全相同；Consumer 在无 followUp 时为普通 join，有 followUp 时为
     * {@link FragmentSpec.ChainedJoinConsumer}（join 输出 → 本地窗口 / 聚合）。
     */
    private static DistributedStage planJoin(String queryId, List<WorkerRegistration> workers, int numPartitions,
                                             String leftCatalog, String leftSchema, String leftTable, List<String> leftKeys,
                                             String rightCatalog, String rightSchema, String rightTable, List<String> rightKeys,
                                             JoinType joinType, long memoryLimit,
                                             DojaoConnector leftProbe, DojaoConnector rightProbe,
                                             FragmentSpec.FollowUp followUp,
                                             String leftFilter, String rightFilter,
                                             List<String> leftColumns, List<String> rightColumns) {
        // 1. round-robin owner 映射 —— co-partitioning 不变量：两侧必须完全一致
        PartitionLayout layout = PartitionLayout.roundRobin(workers, numPartitions);
        int n = layout.numPartitions();

        String leftExchange = queryId + "-L";
        String rightExchange = queryId + "-R";

        // 2. 数据切片（分区策略完全委托连接器；空 → 单切片整表）。
        //    单表 WHERE 谓词已下推进 ReadRequest → 分区切片 / Worker 侧 SQL 均带过滤（数据量骤减）。
        //    列裁剪同理推进到扫描请求 → 连接器只读被引用的列（Hive 宽表 23 列 → 少数列，I/O 骤减）。
        List<ScanSlice> leftSlices = planSlices(leftProbe,
                withFilter(readRequest(leftProbe, leftCatalog, leftSchema, leftTable, leftColumns), leftFilter), n);
        List<ScanSlice> rightSlices = planSlices(rightProbe,
                withFilter(readRequest(rightProbe, rightCatalog, rightSchema, rightTable, rightColumns), rightFilter), n);

        // 3. Producer：切片 i → workers[i % numWorkers]（每切片一个 Producer，独立扫 + 分区 + 推送）。
        //    派发顺序【右表（build 侧）先、左表（probe 侧）后】：HashJoin 每分区先 build 右表（消费
        //    右表 reader 到分区结束）才 probe 左表。若左 Producer 先派发并占满 Worker 任务池槽位，它会
        //    把左表数据塞进尚未被消费的有界 reader 队列而阻塞在满队列上、占住槽位不放 —— 右（build 侧）
        //    Producer 因此永远拿不到槽 → 右表 reader 收不齐 complete → build 永不结束 → 死锁。
        //    右 Producer 先跑完（join 在 buildSide 恒消费右表，必然排空）→ build 完成后 join 转
        //    probeSide 消费左表，左 Producer 流式灌入、队列始终被消费 → 无死锁（与 build-then-probe 执行序对齐）。
        List<FragmentSpec> producers = new ArrayList<>();
        for (int i = 0; i < rightSlices.size(); i++) {
            String w = workers.get(i % workers.size()).info().workerId();
            producers.add(new FragmentSpec.Producer(fragmentId(queryId, "PR", i), w,
                    new FragmentSpec.ScanStep(rightCatalog, rightSlices.get(i)),
                    new FragmentSpec.PartitionStep(rightExchange, rightKeys, n,
                            layout.ownerWorkerIdsList())));
        }
        for (int i = 0; i < leftSlices.size(); i++) {
            String w = workers.get(i % workers.size()).info().workerId();
            producers.add(new FragmentSpec.Producer(fragmentId(queryId, "PL", i), w,
                    new FragmentSpec.ScanStep(leftCatalog, leftSlices.get(i)),
                    new FragmentSpec.PartitionStep(leftExchange, leftKeys, n,
                            layout.ownerWorkerIdsList())));
        }

        // 4. Consumer：每个 Worker 一个，own 它的桶，两侧数据本地 join（有 followUp 时链式执行后续计算）
        int numLeftSenders = leftSlices.size();
        int numRightSenders = rightSlices.size();
        List<FragmentSpec> consumers = new ArrayList<>();
        for (int w = 0; w < workers.size(); w++) {
            String workerId = workers.get(w).info().workerId();
            FragmentSpec.ExchangeInStep leftIn = new FragmentSpec.ExchangeInStep(
                    leftExchange, n, numLeftSenders, layout.ownedMask(workerId));
            FragmentSpec.ExchangeInStep rightIn = new FragmentSpec.ExchangeInStep(
                    rightExchange, n, numRightSenders, layout.ownedMask(workerId));
            FragmentSpec.JoinStep joinStep = new FragmentSpec.JoinStep(leftKeys, rightKeys, joinType,
                    memoryLimit, n, FragmentSpec.JoinAlgorithm.HASH,
                    rightFieldsOf(rightProbe, rightCatalog, rightSchema, rightTable, rightColumns),
                    rightFieldsOf(leftProbe, leftCatalog, leftSchema, leftTable, leftColumns));
            if (followUp == null) {
                consumers.add(new FragmentSpec.Consumer(fragmentId(queryId, "C", w), workerId,
                        leftIn, rightIn, joinStep));
            } else {
                consumers.add(new FragmentSpec.ChainedJoinConsumer(fragmentId(queryId, "CC", w), workerId,
                        leftIn, rightIn, joinStep, followUp));
            }
        }

        return new DistributedStage(layout.ownerWorkerIds(), consumers, producers);
    }

    /**
     * 规划一个<b>广播 join</b>（{@code JoinDecider} 判定「右表够小、左表够大、INNER/LEFT」时）——
     * 右表（build 侧）整份复制到<b>每台</b> Worker，左表（probe 侧）切片就地不动，<b>省掉大表的 shuffle</b>。
     *
     * <p><b>为什么不需要新算子</b>：广播由「分区方式」而非算子形态实现 —— 把 {@code numPartitions}
     * 取成 <b>Worker 数 w</b>（而非默认分区数 n），round-robin owner 映射下每台恰好 own 一个桶：</p>
     * <pre>
     *   build 侧（右表）: REPLICATE —— 整页复制到全部 w 个桶 ⇒ 每台拿到<b>完整一份</b>右表
     *   probe 侧（左表）: FIXED     —— 切片 i 固定发往桶 (i % w) = 本机槽位 ⇒ <b>零跨机</b>
     *   Worker i: 左桶 = 本机左切片，右桶 = 完整右表 → 原 HashJoinOperator 直接可用
     * </pre>
     *
     * <p>内存：{@code JoinStep.memoryLimit} 按 {@code w × U} 下发 —— HashJoinOperator 内部除以
     * {@code numPartitions}(=w) 得每分区预算 {@code U}，正是 {@code JoinDecider} 判定 build 侧装得下的口径。</p>
     *
     * <p><b>派发顺序（反死锁）</b>：build（右）先于 probe（左）—— 与 {@link #planJoin} 同理，
     * HashJoin 每分区必须先 build 完右表再 probe 左表，右表 Producer 先跑完才能排空右 reader。</p>
     *
     * <p><b>限制</b>：只用于单 join（多 join 链的中间层依赖「按 join 键哈希」的 co-partitioning）；
     * 只用于 INNER / LEFT（RIGHT / FULL 复制 build 侧会重复输出「无匹配的 build 行」，见
     * {@code JoinDecider.broadcastSafe}）；派生表 join / join→聚合 暂不走广播。</p>
     */
    private static JoinChainStage planBroadcastJoin(String queryId, List<WorkerRegistration> workers,
                                                    String leftCatalog, String leftSchema, String leftTable,
                                                    List<String> leftKeys, DojaoConnector leftProbe, String leftFilter,
                                                    JoinLink link, long memoryLimit, List<String> leftColumns) {
        // ★ numPartitions = Worker 数（不是默认分区数）：每台恰好 own 一个桶，右桶 = 完整右表
        PartitionLayout layout = PartitionLayout.roundRobin(workers, workers.size());
        int w = layout.numPartitions();
        List<String> ownerWorkerIds = layout.ownerWorkerIdsList();

        String leftExchange = queryId + "-BL";
        String rightExchange = queryId + "-BR";

        List<ScanSlice> leftSlices = planSlices(leftProbe,
                withFilter(readRequest(leftProbe, leftCatalog, leftSchema, leftTable, leftColumns), leftFilter), w);
        List<ScanSlice> rightSlices = planSlices(link.rightProbe(),
                withFilter(readRequest(link.rightProbe(), link.rightCatalog(), link.rightSchema(),
                        link.rightTable(), link.rightColumns()), link.rightFilter()), w);

        // 1. Producer：build（右表，REPLICATE）先派发，probe（左表，FIXED）后派发
        List<FragmentSpec> producers = new ArrayList<>();
        for (int i = 0; i < rightSlices.size(); i++) {
            String wid = workers.get(i % workers.size()).info().workerId();
            producers.add(new FragmentSpec.Producer(fragmentId(queryId, "BR", i), wid,
                    new FragmentSpec.ScanStep(link.rightCatalog(), rightSlices.get(i)),
                    FragmentSpec.PartitionStep.broadcast(rightExchange, link.rightKeys(), w, ownerWorkerIds,
                            FragmentSpec.Distribution.REPLICATE, 0)));
        }
        for (int i = 0; i < leftSlices.size(); i++) {
            int bucket = i % workers.size();   // 本生产者所在 Worker 的槽位 = 目标桶（零跨机）
            String wid = workers.get(bucket).info().workerId();
            producers.add(new FragmentSpec.Producer(fragmentId(queryId, "BL", i), wid,
                    new FragmentSpec.ScanStep(leftCatalog, leftSlices.get(i)),
                    FragmentSpec.PartitionStep.broadcast(leftExchange, leftKeys, w, ownerWorkerIds,
                            FragmentSpec.Distribution.FIXED, bucket)));
        }

        // 2. Consumer：每台一个，own 自己那一个桶；两侧 numPartitions 都是 w
        //    memoryLimit × w —— HashJoinOperator 内部 /numPartitions 后每分区预算回到 U
        long fragmentMemory = memoryLimit * w;
        List<FragmentSpec> consumers = new ArrayList<>();
        for (int i = 0; i < w; i++) {
            String wid = workers.get(i).info().workerId();
            FragmentSpec.ExchangeInStep leftIn = new FragmentSpec.ExchangeInStep(
                    leftExchange, w, leftSlices.size(), layout.ownedMask(wid));
            FragmentSpec.ExchangeInStep rightIn = new FragmentSpec.ExchangeInStep(
                    rightExchange, w, rightSlices.size(), layout.ownedMask(wid));
            consumers.add(new FragmentSpec.Consumer(fragmentId(queryId, "BC", i), wid, leftIn, rightIn,
                    new FragmentSpec.JoinStep(leftKeys, link.rightKeys(), link.joinType(),
                            fragmentMemory, w, FragmentSpec.JoinAlgorithm.BROADCAST, rightFieldsOf(link),
                            rightFieldsOf(leftProbe, leftCatalog, leftSchema, leftTable, leftColumns))));
        }

        System.err.printf("[StageScheduler] 广播 join：右表(%s.%s.%s) 复制到 %d 台，左表就地不 shuffle%n",
                link.rightCatalog(), link.rightSchema(), link.rightTable(), w);
        // 派发顺序（反死锁）：消费者先（注册交换读端）→ 生产者（build 右先、probe 左后）
        List<FragmentSpec> fragments = new ArrayList<>(consumers.size() + producers.size());
        fragments.addAll(consumers);
        fragments.addAll(producers);
        return new JoinChainStage(fragments, w, layout.ownerWorkerIds());
    }

    /**
     * 把 {@code BROADCAST} 结论降级为哈希 —— 广播只在「<b>末层</b> + 无 followUp」成立
     * （见 {@link #planJoinChain} 的末层切分；单 join 见 {@link #planBroadcastJoin}）。
     *
     * <p>其余场景一律回退哈希：中间层广播要求「上一层 Consumer 片段直接充当下一层执行者、
     * 膨胀的中间结果原地不动」，是一套新的片段编排（本期未做）；带同 key 后续计算（聚合 / 窗口）
     * 时广播的「probe 侧就地分桶」会让同组散到不同 Worker → 结果错。两者 Worker 侧是同一算子，
     * 只是标注不同，降级零代价。</p>
     */
    public static List<JoinLink> downgradeBroadcast(List<JoinLink> links) {
        if (links.stream().noneMatch(l -> l.algorithm() == FragmentSpec.JoinAlgorithm.BROADCAST)) {
            return links;
        }
        return links.stream()
                .map(l -> l.algorithm() != FragmentSpec.JoinAlgorithm.BROADCAST ? l
                        : new JoinLink(l.leftKeys(), l.rightKeys(), l.joinType(),
                                l.rightCatalog(), l.rightSchema(), l.rightTable(), l.rightProbe(),
                                l.rightFilter(), FragmentSpec.JoinAlgorithm.HASH, l.rightColumns()))
                .toList();
    }

    /**
     * 规划一个<b>左深多 join 链</b>（{@code (T0 ⋈J1 T1) ⋈J2 T2 ⋈J3 ...}）的分布式执行舞台：
     * 每层 join 的消费者<b>不回传 Master</b>，而是把 join 输出按下一层 join 的左键重新哈希分区，
     * 推给下一层交换（{@link FragmentSpec.MiddleJoinConsumer}）；最末层 join 默认用普通
     * {@link FragmentSpec.Consumer} 回传 Master，传了 {@code followUp}（聚合 / 窗口）时用
     * {@link FragmentSpec.ChainedJoinConsumer} —— join 输出留本 Worker 做同 key 后续计算再回传
     * （零二次交换，Master 只收结果，不吸原始 join 行）。
     *
     * <p><b>exchange 命名</b>：join#k 的左右交换 = {@code queryId + "-J" + k + "-L" / "-R"}。
     * 交换 {@code exchange#k-L}（k≥2）的发送方 = join#(k-1) 的 MiddleJoinConsumer（每 Worker 一个，
     * 故 {@code numSenders = workers.size()}）；k=1 时发送方 = 首个基表 T0 的 Producer 切片。</p>
     *
     * <p><b>派发顺序（反死锁）</b>：① 最终 Consumer（注册 {@code exchange#(N-1)-L/-R} 读端）；
     * ② MiddleJoinConsumer 按 join 层<b>逆序</b>（join#(N-2) → join#1：join#k 要推
     * {@code exchange#(k+1)-L}，必须先有 join#(k+1) 的读端）；③ 基表 Producers 按
     * build(右) 先于 probe(左)：T1, T0, T2, T3, ..., T(N-1)。</p>
     *
     * @param queryId          所属查询（exchangeId / fragmentId 前缀，查询间隔离）
     * @param workers          参与 Worker（健康节点，非空）
     * @param numPartitions    哈希分区数（应 ≥ worker 数）
     * @param firstCatalog     首个基表 T0 的 catalog / schema / table
     * @param firstLeftKeys    join#1 的 probe（左）键 —— T0 的 join key 列
     * @param firstProbe       T0 的探针连接器
     * @param links            join#1..#(N-1) 的 {@link JoinLink} 列表（个数 ≥ 2）
     * @param memoryLimit      每个溢写算子的内存预算
     * @param followUp         最末层 join 的同 key 后续计算（聚合 / 窗口）。非 null 时最末层
     *                         Consumer 变成 {@link FragmentSpec.ChainedJoinConsumer} —— join 输出
     *                         留在本 Worker 直接做后续计算再回传（零二次交换）。调用方须保证
     *                         <b>分组键包含最末层 join 的分区键</b>（last link 左键），否则同组跨分区
     *                         聚合不完整、结果错误。null → 普通 Consumer（现行为）。
     */
    public static JoinChainStage planJoinChain(String queryId, List<WorkerRegistration> workers, int numPartitions,
                                               String firstCatalog, String firstSchema, String firstTable,
                                               List<String> firstLeftKeys, DojaoConnector firstProbe,
                                               String firstFilter,
                                               List<JoinLink> links, long memoryLimit,
                                               FragmentSpec.FollowUp followUp) {
        return planJoinChain(queryId, workers, numPartitions, firstCatalog, firstSchema, firstTable,
                firstLeftKeys, firstProbe, firstFilter, links, memoryLimit, followUp, null);
    }

    /** {@link #planJoinChain} 的列裁剪重载（{@code firstColumns} 空 = 首表读全列）。 */
    public static JoinChainStage planJoinChain(String queryId, List<WorkerRegistration> workers, int numPartitions,
                                               String firstCatalog, String firstSchema, String firstTable,
                                               List<String> firstLeftKeys, DojaoConnector firstProbe,
                                               String firstFilter,
                                               List<JoinLink> links, long memoryLimit,
                                               FragmentSpec.FollowUp followUp,
                                               List<String> firstColumns) {
        return planJoinChain(queryId, workers, numPartitions, firstCatalog, firstSchema, firstTable,
                firstLeftKeys, firstProbe, firstFilter, links, memoryLimit, followUp, firstColumns, null);
    }

    /**
     * {@link #planJoinChain} 的<b>派生源</b>重载：{@code sources} 与「T0, T1, ..., T(N-1)」一一对应
     * （下标 0 = 首表 T0；下标 k = {@code links.get(k-1)} 的右表），元素为 {@code null} 即普通基表扫描，
     * 非 null 则该层关系是派生表（子查询 / CTE）—— 由「扫派生基表 → 按分组键分区 → 每分区本地聚合
     * （HAVING）→ 按本层 join 键重哈希」的迷你流水线喂入本层交换，<b>数据不回传 Master</b>。
     *
     * <p>{@code sources == null} 时与旧签名逐字节同行为（既有调用点零影响）。</p>
     */
    public static JoinChainStage planJoinChain(String queryId, List<WorkerRegistration> workers, int numPartitions,
                                               String firstCatalog, String firstSchema, String firstTable,
                                               List<String> firstLeftKeys, DojaoConnector firstProbe,
                                               String firstFilter,
                                               List<JoinLink> links, long memoryLimit,
                                               FragmentSpec.FollowUp followUp,
                                               List<String> firstColumns,
                                               List<DerivedSource> sources) {
        DerivedSource firstSource = sourceAt(sources, 0);
        // 广播 join（仅单 join + 无 followUp）：分区方式与哈希完全不同，走专门切片
        if (links.size() == 1 && links.get(0).algorithm() == FragmentSpec.JoinAlgorithm.BROADCAST
                && followUp == null) {
            return planBroadcastJoin(queryId, workers, firstCatalog, firstSchema, firstTable, firstLeftKeys,
                    firstProbe, firstFilter, links.get(0), memoryLimit, firstColumns);
        }

        // ★链上广播（P1-a）只在「末层 + 无 followUp」成立；其余 BROADCAST 结论一律降级哈希
        //   （带同 key 后续计算时广播的「probe 侧就地分桶」会让同组散到不同 Worker → 结果错；
        //    中间层广播需要新的片段编排，本期不做）。
        boolean lastBroadcast = links.size() >= 2 && followUp == null
                && links.get(links.size() - 1).algorithm() == FragmentSpec.JoinAlgorithm.BROADCAST;
        if (!lastBroadcast) {
            links = downgradeBroadcast(links);
        }

        PartitionLayout layout = PartitionLayout.roundRobin(workers, numPartitions);
        int n = layout.numPartitions();
        int numWorkers = workers.size();
        int numJoins = links.size();                      // N-1 层 join
        List<String> ownerWorkerIds = layout.ownerWorkerIdsList();

        // CROSS 环节（非相关标量子查询的单值广播 build 侧）<b>只允许在末层</b>：中间层 CROSS 的输出
        // 还得按下一层左键重哈希，而 CROSS 无键可依、且派生源体量不可估 —— Planner 已保证不产生这种
        // 形状，此处 fail-fast（宁响亮报错，不静默编排错）。
        for (int k = 0; k < numJoins - 1; k++) {
            if (links.get(k).joinType() == JoinType.CROSS) {
                throw new IllegalArgumentException(
                        "planJoinChain: CROSS 环节只允许出现在最末层（link#" + k + "/" + numJoins + "）");
            }
        }

        // 末层广播：分区方式取「Worker 数 bw」（每台恰好 own 一个桶）——
        //   build 侧（末层右表）REPLICATE 整份复制到全部 bw 个桶；
        //   probe 侧（末层左表 = 上一层 join 的输出）FIXED 就地入桶，上一层 MiddleJoinConsumer
        //   本机算完直接推回本机 → 膨胀的中间结果原地不动（省掉整个 shuffle）。
        //   中间层仍走 n 分区 HASH —— 末层广播只换「最后一层的行去哪个桶」。
        PartitionLayout lastLayout = lastBroadcast ? PartitionLayout.roundRobin(workers, numWorkers) : layout;
        int bw = lastLayout.numPartitions();
        List<String> lastOwnerWorkerIds = lastBroadcast ? lastLayout.ownerWorkerIdsList() : ownerWorkerIds;

        // 基表切片：T0（首个 probe）与 T1..T(N)（各 join 的 build 右表）。
        //    每个基表带自己的单表 WHERE 下推谓词 → 分区切片 / Worker 侧 SQL 均带过滤（数据量骤减）；
        //    列裁剪同样落到每个基表的扫描请求（各层 {@code rightColumns} / 首表 {@code firstColumns}）。
        //    ★派生源（子查询 / CTE）占用的层不做直扫切片 —— 该层交换由派生聚合流水线喂入（见第 4 步）。
        List<ScanSlice> firstSlices = firstSource == null
                ? planSlices(firstProbe,
                        withFilter(readRequest(firstProbe, firstCatalog, firstSchema, firstTable, firstColumns),
                                firstFilter), n)
                : List.of();
        // 派生源的基表切片<b>只规划一次</b>：既用于发送方数，也喂给 emitDerivedSource ——
        // 两次独立调用可能拿到不同的 HDFS 列举顺序，发送方数一旦与实发片段数不符，
        // ShuffleReader 会提早收齐 partitionEnd → 后到页被丢（静默少行）。
        List<List<ScanSlice>> sourceSlices = new ArrayList<>(numJoins + 1);   // 下标 = 源层号（0 = T0）
        for (int k = 0; k <= numJoins; k++) {
            DerivedSource ds = sourceAt(sources, k);
            sourceSlices.add(ds == null ? List.of() : planSlices(ds.innerProbe(), ds.innerRequest(), n));
        }
        // 每侧交换的<b>发送方数</b>：普通基表 = 该侧切片数；聚合型派生源 = n（n 个 MiddleAggregateConsumer
        // 各对全部桶发一次 complete）；投影型派生源 = 基表切片数（普通 Producer 直接写该交换）。
        // 写错会让 ShuffleReader 提早收齐 partitionEnd → 后到数据被丢（静默少行）。
        int firstSenders = firstSource == null ? firstSlices.size()
                : derivedSenders(firstSource, sourceSlices.get(0), n);
        List<List<ScanSlice>> rightSlices = new ArrayList<>(numJoins);
        int[] rightSenderCounts = new int[numJoins + 1];        // 下标 = 层号 k（1..numJoins）
        for (int k = 1; k <= numJoins; k++) {
            JoinLink link = links.get(k - 1);
            if (sourceAt(sources, k) != null) {
                rightSlices.add(List.of());
                rightSenderCounts[k] = derivedSenders(sourceAt(sources, k), sourceSlices.get(k), n);
                continue;
            }
            int part = (lastBroadcast && k == numJoins) ? bw : n;   // 末层广播的右表按 bw 切片
            List<ScanSlice> slices = planSlices(link.rightProbe(),
                    withFilter(readRequest(link.rightProbe(), link.rightCatalog(), link.rightSchema(),
                            link.rightTable(), link.rightColumns()), link.rightFilter()), part);
            rightSlices.add(slices);
            rightSenderCounts[k] = slices.size();
        }

        List<FragmentSpec> fragments = new ArrayList<>();

        // 1. 最终 Consumer（join#N）：先派发，注册 exchange#N-L/-R 读端
        JoinLink last = links.get(numJoins - 1);
        String lastLeftExchange = queryId + "-J" + numJoins + "-L";
        String lastRightExchange = queryId + "-J" + numJoins + "-R";
        List<ScanSlice> lastRightSlices = rightSlices.get(numJoins - 1);
        // 左交换发送方：单 join = firstSlices 个 T0 Producer；多 join = numWorkers 个中间层
        // MiddleJoinConsumer（每个对全部桶发一次 complete）。写错会让 ShuffleReader 提早收齐
        // partitionEnd → 后到 Producer 的页被丢（静默少行）。
        int lastLeftSenders = (numJoins == 1) ? firstSenders : numWorkers;
        int lastPart = lastBroadcast ? bw : n;
        // 广播时 memoryLimit × bw —— HashJoinOperator 内部 /numPartitions(=bw) 后每分区预算回到 U
        long lastMemory = lastBroadcast ? memoryLimit * bw : memoryLimit;
        for (int w = 0; w < numWorkers; w++) {
            String workerId = workers.get(w).info().workerId();
            FragmentSpec.ExchangeInStep leftIn = new FragmentSpec.ExchangeInStep(
                    lastLeftExchange, lastPart, lastLeftSenders, lastLayout.ownedMask(workerId));
            FragmentSpec.ExchangeInStep rightIn = new FragmentSpec.ExchangeInStep(
                    lastRightExchange, lastPart, rightSenderCounts[numJoins], lastLayout.ownedMask(workerId));
            FragmentSpec.JoinStep joinStep = new FragmentSpec.JoinStep(
                    last.leftKeys(), last.rightKeys(), last.joinType(), lastMemory, lastPart, last.algorithm(),
                    declaredFieldsFor(last, sourceAt(sources, numJoins)),
                    leftFieldsForChain(numJoins, firstCatalog, firstSchema, firstTable, firstColumns,
                            firstProbe, links, sources));
            // 有 followUp（聚合 / 窗口）时最末层 Consumer 链式执行：join 输出留在本 Worker 直接
            // 做后续计算再回传（零二次交换，master 只收聚合/窗口结果页，不吸原始 join 行）。
            String id = fragmentId(queryId, "CJ", numJoins, w);
            if (followUp == null) {
                fragments.add(new FragmentSpec.Consumer(id, workerId, leftIn, rightIn, joinStep));
            } else {
                fragments.add(new FragmentSpec.ChainedJoinConsumer(id, workerId, leftIn, rightIn, joinStep, followUp));
            }
        }

        // 2. 末层广播：build 侧（T_N，右表）Producer 紧跟 Consumer 派发 —— 与 planBroadcastJoin 同款。
        //    末层 Consumer 必须先 build 完右表才 probe 左表，若 build 侧 Producer 排在最后，
        //    前面被「左桶写满 → 阻塞」的中间层会占住全部任务线程 → 排队的 build Producer 永远起不来
        //    → 级联死锁。其余层（T1 先、T0 次）顺序不变。
        if (lastBroadcast) {
            emitRightProducers(fragments, queryId, numJoins, last, lastRightSlices,
                    workers, numWorkers, bw, lastOwnerWorkerIds, true);
        }

        // 3. MiddleJoinConsumer（join#(N-1) → join#1 逆序）：join#k 输出按下一层左键重哈希推走
        for (int k = numJoins - 1; k >= 1; k--) {
            JoinLink link = links.get(k - 1);
            JoinLink next = links.get(k);                 // 下一层 join：其左键用于重哈希
            String leftExchange = queryId + "-J" + k + "-L";
            String rightExchange = queryId + "-J" + k + "-R";
            String nextLeftExchange = queryId + "-J" + (k + 1) + "-L";
            int leftSenders = (k == 1) ? firstSenders : numWorkers;
            int rightSenders = rightSenderCounts[k];
            // 喂给末层的中间层（k = N-1）：广播时改走 FIXED —— 本机 join 输出就地入本机桶，
            // 不再按末层左键重哈希（这正是广播省掉的那次 shuffle）。
            boolean feedBroadcast = lastBroadcast && k == numJoins - 1;
            for (int w = 0; w < numWorkers; w++) {
                String workerId = workers.get(w).info().workerId();
                FragmentSpec.ExchangeInStep leftIn = new FragmentSpec.ExchangeInStep(
                        leftExchange, n, leftSenders, layout.ownedMask(workerId));
                FragmentSpec.ExchangeInStep rightIn = new FragmentSpec.ExchangeInStep(
                        rightExchange, n, rightSenders, layout.ownedMask(workerId));
                FragmentSpec.JoinStep joinStep = new FragmentSpec.JoinStep(
                        link.leftKeys(), link.rightKeys(), link.joinType(), memoryLimit, n, link.algorithm(),
                        declaredFieldsFor(link, sourceAt(sources, k)),
                        leftFieldsForChain(k, firstCatalog, firstSchema, firstTable, firstColumns,
                                firstProbe, links, sources));
                FragmentSpec.PartitionStep nextPartition = feedBroadcast
                        ? FragmentSpec.PartitionStep.broadcast(nextLeftExchange, next.leftKeys(), bw,
                                lastOwnerWorkerIds, FragmentSpec.Distribution.FIXED, w)
                        // ★左分区键：普通哈希 join = leftKeys（共分区不变量）；末层 CROSS 无 join 键 ⇒
                        //   用左表全列打散（拿空键分区会把中间结果全塞进 0 号桶 → 退化单机 + REPLICATE
                        //   的 build 侧只被 0 号桶消费 ⇒ 其余分区的左行一行都 join 不上）。
                        : new FragmentSpec.PartitionStep(nextLeftExchange, next.leftPartitionKeys(),
                                n, ownerWorkerIds);
                fragments.add(new FragmentSpec.MiddleJoinConsumer(
                        fragmentId(queryId, "MJ", k, w), workerId, leftIn, rightIn, joinStep, nextPartition));
            }
        }

        // 4. 基表 Producers：build(右) 先于 probe(左) —— T1, T0, T2, T3, ..., T(N-1)
        //    （末层广播的 T_N 已在第 2 步提前派发，此处 k < numJoins 时跳过）
        //    ★派生源（子查询 / CTE）占用的层不发直扫 Producer，改发「派生聚合流水线」
        //      （{@link #emitDerivedSource}）—— 读端（本层 join 消费者）已在第 1/3 步注册，
        //      流水线内部「聚合消费者先、派生基表 Producer 后」同样是读者先于写者。
        //    T1（join#1 build 右表）
        JoinLink first = links.get(0);
        DerivedSource firstRightSource = sourceAt(sources, 1);
        if (firstRightSource != null) {
            emitDerivedSource(fragments, queryId, 1, firstRightSource, layout, workers, numWorkers, n,
                    ownerWorkerIds, queryId + "-J1-R", first.rightKeys(), memoryLimit, sourceSlices.get(1),
                    first.joinType() == JoinType.CROSS);
        } else {
            emitRightProducers(fragments, queryId, 1, first, rightSlices.get(0),
                    workers, numWorkers, n, ownerWorkerIds, false);
        }
        //    T0（join#1 probe 左表）
        String firstLeftExchange = queryId + "-J1-L";
        if (firstSource != null) {
            // T0 派生源不是 CROSS 的 build 侧（Planner 保证单值广播只出现在某个右表层）→ 不复制
            emitDerivedSource(fragments, queryId, 0, firstSource, layout, workers, numWorkers, n,
                    ownerWorkerIds, firstLeftExchange, firstLeftKeys, memoryLimit, sourceSlices.get(0), false);
        } else {
            for (int i = 0; i < firstSlices.size(); i++) {
                String w = workers.get(i % numWorkers).info().workerId();
                fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "PL", i), w,
                        new FragmentSpec.ScanStep(firstCatalog, firstSlices.get(i)),
                        new FragmentSpec.PartitionStep(firstLeftExchange, firstLeftKeys, n, ownerWorkerIds)));
            }
        }
        //    T2 .. T(N-1)（join#k≥2 的 build 右表）
        for (int k = 2; k <= numJoins; k++) {
            if (lastBroadcast && k == numJoins) {
                continue;   // 末层广播的 T_N 已在第 2 步提前派发
            }
            JoinLink link = links.get(k - 1);
            DerivedSource ds = sourceAt(sources, k);
            if (ds != null) {
                emitDerivedSource(fragments, queryId, k, ds, layout, workers, numWorkers, n,
                        ownerWorkerIds, queryId + "-J" + k + "-R", link.rightKeys(), memoryLimit,
                        sourceSlices.get(k), link.joinType() == JoinType.CROSS);
            } else {
                emitRightProducers(fragments, queryId, k, link, rightSlices.get(k - 1),
                        workers, numWorkers, n, ownerWorkerIds, false);
            }
        }

        return new JoinChainStage(fragments, numWorkers, layout.ownerWorkerIds());
    }

    /**
     * 发射某层 build 侧（右表）的一整组基表 Producer：切片 i → {@code workers[i % numWorkers]}，
     * 按 {@code numPartitions} 个桶分区（{@code broadcast} = 广播的 REPLICATE 模式，整页复制到全部桶）。
     */
    private static void emitRightProducers(List<FragmentSpec> fragments, String queryId, int k, JoinLink link,
                                           List<ScanSlice> slices, List<WorkerRegistration> workers,
                                           int numWorkers, int numPartitions, List<String> ownerWorkerIds,
                                           boolean broadcast) {
        String rightExchange = queryId + "-J" + k + "-R";
        for (int i = 0; i < slices.size(); i++) {
            String w = workers.get(i % numWorkers).info().workerId();
            FragmentSpec.PartitionStep ps = broadcast
                    ? FragmentSpec.PartitionStep.broadcast(rightExchange, link.rightKeys(), numPartitions,
                            ownerWorkerIds, FragmentSpec.Distribution.REPLICATE, 0)
                    : new FragmentSpec.PartitionStep(rightExchange, link.rightKeys(), numPartitions, ownerWorkerIds);
            fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "PR", k, i), w,
                    new FragmentSpec.ScanStep(link.rightCatalog(), slices.get(i)), ps));
        }
    }

    /** {@code sources} 列表第 {@code idx} 个元素（列表 null / 越界 / 元素 null → null = 普通基表扫描）。 */
    private static DerivedSource sourceAt(List<DerivedSource> sources, int idx) {
        return (sources == null || idx < 0 || idx >= sources.size()) ? null : sources.get(idx);
    }

    /**
     * 派生源在该层交换上的<b>发送方数</b>：
     * 聚合型 = {@code n}（n 个 MiddleAggregateConsumer 各对全部桶发一次 complete）；
     * 投影型 = 基表切片数（普通 Producer 直接写该交换，无中间聚合层）。
     */
    private static int derivedSenders(DerivedSource ds, List<ScanSlice> innerSlices, int n) {
        if (ds.isProjection()) {
            return innerSlices.size();
        }
        // 全局聚合派生源（无分组键 ⇒ 恒 1 行）：只发<b>一个</b> MiddleAggregateConsumer（见
        // {@link #emitDerivedSource}），它把该行 REPLICATE 到全部 n 个桶 —— 发送方数是 1，不是 n。
        return isGlobalAgg(ds) ? 1 : n;
    }

    /**
     * true = <b>聚合型</b>派生源且<b>无分组键</b>（全局聚合，恒 1 行）。
     *
     * <p>这就是「非相关标量子查询」去相关后的派生体（{@code (SELECT count(*) FROM 内层 WHERE 纯内层谓词)}）：
     * 外层按 CROSS JOIN 与之连接 ⇒ 需要「单值广播」通道 —— 一个聚合消费者算出这 1 行，再整份复制到
     * 每个 join 分区（派生源框架原本按分组键哈希分区，表达不了「广播」）。</p>
     */
    private static boolean isGlobalAgg(DerivedSource ds) {
        return !ds.isProjection() && !ds.isWindow()
                && (ds.innerKeys() == null || ds.innerKeys().isEmpty());
    }

    /**
     * 某层 build 侧的<b>声明列</b>：派生源 → 派生体输出列（分组键 + 聚合结果，无连接器元数据可查）；
     * 普通基表 → 连接器元数据（列集取该层实际扫描列）。二者都必须与实际到达 Worker 的页同列同序，
     * 否则 build 侧空桶退回声明值时该分区列数与其他分区不同 → 下游越界。
     */
    private static List<Field> declaredFieldsFor(JoinLink link, DerivedSource ds) {
        return ds != null ? derivedFields(ds) : rightFieldsOf(link);
    }

    /**
     * 发射一层<b>派生源</b>（子查询 / CTE）的完整聚合流水线，把派生体喂进该层 join 交换：
     *
     * <pre>
     *   扫派生基表(列裁剪 + 内层 WHERE) ─► Partition(分组键) ─► 每分区本地聚合 ─(HAVING)─►
     *       Partition(本层 join 键) ─► {@code targetExchange}
     * </pre>
     *
     * <p>与两表派生 join（{@link #planDerivedJoin} 的 MiddleAggregateConsumer 段）同一套机制，
     * 只是接在多表链的任意一层上。分段交换（{@code aggExchange#p}）使聚合评估并行度 = 分区数、
     * 不与 Worker 数绑定；每 key 全行落同一桶 ⇒ 本地聚合即完整聚合（co-partitioning 不变量）。</p>
     *
     * <p><b>派发顺序</b>：聚合消费者<b>先</b>（注册聚合交换读端 + 写 {@code targetExchange}，
     * 其读者已在调用方注册）、派生基表 Producer <b>后</b>（写端在读者之后）。</p>
     *
     * @param idx            源下标（0 = 首表 T0；k = 第 k 层 join 的右表），仅用于片段 ID / 交换名去重
     * @param targetExchange 该层 join 交换名（T0 派生 → {@code -J1-L}；第 k 层右表派生 → {@code -Jk-R}）
     * @param targetKeys     该层 join 在本侧的键（T0 → firstLeftKeys；右表 → {@code link.rightKeys()}）
     * @param replicate      true = <b>单值广播</b>：本层 join 是 CROSS JOIN，build 侧是全局聚合派生源
     *                       （恒 1 行）→ 该行必须 REPLICATE 到全部 n 个桶（按 {@code targetKeys} 哈希会
     *                       全落 0 号桶 ⇒ 其余分区 build 侧为空 ⇒ 静默丢行）。仅对全局聚合派生源有效。
     */
    private static void emitDerivedSource(List<FragmentSpec> fragments, String queryId, int idx,
                                          DerivedSource ds, PartitionLayout layout,
                                          List<WorkerRegistration> workers, int numWorkers, int n,
                                          List<String> ownerWorkerIds, String targetExchange,
                                          List<String> targetKeys, long memoryLimit,
                                          List<ScanSlice> innerSlices, boolean replicate) {
        if (ds.isProjection()) {
            // 投影型派生源：扫基表 → [逐行投影派生列] → 按本层 join 键分区 → targetExchange。
            // 发送方数 = 基表切片数（不是 n）—— 调用方据此算 ShuffleReader 的 numSenders。
            List<GroupKeyExpression> gk = new ArrayList<>(ds.projections().size());
            for (DerivedProjection p : ds.projections()) {
                gk.add(new GroupKeyExpression(p.column(), p.exprText()));
            }
            for (int i = 0; i < innerSlices.size(); i++) {
                String w = workers.get(i % numWorkers).info().workerId();
                fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "DIG" + idx, i), w,
                        new FragmentSpec.ScanStep(ds.innerRequest().catalog(), innerSlices.get(i)),
                        new FragmentSpec.PartitionStep(targetExchange, targetKeys, n, ownerWorkerIds), gk));
            }
            return;
        }
        if (ds.isWindow()) {
            // 窗口型派生源：扫基表 → 按窗口 PARTITION BY 分区 → 每分区本地窗口 → 按本层 join 键分区
            // → targetExchange。发送方数 = n（n 个 MiddleWindowConsumer 各对全部桶发一次 complete）。
            DerivedWindow win = ds.window();
            String winExchange = queryId + "-DWG" + idx;
            List<String> winPerPartition = new ArrayList<>(n);
            for (int p = 0; p < n; p++) {
                winPerPartition.add(winExchange + "#" + p);
            }
            FragmentSpec.WindowStep windowStep = new FragmentSpec.WindowStep(
                    win.partitionBy(), win.orderBy(), win.ascending(), win.functions(), memoryLimit, n);
            // ① 每分区一个 MiddleWindowConsumer：own 桶 → 本地窗口 → 按本层 join 键重哈希 → targetExchange
            for (int p = 0; p < n; p++) {
                boolean[] owned = new boolean[n];
                owned[p] = true;
                fragments.add(new FragmentSpec.MiddleWindowConsumer(
                        fragmentId(queryId, "DWG" + idx, p), layout.ownerOf(p),
                        new FragmentSpec.ExchangeInStep(winExchange + "#" + p, n, innerSlices.size(), owned),
                        windowStep,
                        new FragmentSpec.PartitionStep(targetExchange, targetKeys, n, ownerWorkerIds)));
            }
            // ② 派生基表 Producer：扫切片 → 按窗口 PARTITION BY 分区进窗口交换（每分区一个独立交换标识）
            for (int i = 0; i < innerSlices.size(); i++) {
                String wid = workers.get(i % numWorkers).info().workerId();
                fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "DIG" + idx, i), wid,
                        new FragmentSpec.ScanStep(ds.innerRequest().catalog(), innerSlices.get(i)),
                        new FragmentSpec.PartitionStep(winExchange, win.partitionBy(), n, ownerWorkerIds,
                                winPerPartition)));
            }
            return;
        }
        String aggExchange = queryId + "-DAG" + idx;
        // 全局聚合（无分组键 ⇒ 恒 1 行）：空键哈希把所有行送进 0 号桶 ⇒ 只有一个消费者真正有数据，
        // 其余消费者空转（还要各自「派发 + 等 partitionEnd」）。故直接只开<b>一个</b>分区/一个消费者
        // （聚合算子空输入时的产出语义与单机那一个算子逐字一致 —— 不引入第二个聚合算子的分歧）。
        boolean globalAgg = isGlobalAgg(ds);
        int aggParts = globalAgg ? 1 : n;
        List<String> perPartition = new ArrayList<>(aggParts);
        for (int p = 0; p < aggParts; p++) {
            perPartition.add(aggExchange + "#" + p);
        }
        // ① 每分区一个 MiddleAggregateConsumer：own 桶 → [CASE 派生列] → 本地聚合 → (HAVING) →
        //    按本层 join 键重哈希（或全局聚合时 REPLICATE 单值广播）→ 推 targetExchange（不回传 Master）
        List<FragmentSpec.AggregationDto> dtos = ds.aggregations().stream()
                .map(FragmentSpec.AggregationDto::of).toList();
        List<CaseProjection> projs = ds.caseProjections() == null ? List.of() : ds.caseProjections();
        FragmentSpec.AggregateStep aggStep =
                new FragmentSpec.AggregateStep(ds.innerKeys(), dtos, projs, memoryLimit, aggParts);
        // 目标分区：CROSS 的 build 侧（全局聚合单值广播）→ REPLICATE 整份复制到全部 n 个桶；
        // 否则按本层 join 键哈希（1 行的全局聚合也会被哈希进唯一正确的桶 —— 对等值派生 join 成立）。
        FragmentSpec.PartitionStep aggTarget = replicate
                ? FragmentSpec.PartitionStep.broadcast(targetExchange, List.of(), n, ownerWorkerIds,
                        FragmentSpec.Distribution.REPLICATE, 0)
                : new FragmentSpec.PartitionStep(targetExchange, targetKeys, n, ownerWorkerIds);
        for (int p = 0; p < aggParts; p++) {
            boolean[] owned = new boolean[aggParts];
            owned[p] = true;
            fragments.add(new FragmentSpec.MiddleAggregateConsumer(
                    fragmentId(queryId, "DAG" + idx, p), layout.ownerOf(p),
                    new FragmentSpec.ExchangeInStep(aggExchange + "#" + p, aggParts, innerSlices.size(), owned),
                    aggStep, aggTarget, ds.havingFilter()));
        }
        // ② 派生基表 Producer：扫切片 → 按分组键分区进聚合交换（每分区一个独立交换标识）
        for (int i = 0; i < innerSlices.size(); i++) {
            String w = workers.get(i % numWorkers).info().workerId();
            fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "DIG" + idx, i), w,
                    new FragmentSpec.ScanStep(ds.innerRequest().catalog(), innerSlices.get(i)),
                    new FragmentSpec.PartitionStep(aggExchange, ds.innerKeys(), aggParts, ownerWorkerIds,
                            perPartition)));
        }
    }

    /**
     * 规划一个<b>join → 二次 shuffle 聚合</b>的执行舞台（非共分区 GROUP BY）：join 链与
     * {@link #planJoinChain} 完全一致（中间层重哈希 + 基表 Producer），唯一差异在最末层 join ——
     * 它不直接回传 Master，而是作为 {@link FragmentSpec.MiddleJoinConsumer} 把 join 输出按
     * <b>分组键</b>重哈希进一个每分区独立的聚合交换，再由每分区一个 {@link FragmentSpec.AggregateConsumer}
     * 做最终聚合后回传 Master。数据流全程留在 Worker：join 行不跨 Master，Master 只收最终组。
     *
     * <pre>
     *  扫描 T0/T1/... ─► Partition(join 键) ─► [中间 join 层重哈希] ─► 最末层 join
     *                                                          └─► Partition(分组键) ─► 每分区 AggregateConsumer ─► Master
     * </pre>
     *
     * <p><b>正确性</b>：分组键经同一 round-robin owner 映射哈希 → 每个分组落唯一 bucket → 唯一
     * {@code AggregateConsumer} 本地聚合即最终结果（co-partitioning 不变量），Master 零合并。
     * 适用场景：分组键 ≠ join 键 / 未覆盖最末层 join 分区键（组跨分区）—— 链式零二次交换
     * （{@code planChainedJoin} / {@code planJoinChain} + followUp）无法覆盖时由 Planner 调用。</p>
     *
     * <p><b>派发顺序（反死锁）</b>：① 每分区 AggregateConsumer（注册聚合交换读端）→
     * ② 最末层 join 的 MiddleJoinConsumer（注册 {@code exchange#N-L/-R} 读端，输出重哈希推聚合交换）→
     * ③ 中间层 MiddleJoinConsumer 逆序 → ④ 基表 Producers（build 先 probe）。</p>
     *
     * @param queryId          所属查询（exchangeId / fragmentId 前缀，查询间隔离）
     * @param workers          参与 Worker（健康节点，非空）
     * @param numPartitions    哈希分区数（应 ≥ worker 数）
     * @param firstCatalog     首个基表 T0 的 catalog / schema / table
     * @param firstLeftKeys    join#1 的 probe（左）键 —— T0 的 join key 列
     * @param firstProbe       T0 的探针连接器
     * @param firstFilter      T0 的单表 WHERE 下推谓词（已去限定符，可 null）
     * @param links            join#1..#(N-1) 的 {@link JoinLink} 列表（个数 ≥ 1）
     * @param memoryLimit      每个溢写算子的内存预算
     * @param groupKeys        GROUP BY 键（分组键；空 = 全局聚合，哈希落单桶单分区）
     * @param aggregations     聚合函数（AggregationSpec）
     * @param caseProjections  聚合输入的 CASE/IF 派生列投影（AggregateConsumer 头先算再聚合，可空）
     */
    public static JoinChainStage planJoinAggregation(String queryId, List<WorkerRegistration> workers,
                                                     int numPartitions,
                                                     String firstCatalog, String firstSchema, String firstTable,
                                                     List<String> firstLeftKeys, DojaoConnector firstProbe,
                                                     String firstFilter,
                                                     List<JoinLink> links, long memoryLimit,
                                                     List<String> groupKeys, List<AggregationSpec> aggregations,
                                                     List<CaseProjection> caseProjections) {
        return planJoinAggregation(queryId, workers, numPartitions, firstCatalog, firstSchema, firstTable,
                firstLeftKeys, firstProbe, firstFilter, links, memoryLimit,
                groupKeys, aggregations, caseProjections, null);
    }

    /** {@link #planJoinAggregation} 的列裁剪重载（{@code firstColumns} 空 = 首表读全列）。 */
    public static JoinChainStage planJoinAggregation(String queryId, List<WorkerRegistration> workers,
                                                     int numPartitions,
                                                     String firstCatalog, String firstSchema, String firstTable,
                                                     List<String> firstLeftKeys, DojaoConnector firstProbe,
                                                     String firstFilter,
                                                     List<JoinLink> links, long memoryLimit,
                                                     List<String> groupKeys, List<AggregationSpec> aggregations,
                                                     List<CaseProjection> caseProjections,
                                                     List<String> firstColumns) {
        return planJoinAggregation(queryId, workers, numPartitions, firstCatalog, firstSchema, firstTable,
                firstLeftKeys, firstProbe, firstFilter, links, memoryLimit, groupKeys, aggregations,
                caseProjections, firstColumns, null);
    }

    /**
     * 完整形态：{@code expand} 非空 = 多维聚合展开（{@code ROLLUP/CUBE/GROUPING SETS}）——
     * 只挂在<b>最末层</b> {@link FragmentSpec.MiddleJoinConsumer} 上（join 输出之后、按分组键重哈希
     * 之前复制行），于是整条 join 链只跑一次、基表只扫一次。
     */
    public static JoinChainStage planJoinAggregation(String queryId, List<WorkerRegistration> workers,
                                                     int numPartitions,
                                                     String firstCatalog, String firstSchema, String firstTable,
                                                     List<String> firstLeftKeys, DojaoConnector firstProbe,
                                                     String firstFilter,
                                                     List<JoinLink> links, long memoryLimit,
                                                     List<String> groupKeys, List<AggregationSpec> aggregations,
                                                     List<CaseProjection> caseProjections,
                                                     List<String> firstColumns,
                                                     FragmentSpec.ExpandStep expand) {
        PartitionLayout layout = PartitionLayout.roundRobin(workers, numPartitions);
        int n = layout.numPartitions();
        int numWorkers = workers.size();
        int numJoins = links.size();
        List<String> ownerWorkerIds = layout.ownerWorkerIdsList();

        // 基表切片：T0（首个 probe）与 T1..T(N-1)（各 join 的 build 右表），各自带单表 WHERE 下推谓词 + 列裁剪
        List<ScanSlice> firstSlices = planSlices(firstProbe,
                withFilter(readRequest(firstProbe, firstCatalog, firstSchema, firstTable, firstColumns),
                        firstFilter), n);
        List<List<ScanSlice>> rightSlices = new ArrayList<>(numJoins);
        for (int k = 1; k <= numJoins; k++) {
            JoinLink link = links.get(k - 1);
            rightSlices.add(planSlices(link.rightProbe(),
                    withFilter(readRequest(link.rightProbe(), link.rightCatalog(), link.rightSchema(),
                            link.rightTable(), link.rightColumns()), link.rightFilter()), n));
        }

        // ★全局聚合（无分组键）压缩到单分区：空键的行哈希恒为 0，只会有 0 号桶收到数据。
        //  若仍按 n 个分区派发，1..n-1 号 AggregateConsumer 收到 0 行 → emitEmptyInput 各补一行
        //  (count=0/sum=NULL)，AbstractDistributedOperator 原样转发 ⇒ 最终结果多出 n-1 行假数据；
        //  同时全部 join 行挤进单个桶/单台 Worker，其余消费者空转（与 planAggregation 同口径）。
        int aggParts = groupKeys.isEmpty() ? 1 : n;

        // 聚合交换：每分区独立交换标识（发送端按桶路由到对应分区的独立接收端，镜像 planAggregation）
        String aggExchange = queryId + "-JAGG";
        List<String> aggPerPartitionExchanges = new ArrayList<>(aggParts);
        for (int p = 0; p < aggParts; p++) {
            aggPerPartitionExchanges.add(aggExchange + "#" + p);
        }
        List<FragmentSpec.AggregationDto> dtos = aggregations.stream()
                .map(FragmentSpec.AggregationDto::of).toList();
        List<CaseProjection> projs = caseProjections == null ? List.of() : caseProjections;

        List<FragmentSpec> fragments = new ArrayList<>();

        // 1. 每分区一个 AggregateConsumer（先派发，注册聚合交换读端）→ 回传 Master（terminal consumer）。
        //    发送方 = numWorkers 个最末层 MiddleJoinConsumer（每个对全部桶发 complete）。
        FragmentSpec.AggregateStep aggStep = new FragmentSpec.AggregateStep(
                groupKeys, dtos, projs, memoryLimit, aggParts);
        for (int p = 0; p < aggParts; p++) {
            String workerId = layout.ownerOf(p);
            boolean[] owned = new boolean[aggParts];
            owned[p] = true;
            fragments.add(new FragmentSpec.AggregateConsumer(fragmentId(queryId, "JAG", p), workerId,
                    new FragmentSpec.ExchangeInStep(aggExchange + "#" + p, aggParts, numWorkers, owned),
                    aggStep));
        }

        // 2. 最末层 join（join#N）→ MiddleJoinConsumer：join 输出按分组键重哈希 → 推聚合交换（不回传 Master）。
        JoinLink last = links.get(numJoins - 1);
        String lastLeftExchange = queryId + "-J" + numJoins + "-L";
        String lastRightExchange = queryId + "-J" + numJoins + "-R";
        List<ScanSlice> lastRightSlices = rightSlices.get(numJoins - 1);
        // 单 join：左交换发送方 = firstSlices 个 T0 Producer；多 join：发送方 = numWorkers 个中间层
        int lastLeftSenders = (numJoins == 1) ? firstSlices.size() : numWorkers;
        for (int w = 0; w < numWorkers; w++) {
            String workerId = workers.get(w).info().workerId();
            FragmentSpec.ExchangeInStep leftIn = new FragmentSpec.ExchangeInStep(
                    lastLeftExchange, n, lastLeftSenders, layout.ownedMask(workerId));
            FragmentSpec.ExchangeInStep rightIn = new FragmentSpec.ExchangeInStep(
                    lastRightExchange, n, lastRightSlices.size(), layout.ownedMask(workerId));
            FragmentSpec.JoinStep joinStep = new FragmentSpec.JoinStep(
                    last.leftKeys(), last.rightKeys(), last.joinType(), memoryLimit, n, last.algorithm(),
                    rightFieldsOf(last));
            FragmentSpec.PartitionStep nextPartition = new FragmentSpec.PartitionStep(
                    aggExchange, groupKeys, aggParts, ownerWorkerIds, aggPerPartitionExchanges);
            // ★多维聚合展开只挂最末层：join 链 / 基表扫描都只做一次，展开后按 (维度…, gid) 重哈希
            FragmentSpec.MiddleJoinConsumer lastJoin = new FragmentSpec.MiddleJoinConsumer(
                    fragmentId(queryId, "JMG", numJoins, w), workerId,
                    leftIn, rightIn, joinStep, nextPartition);
            fragments.add(expand == null ? lastJoin : lastJoin.withExpand(expand));
        }

        // 3. 中间层 MiddleJoinConsumer（join#(N-1) → join#1 逆序）：join#k 输出按下一层左键重哈希推走
        for (int k = numJoins - 1; k >= 1; k--) {
            JoinLink link = links.get(k - 1);
            JoinLink next = links.get(k);
            String leftExchange = queryId + "-J" + k + "-L";
            String rightExchange = queryId + "-J" + k + "-R";
            String nextLeftExchange = queryId + "-J" + (k + 1) + "-L";
            int leftSenders = (k == 1) ? firstSlices.size() : numWorkers;
            int rightSenders = rightSlices.get(k - 1).size();
            for (int w = 0; w < numWorkers; w++) {
                String workerId = workers.get(w).info().workerId();
                FragmentSpec.ExchangeInStep leftIn = new FragmentSpec.ExchangeInStep(
                        leftExchange, n, leftSenders, layout.ownedMask(workerId));
                FragmentSpec.ExchangeInStep rightIn = new FragmentSpec.ExchangeInStep(
                        rightExchange, n, rightSenders, layout.ownedMask(workerId));
                FragmentSpec.JoinStep joinStep = new FragmentSpec.JoinStep(
                        link.leftKeys(), link.rightKeys(), link.joinType(), memoryLimit, n, link.algorithm(),
                        rightFieldsOf(link));
                FragmentSpec.PartitionStep nextPartition = new FragmentSpec.PartitionStep(
                        nextLeftExchange, next.leftKeys(), n, ownerWorkerIds);
                fragments.add(new FragmentSpec.MiddleJoinConsumer(
                        fragmentId(queryId, "MJ", k, w), workerId, leftIn, rightIn, joinStep, nextPartition));
            }
        }

        // 4. 基表 Producers：build(右) 先于 probe(左) —— T1, T0, T2, ..., T(N-1)
        JoinLink first = links.get(0);
        String firstRightExchange = queryId + "-J1-R";
        List<ScanSlice> firstRightSlices = rightSlices.get(0);
        for (int i = 0; i < firstRightSlices.size(); i++) {
            String w = workers.get(i % numWorkers).info().workerId();
            fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "PR", 1, i), w,
                    new FragmentSpec.ScanStep(first.rightCatalog(), firstRightSlices.get(i)),
                    new FragmentSpec.PartitionStep(firstRightExchange, first.rightKeys(), n, ownerWorkerIds)));
        }
        String firstLeftExchange = queryId + "-J1-L";
        for (int i = 0; i < firstSlices.size(); i++) {
            String w = workers.get(i % numWorkers).info().workerId();
            fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "PL", i), w,
                    new FragmentSpec.ScanStep(firstCatalog, firstSlices.get(i)),
                    new FragmentSpec.PartitionStep(firstLeftExchange, firstLeftKeys, n, ownerWorkerIds)));
        }
        for (int k = 2; k <= numJoins; k++) {
            JoinLink link = links.get(k - 1);
            List<ScanSlice> slices = rightSlices.get(k - 1);
            String rightExchange = queryId + "-J" + k + "-R";
            for (int i = 0; i < slices.size(); i++) {
                String w = workers.get(i % numWorkers).info().workerId();
                fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "PR", k, i), w,
                        new FragmentSpec.ScanStep(link.rightCatalog(), slices.get(i)),
                        new FragmentSpec.PartitionStep(rightExchange, link.rightKeys(), n, ownerWorkerIds)));
            }
        }

        // 回传 Master 的 terminal consumer 数 = aggParts（全局聚合压缩后只有 1 个 AggregateConsumer）
        return new JoinChainStage(fragments, aggParts, layout.ownerWorkerIds());
    }

    /**
     * 规划一个<b>join → 二次 shuffle 聚合 → 窗口</b>的执行舞台（聚合 + 窗口两级<b>全下沉 Worker</b>）：
     * 聚合段与 {@link #planJoinAggregation} 完全一致（join 链中间层重哈希 + 基表 Producer + 最末层
     * {@code MiddleJoinConsumer} 按<b>分组键</b>重哈希进每分区聚合交换），唯一差异在聚合终点 —— 不再
     * 回传 Master：每分区一个<b>非终端</b> {@code MiddleAggregateConsumer} 本地聚合后按<b>窗口
     * PARTITION BY</b>再重哈希推一个窗口交换，由每 Worker 一个终端 {@code WindowConsumer} 收齐 →
     * 本地窗口排序 + 逐行计算 → 回传 Master（或带 sink 就地写走，数据不回 Master）。
     *
     * <pre>
     * 扫描 T0/T1/... ─► Partition(join 键) ─► [中间 join 层重哈希] ─► 最末层 join
     *     └─► Partition(分组键) ─► MiddleAggregateConsumer(聚合) ─► Partition(窗口键) ─► WindowConsumer ─► Master/sink
     * </pre>
     *
     * <p><b>正确性</b>：分组键与窗口键各自经同一 round-robin owner 映射哈希 → 每个分组落唯一桶做完整
     * 聚合、每个窗口键落唯一桶做本地排序即该键完整窗口（co-partitioning 不变量），Master 零合并。
     * 全局窗口（空 PARTITION BY）：空键哈希恒 0 → 全落 pid 0 → worker 0 单分区全局排序（已知边界，
     * 全局排序本质不可并行，与既有 {@code planWindow} 语义一致）。</p>
     *
     * <p><b>派发顺序（反死锁）</b>：① 每 Worker WindowConsumer（注册窗口交换读端）→ ② 每分区
     * MiddleAggregateConsumer（注册聚合交换读端，输出重哈希推窗口交换）→ ③ 最末层 join 的
     * MiddleJoinConsumer（注册 join 交换读端）→ ④ 中间层逆序 → ⑤ 基表 Producers（build 先 probe）。</p>
     */
    public static JoinChainStage planJoinAggregateWindow(String queryId, List<WorkerRegistration> workers,
                                                         int numPartitions,
                                                         String firstCatalog, String firstSchema, String firstTable,
                                                         List<String> firstLeftKeys, DojaoConnector firstProbe,
                                                         String firstFilter,
                                                         List<JoinLink> links, long memoryLimit,
                                                         List<String> groupKeys, List<AggregationSpec> aggregations,
                                                         List<CaseProjection> caseProjections,
                                                         List<String> windowPartitionBy, List<String> windowOrderBy,
                                                         List<Boolean> windowAscending,
                                                         List<WindowFunctionSpec> windowFunctions) {
        return planJoinAggregateWindow(queryId, workers, numPartitions, firstCatalog, firstSchema, firstTable,
                firstLeftKeys, firstProbe, firstFilter, links, memoryLimit, groupKeys, aggregations,
                caseProjections, windowPartitionBy, windowOrderBy, windowAscending, windowFunctions, null);
    }

    /** {@link #planJoinAggregateWindow} 的列裁剪重载（{@code firstColumns} 空 = 首表读全列）。 */
    public static JoinChainStage planJoinAggregateWindow(String queryId, List<WorkerRegistration> workers,
                                                         int numPartitions,
                                                         String firstCatalog, String firstSchema, String firstTable,
                                                         List<String> firstLeftKeys, DojaoConnector firstProbe,
                                                         String firstFilter,
                                                         List<JoinLink> links, long memoryLimit,
                                                         List<String> groupKeys, List<AggregationSpec> aggregations,
                                                         List<CaseProjection> caseProjections,
                                                         List<String> windowPartitionBy, List<String> windowOrderBy,
                                                         List<Boolean> windowAscending,
                                                         List<WindowFunctionSpec> windowFunctions,
                                                         List<String> firstColumns) {
        PartitionLayout layout = PartitionLayout.roundRobin(workers, numPartitions);
        int n = layout.numPartitions();
        int numWorkers = workers.size();
        int numJoins = links.size();
        List<String> ownerWorkerIds = layout.ownerWorkerIdsList();

        // 基表切片（同 planJoinAggregation）：T0（首个 probe）与 T1..T(N-1)（各 join 的 build 右表）
        List<ScanSlice> firstSlices = planSlices(firstProbe,
                withFilter(readRequest(firstProbe, firstCatalog, firstSchema, firstTable, firstColumns),
                        firstFilter), n);
        List<List<ScanSlice>> rightSlices = new ArrayList<>(numJoins);
        for (int k = 1; k <= numJoins; k++) {
            JoinLink link = links.get(k - 1);
            rightSlices.add(planSlices(link.rightProbe(),
                    withFilter(readRequest(link.rightProbe(), link.rightCatalog(), link.rightSchema(),
                            link.rightTable(), link.rightColumns()), link.rightFilter()), n));
        }

        // ★全局聚合（无分组键）压缩到单分区（同 planJoinAggregation）：空键哈希恒 0，
        //  否则其余聚合消费者各补一行 count=0/sum=NULL 假数据流入窗口段。
        int aggParts = groupKeys.isEmpty() ? 1 : n;

        // 聚合交换（每分区独立）+ 窗口交换（单交换）
        String aggExchange = queryId + "-JAGG";
        List<String> aggPerPartitionExchanges = new ArrayList<>(aggParts);
        for (int p = 0; p < aggParts; p++) {
            aggPerPartitionExchanges.add(aggExchange + "#" + p);
        }
        String windowExchange = queryId + "-AW";
        List<FragmentSpec.AggregationDto> dtos = aggregations.stream()
                .map(FragmentSpec.AggregationDto::of).toList();
        List<CaseProjection> projs = caseProjections == null ? List.of() : caseProjections;

        List<FragmentSpec> fragments = new ArrayList<>();

        // 1. 每 Worker 一个终端 WindowConsumer（先派发，注册窗口交换读端）→ 回传 Master / 就地写。
        //    窗口交换发送方 = aggParts 个 MiddleAggregateConsumer（各对全部桶发 complete）。
        FragmentSpec.WindowStep windowStep = new FragmentSpec.WindowStep(
                windowPartitionBy, windowOrderBy, windowAscending, windowFunctions, memoryLimit, n);
        for (int w = 0; w < numWorkers; w++) {
            String workerId = workers.get(w).info().workerId();
            fragments.add(new FragmentSpec.WindowConsumer(fragmentId(queryId, "AWC", w), workerId,
                    new FragmentSpec.ExchangeInStep(windowExchange, n, aggParts, layout.ownedMask(workerId)),
                    windowStep));
        }

        // 2. 每分区一个 MiddleAggregateConsumer（派发 ownerOf(p)，注册聚合交换读端）：own 桶 → 本地
        //    聚合 → 按窗口键重哈希推窗口交换（不回传 Master）。聚合交换发送方 = numWorkers 个最末层 join。
        FragmentSpec.AggregateStep aggStep = new FragmentSpec.AggregateStep(
                groupKeys, dtos, projs, memoryLimit, aggParts);
        for (int p = 0; p < aggParts; p++) {
            String workerId = layout.ownerOf(p);
            boolean[] owned = new boolean[aggParts];
            owned[p] = true;
            fragments.add(new FragmentSpec.MiddleAggregateConsumer(fragmentId(queryId, "MAW", p), workerId,
                    new FragmentSpec.ExchangeInStep(aggExchange + "#" + p, aggParts, numWorkers, owned),
                    aggStep,
                    new FragmentSpec.PartitionStep(windowExchange, windowPartitionBy, n, ownerWorkerIds),
                    null));
        }

        // 3. 最末层 join（join#N）→ MiddleJoinConsumer：join 输出按分组键重哈希 → 推聚合交换（不回传 Master）
        JoinLink last = links.get(numJoins - 1);
        String lastLeftExchange = queryId + "-J" + numJoins + "-L";
        String lastRightExchange = queryId + "-J" + numJoins + "-R";
        List<ScanSlice> lastRightSlices = rightSlices.get(numJoins - 1);
        int lastLeftSenders = (numJoins == 1) ? firstSlices.size() : numWorkers;
        for (int w = 0; w < numWorkers; w++) {
            String workerId = workers.get(w).info().workerId();
            FragmentSpec.ExchangeInStep leftIn = new FragmentSpec.ExchangeInStep(
                    lastLeftExchange, n, lastLeftSenders, layout.ownedMask(workerId));
            FragmentSpec.ExchangeInStep rightIn = new FragmentSpec.ExchangeInStep(
                    lastRightExchange, n, lastRightSlices.size(), layout.ownedMask(workerId));
            FragmentSpec.JoinStep joinStep = new FragmentSpec.JoinStep(
                    last.leftKeys(), last.rightKeys(), last.joinType(), memoryLimit, n, last.algorithm(),
                    rightFieldsOf(last));
            FragmentSpec.PartitionStep nextPartition = new FragmentSpec.PartitionStep(
                    aggExchange, groupKeys, aggParts, ownerWorkerIds, aggPerPartitionExchanges);
            fragments.add(new FragmentSpec.MiddleJoinConsumer(fragmentId(queryId, "JMG", numJoins, w), workerId,
                    leftIn, rightIn, joinStep, nextPartition));
        }

        // 4. 中间层 MiddleJoinConsumer（join#(N-1) → join#1 逆序）：join#k 输出按下一层左键重哈希推走
        for (int k = numJoins - 1; k >= 1; k--) {
            JoinLink link = links.get(k - 1);
            JoinLink next = links.get(k);
            String leftExchange = queryId + "-J" + k + "-L";
            String rightExchange = queryId + "-J" + k + "-R";
            String nextLeftExchange = queryId + "-J" + (k + 1) + "-L";
            int leftSenders = (k == 1) ? firstSlices.size() : numWorkers;
            int rightSenders = rightSlices.get(k - 1).size();
            for (int w = 0; w < numWorkers; w++) {
                String workerId = workers.get(w).info().workerId();
                FragmentSpec.ExchangeInStep leftIn = new FragmentSpec.ExchangeInStep(
                        leftExchange, n, leftSenders, layout.ownedMask(workerId));
                FragmentSpec.ExchangeInStep rightIn = new FragmentSpec.ExchangeInStep(
                        rightExchange, n, rightSenders, layout.ownedMask(workerId));
                FragmentSpec.JoinStep joinStep = new FragmentSpec.JoinStep(
                        link.leftKeys(), link.rightKeys(), link.joinType(), memoryLimit, n, link.algorithm(),
                        rightFieldsOf(link));
                FragmentSpec.PartitionStep nextPartition = new FragmentSpec.PartitionStep(
                        nextLeftExchange, next.leftKeys(), n, ownerWorkerIds);
                fragments.add(new FragmentSpec.MiddleJoinConsumer(
                        fragmentId(queryId, "MJ", k, w), workerId, leftIn, rightIn, joinStep, nextPartition));
            }
        }

        // 5. 基表 Producers：build(右) 先于 probe(左) —— T1, T0, T2, ..., T(N-1)
        JoinLink first = links.get(0);
        String firstRightExchange = queryId + "-J1-R";
        List<ScanSlice> firstRightSlices = rightSlices.get(0);
        for (int i = 0; i < firstRightSlices.size(); i++) {
            String w = workers.get(i % numWorkers).info().workerId();
            fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "PR", 1, i), w,
                    new FragmentSpec.ScanStep(first.rightCatalog(), firstRightSlices.get(i)),
                    new FragmentSpec.PartitionStep(firstRightExchange, first.rightKeys(), n, ownerWorkerIds)));
        }
        String firstLeftExchange = queryId + "-J1-L";
        for (int i = 0; i < firstSlices.size(); i++) {
            String w = workers.get(i % numWorkers).info().workerId();
            fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "PL", i), w,
                    new FragmentSpec.ScanStep(firstCatalog, firstSlices.get(i)),
                    new FragmentSpec.PartitionStep(firstLeftExchange, firstLeftKeys, n, ownerWorkerIds)));
        }
        for (int k = 2; k <= numJoins; k++) {
            JoinLink link = links.get(k - 1);
            List<ScanSlice> slices = rightSlices.get(k - 1);
            String rightExchange = queryId + "-J" + k + "-R";
            for (int i = 0; i < slices.size(); i++) {
                String w = workers.get(i % numWorkers).info().workerId();
                fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "PR", k, i), w,
                        new FragmentSpec.ScanStep(link.rightCatalog(), slices.get(i)),
                        new FragmentSpec.PartitionStep(rightExchange, link.rightKeys(), n, ownerWorkerIds)));
            }
        }

        // 回传 Master 的 terminal consumer 数 = numWorkers（每 Worker 一个 WindowConsumer）
        return new JoinChainStage(fragments, numWorkers, layout.ownerWorkerIds());
    }

    /**
     * 规划一个 GROUP BY 聚合的分布式执行舞台（单交换）：扫表 → 按聚合 key 哈希分区 →
     * 归属 Worker 本地聚合 → 回传 Master。owner 映射与同查询其他 shuffle 完全一致。
     *
     * <p><b>每分区一个消费者</b>（消费端并行度 = 分区数，不与 Worker 数绑定）：分区 pid 的
     * AggregateConsumer 派发到 {@code layout.ownerOf(pid)}、只收自己这一个桶（单 bit 归属掩码，
     * 其余桶构造即结束、零内存），每分区独立交换标识（{@code exchange + "#" + pid}）。这样
     * 分区数据量自始至终贯穿同一并行度 —— 上游开几个分区，聚合评估就有几个并行消费者
     * （collect-evaluate 型每分区独立调 sidecar，Python 进程数跟分区数据走，而非跟 Worker 数走）。</p>
     *
     * <p><b>{@code scanParallelism} 与 {@code numPartitions} 刻意分开</b>：切片数是<b>扫描</b>的
     * 并行度，桶数是<b>聚合</b>的并行度，两者不必相等。全局聚合（无分组键）的行哈希恒为 0 ⇒
     * 只会有 0 号桶收到数据，此时把 {@code numPartitions} 压到 1（见 Planner）能免掉 n-1 个
     * 收不到任何数据、却仍要逐个派发并等 {@code partitionEnd} 的空消费者（实测 8 消费者 / 3 Worker
     * 下这段空转尾巴约 6.7s）；但扫描仍应按真实并行度切片 —— 若跟着压到 1，切片数也退化成 1，
     * 读又变回单线程串行。</p>
     */
    public static DistributedStage planAggregation(String queryId, List<WorkerRegistration> workers,
                                                   int numPartitions, int scanParallelism,
                                                   ReadRequest baseRequest,
                                                   List<String> keyColumns, List<AggregationSpec> aggregations,
                                                   List<CaseProjection> caseProjections,
                                                   List<GroupKeyExpression> groupKeyExpressions,
                                                   long memoryLimit, DojaoConnector probe) {
        return planAggregation(queryId, workers, numPartitions, scanParallelism, baseRequest, keyColumns,
                aggregations, caseProjections, groupKeyExpressions, memoryLimit, probe, null, null);
    }

    /**
     * 带多维聚合展开的形态：{@code expand} 非空时每个 Producer 在扫描（与 GROUP BY 派生列）之后、
     * 按分组键哈希分区之前插一层 {@code ExpandOperator} —— 表只扫一次，各维度组合由放大后的行
     * 在聚合端分开（{@code __grouping_id} 是分组键的一部分）。
     */
    public static DistributedStage planAggregation(String queryId, List<WorkerRegistration> workers,
                                                   int numPartitions, int scanParallelism,
                                                   ReadRequest baseRequest,
                                                   List<String> keyColumns, List<AggregationSpec> aggregations,
                                                   List<CaseProjection> caseProjections,
                                                   List<GroupKeyExpression> groupKeyExpressions,
                                                   long memoryLimit, DojaoConnector probe,
                                                   FragmentSpec.ExpandStep expand) {
        return planAggregation(queryId, workers, numPartitions, scanParallelism, baseRequest, keyColumns,
                aggregations, caseProjections, groupKeyExpressions, memoryLimit, probe, null, expand);
    }

    /** 带二级聚合（外层全局聚合下沉）的完整形态；{@code followUp == null} 等价于不带二级聚合。 */
    public static DistributedStage planAggregation(String queryId, List<WorkerRegistration> workers,
                                                   int numPartitions, int scanParallelism,
                                                   ReadRequest baseRequest,
                                                   List<String> keyColumns, List<AggregationSpec> aggregations,
                                                   List<CaseProjection> caseProjections,
                                                   List<GroupKeyExpression> groupKeyExpressions,
                                                   long memoryLimit, DojaoConnector probe,
                                                   FragmentSpec.AggregateStep followUp) {
        return planAggregation(queryId, workers, numPartitions, scanParallelism, baseRequest, keyColumns,
                aggregations, caseProjections, groupKeyExpressions, memoryLimit, probe, followUp, null);
    }

    /** 完整形态：二级聚合 + 多维聚合展开。 */
    public static DistributedStage planAggregation(String queryId, List<WorkerRegistration> workers,
                                                   int numPartitions, int scanParallelism,
                                                   ReadRequest baseRequest,
                                                   List<String> keyColumns, List<AggregationSpec> aggregations,
                                                   List<CaseProjection> caseProjections,
                                                   List<GroupKeyExpression> groupKeyExpressions,
                                                   long memoryLimit, DojaoConnector probe,
                                                   FragmentSpec.AggregateStep followUp,
                                                   FragmentSpec.ExpandStep expand) {
        PartitionLayout layout = PartitionLayout.roundRobin(workers, numPartitions);
        int n = layout.numPartitions();
        String exchange = queryId + "-AGG";
        // 每分区独立交换标识：发送端按桶路由到对应分区的独立接收端（consumer 与 partition 一一对应）
        List<String> perPartitionExchanges = new ArrayList<>(n);
        for (int p = 0; p < n; p++) {
            perPartitionExchanges.add(exchange + "#" + p);
        }

        // 切片按「扫描并行度」规划，不按桶数：桶数可能被压到 1（全局聚合），但数据仍要多路并行读。
        // 消费者侧的 numSenders 取 slices.size()，与此处必须一致。
        List<ScanSlice> slices = planSlices(probe, baseRequest, scanParallelism);
        List<FragmentSpec> producers = new ArrayList<>();
        // 生产者落点：桶数 > 1 时生产者写「全部桶」，落哪台都有远程份，按 Worker 轮转铺开读并行度即可；
        // 桶数 = 1（全局聚合）时全部行只进 0 号桶，生产者落「非归属 Worker」= 整片数据跨进程走 Flight
        // （发送侧逐格 transferTo + 接收侧 copyPage，实测比同机 LocalExchangeChannel 零拷贝慢约 5×）
        // —— 故与唯一消费者同机，读并行度照样按切片数保留（同机多片段 = 多线程读）。
        String soleOwner = n == 1 ? layout.ownerOf(0) : null;
        for (int i = 0; i < slices.size(); i++) {
            String w = soleOwner != null ? soleOwner : workers.get(i % workers.size()).info().workerId();
            FragmentSpec.Producer producer = new FragmentSpec.Producer(fragmentId(queryId, "PA", i), w,
                    new FragmentSpec.ScanStep(baseRequest.catalog(), slices.get(i)),
                    new FragmentSpec.PartitionStep(exchange, keyColumns, n,
                            layout.ownerWorkerIdsList(), perPartitionExchanges),
                    groupKeyExpressions);
            producers.add(expand == null ? producer : producer.withExpand(expand));
        }

        List<FragmentSpec> consumers = new ArrayList<>();
        for (int p = 0; p < n; p++) {
            String workerId = layout.ownerOf(p);
            // 单分区归属掩码：该消费者只收自己这个分区（其余桶立即结束、零内存）
            boolean[] owned = new boolean[n];
            owned[p] = true;
            List<FragmentSpec.AggregationDto> dtos = aggregations.stream()
                    .map(FragmentSpec.AggregationDto::of).toList();
            FragmentSpec.AggregateConsumer consumer = new FragmentSpec.AggregateConsumer(
                    fragmentId(queryId, "CA", p), workerId,
                    new FragmentSpec.ExchangeInStep(exchange + "#" + p, n, slices.size(), owned),
                    new FragmentSpec.AggregateStep(keyColumns, dtos, caseProjections, memoryLimit, n));
            // 二级聚合（外层全局聚合下沉）：紧随本地聚合输出执行，每分区只回传 1 行 partial。
            // ★numPartitions 必须与本地聚合一致：二级聚合按上游页的 pid 分槽，槽数组小了越界。
            if (followUp != null) {
                consumer = consumer.withFollowUp(followUp.numPartitions() == n ? followUp
                        : new FragmentSpec.AggregateStep(followUp.keyColumns(), followUp.aggregations(),
                        followUp.caseProjections(), followUp.memoryLimit(), n));
            }
            consumers.add(consumer);
        }
        return new DistributedStage(layout.ownerWorkerIds(), consumers, producers);
    }

    /**
     * 规划一个<b>裸表聚合 → 窗口</b>的执行舞台（聚合 + 窗口两级<b>全下沉 Worker</b>）：聚合段与
     * {@link #planAggregation} 一致（扫表 → 按分组键哈希分区 → 每分区独立聚合交换），唯一差异在
     * 聚合终点 —— 不再回传 Master：每分区一个<b>非终端</b> {@code MiddleAggregateConsumer} 本地
     * 聚合后按<b>窗口 PARTITION BY</b>再重哈希推一个窗口交换，由每 Worker 一个终端
     * {@code WindowConsumer} 收齐 → 本地窗口排序 + 逐行计算 → 回传 Master（或带 sink 就地写走）。
     *
     * <pre>
     * 扫表(WHERE) ─► Partition(分组键) ─► MiddleAggregateConsumer(聚合) ─► Partition(窗口键) ─► WindowConsumer ─► Master/sink
     * </pre>
     *
     * <p><b>正确性</b>：分组键与窗口键各自经同一 round-robin owner 映射哈希 → 每个分组落唯一桶做完整
     * 聚合、每个窗口键落唯一桶做本地排序即该键完整窗口（co-partitioning 不变量），Master 零合并。
     * 全局窗口（空 PARTITION BY）：空键哈希恒 0 → 全落 pid 0 → worker 0 单分区全局排序（已知边界）。</p>
     *
     * <p><b>派发顺序（反死锁）</b>：① 每 Worker WindowConsumer（注册窗口交换读端）→ ② 每分区
     * MiddleAggregateConsumer（注册聚合交换读端，输出重哈希推窗口交换）→ ③ 基表 Producers。</p>
     */
    public static JoinChainStage planAggregateWindow(String queryId, List<WorkerRegistration> workers,
                                                     int numPartitions, ReadRequest baseRequest,
                                                     List<String> keyColumns, List<AggregationSpec> aggregations,
                                                     List<CaseProjection> caseProjections, long memoryLimit,
                                                     DojaoConnector probe,
                                                     List<String> windowPartitionBy, List<String> windowOrderBy,
                                                     List<Boolean> windowAscending,
                                                     List<WindowFunctionSpec> windowFunctions) {
        PartitionLayout layout = PartitionLayout.roundRobin(workers, numPartitions);
        int n = layout.numPartitions();
        int numWorkers = workers.size();
        List<String> ownerWorkerIds = layout.ownerWorkerIdsList();
        String aggExchange = queryId + "-AGG";
        // 每分区独立聚合交换（镜像 planAggregation）：发送端按桶路由到对应分区的独立接收端
        List<String> perPartitionExchanges = new ArrayList<>(n);
        for (int p = 0; p < n; p++) {
            perPartitionExchanges.add(aggExchange + "#" + p);
        }
        String windowExchange = queryId + "-AW";
        List<FragmentSpec.AggregationDto> dtos = aggregations.stream()
                .map(FragmentSpec.AggregationDto::of).toList();
        List<CaseProjection> projs = caseProjections == null ? List.of() : caseProjections;

        List<ScanSlice> slices = planSlices(probe, baseRequest, n);
        List<FragmentSpec> fragments = new ArrayList<>();

        // 1. 每 Worker 一个终端 WindowConsumer（先派发，注册窗口交换读端）→ 回传 Master / 就地写。
        //    窗口交换发送方 = n 个 MiddleAggregateConsumer（各对全部桶发 complete）→ numSenders = n。
        FragmentSpec.WindowStep windowStep = new FragmentSpec.WindowStep(
                windowPartitionBy, windowOrderBy, windowAscending, windowFunctions, memoryLimit, n);
        for (int w = 0; w < numWorkers; w++) {
            String workerId = workers.get(w).info().workerId();
            fragments.add(new FragmentSpec.WindowConsumer(fragmentId(queryId, "AWC", w), workerId,
                    new FragmentSpec.ExchangeInStep(windowExchange, n, n, layout.ownedMask(workerId)),
                    windowStep));
        }

        // 2. 每分区一个 MiddleAggregateConsumer（派发 ownerOf(p)）：own 桶 → 本地聚合 →
        //    按窗口键重哈希推窗口交换（不回传 Master）。单桶归属掩码 + 独立交换标识（镜像 planAggregation）。
        FragmentSpec.AggregateStep aggStep = new FragmentSpec.AggregateStep(keyColumns, dtos, projs, memoryLimit, n);
        for (int p = 0; p < n; p++) {
            String workerId = layout.ownerOf(p);
            boolean[] owned = new boolean[n];
            owned[p] = true;
            fragments.add(new FragmentSpec.MiddleAggregateConsumer(fragmentId(queryId, "MAW", p), workerId,
                    new FragmentSpec.ExchangeInStep(aggExchange + "#" + p, n, slices.size(), owned),
                    aggStep,
                    new FragmentSpec.PartitionStep(windowExchange, windowPartitionBy, n, ownerWorkerIds),
                    null));
        }

        // 3. 基表 Producers（后派发）：扫切片 → 按分组键哈希分区 → 推聚合交换（路由到每分区独立接收端）
        for (int i = 0; i < slices.size(); i++) {
            String w = workers.get(i % numWorkers).info().workerId();
            fragments.add(new FragmentSpec.Producer(fragmentId(queryId, "PA", i), w,
                    new FragmentSpec.ScanStep(baseRequest.catalog(), slices.get(i)),
                    new FragmentSpec.PartitionStep(aggExchange, keyColumns, n, ownerWorkerIds,
                            perPartitionExchanges)));
        }

        // 回传 Master 的 terminal consumer 数 = numWorkers（每 Worker 一个 WindowConsumer）
        return new JoinChainStage(fragments, numWorkers, layout.ownerWorkerIds());
    }

    /**
     * 规划一个 <b>UNION distinct 的去重舞台</b>（Master 侧全列去重的分布式化，消除最后一处
     * Master 单机聚合）：<b>无 Producer 片段</b> —— 发送方恒为 Master。Master 端 UnionAllOperator
     * 把两腿对齐成统一规范列名后，由 DistributedUnionDistinctOperator 按全部输出列哈希分区，经
     * Master→Worker 数据直推把每个桶路由到对应分区的独立接收端；接收端 = 每分区一个
     * {@link FragmentSpec.AggregateConsumer}（空聚合 = 全列 DISTINCT，每组一行），本地去重后回传 Master。
     *
     * <pre>
     *  两腿扫描 ─► Master UnionAll（列名对齐, pid=0）─► 按全列哈希分区 ─(Master 直推)─►
     *     每分区 AggregateConsumer（空聚合 = DISTINCT）─► Master
     * </pre>
     *
     * <p><b>正确性</b>：同一元组按全列哈希恒落唯一桶 → 唯一 AggregateConsumer 本地去重即该元组
     * 的完整去重，各分区去重结果并集 = 全局 distinct（co-partitioning 不变量）。Master 不持有
     * 去重态，只在数据路径上做无状态路由转发（有界在途批缓冲）。</p>
     *
     * <p><b>每分区一个消费者</b>（镜像 {@link #planAggregation}）：分区 p 的数据经交换
     * {@code exchange + "#" + p} 路由到 {@code ownerOf(p)}，只收自己这一个桶（单 bit 归属掩码）；
     * {@code numSenders = 1} = Master 泵是唯一发送方（收齐它一个 complete 即 partitionEnd）。</p>
     *
     * @param queryId       所属查询（exchangeId / fragmentId 前缀，查询间隔离）
     * @param workers       参与 Worker（健康节点，非空）
     * @param numPartitions 哈希分区数（应 ≥ worker 数）
     * @param keyColumns    全部输出列（UNION distinct 的逐行去重键 = 规范 union 输出列）
     * @param memoryLimit   每个 AggregateConsumer 的溢写内存预算
     */
    public static UnionDistinctStage planUnionDistinct(String queryId, List<WorkerRegistration> workers,
                                                       int numPartitions, List<String> keyColumns,
                                                       long memoryLimit) {
        PartitionLayout layout = PartitionLayout.roundRobin(workers, numPartitions);
        int n = layout.numPartitions();
        String exchange = queryId + "-UD";
        // 每分区独立交换标识（镜像 planAggregation）：Master 泵按桶路由到对应分区的独立接收端
        List<String> perPartitionExchanges = new ArrayList<>(n);
        for (int p = 0; p < n; p++) {
            perPartitionExchanges.add(exchange + "#" + p);
        }

        List<FragmentSpec> fragments = new ArrayList<>();
        // 空聚合 = DISTINCT：以全部输出列作分组键、聚合列为空，即"每组一行"（NULL 视为同一组）
        FragmentSpec.AggregateStep aggStep =
                new FragmentSpec.AggregateStep(keyColumns, List.of(), memoryLimit, n);
        for (int p = 0; p < n; p++) {
            String workerId = layout.ownerOf(p);
            boolean[] owned = new boolean[n];
            owned[p] = true;
            fragments.add(new FragmentSpec.AggregateConsumer(fragmentId(queryId, "UD", p), workerId,
                    new FragmentSpec.ExchangeInStep(exchange + "#" + p, n, /*numSenders=*/1, owned),
                    aggStep));
        }
        return new UnionDistinctStage(fragments, n, layout.ownerWorkerIds(), perPartitionExchanges);
    }

    /**
     * 规划一个窗口函数的分布式执行舞台（单交换）：扫表 → 按 PARTITION BY 哈希分区 →
     * 归属 Worker 本地排序 + 逐行计算 → 回传 Master。owner 映射与同查询其他 shuffle 完全一致。
     */
    public static DistributedStage planWindow(String queryId, List<WorkerRegistration> workers,
                                              int numPartitions, ReadRequest baseRequest,
                                              List<String> partitionBy, List<String> orderBy,
                                              List<Boolean> ascending, List<WindowFunctionSpec> functions,
                                              long memoryLimit, WindowFilterConfig filter, DojaoConnector probe) {
        PartitionLayout layout = PartitionLayout.roundRobin(workers, numPartitions);
        int n = layout.numPartitions();
        String exchange = queryId + "-W";

        List<ScanSlice> slices = planSlices(probe, baseRequest, n);
        List<FragmentSpec> producers = new ArrayList<>();
        for (int i = 0; i < slices.size(); i++) {
            String w = workers.get(i % workers.size()).info().workerId();
            producers.add(new FragmentSpec.Producer(fragmentId(queryId, "PW", i), w,
                    new FragmentSpec.ScanStep(baseRequest.catalog(), slices.get(i)),
                    new FragmentSpec.PartitionStep(exchange, partitionBy, n, layout.ownerWorkerIdsList())));
        }

        List<FragmentSpec> consumers = new ArrayList<>();
        for (int w = 0; w < workers.size(); w++) {
            String workerId = workers.get(w).info().workerId();
            consumers.add(new FragmentSpec.WindowConsumer(fragmentId(queryId, "CW", w), workerId,
                    new FragmentSpec.ExchangeInStep(exchange, n, slices.size(), layout.ownedMask(workerId)),
                    new FragmentSpec.WindowStep(partitionBy, orderBy, ascending, functions, memoryLimit, n,
                            filter)));
        }
        return new DistributedStage(layout.ownerWorkerIds(), consumers, producers);
    }

    /**
     * 规划一个<b>派生表分布式 join</b>（派生表 = 单表 GROUP BY，可作 join 的<b>任一 SQL 侧</b>）的执行舞台。
     * 派生表一侧整条链路 —— 扫派生基表（含内层 WHERE 下推）→ 按分组键分区 → 每分区本地聚合 →
     * （派生内层 HAVING：聚合输出行级过滤）→ 按派生表 join 键重哈希分区 —— <b>全部留在 Worker</b>，
     * 只有最终 join 输出回传 Master（唯一一次跨 Master 的数据）。SQL 另一侧 = 裸表（peer），
     * 扫表 → 按 peer join 键分区 → 进同一 join。
     *
     * <pre>
     *  扫 派生基表(WHERE) ─► Partition(分组键) ─► 本地聚合 ─(HAVING)─► Partition(派生 join 键) ─┐
     *                                                                                   ├─► 每 Worker 本地 join ─► Master
     *  扫 peer 表(WHERE) ────► Partition(peer join 键) ────────────────────────────────────┘
     * </pre>
     *
     * <p>两关系按 <b>SQL 顺序</b>进 HashJoinOperator 的两个通道（SQL 左 → operator 左 / probe，
     * SQL 右 → operator 右 / build）：join 键顺序、输出列顺序、LEFT/RIGHT/FULL 空值扩展语义全部
     * 与单机一致 —— 派生表在 LEFT 或 RIGHT 只改变它落在哪个通道、交换名取哪侧（{@code -DL} /
     * {@code -DR} 固定 = SQL 左 / SQL 右侧关系的最终流，{@code derivedOnLeft} 决定派生流在哪个名字上）。</p>
     *
     * <p>全部 shuffle 共用同一 round-robin owner 映射（co-partitioning 不变量）：join 键 == 分组键时
     * 聚合后的重哈希是<b>恒等</b>（每行哈希回原分区，全走本地自桶、零跨机）；跨键时是真实 re-shuffle
     * —— 统一模型覆盖两种情形。派生表一侧的 join 键须为该派生体输出列（分组键或聚合输出列），由调用方校验。</p>
     *
     * <p><b>派发顺序（反死锁）</b>：① 最终 join Consumer（注册左右读端）→ ② MiddleAggregateConsumer
     * 按分区（同步注册各聚合交换读端，聚合结果重哈希推派生流）→ ③ 喂 join build 侧的 Producer 先于
     * probe 侧 —— 派生流是 build 侧（派生在 SQL 右）时先派生基表后 peer 表，peer 表是 build 侧
     * （派生在 SQL 左）时反之 —— join 先 build 完再 probe，无阻塞死锁。</p>
     *
     * @param queryId          所属查询（exchangeId / fragmentId 前缀，查询间隔离）
     * @param workers          参与 Worker（健康节点，非空）
     * @param numPartitions    哈希分区数（应 ≥ worker 数）
     * @param derivedOnLeft    true → 派生表是 SQL 左侧关系（operator probe 侧）；false → SQL 右侧（build 侧）
     * @param innerRequest     派生表基表扫描请求（已列裁剪 + 派生内层 WHERE 下推）
     * @param innerKeys        派生表聚合分组键（= 派生基表分区键）
     * @param aggregations     派生表聚合函数（AggregationSpec，输出列名 = 派生体输出列）
     * @param innerProbe       派生基表探针连接器（分区规划用）
     * @param derivedJoinKeys  派生表一侧的 join 键（ON 条件中引用派生体输出的列）
     * @param havingFilter     派生内层 HAVING 谓词（已做聚合→输出列名替换、列 ∈ 聚合输出，可 null）
     * @param peerCatalog      peer（裸表）侧 catalog / schema / table
     * @param peerKeys         peer 表 join 键
     * @param peerProbe        peer 表探针连接器
     * @param peerFilter       peer 表单表 WHERE 下推谓词（已去限定符，可 null）
     * @param joinType         最终 join 类型（INNER / LEFT / RIGHT / FULL）
     * @param memoryLimit      每个溢写算子的内存预算
     * @param caseProjections  派生内层聚合输入的 CASE/IF 派生列投影（DA 头先算派生列再聚合，可空）
     * @param peerColumns      peer 表扫描列集（已按查询引用裁剪，收窄 DR 切片输出避免回传多余列）
     * @param joinAlgorithm    最终 join 在 Worker 上的算法（HASH fail-fast；SORT_MERGE 可溢写）
     */
    public static JoinChainStage planDerivedJoin(String queryId, List<WorkerRegistration> workers, int numPartitions,
                                                 boolean derivedOnLeft,
                                                 ReadRequest innerRequest, List<String> innerKeys,
                                                 List<AggregationSpec> aggregations,
                                                 List<CaseProjection> caseProjections, DojaoConnector innerProbe,
                                                 List<String> derivedJoinKeys, String havingFilter,
                                                 String peerCatalog, String peerSchema, String peerTable,
                                                 List<String> peerKeys, List<String> peerColumns,
                                                 DojaoConnector peerProbe, String peerFilter,
                                                 JoinType joinType, long memoryLimit,
                                                 List<DerivedProjection> projections, DerivedWindow window,
                                                 FragmentSpec.JoinAlgorithm joinAlgorithm) {
        PartitionLayout layout = PartitionLayout.roundRobin(workers, numPartitions);
        int n = layout.numPartitions();
        int numWorkers = workers.size();
        List<String> ownerWorkerIds = layout.ownerWorkerIdsList();

        String aggExchange = queryId + "-DAGG";
        String windowExchange = queryId + "-DWIN";
        // -DL / -DR 恒 = SQL 左侧 / 右侧关系的最终 join 流；派生表在哪个 SQL 侧，派生流就占哪个名
        String leftExchange = queryId + "-DL";
        String rightExchange = queryId + "-DR";
        String derivedExchange = derivedOnLeft ? leftExchange : rightExchange;
        String peerExchange = derivedOnLeft ? rightExchange : leftExchange;
        // 派生聚合每分区独立交换（镜像 planAggregation）：发送端按桶路由到对应分区的独立接收端
        List<String> aggPerPartitionExchanges = new ArrayList<>(n);
        List<String> winPerPartition = new ArrayList<>(n);
        for (int p = 0; p < n; p++) {
            aggPerPartitionExchanges.add(aggExchange + "#" + p);
            winPerPartition.add(windowExchange + "#" + p);
        }

        List<ScanSlice> innerSlices = planSlices(innerProbe, innerRequest, n);
        // 投影型派生源（无聚合，逐行表达式投影）→ 派生流由普通 Producer 直接写；
        // 聚合型 → n 个 MiddleAggregateConsumer 各对全部桶发一次 complete。发送方数由此决定。
        boolean projectionSource = projections != null && !projections.isEmpty();
        boolean windowSource = window != null;
        // peer 扫描按查询已裁剪的列集收窄：join 输出物理宽度 = 左通道 + 右通道（各为裁剪后流），
        // 恰好等于 outCols —— 避免回传全量 peer 行（查询只引用少数列时省带宽，列序/宽度不漂移）。
        ReadRequest peerBase = ReadRequest.of(peerCatalog, peerSchema, peerTable);
        List<ScanSlice> peerSlices = planSlices(peerProbe,
                withFilter(peerColumns == null || peerColumns.isEmpty()
                        ? peerBase
                        : peerProbe.pruneColumns(peerBase, peerColumns), peerFilter), n);

        List<FragmentSpec> fragments = new ArrayList<>();
        List<FragmentSpec.AggregationDto> dtos = aggregations.stream()
                .map(FragmentSpec.AggregationDto::of).toList();
        // SUM(CASE WHEN ...) 等聚合输入的 CASE/IF 派生列：DA 头本地先逐行算派生列再聚合（与单机一致）
        List<CaseProjection> projs = caseProjections == null ? List.of() : caseProjections;

        // build 侧（SQL 右）声明列：派生流没有连接器，只能按「分组键 + 聚合输出」反推；peer 直接查元数据。
        // 二者都不可得 → null（Worker 退回「收到过右页才知道类型」的旧行为）。
        List<Field> derivedFields = windowSource
                ? windowSideFields(innerProbe, innerRequest, window)
                : projectionSource
                    ? projectionSideFields(innerProbe, innerRequest, projections)
                    : derivedSideFields(innerProbe, innerRequest, innerKeys, aggregations, projs);
        // 声明列必须与上面的裁剪后扫描一致（否则空 build 桶退回声明值时列数与实收页不同 → 下游越界）
        List<Field> peerFields = rightFieldsOf(peerProbe, peerCatalog, peerSchema, peerTable, peerColumns);
        List<Field> buildFields = derivedOnLeft ? peerFields : derivedFields;

        // 1. 最终 join Consumer（每 Worker 一个，回传 Master）：SQL 左/右两关系的流 → 本地 join。
        //    派生流发送方 = n 个 MiddleAggregateConsumer（各对全部桶发 complete）或 #基表切片 Producer；
        //    peer 流发送方 = #peer 切片 Producer。
        int derivedSenderCount = projectionSource ? innerSlices.size() : n;
        for (int w = 0; w < numWorkers; w++) {
            String workerId = workers.get(w).info().workerId();
            FragmentSpec.ExchangeInStep derivedIn =
                    new FragmentSpec.ExchangeInStep(derivedExchange, n, derivedSenderCount,
                            layout.ownedMask(workerId));
            FragmentSpec.ExchangeInStep peerIn =
                    new FragmentSpec.ExchangeInStep(peerExchange, n, peerSlices.size(), layout.ownedMask(workerId));
            FragmentSpec.ExchangeInStep leftIn = derivedOnLeft ? derivedIn : peerIn;
            FragmentSpec.ExchangeInStep rightIn = derivedOnLeft ? peerIn : derivedIn;
            List<String> leftKeys = derivedOnLeft ? derivedJoinKeys : peerKeys;
            List<String> rightKeys = derivedOnLeft ? peerKeys : derivedJoinKeys;
            fragments.add(new FragmentSpec.Consumer(fragmentId(queryId, "DC", w), workerId,
                    leftIn, rightIn,
                    new FragmentSpec.JoinStep(leftKeys, rightKeys, joinType, memoryLimit, n,
                            joinAlgorithm == null ? FragmentSpec.JoinAlgorithm.HASH : joinAlgorithm,
                            buildFields,
                            derivedOnLeft ? derivedFields : peerFields)));
        }

        // 2. 派生流中间层：聚合型 → MiddleAggregateConsumer（每分区一个，派发 ownerOf(p)：own 桶 →
        //    [CASE 派生列] → 本地聚合 → (HAVING 过滤) → 按派生 join 键重哈希 → 推派生流）。
        //    投影型 → 无聚合层，派生基表 Producer 直接写派生流（见第 3 步）。
        if (windowSource) {
            // 窗口型 → MiddleWindowConsumer（每分区一个，派发 ownerOf(p)：own 桶 → 本地窗口 →
            //   按派生 join 键重哈希 → 推派生流）。发送方数 = n（各对全部桶发一次 complete）。
            FragmentSpec.WindowStep windowStep = new FragmentSpec.WindowStep(
                    window.partitionBy(), window.orderBy(), window.ascending(),
                    window.functions(), memoryLimit, n);
            for (int p = 0; p < n; p++) {
                String workerId = layout.ownerOf(p);
                boolean[] owned = new boolean[n];
                owned[p] = true;
                fragments.add(new FragmentSpec.MiddleWindowConsumer(fragmentId(queryId, "DW", p), workerId,
                        new FragmentSpec.ExchangeInStep(windowExchange + "#" + p, n, innerSlices.size(), owned),
                        windowStep,
                        new FragmentSpec.PartitionStep(derivedExchange, derivedJoinKeys, n, ownerWorkerIds)));
            }
        } else if (!projectionSource) {
            FragmentSpec.AggregateStep aggStep =
                    new FragmentSpec.AggregateStep(innerKeys, dtos, projs, memoryLimit, n);
            for (int p = 0; p < n; p++) {
                String workerId = layout.ownerOf(p);
                boolean[] owned = new boolean[n];
                owned[p] = true;
                fragments.add(new FragmentSpec.MiddleAggregateConsumer(fragmentId(queryId, "DA", p), workerId,
                        new FragmentSpec.ExchangeInStep(aggExchange + "#" + p, n, innerSlices.size(), owned),
                        aggStep,
                        new FragmentSpec.PartitionStep(derivedExchange, derivedJoinKeys, n, ownerWorkerIds),
                        havingFilter));
            }
        }

        // 3. Producer（先 build 侧后 probe 侧，反死锁）：
        //    派生在 SQL 右（派生流 = build）→ 派生基表（喂聚合 → DA → build）先、peer 表（probe）后；
        //    派生在 SQL 左（peer = build）→ peer 表先、派生基表（probe 链最深处）后。
        List<FragmentSpec> peerProducers = new ArrayList<>(peerSlices.size());
        for (int i = 0; i < peerSlices.size(); i++) {
            String w = workers.get(i % numWorkers).info().workerId();
            peerProducers.add(new FragmentSpec.Producer(fragmentId(queryId, "DR", i), w,
                    new FragmentSpec.ScanStep(peerCatalog, peerSlices.get(i)),
                    new FragmentSpec.PartitionStep(peerExchange, peerKeys, n, ownerWorkerIds)));
        }
        List<FragmentSpec> innerProducers = new ArrayList<>(innerSlices.size());
        List<GroupKeyExpression> projExprs = new ArrayList<>();
        if (projectionSource) {
            // 投影型：扫基表 → [逐行投影派生列]（Producer 侧、哈希分区之前）→ 直接按派生 join 键分区
            for (DerivedProjection p : projections) {
                projExprs.add(new GroupKeyExpression(p.column(), p.exprText()));
            }
        }
        for (int i = 0; i < innerSlices.size(); i++) {
            String w = workers.get(i % numWorkers).info().workerId();
            FragmentSpec.PartitionStep ps = projectionSource
                    ? new FragmentSpec.PartitionStep(derivedExchange, derivedJoinKeys, n, ownerWorkerIds)
                    : windowSource
                        ? new FragmentSpec.PartitionStep(windowExchange, window.partitionBy(), n, ownerWorkerIds,
                                winPerPartition)
                        : new FragmentSpec.PartitionStep(aggExchange, innerKeys, n, ownerWorkerIds,
                                aggPerPartitionExchanges);
            innerProducers.add(new FragmentSpec.Producer(fragmentId(queryId, "DIG", i), w,
                    new FragmentSpec.ScanStep(innerRequest.catalog(), innerSlices.get(i)), ps, projExprs));
        }
        fragments.addAll(derivedOnLeft ? peerProducers : innerProducers);
        fragments.addAll(derivedOnLeft ? innerProducers : peerProducers);

        return new JoinChainStage(fragments, numWorkers, layout.ownerWorkerIds());
    }

    /**
     * 给<b>终端 Consumer</b>附加分布式写目标（{@code sink}）：Producer / MiddleJoinConsumer /
     * MiddleAggregateConsumer 等非终端片段原样返回（它们的数据本就留在 Worker 侧流向下一层，
     * 只有最终回传 Master 的 Consumer 才可能改走 runAndSink 就地写走）。
     */
    public static List<FragmentSpec> attachSink(List<FragmentSpec> fragments, FragmentSpec.SinkStep sink) {
        if (sink == null) {
            return fragments;
        }
        List<FragmentSpec> out = new ArrayList<>(fragments.size());
        for (FragmentSpec f : fragments) {
            out.add(switch (f) {
                case FragmentSpec.Consumer c -> c.withSink(sink);
                case FragmentSpec.AggregateConsumer a -> a.withSink(sink);
                case FragmentSpec.WindowConsumer w -> w.withSink(sink);
                case FragmentSpec.ChainedJoinConsumer cj -> cj.withSink(sink);
                default -> f;
            });
        }
        return out;
    }

    /**
     * 给<b>终端 Consumer</b>附加片内 top-N 截断尾（{@code tail}，ORDER BY+LIMIT 下推）：
     * Producer / Middle* 等非终端片段原样返回（只有最终回传 Master 的 Consumer 才在本地排序
     * + 截断后回传候选集）。sink 与 tail 独立叠加（{@code withSink} 保留 tail、{@code withTail}
     * 保留 sink）。
     */
    public static List<FragmentSpec> attachTail(List<FragmentSpec> fragments, FragmentSpec.SortLimitStep tail) {
        if (tail == null) {
            return fragments;
        }
        List<FragmentSpec> out = new ArrayList<>(fragments.size());
        for (FragmentSpec f : fragments) {
            out.add(switch (f) {
                case FragmentSpec.Consumer c -> c.withTail(tail);
                case FragmentSpec.AggregateConsumer a -> a.withTail(tail);
                case FragmentSpec.WindowConsumer w -> w.withTail(tail);
                case FragmentSpec.ChainedJoinConsumer cj -> cj.withTail(tail);
                default -> f;
            });
        }
        return out;
    }

    /**
     * 规划切片：连接器说是几个片就派几个片；说"分不了"（空）→ 退化为单切片整表扫描。
     *
     * <p><b>绝不按 Worker 数截断切片。</b>少派一个切片 = 少读一份数据，且是静默的（查询照常
     * FINISHED，只是结果少行）。切片数超过 Worker 数时由调用方 round-robin 摊开（
     * {@code workers.get(i % workers.size())}），一个 Worker 顺序跑多个切片。</p>
     */
    private static List<ScanSlice> planSlices(DojaoConnector probe, ReadRequest base, int parallelism) {
        List<ScanSlice> slices = probe.planSplits(base, Math.max(1, parallelism));
        return slices.isEmpty() ? List.of(ScanSlice.whole(base)) : slices;
    }

    /** 单表 WHERE 下推谓词注入读取请求（null / 空 → 原样返回）。 */
    private static ReadRequest withFilter(ReadRequest req, String filter) {
        return filter == null || filter.isBlank() ? req : req.withFilter(filter);
    }

    private static String fragmentId(String queryId, String role, int idx) {
        return queryId + "-" + role + idx;
    }

    /** 多 join 链：role 带 join 层号（{@code MJ2_0} = join#2 的 0 号 Worker）。 */
    private static String fragmentId(String queryId, String role, int a, int b) {
        return queryId + "-" + role + a + "_" + b;
    }
}
