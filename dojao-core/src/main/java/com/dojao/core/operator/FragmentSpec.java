package com.dojao.core.operator;

import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.bean.Split;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 片段执行描述 —— Master 把一个分布式算子（join / 聚合 / 窗口）切成两类片段，派发给 Worker 重建执行：
 *
 * <ul>
 *   <li><b>Producer（发送端）</b>：Worker 本地扫一份数据切片 → 按 key 哈希分区 →
 *       经 {@code ExchangeChannel} 推给各桶归属 Worker。自包含（ScanStep + PartitionStep）；</li>
 *   <li><b>Consumer（接收端）</b>：Worker 接 own 桶数据 → 本地执行算子 → 输出页回传 Master。
 *       三种形态自包含：
 *       <ul>
 *         <li>{@link Consumer}：两侧 own 桶 → 本地 {@link HashJoinOperator}（JoinStep）；</li>
 *         <li>{@link AggregateConsumer}：own 桶 → 本地 {@link HashAggregationOperator}（AggregateStep）；</li>
 *         <li>{@link WindowConsumer}：own 桶 → 本地 {@link WindowSortOperator}+{@link WindowFunctionOperator}
 *             （WindowStep）；</li>
 *         <li>{@link ChainedJoinConsumer}：两侧 own 桶 → 本地 join → <b>同 key 后续计算</b>
 *             （{@link FollowUp}：窗口 / 聚合）。join 输出零二次交换，直接在 Worker 侧本地执行，
 *             即 Phase 2 "同 key 只需一次分区、所有操作本地执行"。</li>
 *         <li>{@link MiddleJoinConsumer}：两侧 own 桶 → 本地 join → <b>按下一层 join 左键
 *             重新哈希分区</b> → 推给下一层交换（多 join 链中间层，不回传 Master）。</li>
 *         <li>{@link MiddleAggregateConsumer}：own 桶 → 本地 {@link HashAggregationOperator} →
 *             <b>按派生表 join 键重新哈希分区</b> → 推给派生表一侧的 join 交换（派生表分布式
 *             join 的聚合中间层，不回传 Master）。</li>
 *       </ul></li>
 * </ul>
 *
 * <p>不变量：同一查询内所有 shuffle 用<b>相同的 owner 映射</b>与分区数（co-partitioning，
 * 见 {@link com.dojao.core.runtime.master.PartitionLayout}）：同 key 的行按同一哈希落同一桶 →
 * 落在同一 Worker，结果才正确。本描述是传输载荷（伪分布式跨线程传递 / 跨进程 Flight 序列化），
 * 不引用任何运行期算子 —— 聚合函数实例以 {@link AggregationDto#aggregateName} 字符串传输，
 * Worker 侧按名重建。</p>
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/8/20
 */
public sealed interface FragmentSpec
        permits FragmentSpec.Producer, FragmentSpec.Consumer,
                FragmentSpec.AggregateConsumer, FragmentSpec.WindowConsumer,
                FragmentSpec.ChainedJoinConsumer, FragmentSpec.MiddleJoinConsumer,
                FragmentSpec.MiddleAggregateConsumer, FragmentSpec.MiddleWindowConsumer {

    /** 片段唯一 ID（查询内唯一，兼作任务 ID）。 */
    String fragmentId();

    /** 本片段所在 WorkerId。 */
    String selfWorkerId();

    /** true → Producer（扫切片 → 推送）；false → Consumer（接桶 → 本地算子 → 回传）。 */
    boolean isProducer();

    /**
     * 本片段是否<b>向下游 shuffle 写页</b>（别人要等它推进）—— 决定 Worker 侧跑在哪个线程池。
     *
     * <p>与 {@link #isProducer()} 不同：中间层消费者（MiddleJoin / MiddleAggregate /
     * MiddleWindow，以及带下一分区的 ChainedJoinConsumer）虽然「消费上游」，但也<b>生产下游交换</b>。
     * 它们必须和 Producer 一样跑在弹性叶子池：若放进固定尺寸的主池队列，可能排在「等自己产数据」
     * 的终端消费者后面 —— 2026-11-11 线上 3 表 join 死锁正是 6 个 AggregateConsumer 先占满主池、
     * MiddleJoinConsumer（JAGG 的唯一生产者）排在第 7 位，形成线程等待环。</p>
     *
     * <p><b>不变量</b>：主池只放「纯消费者」（没有任何 shuffle 输出），它们之间不存在互相等待；
     * 任何写 shuffle 的片段都走弹性叶子池，保证「打破环的那个片段」永远能拿到线程。</p>
     */
    default boolean writesShuffle() {
        if (isProducer()) {
            return true;
        }
        return this instanceof MiddleJoinConsumer
                || this instanceof MiddleAggregateConsumer
                || this instanceof MiddleWindowConsumer
                || (this instanceof ChainedJoinConsumer c && c.nextPartition() != null);
    }

    /** 人类可读摘要（派发日志用）。 */
    String describe();

    // ==================== 步骤 ====================

    /**
     * 扫描一份数据切片 —— 由「读什么」（{@link #request()}，已带片内范围谓词）与「连接器私有
     * 定位信息」（{@link #split()} 的不透明字节）两部分组成。
     *
     * <p>{@link #split()} 为空载荷（{@link com.dojao.spi.bean.Split#WHOLE}）时即老形态：切片完全
     * 由 {@code request} 描述 —— 谓词型连接器（MySQL / PostgreSQL）与单切片整表扫描都是这种。
     * 读文件型连接器（Hive）用载荷携带文件清单，引擎只搬运不解释。</p>
     */
    record ScanStep(String catalog, ReadRequest request, Split split) {
        /** 无私有载荷的切片（谓词型连接器 / 单切片整表扫描）。 */
        public ScanStep(String catalog, ReadRequest request) {
            this(catalog, request, Split.WHOLE);
        }

        /** 由规划期切片构造（{@link ScanSlice} 自带读请求 + 私有载荷）。 */
        public ScanStep(String catalog, ScanSlice slice) {
            this(catalog, slice.request(),
                    slice.split() == null ? Split.WHOLE : slice.split());
        }

        /** 是否携带连接器私有切片载荷。 */
        public boolean hasSplit() {
            return split != null && split.hasPayload();
        }
    }

    /**
     * 分区方式 —— 决定 {@code HashPartitionOperator} 把每行发往哪个桶。
     *
     * <p>前两种是<b>均匀打散</b>（配合 numPartitions = 分区数，用于 co-partitioning join）；
     * 后两种是<b>广播</b>专用（配合 numPartitions = Worker 数 w，每台恰好 own 一个桶）：
     * build 侧整份复制到每台、probe 侧就地不动，用<b>分区方式</b>而非新算子实现了广播 join
     * （见 {@link JoinAlgorithm#BROADCAST}）。</p>
     */
    enum Distribution {
        /** 按 {@code keyColumns} 哈希取桶（默认）：同 key 落同桶 = co-partitioning 不变量。 */
        HASH,
        /**
         * 整页复制到<b>全部</b>桶（广播的 build 侧）：忽略 keyColumns，每台 Worker 拿到完整一份
         * 右表副本 → 任意 probe 行都能在本机找到匹配。
         */
        REPLICATE,
        /**
         * 按<b>生产者自己的槽位</b>固定发往一个桶（广播的 probe 侧）：不做哈希、切片在本机就地入桶
         * —— 大表零跨机移动（这正是广播要省掉的 shuffle）。
         */
        FIXED
    }

    /**
     * 按 key 哈希分区，把每桶数据推到归属 Worker（owner 映射与同查询其他 shuffle 一致）。
     *
     * <p>{@code exchangeIds[pid]} = 分区 pid 的交换标识。join / window 用
     * {@code nCopies(numPartitions, exchangeId)} 恒同（每 Worker 一个消费者，单 exchange 路由）；
     * 聚合用「每分区一个消费者」形态时各分区独立（{@code exchange + "#" + pid}），使消费端
     * 并行度与分区数一致、不与 Worker 数绑定。</p>
     *
     * <p>{@code distribution} 默认 {@link Distribution#HASH}（既有行为）。{@link Distribution#FIXED}
     * 时 {@code fixedBucket} 指定目标桶号（= 生产者所在 Worker 的槽位）。</p>
     */
    record PartitionStep(String exchangeId, List<String> keyColumns, int numPartitions,
                         List<String> ownerWorkerIds, List<String> exchangeIds,
                         Distribution distribution, int fixedBucket) {

        /** 兼容哈希分区（既有调用点）：{@code distribution = HASH}，{@code fixedBucket} 无用。 */
        public PartitionStep(String exchangeId, List<String> keyColumns, int numPartitions,
                             List<String> ownerWorkerIds, List<String> exchangeIds) {
            this(exchangeId, keyColumns, numPartitions, ownerWorkerIds, exchangeIds, Distribution.HASH, 0);
        }

        /** 广播：{@code REPLICATE}（build 侧）/ {@code FIXED}（probe 侧，{@code fixedBucket} = 本机槽位）。 */
        public static PartitionStep broadcast(String exchangeId, List<String> keyColumns, int numPartitions,
                                             List<String> ownerWorkerIds, Distribution distribution,
                                             int fixedBucket) {
            return new PartitionStep(exchangeId, keyColumns, numPartitions, ownerWorkerIds,
                    Collections.nCopies(Math.max(1, numPartitions), exchangeId), distribution, fixedBucket);
        }
        /** 兼容单 exchangeId 形态（join / window）：所有分桶共用同一交换标识。 */
        public PartitionStep(String exchangeId, List<String> keyColumns, int numPartitions,
                             List<String> ownerWorkerIds) {
            this(exchangeId, keyColumns, numPartitions, ownerWorkerIds,
                    Collections.nCopies(Math.max(1, numPartitions), exchangeId));
        }

        /** 分区 pid 的交换标识（聚合按分区一个消费者时各异；join/window 恒为 exchangeId）。 */
        public String exchangeIdFor(int pid) {
            return exchangeIds.get(pid);
        }
    }

    /** 接收端：本 Worker 拥有的桶（owned[pid]=true 才收数据；非归属桶立即结束、零内存）。 */
    record ExchangeInStep(String exchangeId, int numPartitions, int numSenders, boolean[] owned) {
    }

    /**
     * 本地 join 算法 —— <b>Master 按两侧数据量选定</b>（见 {@code StageScheduler}），随 JoinStep 下发，
     * Worker 按名重建对应算子（两侧输入都是本机 own 桶，单机 / 分布式共用同一份算子代码）。
     *
     * <p>为什么由 Master 定：只有规划期拿得到两侧的<b>数据量</b>（连接器统计），而算法选择是数据量的
     * 函数 —— 放进 Worker 就得再把统计传一遍，且两边判断可能不一致。这里只传"结论"。</p>
     */
    enum JoinAlgorithm {
        /**
         * 分区哈希 join：build 侧（右表）必须装进「每分区内存预算」（{@code memoryLimit / numPartitions}）。
         * 数据量小、无外存 I/O，最快；超预算会 fail-fast 报错（不静默降级）。
         */
        HASH,
        /**
         * 排序归并 join：两侧排序阶段各自可溢写（三槽 + Arrow 文件），不受"build 侧必须装内存"限制。
         * build 侧（右表）估算数据量超出每分区预算时选它 —— 即「内存满了就溢写」的兜底。
         * 代价 = 两次排序 + 归并，比 HASH 慢，但数据量再大也不会因单侧内存而失败。
         */
        SORT_MERGE,
        /**
         * 广播 join：build 侧（右表，够小）整份复制到<b>每一台</b> Worker，probe 侧（左表）切片
         * 就地不动 —— 省掉大表的 shuffle。
         *
         * <p>Worker 侧仍跑 {@code HashJoinOperator}：广播由<b>分区方式</b>而非算子形态实现 ——
         * build 侧 Producer 用 {@link PartitionStep} 的 <b>replicate</b> 模式（每行发往全部 owner），
         * probe 侧用 <b>fixedPartition</b> 模式（每切片发往自己那台、零跨机）。两侧 numPartitions
         * 都取 Worker 数 w，于是每台 Worker 恰好 own 一个桶，其右桶 = 完整右表、左桶 = 本机左切片，
         * 原 HashJoinOperator 直接可用（build 侧内存预算 {@code memoryLimit} 按 {@code U×w} 下发）。</p>
         */
        BROADCAST
    }

    /**
     * 本地 join（两个 ShuffleReader 作 PartitionedSource，与单机 join 同一算子代码）。
     *
     * @param declaredRightFields 右表（build 侧）<b>全列</b>，计划期由 Master 下发（{@code columns == null}
     *                            的全表列扫描 ⇒ 与 Worker 实收右表页同列同序）。用于 build 侧<b>一页都没收到</b>
     *                            的分区（哈希分发下空桶是常态）：没有它，该分区只能按「实收页」推断输出
     *                            schema → 退化成「只有左列」的短 schema → 同算子各分区页列数不一致，
     *                            下游按首列 initialize 后在 {@code PartitionedSourceOperator} /
     *                            {@code HashPartitionOperator} 处 {@code IndexOutOfBoundsException}
     *                            （顺序相反时静默丢列）。null/空 = 未声明（旧行为）。
     * @param declaredLeftFields  左表（probe 侧）<b>全列</b>，语义与 {@code declaredRightFields} 对称：
     *                            RIGHT / FULL join 的某分区<b>左表一页都没收到</b>时，要输出
     *                            「右行 + 左列全 NULL」的孤儿行，没有左列类型就造不出输出 schema
     *                            （修复前 SortMergeJoin 直接抛错、Hash 路径静默丢孤儿行）。
     *                            null/空 = 未声明（旧行为）。
     * @param onLeftFilter        JOIN ON 中<b>只引用左（probe）侧</b>的条件（已去限定符、可空）。
     *                            它不是连接键，而是「匹配判定」的一部分：条件为假/NULL 的行视为<b>未匹配</b>
     *                            —— LEFT/FULL 仍要把它作为孤儿行输出（右列补 NULL）。Worker 用
     *                            {@code HashJoinOperator} 在 probe 页上整页求值一次。
     * @param onRightFilter       对称：只引用右（build）侧的条件；条件为假/NULL 的 build 行不参与匹配，
     *                            RIGHT/FULL 仍作为孤儿行输出（左列补 NULL）。
     *
     * <p><b>为什么不能提前下推到扫描端</b>：外连接的保留侧若提前过滤，未匹配行会被整行删掉，
     * 而 SQL 语义要求它输出「本行 + 对侧全 NULL」。所以只能在 join 的匹配判定里生效。</p>
     */
    record JoinStep(List<String> leftKeyColumns, List<String> rightKeyColumns,
                    JoinType joinType, long memoryLimit, int numPartitions,
                    JoinAlgorithm algorithm, List<Field> declaredRightFields,
                    List<Field> declaredLeftFields, List<JoinAlias> outputAliases,
                    String onLeftFilter, String onRightFilter) {

        public JoinStep {
            outputAliases = outputAliases == null ? List.of() : List.copyOf(outputAliases);
            onLeftFilter = blankToNull(onLeftFilter);
            onRightFilter = blankToNull(onRightFilter);
        }

        private static String blankToNull(String s) {
            return s == null || s.isBlank() ? null : s;
        }

        /** 无 ON 单侧残差谓词（绝大多数 join）。 */
        public JoinStep(List<String> leftKeyColumns, List<String> rightKeyColumns,
                        JoinType joinType, long memoryLimit, int numPartitions,
                        JoinAlgorithm algorithm, List<Field> declaredRightFields,
                        List<Field> declaredLeftFields, List<JoinAlias> outputAliases) {
            this(leftKeyColumns, rightKeyColumns, joinType, memoryLimit, numPartitions, algorithm,
                    declaredRightFields, declaredLeftFields, outputAliases, null, null);
        }

        /** 附加 ON 单侧残差谓词（外连接保留侧在 ON 中的单侧过滤）。 */
        public JoinStep withOnFilters(String onLeft, String onRight) {
            return new JoinStep(leftKeyColumns, rightKeyColumns, joinType, memoryLimit, numPartitions,
                    algorithm, declaredRightFields, declaredLeftFields, outputAliases,
                    onLeft, onRight);
        }

        /** 旧签名（无输出别名）：与 {@code outputAliases = []} 逐字节同行为。 */
        public JoinStep(List<String> leftKeyColumns, List<String> rightKeyColumns,
                        JoinType joinType, long memoryLimit, int numPartitions,
                        JoinAlgorithm algorithm, List<Field> declaredRightFields,
                        List<Field> declaredLeftFields) {
            this(leftKeyColumns, rightKeyColumns, joinType, memoryLimit, numPartitions,
                    algorithm, declaredRightFields, declaredLeftFields, List.of());
        }

        /** 哈希 join（默认；旧调用点 / 小数据量）。 */
        public JoinStep(List<String> leftKeyColumns, List<String> rightKeyColumns,
                        JoinType joinType, long memoryLimit, int numPartitions) {
            this(leftKeyColumns, rightKeyColumns, joinType, memoryLimit, numPartitions,
                    JoinAlgorithm.HASH, null, null);
        }

        /** 带算法、无声明列（多 join 链中间层等：由调用方按需补 declared*Fields）。 */
        public JoinStep(List<String> leftKeyColumns, List<String> rightKeyColumns,
                        JoinType joinType, long memoryLimit, int numPartitions,
                        JoinAlgorithm algorithm) {
            this(leftKeyColumns, rightKeyColumns, joinType, memoryLimit, numPartitions, algorithm, null, null);
        }

        /** 仅声明右表列（旧签名；左表未声明）。 */
        public JoinStep(List<String> leftKeyColumns, List<String> rightKeyColumns,
                        JoinType joinType, long memoryLimit, int numPartitions,
                        JoinAlgorithm algorithm, List<Field> declaredRightFields) {
            this(leftKeyColumns, rightKeyColumns, joinType, memoryLimit, numPartitions,
                    algorithm, declaredRightFields, null);
        }

        /** 附加 join 输出别名副本（外连接同名列消歧）：Worker 在 join 之后、后续计算之前套用。 */
        public JoinStep withOutputAliases(List<JoinAlias> aliases) {
            return new JoinStep(leftKeyColumns, rightKeyColumns, joinType, memoryLimit, numPartitions,
                    algorithm, declaredRightFields, declaredLeftFields, aliases,
                    onLeftFilter, onRightFilter);
        }
    }

    /**
     * join 输出别名规则（传输 DTO）：把上游第 {@code occurrence}（从 1 起）个 {@code sourceName}
     * 复制一份命名为 {@code aliasName} —— 等价于 {@code ColumnAliasOperator.Alias}。
     *
     * <p>外连接同名列消歧在 Master 单机路径由 {@code ColumnAliasOperator} 完成；链式分布式执行时
     * 该算子被折叠进 Worker 片段（join 输出 → 别名副本 → 窗口 / 聚合），否则窗口链无法识别上游
     * 分布式 join，会退回单机。FragmentSpec 不引用运行期算子类型，故用本 DTO。</p>
     */
    record JoinAlias(String sourceName, int occurrence, String aliasName) {
    }

    /** 聚合函数的传输 DTO —— 用 {@code aggregateName} 替代运行期聚合函数实例（FragmentSpec 不得引用运行期算子）。 */
    record AggregationDto(String inputColumn, String outputColumn, String aggregateName,
                          List<String> collectInputColumns) {

        /** 增量型 / 旧调用点：无 collect 输入列。 */
        public AggregationDto(String inputColumn, String outputColumn, String aggregateName) {
            this(inputColumn, outputColumn, aggregateName, List.of());
        }

        /**
         * 从运行期 {@link AggregationSpec} 转换（Master 侧，派发前）。
         * collect-evaluate 型（{@link AggregationSpec#isCollect()}）下 {@code aggregate} 为 null，
         * 函数名取自 {@code collect}，实际收集列走 {@code collectInputColumns}；增量型取自
         * {@code aggregate}。Worker 侧按名重建时先查
         * {@link com.dojao.spi.function.CollectAggregateRegistry} 再查增量注册表。
         */
        public static AggregationDto of(AggregationSpec spec) {
            String name = spec.isCollect()
                    ? spec.collect().functionName()
                    : spec.aggregate().functionName();
            return new AggregationDto(spec.inputColumn(), spec.outputColumn(), name,
                    spec.collectInputColumns());
        }
    }

    /**
     * 链式 join 片段的本地后续计算 —— 与 join 同 key（零交换），直接在 Worker 侧 join 输出上执行，
     * 结果回传 Master。窗口 / 聚合二选一（sealed）。
     */
    sealed interface FollowUp permits FragmentSpec.WindowStep, FragmentSpec.AggregateStep {
    }

    /** 本地 HashAggregation：own 桶数据按 key 聚合，输出页回传 Master。 */
    record AggregateStep(List<String> keyColumns, List<AggregationDto> aggregations,
                         List<CaseProjection> caseProjections,
                         long memoryLimit, int numPartitions) implements FollowUp {
        /** 无 CASE 派生列（链式聚合，join→同 key 聚合暂不支持 CASE）。 */
        public AggregateStep(List<String> keyColumns, List<AggregationDto> aggregations,
                             long memoryLimit, int numPartitions) {
            this(keyColumns, aggregations, List.of(), memoryLimit, numPartitions);
        }

        /** 复制并替换分区数（舞台归一化，其余字段原样保留；禁止位置参数重建丢字段）。 */
        public AggregateStep withNumPartitions(int n) {
            return new AggregateStep(keyColumns, aggregations, caseProjections, memoryLimit, n);
        }
    }

    /**
     * 本地窗口：own 桶数据按 PARTITION BY 分组排序、逐行计算窗口函数，输出页回传 Master。
     *
     * <p>{@code filter} = 提前过滤下推（外层 {@code WHERE rn <= N} 等，可为 null）：Worker 侧
     * {@link WindowSortOperator#setFilterConfig} 在 Phase 1 末尾就地裁剪，避免整组回传 Master 再过滤。</p>
     *
     * <p>{@code computed} = 窗口键 / 值参数表达式需要在<b>窗口排序之前</b>物化的合成列
     * （{@link GroupKeyExpression}，如 {@code coalesce(province,'NA')} → {@code __win_key_0}）。
     * 由本片段的执行器用 {@link ExpressionProjectOperator} 逐行求值后追加；{@code partitionBy} /
     * {@code orderBy} / 函数参数已改写为引用这些合成列。空 = 键都是裸列名（老行为）。</p>
     *
     * <p>{@code outputColumns} = 本组窗口计算完成后的对外列（上游列 + 本组及之前各组的窗口输出列）。
     * 非空时执行器在窗口之后用它做一次投影，把 {@code computed} 合成列裁掉；空 = 不裁剪。</p>
     */
    record WindowStep(List<String> partitionBy, List<String> orderBy, List<Boolean> ascending,
                      List<WindowFunctionSpec> functions, long memoryLimit, int numPartitions,
                      WindowFilterConfig filter, List<GroupKeyExpression> computed,
                      List<String> outputColumns) implements FollowUp {

        public WindowStep {
            computed = computed == null ? List.of() : List.copyOf(computed);
            outputColumns = outputColumns == null ? List.of() : List.copyOf(outputColumns);
        }

        /** 无键表达式物化 / 无输出裁剪（老调用点兼容）。 */
        public WindowStep(List<String> partitionBy, List<String> orderBy, List<Boolean> ascending,
                          List<WindowFunctionSpec> functions, long memoryLimit, int numPartitions,
                          WindowFilterConfig filter) {
            this(partitionBy, orderBy, ascending, functions, memoryLimit, numPartitions, filter,
                    List.of(), List.of());
        }

        /** 无下推过滤（老调用点兼容）。 */
        public WindowStep(List<String> partitionBy, List<String> orderBy, List<Boolean> ascending,
                          List<WindowFunctionSpec> functions, long memoryLimit, int numPartitions) {
            this(partitionBy, orderBy, ascending, functions, memoryLimit, numPartitions, null);
        }

        /** 复制并替换过滤配置（下层算子不可变，下推时重建）。 */
        public WindowStep withFilter(WindowFilterConfig f) {
            return new WindowStep(partitionBy, orderBy, ascending, functions, memoryLimit, numPartitions,
                    f, computed, outputColumns);
        }

        /** 复制并替换合成列（跨交换边界后由发送端先行物化时清空）。 */
        public WindowStep withComputed(List<GroupKeyExpression> c) {
            return new WindowStep(partitionBy, orderBy, ascending, functions, memoryLimit, numPartitions,
                    filter, c, outputColumns);
        }

        /** 清空合成列（数据已在上游物化，执行器不再重复求值）。 */
        public WindowStep withoutComputed() {
            return computed.isEmpty() ? this : withComputed(List.of());
        }

        /** 复制并替换分区数（舞台归一化，其余字段原样保留；禁止位置参数重建丢字段）。 */
        public WindowStep withNumPartitions(int n) {
            return new WindowStep(partitionBy, orderBy, ascending, functions, memoryLimit, n,
                    filter, computed, outputColumns);
        }
    }

    /**
     * 分布式写目标 —— 挂在终端 Consumer 上，把该分区算好的最终输出<b>就地写走、不回传 Master</b>。
     *
     * <p>Worker 侧 {@code FragmentExecutor} 见到 {@code sink != null} 时改走 {@code runAndSink}：
     * 逐页 {@code connector.write(WriteRequest)} 写走 + 计数，最后只回传完成 ack + 行数
     * （数据永不回 Master）。<b>批量型（默认）</b>：某分区失败、其他分区已写数据不删除。</p>
     *
     * <p>{@code targetColumns}：目标表列名（按位置对应输出页列），null → 用页 schema 列名。</p>
     */
    record SinkStep(String catalog, String schema, String table, List<String> targetColumns) {
    }

    /**
     * 片内 top-N 截断尾（顶层 ORDER BY + LIMIT/OFFSET 下推）—— 挂在<b>终端</b> Consumer 上：
     * Worker 把本分区最终结果按 {@code orderByKeys} 本地排序后只回传前 {@code offset+limit} 行，
     * Master 再在候选集上做全局排序取 {@code [offset, offset+limit)}（top-N 定理：全局前 N 必在
     * 自己片内前 N，见仓库根 {@code orderby_limit_pushdown_architecture.md}）。
     *
     * <p>键必须是<b>该分片输出 schema 的列</b>（ORDER BY 键已被限定为 SELECT 输出列，且无
     * Master 中间变换挡在分片与全局 Sort 之间时才下推）；{@code limit < 0} = 只排不截断（Phase 2
     * 全排序用，本期不产生）。{@code memoryLimit} 为片内 WindowSort 内存上限（超限溢写）。</p>
     */
    record SortLimitStep(List<String> orderByKeys, List<Boolean> ascending,
                         long offset, long limit, long memoryLimit) {
        /** true → 排序后截断到 {@code offset+limit} 行；false（limit<0）→ 只排不截断。 */
        public boolean truncates() {
            return limit >= 0;
        }

        /** 本片回传行数上限（= offset + limit；溢出饱和到 Long.MAX_VALUE）。 */
        public long keepRows() {
            if (limit < 0) return Long.MAX_VALUE;
            if (limit > Long.MAX_VALUE - offset) return Long.MAX_VALUE;
            return offset + limit;
        }
    }

    /**
     * 多维聚合展开（{@code GROUP BY ROLLUP/CUBE/GROUPING SETS}）—— 让 N 条腿共用<b>一次</b>扫描
     * 与 join：Worker 在扫描（或最末层 join）输出之后、哈希分区之前插一层
     * {@link com.dojao.core.operator.ExpandOperator}，把每行按 set 个数复制、未参与维度置 NULL、
     * 追加 {@code __grouping_id}，再按 {@code (维度…, __grouping_id)} 做<b>一次</b>聚合。
     *
     * <p>{@code dimColumns} = 全部维度列（聚合分组键的前缀），{@code groupingSets} = 每个维度组合
     * 参与的维度列名。null = 无展开（普通聚合，行为不变）。</p>
     */
    record ExpandStep(List<String> dimColumns, List<List<String>> groupingSets) {

        public ExpandStep {
            dimColumns = dimColumns == null ? List.of() : List.copyOf(dimColumns);
            if (groupingSets == null) {
                groupingSets = List.of();
            } else {
                List<List<String>> copy = new java.util.ArrayList<>(groupingSets.size());
                for (List<String> s : groupingSets) copy.add(s == null ? List.of() : List.copyOf(s));
                groupingSets = List.copyOf(copy);
            }
        }
    }

    /** describe() 的多维聚合展开摘要（无 → 空串）。 */
    static String expandSuffix(ExpandStep expand) {
        return expand == null ? ""
                : " → expand(" + expand.groupingSets().size() + " sets of " + expand.dimColumns() + ")";
    }

    /** describe() 的写目标摘要（无写目标 → 空串）。 */
    static String sinkSuffix(SinkStep sink) {
        return sink == null ? ""
                : " → write " + sink.catalog() + "." + sink.schema() + "." + sink.table();
    }

    /** describe() 的 top-N 尾摘要（无 → 空串）。 */
    static String tailSuffix(SortLimitStep tail) {
        return tail == null ? ""
                : " → localSort(" + String.join(", ", tail.orderByKeys())
                + (tail.truncates() ? ", top=" + tail.keepRows() + ")" : ")");
    }

    /** describe() 的 join 算法摘要（默认 HASH → 空串，日志只在走非默认路径时标出）。 */
    static String joinSuffix(JoinStep join) {
        return join.algorithm() == JoinAlgorithm.HASH ? "" : "[" + join.algorithm() + "]";
    }

    /**
     * 本片段会碰到的 catalog 名（去重、保序）—— Producer 的<b>扫描源</b>、带写目标的 Consumer 的
     * <b>写目标</b>；两者都没有的片段（纯 join / 聚合中间层，数据全靠交换进来）为空。
     *
     * <p>Master 派发时据此把对应 catalog 的<b>定义</b>（type + 连接属性）附在载荷里，与
     * {@code cluster_nodes}（拓扑）/ {@code functions}（函数定义）同款：Worker 是独立 JVM，
     * 不该假设它与 Master 共享元数据库，也不该要求它预置任何 catalog —— 名字在
     * {@link ScanStep#catalog()} 里只是"扫哪个"，定义才是"怎么连"。</p>
     */
    static Set<String> catalogsOf(FragmentSpec spec) {
        Set<String> out = new LinkedHashSet<>();
        switch (spec) {
            case Producer p -> addCatalog(out, p.scan().catalog());
            case Consumer c -> addSink(out, c.sink());
            case AggregateConsumer c -> addSink(out, c.sink());
            case WindowConsumer c -> addSink(out, c.sink());
            case ChainedJoinConsumer c -> addSink(out, c.sink());
            case MiddleJoinConsumer ignored -> { }               // 数据全靠交换进来，不碰数据源
            case MiddleAggregateConsumer ignored -> { }
            case MiddleWindowConsumer ignored -> { }
        }
        return out;
    }

    private static void addSink(Set<String> out, SinkStep sink) {
        if (sink != null) {
            addCatalog(out, sink.catalog());
        }
    }

    /**
     * 登记一个「需要下发连接器定义」的 catalog。物化 CTE 的哨兵 catalog
     * （{@link MaterializedCteConnector#isCteCatalog}）<b>不登记</b>：Worker 不认识它，读取时由
     * 扫描执行点直接换成本地无状态解码连接器（{@link MaterializedCteConnector#wire()}），
     * 数据来自切片载荷本身，不需要任何连接属性。
     */
    private static void addCatalog(Set<String> out, String catalog) {
        if (catalog != null && !MaterializedCteConnector.isCteCatalog(catalog)) {
            out.add(catalog);
        }
    }

    // ==================== 两类片段 ====================

    /** 发送端：扫本地切片 → 哈希分区 → 推送归属 Worker（无 Master 回传）。 */
    record Producer(String fragmentId, String selfWorkerId,
                    ScanStep scan, PartitionStep partition,
                    List<GroupKeyExpression> groupKeyExpressions,
                    ExpandStep expand) implements FragmentSpec {
        /** 无 GROUP BY 表达式（join / 窗口 / 纯扫描的常规 Producer）。 */
        public Producer(String fragmentId, String selfWorkerId,
                        ScanStep scan, PartitionStep partition) {
            this(fragmentId, selfWorkerId, scan, partition, List.of(), null);
        }

        /** 无多维聚合展开（旧调用点）。 */
        public Producer(String fragmentId, String selfWorkerId,
                        ScanStep scan, PartitionStep partition,
                        List<GroupKeyExpression> groupKeyExpressions) {
            this(fragmentId, selfWorkerId, scan, partition, groupKeyExpressions, null);
        }

        /** 附加多维聚合展开：扫描（与 GROUP BY 派生列）之后、哈希分区之前复制行。 */
        public Producer withExpand(ExpandStep e) {
            return new Producer(fragmentId, selfWorkerId, scan, partition, groupKeyExpressions, e);
        }

        @Override
        public boolean isProducer() {
            return true;
        }

        @Override
        public String describe() {
            String gk = groupKeyExpressions.isEmpty() ? ""
                    : " → groupKey(" + groupKeyExpressions.stream()
                            .map(GroupKeyExpression::derivedColumn)
                            .collect(java.util.stream.Collectors.joining(", ")) + ")";
            return "scan " + scan.request().schema() + "." + scan.request().table()
                    + " → hash-partition(" + partition.keyColumns() + ")" + gk
                    + FragmentSpec.expandSuffix(expand);
        }
    }

    /** 接收端：own 桶两侧数据 → 本地 HashJoin → 回传 Master（带 sink 时就地写走）。 */
    record Consumer(String fragmentId, String selfWorkerId,
                    ExchangeInStep leftIn, ExchangeInStep rightIn, JoinStep join,
                    String filterAfterJoin, AggregateStep partialFollowUp,
                    SinkStep sink, SortLimitStep tail) implements FragmentSpec {
        /** 无写目标（SELECT 回传形态）。 */
        public Consumer(String fragmentId, String selfWorkerId,
                        ExchangeInStep leftIn, ExchangeInStep rightIn, JoinStep join) {
            this(fragmentId, selfWorkerId, leftIn, rightIn, join, null, null, null, null);
        }

        /** 无写目标 + join 输出过滤（join 之上的谓词下沉，按顶层 AND 拆分逐条编译）。 */
        public Consumer withFilterAfterJoin(String f) {
            return new Consumer(fragmentId, selfWorkerId, leftIn, rightIn, join, f,
                    partialFollowUp, sink, tail);
        }

        /**
         * 附加二级聚合（外层分组聚合下沉）：本地 join（+过滤）输出上同 Worker 再聚一次。
         * 协分区成立时（分组键覆盖 join 分区键）每分区输出即终态结果，各分区并集 = 最终结果。
         */
        public Consumer withPartialFollowUp(AggregateStep f) {
            return new Consumer(fragmentId, selfWorkerId, leftIn, rightIn, join,
                    filterAfterJoin, f, sink, tail);
        }

        /** 附加分布式写目标：本片段把 join 输出就地写走、不回传数据页（只回传完成 ack）。 */
        public Consumer withSink(SinkStep s) {
            return new Consumer(fragmentId, selfWorkerId, leftIn, rightIn, join,
                    filterAfterJoin, partialFollowUp, s, tail);
        }

        /** 附加片内 top-N 截断尾：本片段按 ORDER BY 键本地排序后只回传前 keep 行。 */
        public Consumer withTail(SortLimitStep t) {
            return new Consumer(fragmentId, selfWorkerId, leftIn, rightIn, join,
                    filterAfterJoin, partialFollowUp, sink, t);
        }

        @Override
        public boolean isProducer() {
            return false;
        }

        @Override
        public String describe() {
            return "local join " + join.leftKeyColumns() + "=" + join.rightKeyColumns()
                    + FragmentSpec.joinSuffix(join)
                    + " (leftIn=" + leftIn.exchangeId() + ", rightIn=" + rightIn.exchangeId() + ")"
                    + (filterAfterJoin == null || filterAfterJoin.isBlank()
                        ? "" : " → filter[" + filterAfterJoin + "]")
                    + (partialFollowUp == null
                        ? "" : " → aggregate " + partialFollowUp.keyColumns())
                    + FragmentSpec.sinkSuffix(sink) + FragmentSpec.tailSuffix(tail);
        }
    }

    /**
     * 接收端：own 桶数据 → 本地 HashAggregation → 回传 Master（带 sink 时就地写走）。
     *
     * <p>{@code followUp} 非空 = 在本地聚合输出上再跑一级聚合（同 Worker、零交换）：
     * 每分区只回传 1 行 partial 聚合，Master 侧 {@code PartialAggregationMergeOperator} 合并
     * 成最终行。用于「分布式聚合之上的全局聚合」下沉 —— 外层 COUNT/SUM/MIN/MAX 不必把
     * 内层全部结果行搬回 Master 单机扫一遍。</p>
     */
    record AggregateConsumer(String fragmentId, String selfWorkerId,
                             ExchangeInStep in, AggregateStep aggregate, AggregateStep followUp,
                             SinkStep sink, SortLimitStep tail) implements FragmentSpec {
        /** 无二级聚合 / 无写目标（SELECT 回传形态）。 */
        public AggregateConsumer(String fragmentId, String selfWorkerId,
                                 ExchangeInStep in, AggregateStep aggregate) {
            this(fragmentId, selfWorkerId, in, aggregate, null, null, null);
        }

        /** 无二级聚合（旧调用点）。 */
        public AggregateConsumer(String fragmentId, String selfWorkerId,
                                 ExchangeInStep in, AggregateStep aggregate,
                                 SinkStep sink, SortLimitStep tail) {
            this(fragmentId, selfWorkerId, in, aggregate, null, sink, tail);
        }

        /** 附加二级聚合（分布式全局聚合下沉的 partial 段）。 */
        public AggregateConsumer withFollowUp(AggregateStep f) {
            return new AggregateConsumer(fragmentId, selfWorkerId, in, aggregate, f, sink, tail);
        }

        /** 附加分布式写目标：本片段把聚合输出就地写走、不回传数据页（只回传完成 ack）。 */
        public AggregateConsumer withSink(SinkStep s) {
            return new AggregateConsumer(fragmentId, selfWorkerId, in, aggregate, followUp, s, tail);
        }

        /** 附加片内 top-N 截断尾：本片段按 ORDER BY 键本地排序后只回传前 keep 行。 */
        public AggregateConsumer withTail(SortLimitStep t) {
            return new AggregateConsumer(fragmentId, selfWorkerId, in, aggregate, followUp, sink, t);
        }

        @Override
        public boolean isProducer() {
            return false;
        }

        @Override
        public String describe() {
            return "local aggregate " + aggregate.keyColumns()
                    + (followUp == null ? "" : " → partial aggregate " + followUp.keyColumns())
                    + " (in=" + in.exchangeId() + ")"
                    + FragmentSpec.sinkSuffix(sink) + FragmentSpec.tailSuffix(tail);
        }
    }

    /** 接收端：own 桶数据 → 本地窗口函数 → 回传 Master（带 sink 时就地写走）。 */
    record WindowConsumer(String fragmentId, String selfWorkerId,
                          ExchangeInStep in, WindowStep window,
                          AggregateStep partialFollowUp,
                          SinkStep sink, SortLimitStep tail) implements FragmentSpec {
        /** 无写目标（SELECT 回传形态）。 */
        public WindowConsumer(String fragmentId, String selfWorkerId,
                              ExchangeInStep in, WindowStep window) {
            this(fragmentId, selfWorkerId, in, window, null, null, null);
        }

        /**
         * 附加二级聚合（外层全局聚合下沉到窗口链终端）：本片段窗口输出后同 Worker 再聚一次，
         * 每分区只回传 1 行 partial，Master 侧 {@code PartialAggregationMergeOperator} 合并。
         */
        public WindowConsumer withPartialFollowUp(AggregateStep f) {
            return new WindowConsumer(fragmentId, selfWorkerId, in, window, f, sink, tail);
        }

        /** 附加分布式写目标：本片段把窗口输出就地写走、不回传数据页（只回传完成 ack）。 */
        public WindowConsumer withSink(SinkStep s) {
            return new WindowConsumer(fragmentId, selfWorkerId, in, window, partialFollowUp, s, tail);
        }

        /** 附加片内 top-N 截断尾：本片段按 ORDER BY 键本地排序后只回传前 keep 行。 */
        public WindowConsumer withTail(SortLimitStep t) {
            return new WindowConsumer(fragmentId, selfWorkerId, in, window, partialFollowUp, sink, t);
        }

        @Override
        public boolean isProducer() {
            return false;
        }

        @Override
        public String describe() {
            return "local window partitionBy=" + window.partitionBy()
                    + (partialFollowUp == null ? "" : " → partial aggregate " + partialFollowUp.keyColumns())
                    + " (in=" + in.exchangeId() + ")"
                    + FragmentSpec.sinkSuffix(sink) + FragmentSpec.tailSuffix(tail);
        }
    }

    /**
     * 接收端（链式）：own 桶两侧数据 → 本地 HashJoin → <b>同 key 后续计算</b>（窗口 / 聚合）→ 回传 Master。
     * 零二次交换：join 输出已在归属 Worker 上按 key 分布，后续计算直接用本地 join 输出即可（cowork without master）。
     */
    record ChainedJoinConsumer(String fragmentId, String selfWorkerId,
                               ExchangeInStep leftIn, ExchangeInStep rightIn,
                               JoinStep join, FollowUp followUp,
                               AggregateStep partialFollowUp,
                               SinkStep sink, SortLimitStep tail,
                               PartitionStep nextPartition,
                               List<GroupKeyExpression> nextComputed) implements FragmentSpec {

        public ChainedJoinConsumer {
            nextComputed = nextComputed == null ? List.of() : List.copyOf(nextComputed);
        }

        /** 无写目标（SELECT 回传形态）。 */
        public ChainedJoinConsumer(String fragmentId, String selfWorkerId,
                                   ExchangeInStep leftIn, ExchangeInStep rightIn,
                                   JoinStep join, FollowUp followUp) {
            this(fragmentId, selfWorkerId, leftIn, rightIn, join, followUp, null, null, null, null, List.of());
        }

        /** 终端形态（无后继重分区）：join→后续计算结果回传 Master / 就地写走。 */
        public ChainedJoinConsumer(String fragmentId, String selfWorkerId,
                                   ExchangeInStep leftIn, ExchangeInStep rightIn,
                                   JoinStep join, FollowUp followUp,
                                   SinkStep sink, SortLimitStep tail) {
            this(fragmentId, selfWorkerId, leftIn, rightIn, join, followUp, null, sink, tail, null, List.of());
        }

        /**
         * 附加二级聚合（外层全局聚合下沉）：在 join→后续聚合输出上同 Worker 再聚一次，
         * 每分区只回传 1 行 partial，Master 侧 {@code PartialAggregationMergeOperator} 合并。
         * 仅对<b>聚合</b> followUp 有意义（窗口 followUp 的外层聚合由窗口链自身承接）。
         */
        public ChainedJoinConsumer withPartialFollowUp(AggregateStep f) {
            return new ChainedJoinConsumer(fragmentId, selfWorkerId, leftIn, rightIn, join, followUp,
                    f, sink, tail, nextPartition, nextComputed);
        }

        /** true → 终端片段；false → join→后续计算输出按 {@link #nextPartition} 继续重分区（不回传 Master）。 */
        public boolean terminal() {
            return nextPartition == null;
        }

        /** 附加分布式写目标：本片段把 join→后续计算输出就地写走、不回传数据页（只回传完成 ack）。 */
        public ChainedJoinConsumer withSink(SinkStep s) {
            return new ChainedJoinConsumer(fragmentId, selfWorkerId, leftIn, rightIn, join, followUp,
                    partialFollowUp, s, tail, nextPartition, nextComputed);
        }

        /** 附加片内 top-N 截断尾：本片段按 ORDER BY 键本地排序后只回传前 keep 行。 */
        public ChainedJoinConsumer withTail(SortLimitStep t) {
            return new ChainedJoinConsumer(fragmentId, selfWorkerId, leftIn, rightIn, join, followUp,
                    partialFollowUp, sink, t, nextPartition, nextComputed);
        }

        /** 非终端形态：join→首窗口分组后按下一窗口键重分区，数据留在 Worker 侧继续流向后续窗口。 */
        public ChainedJoinConsumer withNextPartition(PartitionStep p) {
            return withNextPartition(p, List.of());
        }

        /** 非终端形态 + 下一窗口分组键表达式合成列（重分区前本机物化）。 */
        public ChainedJoinConsumer withNextPartition(PartitionStep p, List<GroupKeyExpression> computed) {
            return new ChainedJoinConsumer(fragmentId, selfWorkerId, leftIn, rightIn, join, followUp,
                    partialFollowUp, sink, tail, p, computed);
        }

        @Override
        public boolean isProducer() {
            return false;
        }

        @Override
        public String describe() {
            String follow = switch (followUp) {
                case WindowStep w -> "window " + w.partitionBy();
                case AggregateStep a -> "aggregate " + a.keyColumns();
            };
            return "local join " + join.leftKeyColumns() + "=" + join.rightKeyColumns()
                    + FragmentSpec.joinSuffix(join)
                    + " → " + follow
                    + (partialFollowUp == null ? "" : " → partial aggregate "
                        + partialFollowUp.keyColumns())
                    + (nextPartition == null ? "" : " → repartition(" + nextPartition.keyColumns()
                        + ") → " + nextPartition.exchangeId())
                    + " (leftIn=" + leftIn.exchangeId()
                    + ", rightIn=" + rightIn.exchangeId() + ")"
                    + FragmentSpec.sinkSuffix(sink) + FragmentSpec.tailSuffix(tail);
        }
    }

    /**
     * 接收端（多 join 链中间层）：own 桶两侧数据 → 本地 HashJoin → <b>把 join 输出按下一层
     * join 的左键重新哈希分区</b> → 推给下一层交换 {@code nextPartition.exchangeId}。
     * <b>不回传 Master</b> —— 数据留在 Worker 侧继续流向下一层 join，靠 exchange 的
     * complete/onComplete 协议让下一层 {@link ShuffleReader} 收齐 {@code partitionEnd}。
     * 只有最末层 join 用普通 {@link Consumer} 回传 Master。
     */
    record MiddleJoinConsumer(String fragmentId, String selfWorkerId,
                              ExchangeInStep leftIn, ExchangeInStep rightIn,
                              JoinStep join, PartitionStep nextPartition,
                              ExpandStep expand, List<GroupKeyExpression> nextComputed) implements FragmentSpec {

        public MiddleJoinConsumer {
            nextComputed = nextComputed == null ? List.of() : List.copyOf(nextComputed);
        }

        /** 无多维聚合展开（普通多 join 链中间层 / 末层聚合）。 */
        public MiddleJoinConsumer(String fragmentId, String selfWorkerId,
                                  ExchangeInStep leftIn, ExchangeInStep rightIn,
                                  JoinStep join, PartitionStep nextPartition) {
            this(fragmentId, selfWorkerId, leftIn, rightIn, join, nextPartition, null, List.of());
        }

        /** 附加多维聚合展开：本地 join 输出之后、按分组键重哈希之前复制行。 */
        public MiddleJoinConsumer withExpand(ExpandStep e) {
            return new MiddleJoinConsumer(fragmentId, selfWorkerId, leftIn, rightIn, join, nextPartition,
                    e, nextComputed);
        }

        /** 附加下一窗口分组键表达式合成列：本地 join 输出之后、重哈希之前本机物化。 */
        public MiddleJoinConsumer withNextComputed(List<GroupKeyExpression> computed) {
            return new MiddleJoinConsumer(fragmentId, selfWorkerId, leftIn, rightIn, join, nextPartition,
                    expand, computed);
        }

        @Override
        public boolean isProducer() {
            return false;
        }

        @Override
        public String describe() {
            return "local join " + join.leftKeyColumns() + "=" + join.rightKeyColumns()
                    + FragmentSpec.joinSuffix(join)
                    + FragmentSpec.expandSuffix(expand)
                    + " → repartition(" + nextPartition.keyColumns() + ")"
                    + " → " + nextPartition.exchangeId()
                    + " (leftIn=" + leftIn.exchangeId() + ", rightIn=" + rightIn.exchangeId() + ")";
        }
    }

    /**
     * 接收端（派生表分布式 join 的聚合中间层）：own 桶原始数据 → 本地 {@link HashAggregationOperator}
     * → （派生内层 HAVING：聚合输出行级过滤）→ <b>把聚合输出按派生表一侧的 join 键重新哈希分区</b>
     * → 推给 join 交换 {@code nextPartition.exchangeId}。<b>不回传 Master</b> —— 派生表 = 单表
     * GROUP BY 的输出留在 Worker 侧继续流向 join，靠 exchange 的 complete/onComplete 协议让下一层
     * {@link ShuffleReader} 收齐 {@code partitionEnd}；只有最终 join 用普通 {@link Consumer} 回传
     * Master。每分区一个该消费者（与 {@link AggregateConsumer} 同构）：聚合评估并行度 = 分区数，
     * 派发到 {@code layout.ownerOf(p)}，{@code in} 只收自己这一个桶。
     *
     * <p>{@code filterAfterAggregate} 为可空派生内层 HAVING 谓词（已做聚合→输出列名替换，
     * 列 ∈ 本地聚合输出列），在 Worker 侧接在 HashAggregation 之后、重分区之前执行 —— 每 key 的
     * 全部行都在同一分区本地聚齐，HAVING 按 key（=聚合输出行）过滤再重分区，语义与单机一致。</p>
     */
    record MiddleAggregateConsumer(String fragmentId, String selfWorkerId,
                                   ExchangeInStep in, AggregateStep aggregate,
                                   AggregateStep followUp,
                                   PartitionStep nextPartition,
                                   String filterAfterAggregate,
                                   List<GroupKeyExpression> nextComputed) implements FragmentSpec {

        public MiddleAggregateConsumer {
            nextComputed = nextComputed == null ? List.of() : List.copyOf(nextComputed);
        }

        /** 无二级聚合 / 无窗口键表达式合成列（普通派生聚合中间层）。 */
        public MiddleAggregateConsumer(String fragmentId, String selfWorkerId,
                                       ExchangeInStep in, AggregateStep aggregate,
                                       PartitionStep nextPartition,
                                       String filterAfterAggregate) {
            this(fragmentId, selfWorkerId, in, aggregate, null, nextPartition, filterAfterAggregate, List.of());
        }

        /** 无二级聚合 + 下一组窗口键表达式合成列（普通派生聚合中间层，保留旧调用点）。 */
        public MiddleAggregateConsumer(String fragmentId, String selfWorkerId,
                                       ExchangeInStep in, AggregateStep aggregate,
                                       PartitionStep nextPartition, String filterAfterAggregate,
                                       List<GroupKeyExpression> nextComputed) {
            this(fragmentId, selfWorkerId, in, aggregate, null, nextPartition,
                    filterAfterAggregate, nextComputed);
        }

        /**
         * 附加<b>二级聚合</b>（派生体为两级聚合：{@code max(sum(x))} 形态）：本地聚合输出再喂给
         * 同 Worker 的第二次 HashAggregation，之后才走 HAVING / 重分区。要求两级分组键相同
         * （协分区不变量：分区键 = 分组键 ⇒ 两级都是分区内终态聚合）。
         */
        public MiddleAggregateConsumer withFollowUp(AggregateStep f) {
            return new MiddleAggregateConsumer(fragmentId, selfWorkerId, in, aggregate, f, nextPartition,
                    filterAfterAggregate, nextComputed);
        }

        /** 附加下一窗口分组键表达式合成列：本地聚合输出之后、重哈希之前本机物化。 */
        public MiddleAggregateConsumer withNextComputed(List<GroupKeyExpression> computed) {
            return new MiddleAggregateConsumer(fragmentId, selfWorkerId, in, aggregate, followUp,
                    nextPartition, filterAfterAggregate, computed);
        }

        @Override
        public boolean isProducer() {
            return false;
        }

        @Override
        public String describe() {
            return "local aggregate " + aggregate.keyColumns()
                    + (followUp == null ? "" : " → aggregate " + followUp.keyColumns())
                    + (filterAfterAggregate == null ? ""
                        : " → having(" + filterAfterAggregate + ")")
                    + " → repartition(" + nextPartition.keyColumns() + ")"
                    + " → " + nextPartition.exchangeId()
                    + " (in=" + in.exchangeId() + ")";
        }
    }

    /**
     * 接收端（派生表分布式 join 的<b>窗口</b>中间层）：own 桶原始数据 → {@link WindowSortOperator}
     * 本地排序 + {@link WindowFunctionOperator} 逐行计算 → <b>把窗口输出按派生表一侧的 join 键
     * 重新哈希分区</b> → 推给 join 交换 {@code nextPartition.exchangeId}。<b>不回传 Master</b>。
     *
     * <p>派生表 = {@code (SELECT cols, win() OVER (PARTITION BY k ORDER BY o) FROM t WHERE p)} 的
     * 输出留在 Worker 侧继续流向 join，靠 exchange 的 complete/onComplete 协议让最终 join 的
     * {@link ShuffleReader} 收齐 {@code partitionEnd}；只有最终 join 用普通 {@link Consumer} 回传
     * Master。每分区一个该消费者（与 {@link MiddleAggregateConsumer} 同构）：窗口计算并行度 =
     * 分区数，派发到 {@code layout.ownerOf(p)}，{@code in} 只收自己这一个桶。</p>
     *
     * <p><b>正确性</b>：窗口的 PARTITION BY 键经同一 round-robin owner 映射哈希 → 每个窗口分区
     * 完整落在一个桶 → 本地排序即该键的完整窗口（co-partitioning 不变量），与单机语义一致。</p>
     */
    record MiddleWindowConsumer(String fragmentId, String selfWorkerId,
                                ExchangeInStep in, WindowStep window,
                                PartitionStep nextPartition,
                                List<GroupKeyExpression> nextComputed) implements FragmentSpec {

        public MiddleWindowConsumer {
            nextComputed = nextComputed == null ? List.of() : List.copyOf(nextComputed);
        }

        /** 无下一组键表达式合成列（普通窗口链中间层）。 */
        public MiddleWindowConsumer(String fragmentId, String selfWorkerId,
                                    ExchangeInStep in, WindowStep window,
                                    PartitionStep nextPartition) {
            this(fragmentId, selfWorkerId, in, window, nextPartition, List.of());
        }

        /** 附加下一窗口分组键表达式合成列：本地窗口输出之后、重哈希之前本机物化。 */
        public MiddleWindowConsumer withNextComputed(List<GroupKeyExpression> computed) {
            return new MiddleWindowConsumer(fragmentId, selfWorkerId, in, window, nextPartition, computed);
        }

        @Override
        public boolean isProducer() {
            return false;
        }

        @Override
        public String describe() {
            return "local window partitionBy=" + window.partitionBy()
                    + " → repartition(" + nextPartition.keyColumns() + ")"
                    + " → " + nextPartition.exchangeId()
                    + " (in=" + in.exchangeId() + ")";
        }
    }
}
