package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import com.dojao.spi.function.AggregateFunction;
import com.dojao.spi.function.AggregateFunctionRegistry;
import com.dojao.spi.util.NestedTypeSupport;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * 窗口函数计算算子 —— 接收来自 {@link WindowSortOperator} 的已排序页面流，
 * 通过逐行比较 PARTITION BY 键值检测分区边界，逐行计算窗口函数，
 * 将结果追加为新列后输出。
 *
 * <h3>分区边界检测</h3>
 * <p>与 {@link HashAggregationOperator} 不同，窗口函数的 PARTITION BY
 * 是逻辑分区而非物理 hash 桶。每一行都可能属于不同的逻辑分区。
 * 因为上游数据已按 (PARTITION BY, ORDER BY) 排序，只需比较相邻行的
 * PARTITION BY 键值是否变化即可检测分区边界。
 *
 * <h3>支持的窗口函数</h3>
 * <ul>
 *   <li>{@link WindowFunctionType#ROW_NUMBER} — 简单行计数，每分区从 1 开始</li>
 *   <li>{@link WindowFunctionType#RANK} — 同值同排名，下一排名跳过（需要 ORDER BY 列）</li>
 *   <li>{@link WindowFunctionType#DENSE_RANK} — 同值同排名，下一排名连续（需要 ORDER BY 列）</li>
 *   <li>{@link WindowFunctionType#SUM} — 运行求和（默认帧）或 ROWS 帧滑窗求和</li>
 *   <li>{@link WindowFunctionType#AVG} — 运行平均（SUM/COUNT）或 ROWS 帧滑窗平均</li>
 * </ul>
 *
 * <h3>ROWS 帧</h3>
 * <p>SUM/AVG/COUNT/MIN/MAX 支持显式 {@code ROWS} 帧（见 {@link WindowFrame}）：帧描述
 * {@code [i - startBack, i - endBack]} 相对当前行的回溯区间。<b>滑动状态是增量维护的</b>（见
 * {@link #advanceFrame}）：SUM/AVG 进出一行各加减一次、MIN/MAX 走单调队列、COUNT 直接由帧上下界算出，
 * 每行 <b>O(1)</b> —— 与帧宽无关。无帧（{@code frame == null}）时保持原「运行窗口」快速路径。
 * 需未来行的 {@code FOLLOWING} 帧在计划期被拒绝（参见 {@link WindowFrame#parse(String)}）。</p>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 *   new WindowFunctionOperator(
 *       sort,
 *       List.of("product_id"),                         // PARTITION BY
 *       List.of(WindowFunctionSpec.rowNumber("rn")),
 *       List.of("duration_seconds")                    // ORDER BY
 *   );
 * }</pre>
 *
 * @author jiaxin
 * @version 1.1
 * @date 2026/7/18
 */
public class WindowFunctionOperator extends AbstractMeasurableOperator implements MergeProgress, Explainable {

    /** EXPLAIN 明细：窗口分区键 / 排序键 / 窗口函数。 */
    @Override
    public List<String> explainLines() {
        return List.of("window partitionBy=" + (partitionByColumns.isEmpty() ? "<all>" : partitionByColumns)
                + " orderBy=" + (orderByColumns.isEmpty() ? "<none>" : orderByColumns)
                + " functions=" + functions.stream()
                        .map(f -> f.type() + (f.functionName() != null ? "(" + f.functionName() + ")" : "")
                                + " AS " + f.outputColumn()).toList());
    }

    private final Operator upstream;
    private final List<String> partitionByColumns;
    private final List<WindowFunctionSpec> functions;
    private final List<String> orderByColumns;
    private final BufferAllocator allocator;

    // ---- Schema ----
    private Schema inputSchema;
    private List<Field> outputFields;
    /** 列名 → 输入列索引（O(1) 查找，避免 findField + indexOf 的可靠性问题） */
    private Map<String, Integer> columnNameToIndex;
    /** PARTITION BY 列在 input 中的列索引 */
    private int[] partitionByIndices;
    /** ORDER BY 列在 input 中的列索引 */
    private int[] orderByIndices;

    // ---- 当前分区状态 ----
    /** 上一个逻辑分区的 PARTITION BY 键值（多键路径用于检测分区边界；跨页延续） */
    private List<Object> lastPartitionByValues;
    /** 单键路径：上一页末行的分区键<b>快照</b>（规范化字节；跨页比较用，避免引用已释放的向量缓冲） */
    private byte[] lastPageEndKey;
    /** 是否已处理过至少一行（用于区分「整个数据流的第一行」与「新页首行」） */
    private boolean hasLastRow;
    private long rowNumber;
    /** 上一行的 ORDER BY 键值列表（用于 RANK/DENSE_RANK 比较） */
    private List<Object> lastOrderByValues;
    private long rankValue;
    private long rankSameCount;
    private long denseRankValue;
    /** 每个函数的运行累加值 */
    private final double[] runningSums;
    private final long[] runningCounts;

    // ---- ROWS 帧状态（仅内置聚合 SUM/AVG/COUNT/MIN/MAX 且显式 ROWS 帧时非 null；null = 运行帧） ----
    /** 每个函数的 ROWS 帧；null = 运行/默认帧（走上面的运行聚合路径） */
    private final WindowFrame[] frames;
    /** 每个帧函数的环形缓冲（容量 = startBack + 1）；非帧函数为 null */
    private double[][] frameRing;
    /** 环形缓冲槽位是否为「非 NULL 值」 */
    private boolean[][] frameRingValid;
    /** 分区内已推入环形缓冲的行数（用于环形索引与「帧内可用行数」判断） */
    private final int[] frameFilled;
    /** 帧内有效值之和（SUM/AVG 增量维护，避免每行重扫整个帧） */
    private final double[] frameAccSum;
    /** 帧内有效值个数（AVG 增量维护） */
    private final long[] frameAccCnt;
    /** MIN/MAX 单调队列的值（容量 = startBack + 1，环形数组） */
    private final double[][] mmVal;
    /** MIN/MAX 单调队列的<b>分区内绝对行号</b>（用于淘汰滑出帧的队头） */
    private final int[][] mmIdx;
    private final int[] mmHead;
    private final int[] mmSize;
    /** SUM/AVG 滑动增量每 {@code 2^RECOMPUTE_SHIFT} 行做一次整帧重算，界定浮点加减漂移。 */
    private static final int RECOMPUTE_SHIFT = 16;

    // ---- LAG / LEAD 状态 ----
    /** LAG 环形缓冲区（每个 LAG 函数一个） */
    private Object[][] lagRings;
    /** LAG 环形缓冲区当前填充量 */
    private int[] lagRingSize;

    // ---- LEAD 跨页前瞻 ----
    /** LEAD 最大偏移量（无 LEAD 函数时为 0，完全不预取后续页） */
    private final int maxLeadOffset;
    /** 当前页（尚未处理完；LEAD 前瞻与页内分区键快照都依赖它保持打开） */
    private Page currentInput;
    /** 已预取、尚未处理的后续页（首元素 = 下一处理页） */
    private final Deque<Page> lookaheadPages = new ArrayDeque<>();
    /** 上游是否已 EOF（避免反复拉取） */
    private boolean upstreamDone;
    /** 本次 processRows 的前瞻链：当前页 + 预取页（供 LEAD 定位目标行） */
    private List<VectorSchemaRoot> leadChainRoots;
    /** 前瞻链的绝对行起始偏移（长度 = 页数 + 1） */
    private int[] leadChainStarts;
    /** 前瞻链总行数 */
    private int leadChainTotal;

    // ---- 聚合状态 ----
    /** 聚合函数注册表（Session 级，可通过构造函数或 setter 注入） */
    private final AggregateFunctionRegistry aggregateFunctionRegistry;
    /** 每个聚合函数的实例（通过 registry 按名称查找） */
    private final AggregateFunction<?>[] aggregateInstances;
    /** 每个聚合函数的运行时状态 */
    private final Object[] aggregateStates;

    // ---- 整分区聚合（无 ORDER BY 的 SUM/AVG/COUNT/MIN/MAX/自定义聚合）----
    /**
     * 哪些函数是「整分区单值」。
     *
     * <p>SQL 默认帧：<b>有 ORDER BY</b> 时是 {@code RANGE UNBOUNDED PRECEDING AND CURRENT ROW}
     * （运行聚合）；<b>无 ORDER BY</b> 时是整分区 {@code UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING}
     * —— 每一行的值都等于整个分区的聚合结果。旧实现无 ORDER BY 也走运行聚合，
     * {@code count(*) OVER (PARTITION BY status)} 因此返回 1,2,3…（用户报障 A2）。</p>
     */
    private final boolean[] partitionTotals;
    /** 是否存在整分区聚合函数（false 时完全走原有流式路径，零开销）。 */
    private final boolean partitionAggregateMode;
    /** 当前分区已缓冲的页片段（按输入顺序）。 */
    private final List<BufferedPage> bufferedPages = new ArrayList<>();
    /** 当前分区的惰性输出器（分区结束时创建，逐页吐给下游）。 */
    private PartitionEmitter emitter;
    /** 当前输入页的引用计数包装（一页可能被两个分区各取一段）。 */
    private PageRef currentPageRef;
    /** 当前输入页中已缓冲到的行号（分区边界把一页切成两半时用于续读）。 */
    private int currentRowOffset;
    /** 已缓冲字节数（超过阈值转溢写）。 */
    private long bufferedBytes;
    /** 缓冲溢写的临时目录（第一次溢写时创建，关闭时删除）。 */
    private Path bufferSpillDir;
    private int bufferSpillSeq;
    /** 当前分区的行累加器（输入列深拷贝；超阈值时溢写并置 null 重新开始）。 */
    private VectorSchemaRoot accRoot;
    private int accRows;
    /** 累加器向量的统一安全容量（所有列容量最小值）。 */
    private int accCapacity;
    /** 当前输入页根 → 累加器的 TransferPair（源页或累加器重建时失效）。 */
    private VectorSchemaRoot appendTpsSource;
    private VectorSchemaRoot appendTpsTarget;
    private List<TransferPair> appendTps;
    /** 当前分区最后缓冲行的 PARTITION BY 键（跨页边界判定用）。 */
    private byte[] lastBufferedKey;
    private List<Object> lastBufferedValues;
    private boolean hasBufferedKey;
    /** 上游是否已 EOF。 */
    private boolean inputDone;
    /** 是否已解析 schema。 */
    private boolean schemaInitialized;

    /** 缓冲区溢写阈值（{@code query.window.partitionAggBufferBytes}，默认 64MB）。 */
    private static final long BUFFER_SPILL_THRESHOLD = ResourceConfig.windowPartitionAggBufferBytes();

    /** 整分区模式输出页的目标行数（控制下游单页内存）。 */
    private static final int WHOLE_PARTITION_PAGE_ROWS = 4096;

    // ---- 每个窗口函数在 functions 列表中的索引映射 ----
    private final Map<WindowFunctionType, List<Integer>> funcIndices;

    /** Phase 2 下推导出值映射：函数类型 → 预计算值（如 ROW_NUMBER → 5, COUNT → 5）。
     * <p>激活时对应函数直接填该值而不逐行计算，因为 Phase 2 只输出单行/组。 */
    private final Map<WindowFunctionType, Long> valueOverrides = new EnumMap<>(WindowFunctionType.class);

    /**
     * @param upstream           上游算子（通常是 WindowSortOperator）
     * @param partitionByColumns PARTITION BY 列名列表
     * @param functions          窗口函数规格列表
     * @param orderByColumns     ORDER BY 列名列表
     */
    public WindowFunctionOperator(Operator upstream,
                                  List<String> partitionByColumns,
                                  List<WindowFunctionSpec> functions,
                                  List<String> orderByColumns) {
        this(upstream, partitionByColumns, functions, orderByColumns, null);
    }

    /**
     * @param upstream           上游算子（通常是 WindowSortOperator）
     * @param partitionByColumns PARTITION BY 列名列表
     * @param functions          窗口函数规格列表
     * @param orderByColumns     ORDER BY 列名列表
     * @param aggregateFunctionRegistry 聚合函数注册表（null 表示无聚合）
     */
    public WindowFunctionOperator(Operator upstream,
                                  List<String> partitionByColumns,
                                  List<WindowFunctionSpec> functions,
                                  List<String> orderByColumns,
                                  AggregateFunctionRegistry aggregateFunctionRegistry) {
        super("WindowFunction", "WindowFunction", 1);
        this.upstream = upstream;
        this.partitionByColumns = partitionByColumns;
        this.functions = functions;
        this.orderByColumns = orderByColumns;
        // 分配器是进程级共享根（NodeMemory）的子 —— 窗口/溢写缓冲的堆外占用进统一账本，受共享根硬上限约束。
        this.allocator = NodeMemory.shared()
                .newOperatorAllocator("window", ResourceConfig.queryDirectLimit());
        this.aggregateFunctionRegistry = aggregateFunctionRegistry;

        this.funcIndices = new EnumMap<>(WindowFunctionType.class);
        for (int i = 0; i < functions.size(); i++) {
            funcIndices.computeIfAbsent(functions.get(i).type(), k -> new ArrayList<>()).add(i);
        }
        this.runningSums = new double[functions.size()];
        this.runningCounts = new long[functions.size()];
        // 为每个 LAG 函数分配环形缓冲区
        this.lagRings = new Object[functions.size()][];
        this.lagRingSize = new int[functions.size()];
        int maxLead = 0;
        for (int i = 0; i < functions.size(); i++) {
            WindowFunctionSpec spec = functions.get(i);
            if (spec.type() == WindowFunctionType.LAG) {
                int off = spec.offset();
                // 规划期已拒绝负数（Planner.parseLagLeadOffset）；这里兜底防直接构造 spec 的调用方
                // 拿到 NegativeArraySizeException 这种无信息异常。offset=0 合法（返回当前行），空数组即可。
                if (off < 0) throw new IllegalArgumentException("LAG 偏移量不能为负数: " + off);
                this.lagRings[i] = new Object[off];
            } else if (spec.type() == WindowFunctionType.LEAD) {
                if (spec.offset() < 0) {
                    throw new IllegalArgumentException("LEAD 偏移量不能为负数: " + spec.offset());
                }
                // offset=0 是当前行，无需前瞻；≥1 时按实际偏移预取。
                maxLead = Math.max(maxLead, spec.offset());
            }
        }
        this.maxLeadOffset = maxLead;
        // 为每个显式 ROWS 帧的内置聚合函数分配环形缓冲（容量 = startBack + 1）
        this.frames = new WindowFrame[functions.size()];
        this.frameRing = new double[functions.size()][];
        this.frameRingValid = new boolean[functions.size()][];
        this.frameFilled = new int[functions.size()];
        this.frameAccSum = new double[functions.size()];
        this.frameAccCnt = new long[functions.size()];
        this.mmVal = new double[functions.size()][];
        this.mmIdx = new int[functions.size()][];
        this.mmHead = new int[functions.size()];
        this.mmSize = new int[functions.size()];
        for (int i = 0; i < functions.size(); i++) {
            WindowFunctionSpec spec = functions.get(i);
            if (spec.frame() != null && isFrameAggregate(spec.type())) {
                this.frames[i] = spec.frame();
                int cap = spec.frame().startBack() + 1;
                this.frameRing[i] = new double[cap];
                this.frameRingValid[i] = new boolean[cap];
                if (spec.type() == WindowFunctionType.MIN || spec.type() == WindowFunctionType.MAX) {
                    this.mmVal[i] = new double[cap];
                    this.mmIdx[i] = new int[cap];
                }
            }
        }
        // 初始化聚合实例和状态
        this.aggregateInstances = new AggregateFunction<?>[functions.size()];
        this.aggregateStates = new Object[functions.size()];
        for (int i = 0; i < functions.size(); i++) {
            WindowFunctionSpec spec = functions.get(i);
            if (spec.type() == WindowFunctionType.AGGREGATE && aggregateFunctionRegistry != null) {
                String name = spec.functionName();
                AggregateFunction<?> agg = aggregateFunctionRegistry.get(name);
                if (agg == null) {
                    throw new IllegalArgumentException("聚合函数未注册: " + name);
                }
                this.aggregateInstances[i] = agg;
                this.aggregateStates[i] = agg.createState();
            }
        }
        // ★ 整分区聚合判定（A2）：SQL 默认帧在「无 ORDER BY」时是
        //   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ⇒ 每一行的聚合值 = 整个分区的聚合结果。
        //   旧实现一律按运行聚合计算，count(*) OVER (PARTITION BY p) 因此返回 1,2,3…。
        this.partitionTotals = new boolean[functions.size()];
        boolean anyTotal = false;
        if (orderByColumns.isEmpty()) {
            for (int i = 0; i < functions.size(); i++) {
                WindowFunctionSpec spec = functions.get(i);
                if (spec.frame() != null) continue;   // 显式 ROWS 帧覆盖默认帧语义
                switch (spec.type()) {
                    case SUM, AVG, COUNT, MIN, MAX -> {
                        this.partitionTotals[i] = true;
                        anyTotal = true;
                    }
                    case AGGREGATE -> {
                        if (this.aggregateInstances[i] != null) {
                            this.partitionTotals[i] = true;
                            anyTotal = true;
                        }
                    }
                    default -> { }
                }
            }
        }
        this.partitionAggregateMode = anyTotal;
        this.lastPartitionByValues = null;
    }

    // ---- Phase 2 Filter 下推接口 ----

    /** Phase 2 Filter 下推：设置预计算的窗口函数值（每分组输出单行时使用）。 */
    public void setValueOverride(WindowFunctionType type, long value) {
        valueOverrides.put(type, value);
    }

    /** 获取下游的 WindowSortOperator（用于 Planner 注入 WindowFilterConfig）。 */
    public WindowSortOperator getWindowSort() {
        return (WindowSortOperator) upstream;
    }

    /** 获取窗口函数规格列表（用于 Planner 匹配下推列）。 */
    public List<WindowFunctionSpec> getFunctionSpecs() {
        return Collections.unmodifiableList(functions);
    }

    /**
     * 归并进度透传下游 {@link WindowSortOperator}：本算子只做逐行窗口计算，重活在排序（可溢写）。
     * Worker 心跳算到算子链顶（本算子）即可覆盖链上全部溢写归并。
     */
    @Override
    public long mergeProgressTicks() {
        return upstream instanceof MergeProgress mp ? mp.mergeProgressTicks() : 0L;
    }

    // ==================== 算子生命周期 ====================
    @Override
    protected void doOpen() throws Exception {
        upstream.open();
    }

    @Override
    protected Page doNext() throws Exception {
        if (partitionAggregateMode) {
            return doNextWholePartition();
        }
        if (currentInput == null) {
            Page input = upstream.next();
            if (input.isEOF()) {
                return Page.eof();
            }
            recordInput(input);
            // ★initSchema 可能抛（嵌套列能力闸门）：此时 currentInput 还没接管，必须就地关页，
            //   否则异常退出后这张页永不释放（doClose 只清 currentInput / lookahead）。
            try {
                initSchema(input.getRoot());
            } catch (Throwable t) {
                input.close();
                releaseInput(input);
                throw t;
            }
            currentInput = input;
        } else if (inputSchema == null) {
            initSchema(currentInput.getRoot());
        }

        // ★ LEAD 前瞻：本页末尾若干行的「未来行」在后续页里 —— 先预取至多 maxLeadOffset 行。
        //   持有页面不关闭（LEAD 只是读值，页本身稍后仍按顺序处理），内存上限 ≈ 一页 + offset 行。
        fillLookahead();

        VectorSchemaRoot root = currentInput.getRoot();
        int rowCount = root.getRowCount();
        VectorSchemaRoot outputRoot = buildOutputRoot(rowCount);
        processRows(root, outputRoot, rowCount);

        Page done = currentInput;
        currentInput = lookaheadPages.pollFirst();
        done.close();
        releaseInput(done);
        return Page.fromRoot(outputRoot);
    }

    /** 首次看到输入页时解析 schema 并构建输出列。 */
    private void initSchema(VectorSchemaRoot root) {
        if (inputSchema != null) return;
        inputSchema = root.getSchema();
        // 构建列名 → 索引映射，用于 O(1) 且可靠地查找聚合列
        columnNameToIndex = new HashMap<>();
        List<Field> fields = inputSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            columnNameToIndex.put(fields.get(i).getName(), i);
        }
        resolvePartitionByIndices();
        resolveOrderByIndices();
        // 嵌套列没有定义窗口语义（分区相等性 / 排序 / 函数参数取值），入口显式拒绝
        for (WindowFunctionSpec spec : functions) {
            if (spec.inputColumn() == null) continue;
            Field arg = inputSchema.findField(spec.inputColumn());
            NestedTypeSupport.requireSupported(arg, "窗口函数 " + spec.type() + " 的参数列");
        }
        buildOutputSchema();
    }

    /**
     * 预取后续页，保证「当前页最后一行 + maxLeadOffset」仍在已持有数据之内。
     *
     * <p>LEAD 的标准语义需要未来行；而 {@code WindowSortOperator} 在流式/溢写路径下可在
     * <b>分组中间</b>切页（见其 {@code streamBatchFileFlat}）—— 旧实现遇到页尾直接返回默认值，
     * 大分组跨页时 {@code lead(x,1)} 会在每个页尾静默变成 NULL。这里按需保持后续页打开。</p>
     */
    private void fillLookahead() {
        if (maxLeadOffset <= 0 || upstreamDone) return;
        int buffered = 0;
        for (Page p : lookaheadPages) {
            buffered += p.getRowCount();
        }
        while (buffered < maxLeadOffset) {
            Page p = upstream.next();
            if (p.isEOF()) {
                upstreamDone = true;
                break;
            }
            if (p.getRowCount() == 0) {
                p.close();   // 空页对 LEAD 无贡献，直接丢弃（不打断预取）
                continue;
            }
            recordInput(p);
            lookaheadPages.addLast(p);
            buffered += p.getRowCount();
        }
    }

    @Override
    protected void doClose() throws Exception {
        if (emitter != null) {
            emitter.close();
            emitter = null;
        }
        if (currentInput != null) {
            try { currentInput.close(); } catch (Exception ignored) { }
            releaseInput(currentInput);
            currentInput = null;
        }
        Page p;
        while ((p = lookaheadPages.pollFirst()) != null) {
            try { p.close(); } catch (Exception ignored) { }
            releaseInput(p);
        }
        if (currentPageRef != null) {
            releasePageRef(currentPageRef);
            currentPageRef = null;
        }
        for (BufferedPage bp : bufferedPages) bp.closeOwned();
        bufferedPages.clear();
        if (accRoot != null) {
            try { accRoot.close(); } catch (Exception ignored) { }
            accRoot = null;
            accRows = 0;
        }
        cleanupSpillDir();
        allocator.close();
        upstream.close();
    }

    // ==================== 核心处理 ====================

    private void processRows(VectorSchemaRoot input, VectorSchemaRoot output, int rowCount) {
        buildLeadChain(input, rowCount);
        int inCols = inputSchema.getFields().size();
        boolean needOrderBy = needOrderByValues();

        // 预创建 TransferPair：copyValueSafe 正确更新 lastSet，避免 fillHoles 清零 var offset。
        List<TransferPair> transfers = new ArrayList<>(inCols);
        for (int col = 0; col < inCols; col++) {
            transfers.add(input.getVector(col).makeTransferPair(output.getVector(col)));
        }

        for (int row = 0; row < rowCount; row++) {
            detectPartitionBoundary(input, row);

            // 逐行深拷贝输入列（copyValueSafe 保证 var lastSet 正确推进）
            for (TransferPair tp : transfers) {
                tp.copyValueSafe(row, row);
            }

            List<Object> currentOrderByValues = needOrderBy ? buildOrderByValues(input, row) : null;
            for (int fi = 0; fi < functions.size(); fi++) {
                computeFunction(fi, functions.get(fi), input, row, currentOrderByValues,
                        output.getVector(inCols + fi));
            }
            if (needOrderBy) lastOrderByValues = currentOrderByValues;
            rowNumber++;
        }

        // 记住「已处理过行」并快照本页末行的分区键：让下一（若本页是分组中间页）分区状态得以延续。
        if (rowCount > 0) {
            hasLastRow = true;
            if (partitionByIndices.length == 1) {
                lastPageEndKey = snapshotKeyBytes(input.getVector(partitionByIndices[0]), rowCount - 1);
            }
        }

        // 设置所有输出向量的 valueCount（窗口函数列也已写入，lastSet 已由 copyValueSafe 推进）
        for (FieldVector vec : output.getFieldVectors()) {
            vec.setValueCount(rowCount);
        }
    }

    /** 构建本次处理的前瞻链（当前页 + 预取页）及其绝对行偏移，供 LEAD 定位目标行。 */
    private void buildLeadChain(VectorSchemaRoot input, int rowCount) {
        leadChainRoots = null;
        leadChainStarts = null;
        leadChainTotal = 0;
        if (maxLeadOffset <= 0) return;
        int pages = 1 + lookaheadPages.size();
        List<VectorSchemaRoot> roots = new ArrayList<>(pages);
        int[] starts = new int[pages + 1];
        roots.add(input);
        starts[1] = rowCount;
        int i = 1;
        for (Page p : lookaheadPages) {
            roots.add(p.getRoot());
            starts[i + 1] = starts[i] + p.getRowCount();
            i++;
        }
        leadChainRoots = roots;
        leadChainStarts = starts;
        leadChainTotal = starts[i];
    }

    /** 目标绝对行所在的前瞻链页下标（页数通常为 1~2，线性查找足够）。 */
    private int leadPageIndex(int absoluteRow) {
        for (int i = 0; i < leadChainRoots.size(); i++) {
            if (absoluteRow < leadChainStarts[i + 1]) return i;
        }
        return leadChainRoots.size() - 1;
    }

    /**
     * 判断两行是否属于同一 PARTITION BY 分区。
     *
     * <p>输入已按 (PARTITION BY, ORDER BY) 排序 ⇒ 端点键相等即可判定整段同分区。
     * 跨页时键分处两个向量，退化为规范化字节比较（键通常 int64，代价极小）。</p>
     */
    private boolean samePartition(VectorSchemaRoot a, int ra, VectorSchemaRoot b, int rb) {
        if (partitionByIndices.length == 0) return true;
        if (partitionByIndices.length == 1) {
            FieldVector va = a.getVector(partitionByIndices[0]);
            FieldVector vb = b.getVector(partitionByIndices[0]);
            if (va == vb) return partitionKeyEquals(va, ra, rb);
            return java.util.Arrays.equals(snapshotKeyBytes(va, ra), snapshotKeyBytes(vb, rb));
        }
        return valuesEqual(buildPartitionByValues(a, ra), buildPartitionByValues(b, rb));
    }

    /**
     * 检测当前行是否进入新分区，进入则重置分区状态。
     *
     * <p>单键路径零分配：直接比较相邻两行的 PARTITION BY 键值（上游已按该键排序，同键必相邻），
     * 避免逐行 {@code buildPartitionByValues} 的 ArrayList 装箱。
     */
    private void detectPartitionBoundary(VectorSchemaRoot input, int row) {
        if (partitionByIndices.length == 0) {
            // 全局窗口（无 PARTITION BY）→ 仅整个数据流的<b>首行</b>触发一次重置（不再每页 row==0 重置，
            // 否则全局窗口跨页时序号/累加值会被错误重置）。
            if (row == 0 && !hasLastRow) resetPartitionState();
            return;
        }
        if (partitionByIndices.length == 1) {
            FieldVector pv = input.getVector(partitionByIndices[0]);
            if (row == 0) {
                // 页首行：整条流的首行 → 重置；否则与上一页末行的分区键快照比较 ——
                // 分组可以跨页，键相同则<b>延续</b>分区状态。
                if (!hasLastRow || !java.util.Arrays.equals(lastPageEndKey, snapshotKeyBytes(pv, row))) {
                    resetPartitionState();
                }
            } else if (!partitionKeyEquals(pv, row, row - 1)) {
                resetPartitionState();   // 同页内：零分配的相邻行比较
            }
            return;
        }
        List<Object> current = buildPartitionByValues(input, row);
        if (isPartitionBoundary(lastPartitionByValues, current)) resetPartitionState();
        lastPartitionByValues = current;
    }

    /**
     * 把某行的分区键快照成规范化字节数组（跨页比较用）。
     *
     * <p>为什么不直接存 {@code getObject()}：{@link org.apache.arrow.vector.ipc.ArrowFileReader} 会复用同一个
     * {@code VectorSchemaRoot}，页间该缓冲可能已被回收/覆盖 —— 存引用会在下一页读到已释放内存而误判边界。
     * 这里把键值拷成独立 {@code byte[]}（定长取固定字节、变长取 offset 区间），跨页安全。null → null。</p>
     */
    private static byte[] snapshotKeyBytes(FieldVector vec, int row) {
        if (vec.isNull(row)) return null;
        ArrowBuf data = vec.getDataBuffer();
        int tw = RowAccumulator.typeWidthOf(vec.getField().getType());
        if (tw > 0) {
            byte[] b = new byte[tw];
            long base = (long) row * tw;
            for (int i = 0; i < tw; i++) b[i] = data.getByte(base + i);
            return b;
        }
        ArrowBuf off = vec.getOffsetBuffer();
        int s = off.getInt((long) row * 4);
        int e = off.getInt((long) (row + 1) * 4);
        byte[] b = new byte[e - s];
        for (int i = 0; i < b.length; i++) b[i] = data.getByte(s + i);
        return b;
    }

    /** 当前 functions 中是否有依赖 ORDER BY 值（RANK/DENSE_RANK）的函数，无则跳过逐行提取。 */
    private boolean needOrderByValues() {
        for (WindowFunctionSpec s : functions) {
            WindowFunctionType t = s.type();
            if (t == WindowFunctionType.RANK || t == WindowFunctionType.DENSE_RANK) return true;
        }
        return false;
    }

    /** 零分配比较同一向量中两行的值是否相等（含 NULL 与变长列）。 */
    private static boolean partitionKeyEquals(FieldVector vec, int a, int b) {
        boolean na = vec.isNull(a), nb = vec.isNull(b);
        if (na || nb) return na && nb;
        int tw = RowAccumulator.typeWidthOf(vec.getField().getType());
        if (tw == 8) return vec.getDataBuffer().getLong((long) a * 8) == vec.getDataBuffer().getLong((long) b * 8);
        if (tw == 4) return vec.getDataBuffer().getInt((long) a * 4) == vec.getDataBuffer().getInt((long) b * 4);
        if (tw == 2) return vec.getDataBuffer().getShort((long) a * 2) == vec.getDataBuffer().getShort((long) b * 2);
        if (tw == 1) return vec.getDataBuffer().getByte(a) == vec.getDataBuffer().getByte(b);
        return varCharEquals(vec, a, b);
    }

    /** 零分配 VarChar/Binary 相等比较：直接比 offset 区间字节。 */
    private static boolean varCharEquals(FieldVector vec, int a, int b) {
        ArrowBuf off = vec.getOffsetBuffer();
        ArrowBuf data = vec.getDataBuffer();
        int sa = off.getInt((long) a * 4), ea = off.getInt((long) (a + 1) * 4);
        int sb = off.getInt((long) b * 4), eb = off.getInt((long) (b + 1) * 4);
        if (ea - sa != eb - sb) return false;
        for (int i = sa, j = sb; i < ea; i++, j++) {
            if (data.getByte(i) != data.getByte(j)) return false;
        }
        return true;
    }

    /**
     * 提取当前行的 PARTITION BY 键值列表。
     */
    private List<Object> buildPartitionByValues(VectorSchemaRoot root, int row) {
        if (partitionByIndices == null || partitionByIndices.length == 0) {
            return List.of();
        }
        List<Object> values = new ArrayList<>(partitionByIndices.length);
        for (int idx : partitionByIndices) {
            FieldVector vec = root.getVector(idx);
            values.add(vec.isNull(row) ? null : vec.getObject(row));
        }
        return values;
    }

    /**
     * 提取当前行的 ORDER BY 键值列表。
     */
    private List<Object> buildOrderByValues(VectorSchemaRoot root, int row) {
        if (orderByIndices == null || orderByIndices.length == 0) {
            return List.of();
        }
        List<Object> values = new ArrayList<>(orderByIndices.length);
        for (int idx : orderByIndices) {
            FieldVector vec = root.getVector(idx);
            values.add(vec.isNull(row) ? null : vec.getObject(row));
        }
        return values;
    }

    /**
     * 判断两个 PARTITION BY 值列表是否代表不同的分区。
     * 当 last 为 null（首行）或两者不相等时，表示进入新分区。
     */
    private boolean isPartitionBoundary(List<Object> last, List<Object> current) {
        if (last == null) return true; // 数据流的第一行
        return !valuesEqual(last, current);
    }

    /**
     * 比较两个值列表是否相等（处理 NULL）。
     */
    private boolean valuesEqual(List<Object> a, List<Object> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            Object va = a.get(i);
            Object vb = b.get(i);
            if (va == null && vb == null) continue;
            if (va == null || vb == null) return false;
            if (!va.equals(vb)) return false;
        }
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void computeFunction(int fi, WindowFunctionSpec spec,
                                  VectorSchemaRoot input, int row,
                                  List<Object> currentOrderBy,
                                  FieldVector outVec) {
        int colIdx = spec.inputColumn() != null
                ? columnNameToIndex.getOrDefault(spec.inputColumn(), -1)
                : -1;

        switch (spec.type()) {
            case ROW_NUMBER -> {
                Long ov = valueOverrides.get(WindowFunctionType.ROW_NUMBER);
                long val = ov != null ? ov : rowNumber + 1;
                ((BigIntVector) outVec).setSafe(row, val);
            }
            case RANK -> {
                Long ov = valueOverrides.get(WindowFunctionType.RANK);
                if (ov != null) {
                    ((BigIntVector) outVec).setSafe(row, ov);
                } else {
                    if (rowNumber == 0) {
                        rankValue = 1;
                        rankSameCount = 1;
                    } else if (!valuesEqual(lastOrderByValues, currentOrderBy)) {
                        rankValue += rankSameCount;
                        rankSameCount = 1;
                    } else {
                        rankSameCount++;
                    }
                    ((BigIntVector) outVec).setSafe(row, rankValue);
                }
            }
            case DENSE_RANK -> {
                Long ov = valueOverrides.get(WindowFunctionType.DENSE_RANK);
                if (ov != null) {
                    ((BigIntVector) outVec).setSafe(row, ov);
                } else {
                    if (rowNumber == 0
                            || !valuesEqual(lastOrderByValues, currentOrderBy)) {
                        denseRankValue++;
                    }
                    ((BigIntVector) outVec).setSafe(row, denseRankValue);
                }
            }
            case SUM -> {
                Long ov = valueOverrides.get(WindowFunctionType.SUM);
                if (ov != null) {
                    ((Float8Vector) outVec).setSafe(row, ov.doubleValue());
                } else if (frames[fi] != null) {
                    FieldVector in = colIdx >= 0 ? input.getVector(colIdx) : null;
                    advanceFrame(fi, WindowFunctionType.SUM, in, row);
                    if (frameAccCnt[fi] == 0) outVec.setNull(row);
                    else ((Float8Vector) outVec).setSafe(row, frameAccSum[fi]);
                } else {
                    double val = colIdx >= 0 ? toDouble(input.getVector(colIdx), row) : 0.0;
                    runningSums[fi] += val;
                    ((Float8Vector) outVec).setSafe(row, runningSums[fi]);
                }
            }
            case AVG -> {
                Long ov = valueOverrides.get(WindowFunctionType.AVG);
                if (ov != null) {
                    ((Float8Vector) outVec).setSafe(row, ov.doubleValue());
                } else if (frames[fi] != null) {
                    FieldVector in = colIdx >= 0 ? input.getVector(colIdx) : null;
                    advanceFrame(fi, WindowFunctionType.AVG, in, row);
                    long cnt = frameAccCnt[fi];
                    if (cnt == 0) outVec.setNull(row);
                    else ((Float8Vector) outVec).setSafe(row, frameAccSum[fi] / cnt);
                } else {
                    double val = colIdx >= 0 ? toDouble(input.getVector(colIdx), row) : 0.0;
                    runningSums[fi] += val;
                    runningCounts[fi]++;
                    double avg = runningCounts[fi] > 0
                            ? runningSums[fi] / runningCounts[fi] : 0.0;
                    ((Float8Vector) outVec).setSafe(row, avg);
                }
            }
            case COUNT -> {
                Long ov = valueOverrides.get(WindowFunctionType.COUNT);
                if (ov != null) {
                    ((BigIntVector) outVec).setSafe(row, ov);
                } else if (frames[fi] != null) {
                    // COUNT 语义：统计帧内行数（与既有非帧 COUNT 一致，不区分 NULL）——
                    // 只需推进「分区内已处理行数」，帧内行数 = 帧上界 - 帧下界 + 1，O(1)。
                    advanceFrame(fi, WindowFunctionType.COUNT, null, row);
                    int lo = frames[fi].endBack(), hi = frameHi(fi);
                    ((BigIntVector) outVec).setSafe(row, hi >= lo ? hi - lo + 1 : 0);
                } else {
                    runningCounts[fi]++;
                    ((BigIntVector) outVec).setSafe(row, runningCounts[fi]);
                }
            }
            case MIN -> {
                Long ov = valueOverrides.get(WindowFunctionType.MIN);
                if (ov != null) {
                    ((Float8Vector) outVec).setSafe(row, ov.doubleValue());
                } else if (frames[fi] != null) {
                    FieldVector in = colIdx >= 0 ? input.getVector(colIdx) : null;
                    advanceFrame(fi, WindowFunctionType.MIN, in, row);
                    int head = mmHead[fi];
                    if (mmSize[fi] == 0) outVec.setNull(row);
                    else ((Float8Vector) outVec).setSafe(row, mmVal[fi][head]);
                } else {
                    double val = colIdx >= 0 ? toDouble(input.getVector(colIdx), row) : 0.0;
                    double currentMin = runningSums[fi];
                    if (rowNumber == 0 || val < currentMin) {
                        runningSums[fi] = val;
                    }
                    ((Float8Vector) outVec).setSafe(row, runningSums[fi]);
                }
            }
            case MAX -> {
                Long ov = valueOverrides.get(WindowFunctionType.MAX);
                if (ov != null) {
                    ((Float8Vector) outVec).setSafe(row, ov.doubleValue());
                } else if (frames[fi] != null) {
                    FieldVector in = colIdx >= 0 ? input.getVector(colIdx) : null;
                    advanceFrame(fi, WindowFunctionType.MAX, in, row);
                    int head = mmHead[fi];
                    if (mmSize[fi] == 0) outVec.setNull(row);
                    else ((Float8Vector) outVec).setSafe(row, mmVal[fi][head]);
                } else {
                    double val = colIdx >= 0 ? toDouble(input.getVector(colIdx), row) : 0.0;
                    double currentMax = runningSums[fi];
                    if (rowNumber == 0 || val > currentMax) {
                        runningSums[fi] = val;
                    }
                    ((Float8Vector) outVec).setSafe(row, runningSums[fi]);
                }
            }
            case LAG -> {
                int off = spec.offset();
                // colIdx < 0（列名解析不到）时不应 getVector(-1) 抛 IndexOutOfBounds —— 按 NULL 处理；
                // 正常情况下规划期已保证列存在。
                Object currentVal = colIdx >= 0 ? input.getVector(colIdx).getObject(row) : null;
                if (off == 0) {
                    // SQL 语义：lag(x, 0) = 当前行（不建环形状态，避免 size % 0 除零）
                    writeVectorValue(outVec, row, currentVal);
                } else {
                    Object[] ring = lagRings[fi];
                    int size = lagRingSize[fi];
                    int slot = size % off;
                    // ★ 必须先读后写：槽位 slot 此刻存的正是「offset 行之前」的值
                    //   （size % off == (size - off) % off）。若先写当前值再读，读到的就是刚被覆盖的本行值
                    //   —— lag 会退化成「恒返回当前行的值」，offset 与默认值形同失效。
                    if (size >= off) {
                        writeVectorValue(outVec, row, ring[slot]);
                    } else {
                        writeVectorValue(outVec, row, spec.defaultValue());
                    }
                    ring[slot] = currentVal;
                    lagRingSize[fi] = size + 1;
                }
            }
            case LEAD -> {
                int off = spec.offset();
                Object leadVal;
                if (off == 0) {
                    // SQL 语义：lead(x, 0) = 当前行（不前瞻）
                    leadVal = colIdx >= 0 ? input.getVector(colIdx).getObject(row) : null;
                } else {
                    leadVal = spec.defaultValue();
                    int target = row + off;
                    if (colIdx >= 0 && leadChainRoots != null && target < leadChainTotal) {
                        int pi = leadPageIndex(target);
                        VectorSchemaRoot targetRoot = leadChainRoots.get(pi);
                        int targetRow = target - leadChainStarts[pi];
                        // ★ 分区边界：不能借到下一个分区的行 —— 那样 lead(x,1) 在分区末行会静默
                        //   返回其他分区的值（旧实现连页内分区边界也没判）。排序保证端点键相等即同分区。
                        if (samePartition(input, row, targetRoot, targetRow)) {
                            leadVal = targetRoot.getVector(colIdx).getObject(targetRow);
                        }
                    }
                }
                writeVectorValue(outVec, row, leadVal);
            }
            case AGGREGATE -> {
                @SuppressWarnings({"rawtypes", "unchecked"})
                AggregateFunction<Object> agg = (AggregateFunction) aggregateInstances[fi];
                Object state = aggregateStates[fi];
                if (agg == null || state == null) {
                    outVec.setNull(row);
                } else {
                    // 读取输入列值 → Object[]
                    Object[] inputs = (colIdx >= 0)
                            ? new Object[]{input.getVector(colIdx).getObject(row)}
                            : new Object[0];
                    agg.accumulate(state, inputs);
                    // 每行 evaluate（OVER 窗口语义）
                    writeVectorValue(outVec, row, agg.evaluate(state));
                }
            }
        }
    }

    // ==================== ROWS 帧 ====================

    /** 是否为「按帧计算」的内置聚合类型（帧仅对这 5 个有意义）。 */
    private static boolean isFrameAggregate(WindowFunctionType type) {
        return type == WindowFunctionType.SUM || type == WindowFunctionType.AVG
                || type == WindowFunctionType.COUNT || type == WindowFunctionType.MIN
                || type == WindowFunctionType.MAX;
    }

    /** 帧上界（回溯偏移）；受分区内已推行数限制（不足 startBack 时只到已推入的最旧一行）。 */
    private int frameHi(int fi) {
        return Math.min(frames[fi].startBack(), frameFilled[fi] - 1);
    }

    /**
     * 推进一个 ROWS 帧函数的<b>滑动聚合状态</b>，每行 O(1)（MIN/MAX 为均摊 O(1)）。
     *
     * <p><b>为什么不再每行重扫整帧</b>：旧实现对每个函数、每一行都从 {@code endBack} 到
     * {@code startBack} 扫一遍环形缓冲，复杂度 <b>O(行数 × 帧宽)</b>。线上
     * {@code ROWS BETWEEN 50000 PRECEDING AND CURRENT ROW} + 4~5 个聚合函数 ⇒ 每行约 20 万次
     * 数组遍历，Phase 2 吞吐掉到 ~5K 行/s。本方法按「滑窗只进出一行」维护状态：</p>
     * <ul>
     *   <li>SUM/AVG —— 增量加减（每 {@code 2^16} 行整帧重算一次，界定浮点漂移）；</li>
     *   <li>MIN/MAX —— 单调队列（队头即极值，队尾弹出不再可能成为极值的旧值）；</li>
     *   <li>COUNT  —— 直接由帧上下界算出（只推进行号）。</li>
     * </ul>
     *
     * <p><b>环形缓冲不变量</b>：容量 {@code cap = startBack + 1}，处理第 {@code t} 行（0 起）之前
     * 持有第 {@code t-cap .. t-1} 行。因此「离开帧的第 t-startBack-1 行」正好占据本行将要覆盖的槽位
     * （先读后写）；「进入帧的第 t-endBack 行」在写入本行后仍在环中，供读取。</p>
     *
     * @param in 聚合输入列；{@code null} 表示无输入列（COUNT 不需要值）
     */
    private void advanceFrame(int fi, WindowFunctionType type, FieldVector in, int row) {
        WindowFrame f = frames[fi];
        int cap = f.startBack() + 1;
        double[] ring = frameRing[fi];
        boolean[] valid = frameRingValid[fi];
        int t = frameFilled[fi];                 // 本行在分区内的绝对行号

        // 1) 离开帧的行（t - startBack - 1）：此刻仍持有其值，读完即会被本行覆盖。
        //    COUNT 不需要值；MIN/MAX 由下面的「按绝对行号淘汰队头」处理。
        long leaving = (long) t - f.startBack() - 1;
        if (leaving >= 0 && (type == WindowFunctionType.SUM || type == WindowFunctionType.AVG)) {
            int pos = (int) (leaving % cap);
            if (valid[pos]) {
                frameAccSum[fi] -= ring[pos];
                frameAccCnt[fi]--;
            }
        }
        // 2) MIN/MAX：先按绝对行号淘汰滑出帧的队头（必须在 push 之前，保证队列不超容量）。
        if (type == WindowFunctionType.MIN || type == WindowFunctionType.MAX) {
            evictMinMax(fi, (long) t - f.startBack());
        }

        // 3) 推入本行采样。
        boolean has = in != null && !in.isNull(row);
        double val = has ? toDouble(in, row) : 0.0;
        int pos = t % cap;
        ring[pos] = val;
        valid[pos] = has;

        // 4) 进入帧的行（t - endBack）：写入本行后，该行（含 endBack=0 时的本行）仍在环中。
        long entering = (long) t - f.endBack();
        if (entering >= 0) {
            int epos = (int) (entering % cap);
            if (valid[epos]) {
                switch (type) {
                    case SUM, AVG -> {
                        frameAccSum[fi] += ring[epos];
                        frameAccCnt[fi]++;
                    }
                    case MIN, MAX -> pushMinMax(fi, (int) entering, ring[epos]);
                    default -> { }
                }
            }
        }

        // 5) SUM/AVG：每 65536 行按环内真实内容整帧重算一次，界定「加/减」漂移（约 0.8 次/行，可忽略）。
        if ((type == WindowFunctionType.SUM || type == WindowFunctionType.AVG)
                && t > 0 && (t & ((1 << RECOMPUTE_SHIFT) - 1)) == 0) {
            recomputeFrameAgg(fi, t);
        }

        frameFilled[fi] = t + 1;
    }

    /** 整帧重算 SUM/AVG 的累加量（仅用于消除浮点漂移；帧宽扫描，执行频率 1/65536 行）。 */
    private void recomputeFrameAgg(int fi, int t) {
        WindowFrame f = frames[fi];
        int cap = f.startBack() + 1;
        double[] ring = frameRing[fi];
        boolean[] valid = frameRingValid[fi];
        int lo = Math.max(0, t - f.startBack());
        int hi = t - f.endBack();
        double sum = 0;
        long cnt = 0;
        for (int r = lo; r <= hi; r++) {
            int p = r % cap;
            if (valid[p]) { sum += ring[p]; cnt++; }
        }
        frameAccSum[fi] = sum;
        frameAccCnt[fi] = cnt;
    }

    /** MIN/MAX 单调队列：弹出队尾不再可能成为极值的旧值，再压入 {@code (absIdx, v)}。 */
    private void pushMinMax(int fi, int absIdx, double v) {
        int cap = frames[fi].startBack() + 1;
        double[] vals = mmVal[fi];
        int[] idxs = mmIdx[fi];
        int head = mmHead[fi];
        int size = mmSize[fi];
        boolean isMin = functions.get(fi).type() == WindowFunctionType.MIN;
        while (size > 0) {
            int back = (head + size - 1) % cap;
            double bv = vals[back];
            if (isMin ? bv >= v : bv <= v) size--;   // 旧值更差或并列 ⇒ 永远不会再被选中
            else break;
        }
        int pos = (head + size) % cap;
        vals[pos] = v;
        idxs[pos] = absIdx;
        mmHead[fi] = head;
        mmSize[fi] = size + 1;
    }

    /** MIN/MAX 单调队列：淘汰绝对行号 &lt; {@code earliest} 的队头（已滑出帧）。 */
    private void evictMinMax(int fi, long earliest) {
        int cap = frames[fi].startBack() + 1;
        int[] idxs = mmIdx[fi];
        int head = mmHead[fi];
        int size = mmSize[fi];
        while (size > 0 && idxs[head] < earliest) {
            head = (head + 1) % cap;
            size--;
        }
        mmHead[fi] = head;
        mmSize[fi] = size;
    }

    private static double toDouble(FieldVector vector, int index) {
        // 数值列走原始缓冲读取（不装箱）；调用方通常已判空，这里再兜一次以保证语义与旧实现一致（NULL→0）。
        if (vector.isNull(index)) return 0.0;
        if (vector instanceof Float8Vector v) return v.get(index);
        if (vector instanceof Float4Vector v) return v.get(index);
        if (vector instanceof BigIntVector v) return v.get(index);
        if (vector instanceof IntVector v) return v.get(index);
        if (vector instanceof SmallIntVector v) return v.get(index);
        if (vector instanceof TinyIntVector v) return v.get(index);
        Object obj = vector.getObject(index);
        if (obj == null) return 0.0;
        if (obj instanceof Number n) return n.doubleValue();
        try {
            return Double.parseDouble(obj.toString());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    /**
     * 将任意值写入向量指定行 —— <b>覆盖全部 Arrow 类型</b>（含 Decimal / Date / Time /
     * TimeStamp（含时区）/ Bool / VarBinary / TinyInt / SmallInt / Duration）。
     *
     * <p>★ 这里直接复用 {@link AbstractSpillableOperator#setVectorValue}：{@code LAG/LEAD} 的输出
     * 向量类型与输入列一致（见 {@link #buildOutputSchema}），而类型覆盖面必须一致 —— 此前本方法
     * 只支持 BigInt/Int/Float/VarChar，DECIMAL 与 TIMESTAMP 输入列全部落入 {@code default → setNull}，
     * 表现为 {@code lag(amount)/lag(order_time)} 恒 NULL（再叠加归并路径的 validity 丢失后显示为
     * 0.00 / 1970-01-01）。写不进去的值（如把无法解析的字符串默认值写给数值列）兜底置 NULL，
     * <b>绝不静默写零</b>。</p>
     */
    private static void writeVectorValue(FieldVector vec, int row, Object val) {
        if (val == null) {
            vec.setNull(row);
            return;
        }
        try {
            AbstractSpillableOperator.setVectorValue(vec, row, val);
        } catch (RuntimeException e) {
            vec.setNull(row);
        }
    }

    // ==================== 分区状态管理 ====================

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void resetPartitionState() {
        rowNumber = 0;
        lastOrderByValues = List.of();
        rankValue = 0;
        rankSameCount = 0;
        denseRankValue = 0;
        Arrays.fill(runningSums, 0.0);
        Arrays.fill(runningCounts, 0);
        // 重置 LAG 环形缓冲区
        Arrays.fill(lagRingSize, 0);
        // 重置 ROWS 帧环形缓冲与滑动聚合状态（槽位内容由 frameFilled 门控，无需清空）
        Arrays.fill(frameFilled, 0);
        Arrays.fill(frameAccSum, 0.0);
        Arrays.fill(frameAccCnt, 0);
        Arrays.fill(mmHead, 0);
        Arrays.fill(mmSize, 0);
        // 重置聚合状态
        for (int i = 0; i < aggregateInstances.length; i++) {
            if (aggregateInstances[i] != null && aggregateStates[i] != null) {
                ((AggregateFunction) aggregateInstances[i]).reset(aggregateStates[i]);
            }
        }
    }

    // ==================== 整分区聚合（无 ORDER BY 的默认帧） ====================

    /**
     * 整分区模式的主循环：先把当前分区的<b>全部行</b>缓冲（必要时溢写），再用两遍扫描输出
     * —— 第一遍算出整分区聚合值，第二遍逐页写出（维持 ROW_NUMBER/RANK/LAG/LEAD 的逐行语义）。
     *
     * <p>为什么要缓冲：无 ORDER BY 的默认帧是 {@code UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING}，
     * 第一行就需要最后一行的数据，流式单遍无法给出正确结果（用户报障 A2）。</p>
     */
    private Page doNextWholePartition() throws Exception {
        while (true) {
            if (emitter != null) {
                Page out = emitter.nextPage();
                if (out != null) return out;
                emitter.close();
                emitter = null;
                bufferedPages.clear();
                continue;
            }
            boolean hasPartition = bufferOnePartition();
            if (!hasPartition) return Page.eof();
            List<BufferedPage> segs = new ArrayList<>(bufferedPages);
            bufferedPages.clear();
            emitter = new PartitionEmitter(segs);
        }
    }

    /**
     * 从上游拉到「一个逻辑分区的结束」为止，把行深拷贝进累加器（超阈值转溢写段）。
     *
     * @return 是否缓冲到至少一个分区；{@code false} 表示上游已尽且无数据
     */
    private boolean bufferOnePartition() throws Exception {
        bufferedPages.clear();
        hasBufferedKey = false;
        lastBufferedKey = null;
        lastBufferedValues = null;
        boolean any = false;
        while (true) {
            if (currentPageRef == null) {
                if (inputDone) {
                    if (!any) return false;
                    finishBufferedPartition();
                    return true;
                }
                Page p = upstream.next();
                if (p.isEOF()) {
                    inputDone = true;
                    if (!any) return false;
                    finishBufferedPartition();
                    return true;
                }
                if (p.getRowCount() == 0) {
                    p.close();
                    continue;
                }
                recordInput(p);
                initSchema(p.getRoot());
                currentPageRef = new PageRef(p);
                currentRowOffset = 0;
                if (any && startsNewPartition(p.getRoot(), 0)) {
                    // 新页首行即下一分区起点：上一分区到此为止，当前页留给下一次调用
                    finishBufferedPartition();
                    return true;
                }
            }
            VectorSchemaRoot root = currentPageRef.page.getRoot();
            int rows = root.getRowCount();
            while (currentRowOffset < rows) {
                int row = currentRowOffset;
                if (any && startsNewPartition(root, row)) {
                    // 分区边界落在页中间：本分区结束，页与行偏移保持不动供下一分区续读
                    finishBufferedPartition();
                    return true;
                }
                appendRow(root, row);
                rememberPartitionKey(root, row);
                any = true;
                currentRowOffset++;
                if ((accRows & 1023) == 0) maybeSpillAccumulator();
            }
            releasePageRef(currentPageRef);
            currentPageRef = null;
            // 页耗尽：继续拉下一页（同一分区可能跨页）
        }
    }

    /** 当前行的 PARTITION BY 键是否与已缓冲的上一行不同（即进入新分区）。 */
    private boolean startsNewPartition(VectorSchemaRoot root, int row) {
        if (partitionByIndices.length == 0 || !hasBufferedKey) return false;
        if (partitionByIndices.length == 1) {
            return !java.util.Arrays.equals(lastBufferedKey,
                    snapshotKeyBytes(root.getVector(partitionByIndices[0]), row));
        }
        return !valuesEqual(lastBufferedValues, buildPartitionByValues(root, row));
    }

    /** 记住当前分区的最后一行 PARTITION BY 键（跨页边界判定）。 */
    private void rememberPartitionKey(VectorSchemaRoot root, int row) {
        if (partitionByIndices.length == 0) return;
        if (partitionByIndices.length == 1) {
            lastBufferedKey = snapshotKeyBytes(root.getVector(partitionByIndices[0]), row);
        } else {
            lastBufferedValues = buildPartitionByValues(root, row);
        }
        hasBufferedKey = true;
    }

    /** 把当前分区尾部的内存累加器收尾成一个内存段（所有权交给 {@link BufferedPage}）。 */
    private void finishBufferedPartition() {
        if (accRows > 0) {
            bufferedPages.add(BufferedPage.memory(accRoot, accRows));
            accRoot = null;
            accRows = 0;
            accCapacity = 0;
            appendTps = null;
            appendTpsSource = null;
            appendTpsTarget = null;
        }
    }

    /** 把源页的一行深拷贝进累加器。 */
    private void appendRow(VectorSchemaRoot src, int srcRow) {
        ensureAccCapacity(accRows + 1);
        if (appendTpsSource != src || appendTpsTarget != accRoot) {
            List<TransferPair> tps = new ArrayList<>(src.getFieldVectors().size());
            for (int c = 0; c < src.getFieldVectors().size(); c++) {
                tps.add(src.getVector(c).makeTransferPair(accRoot.getVector(c)));
            }
            appendTps = tps;
            appendTpsSource = src;
            appendTpsTarget = accRoot;
        }
        for (TransferPair tp : appendTps) {
            tp.copyValueSafe(srcRow, accRows);
        }
        accRows++;
        accRoot.setRowCount(accRows);
    }

    /** 确保累加器全部列容量 ≥ {@code needed}（统一取最小容量，避免宽窄列容量不一致越界）。 */
    private void ensureAccCapacity(int needed) {
        if (accRoot == null) {
            List<FieldVector> vectors = new ArrayList<>();
            for (Field f : inputSchema.getFields()) {
                FieldVector v = f.createVector(allocator);
                v.setInitialCapacity(Math.max(needed, 64));
                v.allocateNew();
                vectors.add(v);
            }
            accRoot = new VectorSchemaRoot(inputSchema.getFields(), vectors, 0);
            accCapacity = AbstractSpillableOperator.minValueCapacity(vectors);
        }
        while (accCapacity < needed) {
            for (FieldVector v : accRoot.getFieldVectors()) v.reAlloc();
            accCapacity = AbstractSpillableOperator.minValueCapacity(accRoot.getFieldVectors());
        }
    }

    /** 累加器超过 {@code query.window.partitionAggBufferBytes} 时落盘，防空分区撑爆堆外内存。 */
    private void maybeSpillAccumulator() {
        if (accRoot == null || accRows == 0) return;
        if (accMemoryBytes() < BUFFER_SPILL_THRESHOLD) return;
        spillAccumulator();
    }

    private long accMemoryBytes() {
        long bytes = 0;
        for (FieldVector v : accRoot.getFieldVectors()) {
            ArrowBuf data = v.getDataBuffer();
            if (data != null) bytes += data.capacity();
            // 只有变长向量才有 offset buffer —— 定长向量调用 getOffsetBuffer() 会抛
            // UnsupportedOperationException（混合窗口规格 + 溢写路径曾因此崩溃）。
            if (v instanceof BaseVariableWidthVector || v instanceof BaseLargeVariableWidthVector) {
                ArrowBuf off = v.getOffsetBuffer();
                if (off != null) bytes += off.capacity();
            }
            ArrowBuf valid = v.getValidityBuffer();
            if (valid != null) bytes += valid.capacity();
        }
        return bytes;
    }

    private void spillAccumulator() {
        try {
            if (bufferSpillDir == null) {
                List<Path> roots = ResourceConfig.spillDirs();
                Path base = (roots == null || roots.isEmpty()) ? null : roots.get(0);
                if (base == null) base = Path.of(System.getProperty("java.io.tmpdir"));
                Files.createDirectories(base);
                bufferSpillDir = Files.createTempDirectory(base, "dojao-winpart-");
            }
            Path file = bufferSpillDir.resolve("p_" + (bufferSpillSeq++) + ".arrow");
            FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            try {
                ArrowFileWriter w = new ArrowFileWriter(accRoot, null, ch);
                w.writeBatch();
                w.close();
            } finally {
                ch.close();
            }
            // ★整分区缓冲落盘同样是真实溢写 I/O：计入算子溢写统计（监控「溢写总量」）。
            //   不记账时，无过滤的大分区窗口在监控上完全看不到任何溢写。
            addSpilledBytes(Files.size(file));
            bufferedPages.add(BufferedPage.file(file, accRows));
            accRoot.close();
            accRoot = null;
            accRows = 0;
            accCapacity = 0;
            appendTps = null;
            appendTpsSource = null;
            appendTpsTarget = null;
        } catch (IOException e) {
            throw new RuntimeException("窗口整分区缓冲溢写失败", e);
        }
    }

    private void cleanupSpillDir() {
        if (bufferSpillDir == null) return;
        try (java.util.stream.Stream<Path> walk = Files.walk(bufferSpillDir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(f -> {
                try { Files.deleteIfExists(f); } catch (Exception ignored) { }
            });
        } catch (Exception ignored) { }
        bufferSpillDir = null;
    }

    private void releasePageRef(PageRef ref) {
        if (ref == null) return;
        if (--ref.refs > 0) return;
        try { ref.page.close(); } catch (Exception ignored) { }
        releaseInput(ref.page);
    }

    /** 读回一个溢写段（每次调用产生独立 root，调用方负责关闭）。 */
    private VectorSchemaRoot loadSpillSegment(Path file) throws IOException {
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
        ArrowFileReader reader = new ArrowFileReader(ch, allocator);
        VectorSchemaRoot loaded = null;
        try {
            List<ArrowBlock> blocks = reader.getRecordBlocks();
            if (blocks.isEmpty()) {
                return new VectorSchemaRoot(inputSchema.getFields(), new ArrayList<>(), 0);
            }
            reader.loadRecordBatch(blocks.get(0));
            loaded = reader.getVectorSchemaRoot();
            int rc = loaded.getRowCount();
            List<FieldVector> copied = new ArrayList<>();
            for (int col = 0; col < loaded.getFieldVectors().size(); col++) {
                FieldVector src = loaded.getVector(col);
                FieldVector dst = src.getField().createVector(allocator);
                dst.setInitialCapacity(Math.max(1, rc));
                dst.allocateNew();
                RowAccumulator.copyColumnBatch(src, dst, 0, 0, rc);
                dst.setValueCount(rc);
                copied.add(dst);
            }
            return new VectorSchemaRoot(loaded.getSchema().getFields(), copied, rc);
        } finally {
            if (loaded != null) {
                try { loaded.close(); } catch (Exception ignored) { }
            }
            try { reader.close(); } catch (Exception ignored) { }
            try { ch.close(); } catch (Exception ignored) { }
        }
    }

    /** 输入页的共享包装（当前页可能因分区边界停在页中间，需保持打开）。 */
    private static final class PageRef {
        final Page page;
        int refs = 1;

        PageRef(Page page) {
            this.page = page;
        }
    }

    /** 一个已缓冲的分区段：内存段（尾部）或溢写文件段。 */
    private static final class BufferedPage {
        final Path file;
        final VectorSchemaRoot memory;
        final int rows;

        private BufferedPage(Path file, VectorSchemaRoot memory, int rows) {
            this.file = file;
            this.memory = memory;
            this.rows = rows;
        }

        static BufferedPage file(Path file, int rows) {
            return new BufferedPage(file, null, rows);
        }

        static BufferedPage memory(VectorSchemaRoot root, int rows) {
            return new BufferedPage(null, root, rows);
        }

        /** 关闭内存段持有的 root（文件段无需关闭）。 */
        void closeOwned() {
            if (memory != null) {
                try { memory.close(); } catch (Exception ignored) { }
            }
        }
    }

    /**
     * 单个分区的惰性输出器：先算整分区聚合值，再按页产出（每页 {@link #WHOLE_PARTITION_PAGE_ROWS} 行）。
     */
    private final class PartitionEmitter implements AutoCloseable {
        private final List<BufferedPage> segments;
        private final long[] segStarts;
        private final long totalRows;
        private final double[] totals;
        private final long[] totalCounts;
        private final Object[] totalObjects;
        private final boolean[] totalSeen;
        private final Map<Integer, VectorSchemaRoot> openRoots = new HashMap<>();

        private long emittedRows;
        private int lastSeg = -1;
        private VectorSchemaRoot copyTpsSource;
        private VectorSchemaRoot copyTpsTarget;
        private List<TransferPair> copyTps;

        PartitionEmitter(List<BufferedPage> segments) {
            this.segments = segments;
            this.segStarts = new long[segments.size() + 1];
            for (int i = 0; i < segments.size(); i++) {
                segStarts[i + 1] = segStarts[i] + segments.get(i).rows;
            }
            this.totalRows = segStarts[segments.size()];
            this.totals = new double[functions.size()];
            this.totalCounts = new long[functions.size()];
            this.totalObjects = new Object[functions.size()];
            this.totalSeen = new boolean[functions.size()];
            // 分区状态复位必须在两遍扫描之前（第一遍可能推进自定义聚合状态）
            resetPartitionState();
            computeTotals();
        }

        private int rowsOf(int si) {
            return segments.get(si).rows;
        }

        private int segmentIndexAt(long g) {
            int lo = 0;
            int hi = segments.size() - 1;
            while (lo < hi) {
                int mid = (lo + hi + 1) >>> 1;
                if (segStarts[mid] <= g) lo = mid;
                else hi = mid - 1;
            }
            return lo;
        }

        private VectorSchemaRoot rootOf(int si) {
            BufferedPage seg = segments.get(si);
            if (seg.memory != null) return seg.memory;
            VectorSchemaRoot cached = openRoots.get(si);
            if (cached != null) return cached;
            try {
                VectorSchemaRoot loaded = loadSpillSegment(seg.file);
                openRoots.put(si, loaded);
                return loaded;
            } catch (IOException e) {
                throw new RuntimeException("读取窗口整分区缓冲失败: " + seg.file, e);
            }
        }

        private void closeRootsBefore(int si) {
            Iterator<Map.Entry<Integer, VectorSchemaRoot>> it = openRoots.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, VectorSchemaRoot> e = it.next();
                if (e.getKey() < si) {
                    try { e.getValue().close(); } catch (Exception ignored) { }
                    it.remove();
                }
            }
        }

        /** 第一遍：整分区聚合值（语义与旧运行聚合的最后一行完全一致）。 */
        @SuppressWarnings({"rawtypes", "unchecked"})
        private void computeTotals() {
            for (int si = 0; si < segments.size(); si++) {
                VectorSchemaRoot root = rootOf(si);
                boolean temp = segments.get(si).memory == null;
                try {
                    int rows = rowsOf(si);
                    for (int r = 0; r < rows; r++) {
                        for (int fi = 0; fi < functions.size(); fi++) {
                            if (!partitionTotals[fi]) continue;
                            WindowFunctionSpec spec = functions.get(fi);
                            int colIdx = spec.inputColumn() != null
                                    ? columnNameToIndex.getOrDefault(spec.inputColumn(), -1) : -1;
                            FieldVector vec = colIdx >= 0 ? root.getVector(colIdx) : null;
                            switch (spec.type()) {
                                case SUM -> {
                                    if (vec != null) totals[fi] += toDouble(vec, r);
                                }
                                case AVG -> {
                                    if (vec != null) totals[fi] += toDouble(vec, r);
                                    totalCounts[fi]++;
                                }
                                case COUNT -> totalCounts[fi]++;
                                case MIN -> {
                                    double v = vec != null ? toDouble(vec, r) : 0.0;
                                    if (!totalSeen[fi]) {
                                        totals[fi] = v;
                                        totalSeen[fi] = true;
                                    } else {
                                        totals[fi] = Math.min(totals[fi], v);
                                    }
                                }
                                case MAX -> {
                                    double v = vec != null ? toDouble(vec, r) : 0.0;
                                    if (!totalSeen[fi]) {
                                        totals[fi] = v;
                                        totalSeen[fi] = true;
                                    } else {
                                        totals[fi] = Math.max(totals[fi], v);
                                    }
                                }
                                case AGGREGATE -> {
                                    AggregateFunction agg = (AggregateFunction) aggregateInstances[fi];
                                    Object state = aggregateStates[fi];
                                    if (agg != null && state != null) {
                                        Object[] inputs = vec != null
                                                ? new Object[]{vec.getObject(r)} : new Object[0];
                                        agg.accumulate(state, inputs);
                                    }
                                }
                                default -> { }
                            }
                        }
                    }
                } finally {
                    if (temp) {
                        openRoots.remove(si);
                        try { root.close(); } catch (Exception ignored) { }
                    }
                }
            }
            for (int fi = 0; fi < functions.size(); fi++) {
                if (partitionTotals[fi] && functions.get(fi).type() == WindowFunctionType.AGGREGATE) {
                    @SuppressWarnings({"rawtypes", "unchecked"})
                    AggregateFunction agg = (AggregateFunction) aggregateInstances[fi];
                    Object state = aggregateStates[fi];
                    totalObjects[fi] = (agg != null && state != null) ? agg.evaluate(state) : null;
                }
            }
        }

        Page nextPage() {
            if (emittedRows >= totalRows) return null;
            int n = (int) Math.min(WHOLE_PARTITION_PAGE_ROWS, totalRows - emittedRows);
            VectorSchemaRoot out = buildOutputRoot(n);
            boolean needOrderBy = needOrderByValues();
            int inCols = inputSchema.getFields().size();
            try {
                for (int r = 0; r < n; r++) {
                    long g = emittedRows + r;
                    int si = segmentIndexAt(g);
                    if (si != lastSeg) {
                        closeRootsBefore(si);
                        lastSeg = si;
                    }
                    VectorSchemaRoot in = rootOf(si);
                    int inRow = (int) (g - segStarts[si]);
                    copyInputRow(in, inRow, out, r);
                    setupLeadChainForRow(si, inRow);
                    List<Object> currentOrderBy = needOrderBy
                            ? buildOrderByValues(in, inRow) : List.of();
                    for (int fi = 0; fi < functions.size(); fi++) {
                        FieldVector outVec = out.getVector(inCols + fi);
                        if (partitionTotals[fi]) {
                            writeTotal(fi, functions.get(fi), outVec, r);
                        } else {
                            computeFunction(fi, functions.get(fi), in, inRow, currentOrderBy, outVec);
                        }
                    }
                    if (needOrderBy) lastOrderByValues = currentOrderBy;
                    rowNumber++;
                }
                for (FieldVector v : out.getFieldVectors()) v.setValueCount(n);
                out.setRowCount(n);
                emittedRows += n;
                return Page.fromRoot(out);
            } catch (RuntimeException e) {
                try { out.close(); } catch (Exception ignored) { }
                throw e;
            }
        }

        private void copyInputRow(VectorSchemaRoot in, int inRow, VectorSchemaRoot out, int outRow) {
            if (copyTpsSource != in || copyTpsTarget != out) {
                List<TransferPair> tps = new ArrayList<>(in.getFieldVectors().size());
                for (int c = 0; c < in.getFieldVectors().size(); c++) {
                    tps.add(in.getVector(c).makeTransferPair(out.getVector(c)));
                }
                copyTps = tps;
                copyTpsSource = in;
                copyTpsTarget = out;
            }
            for (TransferPair tp : copyTps) {
                tp.copyValueSafe(inRow, outRow);
            }
        }

        /** 为 LEAD 构建以当前段为起点的短前瞻链（段内已含整分区数据，无需上游预取）。 */
        private void setupLeadChainForRow(int si, int inRow) {
            if (maxLeadOffset <= 0) {
                leadChainRoots = null;
                leadChainStarts = null;
                leadChainTotal = 0;
                return;
            }
            List<VectorSchemaRoot> roots = new ArrayList<>();
            List<Integer> starts = new ArrayList<>();
            starts.add(0);
            int i = si;
            int target = inRow + maxLeadOffset;
            int cum = 0;
            while (true) {
                roots.add(rootOf(i));
                cum += rowsOf(i);
                starts.add(cum);
                if (cum > target || i + 1 >= segments.size()) break;
                i++;
            }
            leadChainRoots = roots;
            int[] arr = new int[starts.size()];
            for (int k = 0; k < arr.length; k++) arr[k] = starts.get(k);
            leadChainStarts = arr;
            leadChainTotal = cum;
        }

        private void writeTotal(int fi, WindowFunctionSpec spec, FieldVector outVec, int row) {
            Long ov = valueOverrides.get(spec.type());
            switch (spec.type()) {
                case SUM, AVG, MIN, MAX -> {
                    double v = ov != null ? ov.doubleValue() : totals[fi];
                    ((Float8Vector) outVec).setSafe(row, v);
                }
                case COUNT -> {
                    long v = ov != null ? ov : totalCounts[fi];
                    ((BigIntVector) outVec).setSafe(row, v);
                }
                case AGGREGATE -> {
                    Object v = ov != null ? ov.doubleValue() : totalObjects[fi];
                    writeVectorValue(outVec, row, v);
                }
                default -> { }
            }
        }

        @Override
        public void close() {
            for (VectorSchemaRoot r : openRoots.values()) {
                try { r.close(); } catch (Exception ignored) { }
            }
            openRoots.clear();
            for (BufferedPage seg : segments) seg.closeOwned();
            segments.clear();
        }
    }

    // ==================== Schema 解析 ====================

    private void resolvePartitionByIndices() {
        partitionByIndices = new int[partitionByColumns.size()];
        for (int i = 0; i < partitionByColumns.size(); i++) {
            Integer idx = columnNameToIndex.get(partitionByColumns.get(i));
            if (idx == null) {
                throw new IllegalArgumentException(
                        "PARTITION BY column not found: " + partitionByColumns.get(i));
            }
            NestedTypeSupport.requireSupported(inputSchema.getFields().get(idx), "窗口 PARTITION BY 列");
            partitionByIndices[i] = idx;
        }
    }

    private void resolveOrderByIndices() {
        orderByIndices = new int[orderByColumns.size()];
        for (int i = 0; i < orderByColumns.size(); i++) {
            Integer idx = columnNameToIndex.get(orderByColumns.get(i));
            if (idx == null) {
                throw new IllegalArgumentException(
                        "ORDER BY column not found: " + orderByColumns.get(i));
            }
            NestedTypeSupport.requireSupported(inputSchema.getFields().get(idx), "窗口 ORDER BY 列");
            orderByIndices[i] = idx;
        }
    }

    /**
     * 构建输出 schema：输入列 + 窗口函数结果列。
     */
    private void buildOutputSchema() {
        outputFields = new ArrayList<>(inputSchema.getFields());
        for (WindowFunctionSpec spec : functions) {
            Field field = switch (spec.type()) {
                case ROW_NUMBER, RANK, DENSE_RANK, COUNT -> new Field(spec.outputColumn(),
                        FieldType.nullable(new ArrowType.Int(64, true)), null);
                case SUM, AVG, MIN, MAX -> new Field(spec.outputColumn(),
                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                        null);
                case LAG, LEAD -> {
                    // 输出类型与输入列类型一致
                    Integer idx = columnNameToIndex.get(spec.inputColumn());
                    if (idx != null) {
                        Field inputField = inputSchema.getFields().get(idx);
                        yield new Field(spec.outputColumn(),
                                FieldType.nullable(inputField.getType()), null);
                    }
                    yield new Field(spec.outputColumn(),
                            FieldType.nullable(new ArrowType.Utf8()), null);
                }
                case AGGREGATE -> {
                    // 自定义聚合窗口输出默认 double：中位数等返回 Double，若镜像 DECIMAL 输入列类型，
                    // 输出是 DecimalVector，而 writeVectorValue 无 Decimal 分支 → 落入 default → 全 NULL。
                    // 与 AggregationSpec.outputArrowType 的"自定义聚合默认 double"保持一致。
                    yield new Field(spec.outputColumn(),
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
                }
            };
            outputFields.add(field);
        }
    }

    /**
     * 创建输出 VectorSchemaRoot，预分配向量。
     */
    private VectorSchemaRoot buildOutputRoot(int rowCount) {
        List<FieldVector> vectors = new ArrayList<>();
        for (Field field : outputFields) {
            FieldVector vec = field.createVector(allocator);
            vec.setInitialCapacity(rowCount);
            vec.allocateNew();
            vectors.add(vec);
        }
        return new VectorSchemaRoot(outputFields, vectors, rowCount);
    }
}
