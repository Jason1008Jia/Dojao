package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.core.function.aggregate.CollectMapAggregateFunction;
import com.dojao.core.function.aggregate.CollectOrderedListAggregateFunction;
import com.dojao.core.function.aggregate.CollectSupport;
import com.dojao.core.function.aggregate.CountDistinctAggregateFunction;
import com.dojao.core.function.aggregate.MinMaxSupport;
import com.dojao.spi.function.AggregateFunction;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 基于聚合函数状态的哈希表，用于分组聚合。继承 {@link RowHashTable} 的开放寻址基础设施。
 *
 * <h3>共享内存（继承自 RowHashTable）</h3>
 * <ul>
 *   <li>① {@code buckets[]} —— 哈希桶</li>
 *   <li>② {@code rowHashes[]} —— hash 缓存</li>
 *   <li>③ {@code keyVectors} —— Arrow key 列向量</li>
 * </ul>
 *
 * <h3>Aggregation 特有内存</h3>
 * <ul>
 *   <li>④ {@code groupStates} —— Object[]，groupStates[groupIdx][specIdx] = 聚合函数状态</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/7/15
 */
public class AggregationHashTable extends RowHashTable implements Slot {

    private static final ArrowType INT64 = new ArrowType.Int(64, true);
    private static final ArrowType FLOAT64 = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    private static final ArrowType VARCHAR = ArrowType.Utf8.INSTANCE;

    private final AggregationSpec[] specs;
    private final AggregateFunction<?>[] aggregates;
    private final String[] inputColumns;
    /** 第二输入列（collect_map 的 value 列 / collect_ordered_list 的排序列；其余聚合为 null）。 */
    private final String[] valueColumns;

    /**
     * {@code count_distinct} 规格下标（按出现顺序）：ordinal → {@code specs} 下标。
     *
     * <p>溢写时这类规格<b>不</b>写进「每组一行」的标量流，而是按值拆成独立的有序流
     * （见 {@link #buildDistinctSpillRoot(int)}），Phase 2 流式去重计数。</p>
     */
    private final int[] distinctSpecs;
    /** {@code specs} 下标 → distinct ordinal（非 count_distinct 为 -1）。 */
    private final int[] distinctOrdinalOfSpec;

    /**
     * {@code collect_list} / {@code collect_set} 规格下标（按出现顺序）：ordinal → {@code specs} 下标。
     *
     * <p>与 count_distinct 同理：这类状态不写进「每组一行」的标量流，而是按值拆成独立的有序流
     * （见 {@link #buildCollectSpillRoot(int)}），Phase 2 按键流式拼接。</p>
     */
    private final int[] collectSpecs;

    // ==================== 聚合状态 ====================
    private final List<Object[]> groupStates;

    // 输入列索引（首次输入时解析）
    private int[] inputColIndices;
    /** 第二输入列索引（collect_map 的 value 列 / collect_ordered_list 的排序列；非双输入为 -1）。 */
    private int[] valueColIndices;
    private boolean indicesResolved;

    /**
     * 各聚合结果列的 Arrow 类型（首次输入时按<b>输入列类型</b>解析）。
     *
     * <p>★MIN/MAX 必须随输入类型走：日期时间列的输出就是时间戳，字符串列就是字符串，
     * 而不是旧实现的「一律 DOUBLE」—— 否则 {@code MIN(order_time)} 要么被强转失败、要么
     * 只能报 NULL。数值列（Int/FloatingPoint）仍保持旧口径 DOUBLE，不改动既有数值结果。</p>
     */
    private ArrowType[] resolvedOutputTypes;

    /**
     * 各聚合结果列的完整 Field（首次输入时按输入列 Field 解析）。collect_list / collect_set 的
     * {@code list<元素类型>} 需要子字段，{@link ArrowType} 表达不了，故单独保留 Field。
     */
    private Field[] resolvedOutputFields;

    /**
     * 各聚合第二输入列的 Field（首次输入时解析）：collect_ordered_list 的逐值溢写流需要排序列的
     * Arrow 类型建列，其余规格该槽位为 null。
     */
    private Field[] resolvedValueFields;

    // 批量聚合扩展：任一聚合实现 BatchAggregateFunction → 页内按 group 归组后批量累加。
    // 内置五函数不实现 → batchEnabled=false，走原逐行路径，性能零影响。
    private final boolean batchEnabled;
    private final BatchAggregateFunction<?>[] batchAggs;
    // 当前页待批行：groupIdx → 行索引（batchPending 期间由 insertGroup/appendToGroup 收集，
    // addPage 结束后 flushBatch 按 group 批量累加，页 root 在 addPage 返回后才被 close）
    private boolean batchPending;
    private final Map<Integer, List<Integer>> batchRows = new HashMap<>();

    // ==================== 内存追踪 ====================
    private long estimatedMem;
    private int rehashCount;

    /** 文本列快路径的复用暂存（表在同一线程内逐页累加，无需每行分配 byte[]）。 */
    private byte[] utf8Scratch;

    public AggregationHashTable(BufferAllocator allocator,
                                List<String> keyColumns,
                                List<Field> keyFields,
                                List<AggregationSpec> specList) {
        super(allocator, keyColumns, keyFields);

        this.specs = specList.toArray(new AggregationSpec[0]);
        this.aggregates = new AggregateFunction<?>[this.specs.length];
        this.inputColumns = new String[this.specs.length];
        this.valueColumns = new String[this.specs.length];
        this.batchAggs = new BatchAggregateFunction<?>[this.specs.length];
        boolean anyBatch = false;
        for (int i = 0; i < this.specs.length; i++) {
            this.aggregates[i] = this.specs[i].aggregate();
            this.inputColumns[i] = this.specs[i].inputColumn();
            this.valueColumns[i] = this.specs[i].valueColumn();
            if (this.aggregates[i] instanceof BatchAggregateFunction<?> b) {
                this.batchAggs[i] = b;
                anyBatch = true;
            }
        }
        this.batchEnabled = anyBatch;

        int[] ds = new int[this.specs.length];
        int dc = 0;
        this.distinctOrdinalOfSpec = new int[this.specs.length];
        Arrays.fill(this.distinctOrdinalOfSpec, -1);
        for (int i = 0; i < this.specs.length; i++) {
            if (this.aggregates[i] != null && "count_distinct".equals(this.aggregates[i].functionName())) {
                this.distinctOrdinalOfSpec[i] = dc;
                ds[dc++] = i;
            }
        }
        this.distinctSpecs = Arrays.copyOf(ds, dc);

        int[] cs = new int[this.specs.length];
        int cc = 0;
        for (int i = 0; i < this.specs.length; i++) {
            if (this.specs[i].usesCollectSpill()) cs[cc++] = i;
        }
        this.collectSpecs = Arrays.copyOf(cs, cc);

        this.groupStates = new ArrayList<>();
        this.indicesResolved = false;

        // 初始化内存估算
        this.estimatedMem = capacity * 4L; // buckets
        for (FieldVector vec : keyVectors) this.estimatedMem += estimateVectorMem(vec);
        this.estimatedMem += (long) DEFAULT_VECTOR_CAPACITY * 4L; // rowHashes
    }

    // ==================== count_distinct 溢写形态 ====================

    /** 是否含 {@code count_distinct} 规格（决定溢写走「标量流 + 逐值去重流」双流）。 */
    public boolean hasDistinct() {
        return distinctSpecs.length > 0;
    }

    /** {@code count_distinct} 规格个数（= 去重流条数）。 */
    public int distinctSpecCount() {
        return distinctSpecs.length;
    }

    /** distinct ordinal → {@code specs} 下标。 */
    public int distinctSpecIndex(int ordinal) {
        return distinctSpecs[ordinal];
    }

    // ==================== 核心操作 ====================

    /**
     * 已有 key 命中：用当前行累积聚合函数状态。
     * 探测逻辑本身由 {@link RowHashTable#insertOrAppend} 统一处理。
     */
    @Override
    protected void appendToGroup(Page input, int rowIndex, int groupIdx) {
        if (batchPending) {
            // 批量：不逐行累加，收集到 flushBatch 按 group 批量并入
            batchRows.computeIfAbsent(groupIdx, k -> new ArrayList<>(4)).add(rowIndex);
            return;
        }
        updateStates(input, rowIndex, groupIdx);
    }

    /** 每行处理前解析输入列索引（首次输入时）。 */
    @Override
    protected void beforeRow(Page input) {
        ensureIndices(input);
    }

    public void insertOrAppend(VectorSchemaRoot root, int rowIndex) {
        insertOrAppend(Page.fromRoot(root), rowIndex);
    }

    // ==================== Slot 接口 ====================

    @Override public void addRow(Page input, int row) { insertOrAppend(input, row); }

    @Override
    public void addPage(Page input) {
        int n = input.getRowCount();
        if (n == 0) return;
        if (!batchEnabled) {
            for (int row = 0; row < n; row++) insertOrAppend(input, row);
            return;
        }
        // 批量路径：先把整页行按 group 归组（延迟 accumulate，insertGroup/appendToGroup
        // 只收集 (groupIdx, rowIndex)），再按 group 批量累加。页 root 在 addPage 返回后才
        // 被 close（AbstractSpillableOperator.accumulatePage 契约），故本页内 flush 安全。
        batchPending = true;
        batchRows.clear();
        for (int row = 0; row < n; row++) insertOrAppend(input, row);
        batchPending = false;
        flushBatch(input);
    }

    // ==================== insertGroup（聚合版本） ====================

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected int insertGroup(Page input, int rowIndex, int bucket, int hash) {
        for (TransferPair tp : keyTransfers) {
            if (tp != null) tp.copyValueSafe(rowIndex, groupCount);
        }

        // 创建聚合状态
        VectorSchemaRoot root = input.getRoot();
        Object[] states = new Object[specs.length];
        for (int si = 0; si < specs.length; si++) {
            AggregateFunction<Object> aggregate = (AggregateFunction) aggregates[si];
            Object state = aggregate.createState();
            if (!batchPending) {
                // 非批量：首行立即累加（原行为）
                accumulateOne(aggregate, state, root, si, rowIndex);
            }
            states[si] = state;
        }
        groupStates.add(states);
        if (batchPending) {
            // 批量：首行不立即累加，整行收集到 flushBatch 再按 group 批量并入
            batchRows.computeIfAbsent(groupCount, k -> new ArrayList<>(4)).add(rowIndex);
        }

        ensureRowHashesCapacity(groupCount);
        rowHashes[groupCount] = hash;

        buckets[bucket] = groupCount;
        groupCount++;

        estimatedMem += estimateStatesMem(states) + estimateKeyRowMem();

        return groupCount - 1;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void updateStates(Page input, int inputRow, int groupIdx) {
        Object[] states = groupStates.get(groupIdx);
        long before = estimateStatesMem(states);
        VectorSchemaRoot root = input.getRoot();
        for (int si = 0; si < specs.length; si++) {
            AggregateFunction<Object> aggregate = (AggregateFunction) aggregates[si];
            accumulateOne(aggregate, states[si], root, si, inputRow);
        }
        trackStateGrowth(before, states);
    }

    /**
     * 单行累加（逐行 / 批量混合路径共用）。
     *
     * <p>★COUNT(DISTINCT) 快路径：整型/浮点/文本向量直接从 Arrow 向量取裸值写入
     * {@link com.dojao.core.function.aggregate.DistinctSet}，跳过 {@code getObject} 的逐行装箱
     * （千万行高基数场景下这是主要开销）。其余类型与其它聚合仍走原路径，行为不变。</p>
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void accumulateOne(AggregateFunction<Object> aggregate, Object state,
                               VectorSchemaRoot root, int si, int row) {
        if (inputColIndices[si] < 0) {
            aggregate.accumulate(state, new Object[0]);
            return;
        }
        FieldVector vec = root.getVector(inputColIndices[si]);
        // 双输入聚合（collect_map 的 key/value、collect_ordered_list 的 value/排序键）。
        // NULL 由聚合函数自身跳过。
        if (specs[si].isCollectMap() || specs[si].isOrderedList()) {
            Object first = vec.isNull(row) ? null : vec.getObject(row);
            Object second = null;
            if (valueColIndices != null && valueColIndices[si] >= 0) {
                FieldVector vv = root.getVector(valueColIndices[si]);
                second = vv.isNull(row) ? null : vv.getObject(row);
            }
            aggregate.accumulate(state, new Object[]{first, second});
            return;
        }
        // 经 Object 判型：AggregateFunction<Object> 与 CountDistinctAggregateFunction
        // （AggregateFunction<Set<Object>>）参数化不同，直接 instanceof 会被编译器判为不可能。
        Object aggObj = aggregate;
        if (aggObj instanceof CountDistinctAggregateFunction cd
                && accumulateDistinctFast(cd, (Set<Object>) state, vec, row)) {
            return;
        }
        aggregate.accumulate(state, new Object[]{vec.getObject(row)});
    }

    /**
     * COUNT(DISTINCT) 的向量直读快路径：成功返回 true（含 NULL 行 —— 已按 SQL 语义跳过）。
     * 未覆盖的向量类型返回 false，由调用方回退到 {@code getObject}。
     */
    private boolean accumulateDistinctFast(CountDistinctAggregateFunction cd, Set<Object> state,
                                           FieldVector vec, int row) {
        if (vec.isNull(row)) return true;   // COUNT(DISTINCT) 不计 NULL
        if (vec instanceof BigIntVector v) { cd.accumulateLong(state, v.get(row)); return true; }
        if (vec instanceof IntVector v) { cd.accumulateLong(state, v.get(row)); return true; }
        if (vec instanceof SmallIntVector v) { cd.accumulateLong(state, v.get(row)); return true; }
        if (vec instanceof TinyIntVector v) { cd.accumulateLong(state, v.get(row)); return true; }
        if (vec instanceof Float8Vector v) { cd.accumulateDouble(state, v.get(row)); return true; }
        if (vec instanceof Float4Vector v) { cd.accumulateDouble(state, v.get(row)); return true; }
        if (vec instanceof VarCharVector v) {
            return accumulateUtf8(cd, state, v.getOffsetBuffer(), v.getDataBuffer(), row, false);
        }
        if (vec instanceof LargeVarCharVector v) {
            return accumulateUtf8(cd, state, v.getOffsetBuffer(), v.getDataBuffer(), row, true);
        }
        return false;
    }

    /** 从 Arrow 变长列直接拷一段 UTF-8 到复用暂存数组（避免每行 {@code byte[]} 分配）。 */
    private boolean accumulateUtf8(CountDistinctAggregateFunction cd, Set<Object> state,
                                   org.apache.arrow.memory.ArrowBuf offsets,
                                   org.apache.arrow.memory.ArrowBuf data,
                                   int row, boolean largeOffsets) {
        long start;
        long end;
        if (largeOffsets) {
            start = offsets.getLong((long) row * 8L);
            end = offsets.getLong((long) (row + 1) * 8L);
        } else {
            start = offsets.getInt((long) row * 4L) & 0xffffffffL;
            end = offsets.getInt((long) (row + 1) * 4L) & 0xffffffffL;
        }
        int len = (int) (end - start);
        if (len < 0) return false;
        if (utf8Scratch == null || utf8Scratch.length < len) {
            utf8Scratch = new byte[Math.max(64, len)];
        }
        data.getBytes(start, utf8Scratch, 0, len);
        cd.accumulateUtf8(state, utf8Scratch, 0, len);
        return true;
    }

    /**
     * 累加/合并后刷新状态内存计量 —— <b>不只是建组时计一次</b>。
     *
     * <p>COUNT(DISTINCT) 的状态是 {@link Set}：建组时为空，之后每行都可能变大；MIN/MAX 的变长值
     * 也会随输入变化。原先只在 {@code insertGroup} 记账 ⇒ 估算值长期停在建组时的大小，
     * 溢写闸门（{@code estimateMemoryBytes() >= slotBytesThreshold}）永不触发 ⇒ 单组高基数场景
     * 堆被集合单调撑爆 / 长 GC，任务长时间不结束。</p>
     *
     * <p>代价 = 每次累加多做一次 {@code instanceof} 扫描（状态数组长度为聚合个数，通常 1~5），
     * 相对 {@code getObject + accumulate} 可忽略。</p>
     */
    private void trackStateGrowth(long before, Object[] states) {
        long after = estimateStatesMem(states);
        if (after != before) estimatedMem += after - before;
    }

    // ==================== 批量聚合（页内按 group 归组 → 批量累加） ====================

    /** 批量路径收尾：对每个 group，把页内该组的行整批并入各聚合状态。页 root 在 addPage 返回后才被 close。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void flushBatch(Page input) {
        if (batchRows.isEmpty()) return;
        VectorSchemaRoot root = input.getRoot();
        for (Map.Entry<Integer, List<Integer>> e : batchRows.entrySet()) {
            int g = e.getKey();
            List<Integer> rows = e.getValue();
            Object[] states = groupStates.get(g);
            long before = estimateStatesMem(states);
            for (int si = 0; si < specs.length; si++) {
                AggregateFunction<Object> aggregate = (AggregateFunction) aggregates[si];
                if (batchAggs[si] != null) {
                    // 批量聚合：整批并入一次（子 batch 只含该聚合的输入列）
                    try (VectorSchemaRoot sub = selectInputRows(root, rows, si)) {
                        ((BatchAggregateFunction<Object>) batchAggs[si]).accumulateBatch(states[si], sub);
                    }
                } else {
                    // 混合场景（内置 + 批量聚合同 GROUP BY）：非批量聚合逐行并入，语义与逐行路径一致
                    for (int r : rows) accumulateOne(aggregate, states[si], root, si, r);
                }
            }
            trackStateGrowth(before, states);
        }
    }

    /**
     * 从页 root 按行子集构造只含第 si 个聚合输入列的子 batch（独立向量，调用方负责 close）。
     *
     * <p>★列名必须归一成 <b>c0</b>（参数位），与逐行路径
     * {@code ArrowRowCodec.encodeRowRoot} / 标量批量路径 {@code ScalarEvaluator.buildArgRoot}
     * 完全一致：外挂语言（Python 等）按 {@code batch.column("c0")} 按位取参。若沿用上游原始列名
     * （如 {@code stay_seconds}），Python 用户函数会 KeyError → 整条查询 FAILED；</p>
     */
    private VectorSchemaRoot selectInputRows(VectorSchemaRoot root, List<Integer> rows, int si) {
        int col = inputColIndices[si];
        List<Field> fields = new ArrayList<>();
        List<FieldVector> vectors = new ArrayList<>();
        int n = rows.size();
        if (col >= 0) {
            FieldVector src = root.getVector(col);
            FieldVector dst = src.getField().createVector(allocator);
            dst.setInitialCapacity(n);
            dst.allocateNew();
            TransferPair tp = src.makeTransferPair(dst);
            for (int i = 0; i < n; i++) tp.copyValueSafe(rows.get(i), i);
            dst.setValueCount(n);
            // 参数位列名 c0（本表每个聚合规格当前只承载一个输入列；多参聚合按 c0..cn-1 扩展）
            fields.add(new Field("c0", src.getField().getFieldType(), src.getField().getChildren()));
            vectors.add(dst);
        }
        return new VectorSchemaRoot(fields, vectors, n);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void ensureIndices(Page input) {
        if (indicesResolved) return;
        VectorSchemaRoot root = input.getRoot();
        List<Field> fields = root.getSchema().getFields();
        inputColIndices = new int[specs.length];
        valueColIndices = new int[specs.length];
        resolvedOutputTypes = new ArrowType[specs.length];
        resolvedOutputFields = new Field[specs.length];
        resolvedValueFields = new Field[specs.length];
        for (int i = 0; i < specs.length; i++) {
            inputColIndices[i] = indexOf(fields, inputColumns[i]);
            valueColIndices[i] = indexOf(fields, valueColumns[i]);
            Field inputField = inputColIndices[i] >= 0 ? fields.get(inputColIndices[i]) : null;
            Field valueField = valueColIndices[i] >= 0 ? fields.get(valueColIndices[i]) : null;
            ArrowType inputType = inputField == null ? null : inputField.getType();
            resolvedOutputTypes[i] = resultTypeFor(specs[i], inputType);
            resolvedOutputFields[i] = specs[i].outputField(specs[i].outputColumn(), inputField, valueField);
            resolvedValueFields[i] = valueField;
        }
        indicesResolved = true;
    }

    /** 按名解析列下标；名字为 null 或未找到 → -1。 */
    private static int indexOf(List<Field> fields, String name) {
        if (name == null) return -1;
        for (int j = 0; j < fields.size(); j++) {
            if (fields.get(j).getName().equals(name)) return j;
        }
        return -1;
    }

    /**
     * 聚合结果列的 Arrow 类型。
     *
     * <p><b>MIN/MAX 与输入严格同型</b>（日期 → 日期、字符串 → 字符串、布尔 → 布尔、Decimal → Decimal，
     * 数值也保留原宽度：{@code max(int_col)} → INT、{@code min(float_col)} → FLOAT —— 不再漂成 DOUBLE）。
     * 其余聚合沿用 {@link AggregationSpec#outputArrowType()}。</p>
     */
    static ArrowType resultTypeFor(AggregationSpec spec, ArrowType inputType) {
        if (inputType != null && spec.aggregate() != null) {
            String fn = spec.aggregate().functionName();
            if ("min".equals(fn) || "max".equals(fn)) {
                return inputType;
            }
            // DECIMAL 的 SUM/AVG 输出同族 DECIMAL（精度放宽），避免结果退化为浮点。
            if (("sum".equals(fn) || "avg".equals(fn)) && inputType instanceof ArrowType.Decimal d) {
                return AggregationSpec.widenDecimal(d);
            }
            // 整型 SUM 输出 BIGINT（Long 精确累加），不再退化成 double 展示。
            if ("sum".equals(fn) && inputType instanceof ArrowType.Int) {
                return INT64;
            }
        }
        return spec.outputArrowType();
    }

    /** 已解析的结果列类型；输入尚未到达（如全局空输入）时回退静态口径。 */
    private ArrowType outputType(int si) {
        if (resolvedOutputTypes != null && si < resolvedOutputTypes.length
                && resolvedOutputTypes[si] != null) {
            return resolvedOutputTypes[si];
        }
        return specs[si].outputArrowType();
    }

    /**
     * 全局空输入兜底前预解析输出列类型：没有输入页时 {@link #ensureIndices} 不会跑，collect 的
     * 子字段只能按静态兜底（Utf8）—— 与规划期声明的 {@code list<实际元素类型>} 不一致。
     * 调用方（{@code HashAggregationOperator.emitEmptyInput}）有上游 schema，用它对齐一次。
     */
    public void primeEmptyInputFields(List<Field> fields) {
        if (fields == null || fields.isEmpty()) return;
        resolvedOutputTypes = new ArrowType[specs.length];
        resolvedOutputFields = new Field[specs.length];
        resolvedValueFields = new Field[specs.length];
        for (int i = 0; i < specs.length; i++) {
            Field in = fieldByName(fields, inputColumns[i]);
            Field val = fieldByName(fields, valueColumns[i]);
            resolvedOutputTypes[i] = resultTypeFor(specs[i], in == null ? null : in.getType());
            resolvedOutputFields[i] = specs[i].outputField(specs[i].outputColumn(), in, val);
            resolvedValueFields[i] = val;
        }
    }

    private static Field fieldByName(List<Field> fields, String name) {
        int idx = indexOf(fields, name);
        return idx >= 0 ? fields.get(idx) : null;
    }

    /**
     * 结果列 Field；输入尚未到达时按静态口径兜底（collect 的兜底元素类型为 Utf8）。
     */
    private Field outputField(int si) {
        if (resolvedOutputFields != null && si < resolvedOutputFields.length
                && resolvedOutputFields[si] != null) {
            return resolvedOutputFields[si];
        }
        return specs[si].outputField(specs[si].outputColumn(), null);
    }

    // ==================== 输出 ====================

    @SuppressWarnings({"rawtypes", "unchecked"})
    public VectorSchemaRoot buildOutput() {
        List<Field> outputFields = new ArrayList<>();
        List<FieldVector> outputVectors = new ArrayList<>();

        for (FieldVector kv : keyVectors) {
            kv.setValueCount(groupCount);
            outputFields.add(kv.getField());
            outputVectors.add(kv);
        }

        for (int si = 0; si < specs.length; si++) {
            Field outField = outputField(si);
            FieldVector outVec = createVector(outField, allocator);
            outVec.allocateNew();
            AggregateFunction<Object> aggregate = (AggregateFunction) aggregates[si];
            for (int g = 0; g < groupCount; g++) {
                Object state = groupStates.get(g)[si];
                Object result = aggregate.evaluate(state);
                setVectorValue(outVec, g, result);
            }
            outVec.setValueCount(groupCount);
            outputFields.add(outField);
            outputVectors.add(outVec);
        }

        return new VectorSchemaRoot(outputFields, outputVectors, groupCount);
    }

    /**
     * 全局聚合（无 GROUP BY）在<b>空输入</b>时的单行结果：每个聚合取<b>初始状态</b>求值
     * （count = 0，avg/min/max = NULL）。SQL 语义要求无分组键的聚合恒返回 1 行，即使上游
     * 一页数据都没有；由 {@link HashAggregationOperator#emitEmptyInput(int)} 在
     * {@code activeCount == 0} 时补出。
     *
     * <p>不适用分组聚合（键列非空）—— 那种空输入就该是 0 行，调用方必须自己先判
     * {@code keyColumns.isEmpty()}。</p>
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public VectorSchemaRoot buildGlobalEmptyRow() {
        if (!keyColumns.isEmpty()) {
            throw new IllegalStateException("buildGlobalEmptyRow 只适用于无分组键的全局聚合");
        }
        List<Field> fields = new ArrayList<>(specs.length);
        List<FieldVector> vectors = new ArrayList<>(specs.length);
        for (int si = 0; si < specs.length; si++) {
            Field outField = outputField(si);
            FieldVector outVec = createVector(outField, allocator);
            outVec.allocateNew();
            AggregateFunction<Object> aggregate = (AggregateFunction) aggregates[si];
            setVectorValue(outVec, 0, aggregate.evaluate(aggregate.createState()));
            outVec.setValueCount(1);
            fields.add(outField);
            vectors.add(outVec);
        }
        return new VectorSchemaRoot(fields, vectors, 1);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<VectorSchemaRoot> buildOutputBatches(int batchSize) {
        List<VectorSchemaRoot> batches = new ArrayList<>();
        if (groupCount == 0) return batches;

        int offset = 0;
        while (offset < groupCount) {
            // 被取消（算子关闭 / 僵尸片段回收）：立刻收手，绝不在「关到一半」的槽位上继续读。
            // 检查放在批边界 → 已建好的批次完整、自洽，照常返回给调用方（其输出页随后由 doClose
            // 统一 close），不会丢弃半成品批次造成泄漏。
            if (Thread.currentThread().isInterrupted()) return batches;
            int rows = Math.min(batchSize, groupCount - offset);
            List<Field> batchFields = new ArrayList<>();
            List<FieldVector> batchVectors = new ArrayList<>();

            for (FieldVector src : keyVectors) {
                FieldVector dst = (FieldVector) src.makeTransferPair(
                        src.getField().createVector(allocator)).getTo();
                dst.setInitialCapacity(rows); dst.allocateNew();
                TransferPair tp = src.makeTransferPair(dst);
                for (int r = 0; r < rows; r++) tp.copyValueSafe(offset + r, r);
                dst.setValueCount(rows);
                batchFields.add(dst.getField());
                batchVectors.add(dst);
            }

            for (int si = 0; si < specs.length; si++) {
                Field outField = outputField(si);
                FieldVector outVec = createVector(outField, allocator);
                outVec.allocateNew();
                AggregateFunction<Object> aggregate = (AggregateFunction) aggregates[si];
                for (int r = 0; r < rows; r++) {
                    Object state = groupStates.get(offset + r)[si];
                    setVectorValue(outVec, r, aggregate.evaluate(state));
                }
                outVec.setValueCount(rows);
                batchFields.add(outField);
                batchVectors.add(outVec);
            }

            batches.add(new VectorSchemaRoot(batchFields, batchVectors, rows));
            offset += rows;
        }
        return batches;
    }

    // ==================== 溢写（状态列格式） ====================

    public VectorSchemaRoot buildSpillRoot() {
        return buildSpillRoot(true);
    }

    /**
     * 标量聚合的溢写根：每组一行，<b>不含</b> {@code count_distinct} 列。
     *
     * <p>去重集合按值拆成独立流（{@link #buildDistinctSpillRoot(int)}），与本流键空间一致、
     * 布局不同，无法共存于「每组一行」的格式里。</p>
     */
    public VectorSchemaRoot buildScalarSpillRoot() {
        return buildSpillRoot(false);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private VectorSchemaRoot buildSpillRoot(boolean includeDistinct) {
        List<Field> fields = new ArrayList<>();
        List<FieldVector> vectors = new ArrayList<>();

        for (FieldVector kv : keyVectors) {
            kv.setValueCount(groupCount);
            // 键列必须复制：返回的 root 归调用方所有（close() 会释放其中的列）。
            // 直接返回 keyVectors 会让「关闭溢写根」顺手关掉表自己的键列 —— 后续
            // buildDistinctSpillRoot / 下一次溢写读到已释放的缓冲（表现为键全 NULL）。
            FieldVector dst = kv.getField().createVector(allocator);
            dst.setInitialCapacity(groupCount);
            dst.allocateNew();
            TransferPair tp = kv.makeTransferPair(dst);
            for (int g = 0; g < groupCount; g++) tp.copyValueSafe(g, g);
            dst.setValueCount(groupCount);
            fields.add(kv.getField());
            vectors.add(dst);
        }

        for (int si = 0; si < specs.length; si++) {
            AggregateFunction<?> aggregate = aggregates[si];
            String outName = specs[si].outputColumn();
            switch (aggregate.functionName()) {
                case "count", "sum_int64" -> {
                    // sum_int64 与 count 同为 long[] 状态、BIGINT 输出（二级 COUNT DISTINCT 合并列）
                    Field f = new Field(outName + "_s0", FieldType.nullable(INT64), null);
                    BigIntVector bv = new BigIntVector(f, allocator);
                    bv.allocateNew();
                    for (int g = 0; g < groupCount; g++) {
                        long[] state = (long[]) groupStates.get(g)[si];
                        bv.setSafe(g, state[0]);
                    }
                    bv.setValueCount(groupCount);
                    fields.add(f); vectors.add(bv);
                }
                case "sum" -> {
                    // 状态列类型 = 输出类型：DECIMAL 输入 → Decimal 列（精确），数值 → FLOAT64（旧口径）
                    Field f = new Field(outName + "_s0", FieldType.nullable(outputType(si)), null);
                    FieldVector vec = createVector(f, allocator);
                    vec.allocateNew();
                    for (int g = 0; g < groupCount; g++) {
                        Object[] state = (Object[]) groupStates.get(g)[si];
                        setVectorValue(vec, g, state[0]);
                    }
                    vec.setValueCount(groupCount);
                    fields.add(f); vectors.add(vec);
                }
                case "min", "max" -> {
                    // 状态列类型 = 结果列类型：与输入列严格同型（数值保持 INT/FLOAT 原宽度；
                    // 时间戳 / 日期 / 字符串 / 布尔 / Decimal / 二进制）。溢写回读时按此类型还原。
                    Field f = new Field(outName + "_s0", FieldType.nullable(outputType(si)), null);
                    FieldVector vec = createVector(f, allocator);
                    vec.allocateNew();
                    for (int g = 0; g < groupCount; g++) {
                        Object[] state = (Object[]) groupStates.get(g)[si];
                        AbstractSpillableOperator.setVectorValue(vec, g, state[0]);
                    }
                    vec.setValueCount(groupCount);
                    fields.add(f); vectors.add(vec);
                }
                case "avg" -> {
                    for (int ci = 0; ci < 2; ci++) {
                        String label = ci == 0 ? "sum" : "cnt";
                        // sum 列跟随输出类型（DECIMAL → Decimal，数值 → FLOAT64）；cnt 恒 BIGINT
                        Field f = new Field(outName + "_s" + ci + "_" + label,
                                FieldType.nullable(ci == 0 ? outputType(si) : INT64), null);
                        FieldVector vec = createVector(f, allocator);
                        vec.allocateNew();
                        for (int g = 0; g < groupCount; g++) {
                            Object[] state = (Object[]) groupStates.get(g)[si];
                            setVectorValue(vec, g, state[ci]);
                        }
                        vec.setValueCount(groupCount);
                        fields.add(f); vectors.add(vec);
                    }
                }
                case "count_distinct" -> {
                    if (!includeDistinct) break;   // 标量流：去重状态走独立逐值流
                    // 集合状态无处安放为定宽列 → 编码成单列二进制（见 DistinctSet.encode）
                    Field f = new Field(outName + "_s0", FieldType.nullable(VARCHAR), null);
                    VarCharVector vv = new VarCharVector(f, allocator);
                    vv.allocateNew();
                    for (int g = 0; g < groupCount; g++) {
                        @SuppressWarnings("unchecked")
                        Set<Object> state = (Set<Object>) groupStates.get(g)[si];
                        vv.setSafe(g, CountDistinctAggregateFunction.encode(state));
                    }
                    vv.setValueCount(groupCount);
                    fields.add(f); vectors.add(vv);
                }
                case "collect_list", "collect_set" -> {
                    if (!includeDistinct) break;   // 标量流：collect 状态走独立逐值流
                    // 仅供「整根溢写」调试 / 探针使用：整组序列化成一格 List 值。
                    // 生产溢写路径不再走这里（见 buildCollectSpillRoot：每值一行，避免 O(单组值数) 放大）。
                    Field template = outputField(si);
                    Field f = new Field(outName + "_s0", template.getFieldType(), template.getChildren());
                    FieldVector vec = createVector(f, allocator);
                    vec.allocateNew();
                    for (int g = 0; g < groupCount; g++) {
                        Object state = groupStates.get(g)[si];
                        if (state instanceof Collection<?> c && !c.isEmpty()) {
                            setVectorValue(vec, g, c);
                        } else {
                            setVectorValue(vec, g, null);
                        }
                    }
                    vec.setValueCount(groupCount);
                    fields.add(f); vectors.add(vec);
                }
                case "collect_ordered_list" -> {
                    if (!includeDistinct) break;   // 标量流：有序 collect 状态走独立逐值流
                    // 调试 / 探针口径：整组序列化成一格 List 值（已按排序列排好；生产溢写走
                    // buildOrderedCollectSpillRoot：每值一行，附带排序列）。
                    Field template = outputField(si);
                    Field f = new Field(outName + "_s0", template.getFieldType(), template.getChildren());
                    FieldVector vec = createVector(f, allocator);
                    vec.allocateNew();
                    for (int g = 0; g < groupCount; g++) {
                        Object state = groupStates.get(g)[si];
                        setVectorValue(vec, g, ((AggregateFunction) aggregate).evaluate(state));
                    }
                    vec.setValueCount(groupCount);
                    fields.add(f); vectors.add(vec);
                }
                case "collect_map" -> {
                    if (!includeDistinct) break;   // 标量流：map 状态走独立逐值流
                    // 调试 / 探针口径：整组序列化成一格 Map 值（生产溢写走 buildMapCollectSpillRoot）。
                    Field template = outputField(si);
                    Field f = new Field(outName + "_s0", template.getFieldType(), template.getChildren());
                    FieldVector vec = createVector(f, allocator);
                    vec.allocateNew();
                    for (int g = 0; g < groupCount; g++) {
                        Object state = groupStates.get(g)[si];
                        setVectorValue(vec, g, ((AggregateFunction) aggregate).evaluate(state));
                    }
                    vec.setValueCount(groupCount);
                    fields.add(f); vectors.add(vec);
                }
                default -> throw new UnsupportedOperationException(
                        "Spill not supported for aggregate: " + aggregate.functionName());
            }
        }

        return new VectorSchemaRoot(fields, vectors, groupCount);
    }

    public VectorSchemaRoot buildSortedSpillRoot() {
        return buildSortedSpillRoot(true);
    }

    /** 标量流的按键排序溢写根（不含 {@code count_distinct} 列）。 */
    public VectorSchemaRoot buildSortedScalarSpillRoot() {
        return buildSortedSpillRoot(false);
    }

    private VectorSchemaRoot buildSortedSpillRoot(boolean includeDistinct) {
        VectorSchemaRoot unsorted = buildSpillRoot(includeDistinct);
        int n = unsorted.getRowCount();
        AbstractSpillableOperator.checkSpillCancelled();

        // ★比较直接读 Arrow 缓冲（下标排序 + 原始字节/数值比较），**不物化键对象**。
        //   旧实现把每个键取成 Java 对象（变长键 = 每个 new 一个 String）：百万级分组时
        //   一次溢写就凭空造出上百万 String / 装箱，溢出线程的排序阶段被 GC 拖成分钟级；
        //   排序完成前的临时垃圾还会把本来就吃紧的堆再推高一档（2026-11-19 事故）。
        int keyCount = keyColumns.size();
        FieldVector[] keyVecs = new FieldVector[keyCount];
        for (int c = 0; c < keyCount; c++) keyVecs[c] = unsorted.getVector(c);
        int[] sortedIdx = new int[n];
        for (int i = 0; i < n; i++) sortedIdx[i] = i;
        sortIndices(sortedIdx, (a, b) -> {
            for (int c = 0; c < keyCount; c++) {
                int cmp = compareRowsAt(keyVecs[c], a, b);
                if (cmp != 0) return cmp;
            }
            return 0;
        });

        List<Field> fields = unsorted.getSchema().getFields();
        List<FieldVector> srcVectors = unsorted.getFieldVectors();
        List<FieldVector> dstVectors = new ArrayList<>();

        for (int col = 0; col < srcVectors.size(); col++) {
            FieldVector src = srcVectors.get(col);
            FieldVector dst = fields.get(col).createVector(allocator);
            dst.setInitialCapacity(n); dst.allocateNew();
            TransferPair tp = src.makeTransferPair(dst);
            for (int r = 0; r < n; r++) {
                if ((r & 0xFFF) == 0) AbstractSpillableOperator.checkSpillCancelled();
                tp.copyValueSafe(sortedIdx[r], r);
            }
            dst.setValueCount(n);
            dstVectors.add(dst);
        }

        // unsorted 的列全是本次新建的副本（buildSpillRoot 不再外借 keyVectors）⇒ 整体关闭即可。
        unsorted.close();

        return new VectorSchemaRoot(fields, dstVectors, n);
    }

    /**
     * 单个 {@code count_distinct} 规格的<b>逐值溢写根</b>：{@code [key..., value]}，每个去重值一行，
     * 按 {@code (key, value)} 升序。
     *
     * <p>与「整组集合编码成一条 VARCHAR」的旧格式相比：旧格式一行就是整组的值，单次编解码的
     * 内存/耗时都是 O(单组值数) —— 低基数分组 + 超高基数去重值（{@code group by event_type} +
     * 几百万 {@code user_id}）时编解码往返自身就把查询拖死。新格式把值摊平成行，Phase 2 按
     * {@code (key, value)} 有序流式扫描，「相邻等值只计一次」即可去重 ⇒ 峰值内存与单组基数无关
     * （对齐 Window / Collect 的「排序溢写 + 流式归并」模型）。</p>
     *
     * <p>值列类型按<b>状态值的实际 Java 类型</b>选：{@link com.dojao.core.function.aggregate.DistinctSet}
     * 在累加时已把值规范化为 Long / Double / String（日期 / Decimal 等非原生列也一样），故只需
     * BigInt / Float8 / VarChar 三选一，不依赖输入 Arrow 类型。</p>
     *
     * @return 该规格的溢写根；本槽位该规格没有任何去重值时返回 {@code null}（不必写文件）
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public VectorSchemaRoot buildDistinctSpillRoot(int ordinal) {
        int si = distinctSpecs[ordinal];
        int n = 0;
        for (int g = 0; g < groupCount; g++) {
            Set<?> set = (Set<?>) groupStates.get(g)[si];
            if (set != null) n += set.size();
        }
        if (n == 0) return null;

        ArrowType valueType = distinctValueArrowType(si);
        Field valueField = new Field(specs[si].outputColumn() + "_dv",
                FieldType.nullable(valueType), null);

        List<Field> fields = new ArrayList<>(keyColumns.size() + 1);
        List<FieldVector> vectors = new ArrayList<>(keyColumns.size() + 1);
        List<TransferPair> keyTps = new ArrayList<>(keyColumns.size());
        for (FieldVector kv : keyVectors) {
            // ★必须先设 valueCount：TransferPair.copyValueSafe 读源列 isSet 时会校验行号 < valueCount。
            //   buildSpillRoot 里有同样的语句，但本方法可能先被调用（如探针直调 / 未来别的溢写点），
            //   不能依赖调用顺序 —— 否则变长键列抛 IndexOutOfBoundsException。
            kv.setValueCount(groupCount);
            FieldVector dst = kv.getField().createVector(allocator);
            dst.setInitialCapacity(n);
            dst.allocateNew();
            keyTps.add(kv.makeTransferPair(dst));
            fields.add(kv.getField());
            vectors.add(dst);
        }
        FieldVector valueVec = createVector(valueField, allocator);
        valueVec.setInitialCapacity(n);
        valueVec.allocateNew();
        fields.add(valueField);
        vectors.add(valueVec);

        int r = 0;
        for (int g = 0; g < groupCount; g++) {
            Set<?> set = (Set<?>) groupStates.get(g)[si];
            if (set == null || set.isEmpty()) continue;
            for (Object v : set) {
                for (TransferPair tp : keyTps) tp.copyValueSafe(g, r);
                setVectorValue(valueVec, r, v);
                r++;
            }
        }
        for (FieldVector v : vectors) v.setValueCount(n);
        VectorSchemaRoot unsorted = new VectorSchemaRoot(fields, vectors, n);
        try {
            // 原始类型下标排序：单槽位的去重值可达数百万，Integer[] 装箱排序会额外吃掉
            // 几十上百 MB 堆（每元素一个 Integer + 引用），与「按值溢写降低峰值」的目标相悖。
            // 键/值同样只取一次，避免在 O(n log n) 次比较里反复 getObject。
            final int[] idx = new int[n];
            for (int i = 0; i < n; i++) idx[i] = i;
            final int keyCount = keyColumns.size();
            final Object[][] keys = new Object[keyCount][n];
            for (int c = 0; c < keyCount; c++) {
                Object[] col = keys[c];
                for (int i = 0; i < n; i++) col[i] = valueAt(unsorted, c, i);
            }
            final Object[] vals = new Object[n];
            for (int i = 0; i < n; i++) vals[i] = valueAt(unsorted, keyCount, i);
            sortIndices(idx, (a, b) -> {
                for (int c = 0; c < keyCount; c++) {
                    int cmp = compareKeyValues(keys[c][a], keys[c][b]);
                    if (cmp != 0) return cmp;
                }
                return compareValues(vals[a], vals[b]);
            });

            List<FieldVector> dstVectors = new ArrayList<>(vectors.size());
            for (int col = 0; col < vectors.size(); col++) {
                FieldVector src = vectors.get(col);
                FieldVector dst = fields.get(col).createVector(allocator);
                dst.setInitialCapacity(n);
                dst.allocateNew();
                TransferPair tp = src.makeTransferPair(dst);
                for (int i = 0; i < n; i++) tp.copyValueSafe(idx[i], i);
                dst.setValueCount(n);
                dstVectors.add(dst);
            }
            return new VectorSchemaRoot(fields, dstVectors, n);
        } finally {
            unsorted.close();
        }
    }

    /** collect 规格个数（= collect 逐值流条数）。 */
    public int collectSpecCount() {
        return collectSpecs.length;
    }

    /** collect ordinal → {@code specs} 下标。 */
    public int collectSpecIndex(int ordinal) {
        return collectSpecs[ordinal];
    }

    /**
     * {@code collect_list} / {@code collect_set} 的<b>逐值</b>溢写根：{@code [key..., value]}，
     * 一个值一行，按<b>键升序</b>排列（稳定排序 ⇒ 同组内仍是累加顺序；collect_set 的「保序」语义不变）。
     *
     * <p>与 {@link #buildDistinctSpillRoot(int)} 同一思路、少一步「按值排序 + 相邻去重」：
     * {@code collect_list} 必须保留重复值，且值不需要排序。相比旧格式（整组塞进一格 List），
     * 溢写恒定 O(槽内行数)、每次 reAlloc 只碰一行 → 低基数分组 + 几百万值时不再把溢写线程拖死。</p>
     *
     * @return 该规格的溢写根；本槽位该规格没有任何值时返回 {@code null}（不必写文件）
     */
    public VectorSchemaRoot buildCollectSpillRoot(int ordinal) {
        int si = collectSpecs[ordinal];
        if (specs[si].isCollectMap()) return buildMapCollectSpillRoot(si);
        if (specs[si].isOrderedList()) return buildOrderedCollectSpillRoot(si);
        int n = 0;
        for (int g = 0; g < groupCount; g++) {
            Object st = groupStates.get(g)[si];
            if (st instanceof Collection<?> c) n += c.size();
        }
        if (n == 0) return null;

        // 元素类型取自输入列（outputField 已解析子字段；输入未知时兜底 Utf8）
        Field template = outputField(si);
        Field child = template.getChildren().isEmpty() ? null : template.getChildren().get(0);
        Field valueField = new Field(specs[si].outputColumn() + "_cv",
                child != null ? child.getFieldType() : FieldType.nullable(VARCHAR),
                child != null ? child.getChildren() : null);

        List<Field> fields = new ArrayList<>(keyColumns.size() + 1);
        List<FieldVector> vectors = new ArrayList<>(keyColumns.size() + 1);
        List<TransferPair> keyTps = new ArrayList<>(keyColumns.size());
        for (FieldVector kv : keyVectors) {
            // ★必须先设 valueCount：TransferPair.copyValueSafe 会校验源行号 < valueCount
            kv.setValueCount(groupCount);
            FieldVector dst = kv.getField().createVector(allocator);
            dst.setInitialCapacity(n);
            dst.allocateNew();
            keyTps.add(kv.makeTransferPair(dst));
            fields.add(kv.getField());
            vectors.add(dst);
        }
        FieldVector valueVec = createVector(valueField, allocator);
        valueVec.setInitialCapacity(n);
        valueVec.allocateNew();
        fields.add(valueField);
        vectors.add(valueVec);

        // 键排序（稳定）：只排 group 下标，不碰值；与 buildSortedSpillRoot 的输出键序一致。
        int[] order = new int[groupCount];
        for (int g = 0; g < groupCount; g++) order[g] = g;
        if (!keyColumns.isEmpty() && groupCount > 1) {
            Object[] keys = new Object[groupCount];
            for (int g = 0; g < groupCount; g++) keys[g] = extractKey(g);
            sortIndices(order, (a, b) -> compareKeyValues(keys[a], keys[b]));
        }

        int r = 0;
        for (int g : order) {
            Object st = groupStates.get(g)[si];
            if (!(st instanceof Collection<?> c) || c.isEmpty()) continue;
            for (Object v : c) {
                for (TransferPair tp : keyTps) tp.copyValueSafe(g, r);
                setVectorValue(valueVec, r, CollectSupport.export(v));
                r++;
            }
        }
        for (FieldVector v : vectors) v.setValueCount(r);
        return new VectorSchemaRoot(fields, vectors, r);
    }

    /**
     * {@code collect_ordered_list} 的<b>逐值</b>溢写根：{@code [key..., value, orderKey]}，
     * 每个收集值一行，按键有序（稳定排序 ⇒ 组内保持累加顺序；最终顺序在 evaluate 时按排序列重排）。
     *
     * @return 该规格的溢写根；本槽位该规格没有任何值时返回 {@code null}
     */
    private VectorSchemaRoot buildOrderedCollectSpillRoot(int si) {
        int n = 0;
        for (int g = 0; g < groupCount; g++) {
            Object st = groupStates.get(g)[si];
            if (st instanceof CollectOrderedListAggregateFunction.OrderedState os) n += os.size();
        }
        if (n == 0) return null;

        Field template = outputField(si);
        Field child = template.getChildren().isEmpty() ? null : template.getChildren().get(0);
        String outName = specs[si].outputColumn();
        Field valueField = new Field(outName + "_cv",
                child != null ? child.getFieldType() : FieldType.nullable(VARCHAR),
                child != null ? child.getChildren() : null);
        Field orderField = resolvedValueFields != null ? resolvedValueFields[si] : null;
        if (orderField == null) {
            orderField = new Field(outName + "_ov", FieldType.nullable(VARCHAR), null);
        } else {
            orderField = new Field(outName + "_ov", FieldType.nullable(orderField.getType()),
                    orderField.getChildren());
        }

        List<Field> fields = new ArrayList<>(keyColumns.size() + 2);
        List<FieldVector> vectors = new ArrayList<>(keyColumns.size() + 2);
        List<TransferPair> keyTps = new ArrayList<>(keyColumns.size());
        for (FieldVector kv : keyVectors) {
            // ★必须先设 valueCount：TransferPair.copyValueSafe 会校验源行号 < valueCount
            kv.setValueCount(groupCount);
            FieldVector dst = kv.getField().createVector(allocator);
            dst.setInitialCapacity(n);
            dst.allocateNew();
            keyTps.add(kv.makeTransferPair(dst));
            fields.add(kv.getField());
            vectors.add(dst);
        }
        FieldVector valueVec = createVector(valueField, allocator);
        valueVec.setInitialCapacity(n);
        valueVec.allocateNew();
        FieldVector orderVec = createVector(orderField, allocator);
        orderVec.setInitialCapacity(n);
        orderVec.allocateNew();
        fields.add(valueField);
        fields.add(orderField);
        vectors.add(valueVec);
        vectors.add(orderVec);

        // 键排序（稳定）：与其它 collect 流一致，保证 Phase 2 K 路归并按组聚集
        int[] order = new int[groupCount];
        for (int g = 0; g < groupCount; g++) order[g] = g;
        if (!keyColumns.isEmpty() && groupCount > 1) {
            Object[] keys = new Object[groupCount];
            for (int g = 0; g < groupCount; g++) keys[g] = extractKey(g);
            sortIndices(order, (a, b) -> compareKeyValues(keys[a], keys[b]));
        }

        int r = 0;
        for (int g : order) {
            Object st = groupStates.get(g)[si];
            if (!(st instanceof CollectOrderedListAggregateFunction.OrderedState os)
                    || os.size() == 0) continue;
            List<Object> vals = os.values();
            List<Object> keys = os.orderKeys();
            for (int i = 0; i < vals.size(); i++) {
                for (TransferPair tp : keyTps) tp.copyValueSafe(g, r);
                setVectorValue(valueVec, r, CollectSupport.export(vals.get(i)));
                setVectorValue(orderVec, r, CollectSupport.export(keys.get(i)));
                r++;
            }
        }
        for (FieldVector v : vectors) v.setValueCount(r);
        return new VectorSchemaRoot(fields, vectors, r);
    }

    /**
     * {@code collect_map} 的<b>逐值</b>溢写根：{@code [key..., mapKey, mapValue]}，每个 map 条目一行
     * （追加模式下每个列表元素一行），按键有序（稳定排序 ⇒ 组内保持 map 插入顺序 / 列表顺序）。
     *
     * <p>与 collect_list / collect_set 同一思路：溢写恒定 O(槽内条目数)，不把整张 map 塞进一格。</p>
     *
     * @return 该规格的溢写根；本槽位该规格没有任何条目时返回 {@code null}
     */
    private VectorSchemaRoot buildMapCollectSpillRoot(int si) {
        boolean append = specs[si].mapAppend();
        int n = 0;
        for (int g = 0; g < groupCount; g++) {
            Object st = groupStates.get(g)[si];
            if (!(st instanceof CollectMapAggregateFunction.MapState ms)) continue;
            if (append) {
                for (Object v : ms.entries().values()) n += ((Collection<?>) v).size();
            } else {
                n += ms.mapSize();
            }
        }
        if (n == 0) return null;

        Field template = outputField(si);
        Field entries = template.getChildren().get(0);
        Field keyChild = entries.getChildren().get(0);
        Field valueChild = append
                ? entries.getChildren().get(1).getChildren().get(0)
                : entries.getChildren().get(1);
        String outName = specs[si].outputColumn();
        Field mapKeyField = new Field(outName + "_mk",
                FieldType.nullable(keyChild.getType()), keyChild.getChildren());
        Field mapValueField = new Field(outName + "_mv",
                FieldType.nullable(valueChild.getType()), valueChild.getChildren());

        List<Field> fields = new ArrayList<>(keyColumns.size() + 2);
        List<FieldVector> vectors = new ArrayList<>(keyColumns.size() + 2);
        List<TransferPair> keyTps = new ArrayList<>(keyColumns.size());
        for (FieldVector kv : keyVectors) {
            // ★必须先设 valueCount：TransferPair.copyValueSafe 会校验源行号 < valueCount
            kv.setValueCount(groupCount);
            FieldVector dst = kv.getField().createVector(allocator);
            dst.setInitialCapacity(n);
            dst.allocateNew();
            keyTps.add(kv.makeTransferPair(dst));
            fields.add(kv.getField());
            vectors.add(dst);
        }
        FieldVector keyVec = createVector(mapKeyField, allocator);
        keyVec.setInitialCapacity(n);
        keyVec.allocateNew();
        FieldVector valueVec = createVector(mapValueField, allocator);
        valueVec.setInitialCapacity(n);
        valueVec.allocateNew();
        fields.add(mapKeyField);
        fields.add(mapValueField);
        vectors.add(keyVec);
        vectors.add(valueVec);

        // 键排序（稳定）：与其它 collect 流一致，保证 Phase 2 K 路归并按组聚集
        int[] order = new int[groupCount];
        for (int g = 0; g < groupCount; g++) order[g] = g;
        if (!keyColumns.isEmpty() && groupCount > 1) {
            Object[] keys = new Object[groupCount];
            for (int g = 0; g < groupCount; g++) keys[g] = extractKey(g);
            sortIndices(order, (a, b) -> compareKeyValues(keys[a], keys[b]));
        }

        int r = 0;
        for (int g : order) {
            Object st = groupStates.get(g)[si];
            if (!(st instanceof CollectMapAggregateFunction.MapState ms) || ms.mapSize() == 0) continue;
            for (java.util.Map.Entry<Object, Object> e : ms.entries().entrySet()) {
                if (append) {
                    for (Object elem : (Collection<?>) e.getValue()) {
                        for (TransferPair tp : keyTps) tp.copyValueSafe(g, r);
                        setVectorValue(keyVec, r, CollectSupport.export(e.getKey()));
                        setVectorValue(valueVec, r, CollectSupport.export(elem));
                        r++;
                    }
                } else {
                    for (TransferPair tp : keyTps) tp.copyValueSafe(g, r);
                    setVectorValue(keyVec, r, CollectSupport.export(e.getKey()));
                    setVectorValue(valueVec, r, CollectSupport.export(e.getValue()));
                    r++;
                }
            }
        }
        for (FieldVector v : vectors) v.setValueCount(r);
        return new VectorSchemaRoot(fields, vectors, r);
    }

    /** 去重值列类型：Long → BIGINT、Double → FLOAT8、其余（String）→ VARCHAR。 */
    private ArrowType distinctValueArrowType(int si) {
        for (Object[] states : groupStates) {
            Object st = states[si];
            if (!(st instanceof Set<?> set) || set.isEmpty()) continue;
            for (Object v : set) {
                if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
                    return INT64;
                }
                if (v instanceof Number) return FLOAT64;
                return VARCHAR;
            }
        }
        return VARCHAR;
    }

    /** 读一个单元格的稳定值（变长列复制成 String，避免 ArrowFileReader 复用缓冲后失效）。 */
    static Object valueAt(VectorSchemaRoot root, int col, int row) {
        FieldVector v = root.getVector(col);
        if (v.isNull(row)) return null;
        Object raw = v.getObject(row);
        if (raw instanceof byte[] b) return new String(b, StandardCharsets.UTF_8);
        return raw;
    }

    /**
     * 去重值全序（与 {@link #distinctValueArrowType} 的三类一一对应）。
     * 「相邻相等即重复」依赖此序：同一分组的值按本序升序，重复值必然相邻。
     */
    static int compareValues(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        // Arrow 15 的 Text 不实现 Comparable，按 UTF-8 字节序比较（与 equals 的字节相等语义一致）
        if (a instanceof Text ta && b instanceof Text tb) {
            return Arrays.compareUnsigned(ta.getBytes(), tb.getBytes());
        }
        if (a instanceof Number na && b instanceof Number nb) {
            if (a instanceof Double || a instanceof Float || b instanceof Double || b instanceof Float) {
                return Double.compare(na.doubleValue(), nb.doubleValue());
            }
            return Long.compare(na.longValue(), nb.longValue());
        }
        return a.toString().compareTo(b.toString());
    }

    @FunctionalInterface
    private interface IntComparator {
        int compare(int a, int b);
    }

    /** 原始 int 下标归并排序（稳定、O(n) 额外 4n 字节，避免装箱）。 */
    private static void sortIndices(int[] a, IntComparator cmp) {
        int[] tmp = new int[a.length];
        mergeSort(a, tmp, 0, a.length, cmp);
    }

    /**
     * 溢写排序用的两行键值比较 —— <b>零物化</b>，直接读 Arrow 缓冲。
     *
     * <p>顺序与 {@link #compareValues}（Phase 2 K 路归并用的口径）一致：
     * 变长按无符号字节序（与 {@link Text} 的相等/比较语义一致，且 ASCII 数据下与
     * {@code String.compareTo} 相同）、数值按数值、其余（日期 / 时间戳 / Decimal 等）
     * 退回 {@code toString()} 比较。两边口径必须一致，否则同一键在溢写文件内与跨文件归并时
     * 排序不同 ⇒ 相邻键判定失败、重复行。
     *
     * @param v 键列
     * @param a 行号 a
     * @param b 行号 b
     */
    static int compareRowsAt(FieldVector v, int a, int b) {
        boolean na = v.isNull(a), nb = v.isNull(b);
        if (na || nb) return na && nb ? 0 : (na ? -1 : 1);
        if (v instanceof BaseVariableWidthVector vv) return compareVarWidth(vv, a, b);
        if (v instanceof BigIntVector x) return Long.compare(x.get(a), x.get(b));
        if (v instanceof IntVector x) return Integer.compare(x.get(a), x.get(b));
        if (v instanceof SmallIntVector x) return Integer.compare(x.get(a), x.get(b));
        if (v instanceof TinyIntVector x) return Integer.compare(x.get(a), x.get(b));
        if (v instanceof Float8Vector x) return Double.compare(x.get(a), x.get(b));
        if (v instanceof Float4Vector x) return Double.compare(x.get(a), x.get(b));
        if (v instanceof BitVector x) return Integer.compare(x.get(a), x.get(b));
        return compareValues(v.getObject(a), v.getObject(b));
    }

    /** 变长列的字节级比较（UTF-8 无符号字节序 == {@link Text} 的比较语义，零分配）。 */
    private static int compareVarWidth(BaseVariableWidthVector v, int a, int b) {
        ArrowBuf off = v.getOffsetBuffer();
        ArrowBuf data = v.getDataBuffer();
        int s1 = off.getInt((long) a * 4), e1 = off.getInt((long) (a + 1) * 4);
        int s2 = off.getInt((long) b * 4), e2 = off.getInt((long) (b + 1) * 4);
        int l1 = e1 - s1, l2 = e2 - s2;
        int n = Math.min(l1, l2);
        for (int i = 0; i < n; i++) {
            int x = data.getByte((long) s1 + i) & 0xFF;
            int y = data.getByte((long) s2 + i) & 0xFF;
            if (x != y) return Integer.compare(x, y);
        }
        return Integer.compare(l1, l2);
    }

    private static void mergeSort(int[] a, int[] tmp, int lo, int hi, IntComparator cmp) {
        if (hi - lo < 2) return;
        // 每层递归入口一次取消检查：整趟排序是纯 CPU 长循环，没有检查点就收不到取消信号
        AbstractSpillableOperator.checkSpillCancelled();
        int mid = (lo + hi) >>> 1;
        mergeSort(a, tmp, lo, mid, cmp);
        mergeSort(a, tmp, mid, hi, cmp);
        int i = lo, j = mid, k = lo;
        while (i < mid && j < hi) {
            if (cmp.compare(a[j], a[i]) < 0) tmp[k++] = a[j++];
            else tmp[k++] = a[i++];
        }
        while (i < mid) tmp[k++] = a[i++];
        while (j < hi) tmp[k++] = a[j++];
        System.arraycopy(tmp, lo, a, lo, hi - lo);
    }

    /**
     * 已取出的键值比较（与 {@link #compareValues} 同序：Text 按 UTF-8 字节、数值按数值、其余 toString）。
     *
     * <p>★排序期间必须用<b>已物化</b>的键对象比较，不能在比较器里反复 {@code getObject}：
     * 变长列每次 {@code getObject} 都复制字节，n log n 次比较就是 n log n 次分配。</p>
     */
    static int compareKeyValues(Object va, Object vb) {
        if (va instanceof List<?> la && vb instanceof List<?> lb) {
            return ArrowComparators.compareLists(la, lb);
        }
        return compareValues(va, vb);
    }

    public Object extractKey(int row) {
        if (keyColumns.size() == 1) {
            FieldVector vec = keyVectors.get(0);
            if (vec.isNull(row)) return null;
            return toKeyValue(vec.getObject(row));
        }
        Object[] values = new Object[keyColumns.size()];
        for (int i = 0; i < keyColumns.size(); i++) {
            FieldVector vec = keyVectors.get(i);
            values[i] = vec.isNull(row) ? null : toKeyValue(vec.getObject(row));
        }
        return Arrays.asList(values);
    }

    private static Object toKeyValue(Object raw) {
        if (raw == null) return null;
        if (raw instanceof byte[] b) return new String(b, StandardCharsets.UTF_8);
        return raw;
    }

    public AggregationSpec[] getSpecs() { return specs; }

    // ==================== 合并（溢写回读） ====================

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void mergeSpilledRoot(VectorSchemaRoot spillRoot) {
        int spillSize = spillRoot.getRowCount();
        if (spillSize == 0) return;

        List<TransferPair> transfers = new ArrayList<>();
        for (int i = 0; i < keyColumns.size(); i++)
            transfers.add(spillRoot.getVector(keyColumns.get(i)).makeTransferPair(keyVectors.get(i)));

        int colOffset = keyColumns.size();
        int[] stateColCount = new int[specs.length];
        for (int si = 0; si < specs.length; si++)
            stateColCount[si] = aggregates[si].functionName().equals("avg") ? 2 : 1;

        int[] spillKeyIdx = new int[keyColumns.size()];
        for (int i = 0; i < spillKeyIdx.length; i++) spillKeyIdx[i] = i;
        for (int r = 0; r < spillSize; r++) {
            // 与主表插入同一个内容 hash 口径（变长按字节）：否则 spill 合并时同键会落不同桶 → 重复分组。
            int hash = hashRow(spillRoot, spillKeyIdx, r);
            int bucket = bucketOf(hash, capacity);

            int groupIdx;
            while (true) {
                groupIdx = buckets[bucket];
                if (groupIdx == -1) {
                    for (TransferPair tp : transfers) tp.copyValueSafe(r, groupCount);
                    buckets[bucket] = groupCount;
                    groupIdx = groupCount;
                    ensureRowHashesCapacity(groupCount);
                    rowHashes[groupCount] = hash;
                    groupCount++;

                    Object[] states = new Object[specs.length];
                    int col = colOffset;
                    for (int si = 0; si < specs.length; si++) {
                        states[si] = readStateFromSpillCols(spillRoot, r, si, col);
                        col += stateColCount[si];
                    }
                    groupStates.add(states);
                    estimatedMem += estimateStatesMem(states) + estimateKeyRowMem();
                    break;
                }

                boolean equal = true;
                for (int i = 0; i < keyColumns.size(); i++) {
                    FieldVector sv = spillRoot.getVector(i);
                    FieldVector mv = keyVectors.get(i);
                    boolean sn = sv.isNull(r), mn = mv.isNull(groupIdx);
                    if (sn && mn) continue;
                    if (sn != mn) { equal = false; break; }
                    if (!sv.getObject(r).equals(mv.getObject(groupIdx))) { equal = false; break; }
                }
                if (equal) break;
                bucket = (bucket + 1) % capacity;
            }

            Object[] myStates = groupStates.get(groupIdx);
            long before = estimateStatesMem(myStates);
            int col = colOffset;
            for (int si = 0; si < specs.length; si++) {
                Object otherState = readStateFromSpillCols(spillRoot, r, si, col);
                AggregateFunction<Object> aggregate = (AggregateFunction) aggregates[si];
                aggregate.merge(myStates[si], otherState);
                col += stateColCount[si];
            }
            trackStateGrowth(before, myStates);

            if (groupCount > capacity * LOAD_FACTOR) rehash();
        }
    }

    @SuppressWarnings("unchecked")
    private Object readStateFromSpillCols(VectorSchemaRoot root, int row, int specIdx, int startCol) {
        return switch (aggregates[specIdx].functionName()) {
            case "count", "sum_int64" -> {
                Object val = root.getVector(startCol).getObject(row);
                long v = val == null ? 0L : ((Number) val).longValue();
                yield new long[]{v};
            }
            case "sum" -> {
                Object val = root.getVector(startCol).getObject(row);
                // DECIMAL 列 getObject 返回 BigDecimal ⇒ 保持精确；BIGINT 列返回 Long ⇒ 保持整型
                // （不能转 double，否则 sum(CAST(x AS BIGINT)) 溢写后回读丢精度）。
                // ★NULL 状态必须原样保留（空集 / 全 NULL 组）——不能回填 0.0。
                // ★必须用 if/else 而不是三元表达式：Long 与 double 两支会被数值提升统一成
                //   Double，整型状态溢写回读后丢失精度（2^53+1 → 2^53）。
                Object pick;
                if (val == null) {
                    pick = null;
                } else if (val instanceof BigDecimal bd) {
                    pick = bd;
                } else if (val instanceof Long || val instanceof Integer
                        || val instanceof Short || val instanceof Byte) {
                    pick = ((Number) val).longValue();
                } else {
                    pick = ((Number) val).doubleValue();
                }
                yield new Object[]{pick};
            }
            case "min", "max" -> {
                // 溢写列类型可以是时间戳 / 日期 / 字符串 / 数值 —— 原样读回再规范化（数值 → Long/Double）
                Object val = root.getVector(startCol).getObject(row);
                yield new Object[]{MinMaxSupport.normalize(val)};
            }
            case "avg" -> {
                Object sumVal = root.getVector(startCol).getObject(row);
                Object cntVal = root.getVector(startCol + 1).getObject(row);
                Object sum = sumVal instanceof BigDecimal bd ? bd
                        : (sumVal == null ? 0.0d : ((Number) sumVal).doubleValue());
                long cnt = cntVal == null ? 0L : ((Number) cntVal).longValue();
                yield new Object[]{sum, cnt};
            }
            case "count_distinct" -> {
                VarCharVector vv = (VarCharVector) root.getVector(startCol);
                yield CountDistinctAggregateFunction.decode(vv.isNull(row) ? null : vv.get(row));
            }
            case "collect_list", "collect_set" -> {
                // List 列一格 = 整组值；NULL（空组 / 全 NULL 组）→ 空状态。
                Object val = root.getVector(startCol).getObject(row);
                java.util.Collection<?> vals = val instanceof java.util.Collection<?> c ? c : null;
                yield CollectSupport.fromValues(aggregates[specIdx].functionName(), vals);
            }
            case "collect_ordered_list" -> {
                // 整根溢写（调试 / 探针）一格 = 已排序的整组值，排序键置 NULL（稳定排序保序）
                Object val = root.getVector(startCol).getObject(row);
                java.util.Collection<?> vals = val instanceof java.util.Collection<?> c ? c : null;
                yield CollectOrderedListAggregateFunction.fromValues(vals);
            }
            case "collect_map" -> {
                // 整根溢写（调试 / 探针）一格 = 整张 map；逐条目流由 HashAggregationOperator 的
                // collect 通道回读，不走这里。逐条目 replay 走 accumulate，与累加路径同口径。
                Object st = aggregates[specIdx].createState();
                FieldVector vec = root.getVector(startCol);
                if (vec instanceof org.apache.arrow.vector.complex.MapVector map && !map.isNull(row)) {
                    List<FieldVector> kv = ((org.apache.arrow.vector.complex.StructVector) map.getDataVector())
                            .getChildrenFromFields();
                    FieldVector keys = kv.get(0);
                    FieldVector values = kv.get(1);
                    boolean append = specs[specIdx].mapAppend();
                    int start = map.getElementStartIndex(row);
                    int end = map.getElementEndIndex(row);
                    for (int i = start; i < end; i++) {
                        Object k = keys.isNull(i) ? null : keys.getObject(i);
                        Object v = values.isNull(i) ? null : values.getObject(i);
                        if (append && v instanceof java.util.Collection<?> vs) {
                            for (Object elem : vs) {
                                ((AggregateFunction<Object>) aggregates[specIdx])
                                        .accumulate(st, new Object[]{k, elem});
                            }
                        } else {
                            ((AggregateFunction<Object>) aggregates[specIdx])
                                    .accumulate(st, new Object[]{k, v});
                        }
                    }
                }
                yield st;
            }
            default -> throw new UnsupportedOperationException(
                    "Cannot read state for aggregate: " + aggregates[specIdx].functionName());
        };
    }

    // ==================== 溢写 & 合并 ====================

    public AggregationHashTable spillAll() {
        List<Field> kf = keyVectors.stream().map(FieldVector::getField).toList();
        List<AggregationSpec> sl = Arrays.asList(specs);
        AggregationHashTable spilled = new AggregationHashTable(allocator, keyColumns, kf, sl);

        for (FieldVector v : spilled.keyVectors) v.close();
        spilled.keyVectors.clear();

        spilled.groupCount = this.groupCount;
        spilled.capacity = this.capacity;
        spilled.buckets = this.buckets;
        spilled.keyVectors.addAll(this.keyVectors);
        spilled.keyTransfers.addAll(this.keyTransfers);
        spilled.groupStates.addAll(this.groupStates);
        spilled.indicesResolved = this.indicesResolved;
        spilled.inputColIndices = this.inputColIndices;
        spilled.resolvedOutputTypes = this.resolvedOutputTypes;
        spilled.resolvedOutputFields = this.resolvedOutputFields;
        spilled.resolvedValueFields = this.resolvedValueFields;
        spilled.rowHashes = this.rowHashes;
        spilled.rehashCount = this.rehashCount;
        spilled.estimatedMem = this.estimatedMem;

        this.buckets = new int[INITIAL_CAPACITY];
        Arrays.fill(this.buckets, -1);
        this.capacity = INITIAL_CAPACITY;
        this.groupCount = 0;
        this.keyVectors.clear();
        this.keyTransfers.clear();
        this.groupStates.clear();
        this.rowHashes = new int[DEFAULT_VECTOR_CAPACITY];
        this.rehashCount = 0;
        this.estimatedMem = INITIAL_CAPACITY * 4L;
        this.estimatedMem += (long) DEFAULT_VECTOR_CAPACITY * 4L;
        this.lastInputRoot = null;

        return spilled;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void merge(AggregationHashTable other) {
        if (other.groupCount == 0) return;

        List<TransferPair> otherKeyTransfers = new ArrayList<>();
        for (int i = 0; i < keyColumns.size(); i++)
            otherKeyTransfers.add(other.keyVectors.get(i).makeTransferPair(keyVectors.get(i)));

        for (int r = 0; r < other.groupCount; r++) {
            int hash = other.rowHashes[r];
            int bucket = bucketOf(hash, capacity);

            int groupIdx;
            while (true) {
                groupIdx = buckets[bucket];
                if (groupIdx == -1) {
                    for (TransferPair tp : otherKeyTransfers) tp.copyValueSafe(r, groupCount);
                    ensureRowHashesCapacity(groupCount);
                    rowHashes[groupCount] = hash;

                    Object[] statesCopy = new Object[specs.length];
                    for (int si = 0; si < specs.length; si++) {
                        AggregateFunction<Object> aggregate = (AggregateFunction) aggregates[si];
                        Object newState = aggregate.createState();
                        aggregate.merge(newState, other.groupStates.get(r)[si]);
                        statesCopy[si] = newState;
                    }
                    groupStates.add(statesCopy);

                    buckets[bucket] = groupCount;
                    groupCount++;
                    estimatedMem += estimateStatesMem(statesCopy) + estimateKeyRowMem();
                    break;
                }

                boolean equal = true;
                for (int i = 0; i < keyColumns.size(); i++) {
                    FieldVector ov = other.keyVectors.get(i);
                    FieldVector mv = keyVectors.get(i);
                    boolean on = ov.isNull(r), mn = mv.isNull(groupIdx);
                    if (on && mn) continue;
                    if (on != mn) { equal = false; break; }
                    if (!ov.getObject(r).equals(mv.getObject(groupIdx))) { equal = false; break; }
                }
                if (equal) break;
                bucket = (bucket + 1) % capacity;
            }

            Object[] myStates = groupStates.get(groupIdx);
            Object[] otherStates = other.groupStates.get(r);
            long before = estimateStatesMem(myStates);
            for (int si = 0; si < specs.length; si++) {
                AggregateFunction<Object> aggregate = (AggregateFunction) aggregates[si];
                aggregate.merge(myStates[si], otherStates[si]);
            }
            trackStateGrowth(before, myStates);

            if (groupCount > capacity * LOAD_FACTOR) rehash();
        }
    }

    // ==================== 状态查询 ====================

    public int getRehashCount() { return rehashCount; }

    @Override
    public long estimateMemoryBytes() {
        long actualKeyMem = 0;
        for (FieldVector vec : keyVectors) actualKeyMem += vec.getBufferSize();
        return Math.max(estimatedMem, actualKeyMem);
    }

    // ==================== 生命周期 ====================

    @Override
    public void close() {
        super.close();
        groupStates.clear();
    }

    // ==================== 内存 & 向量工具 ====================

    private static long estimateVectorMem(FieldVector vec) {
        if (vec instanceof BigIntVector) return (long) DEFAULT_VECTOR_CAPACITY * 8 + DEFAULT_VECTOR_CAPACITY / 8L;
        if (vec instanceof Float8Vector) return (long) DEFAULT_VECTOR_CAPACITY * 8 + DEFAULT_VECTOR_CAPACITY / 8L;
        if (vec instanceof VarCharVector) return (long) DEFAULT_VECTOR_CAPACITY * 4 + DEFAULT_VECTOR_CAPACITY / 8L;
        return 4096L;
    }

    /**
     * 单个分组在哈希表里的<b>堆内</b>固定开销（与聚合状态无关的部分）：
     * {@code groupStates} 里的 {@code Object[]}（16B 头 + 每 spec 一个引用）、{@code ArrayList} 槽引用、
     * 开放寻址的 {@code buckets[]/rowHashes[]}（每行 8B，按装载因子与 2 倍扩容再摊 ~1.5×）
     * 以及 {@code groupStates} 增长时的数组重分配余量。
     *
     * <p>★旧值只有 {@code keyColumns × 16}（≈16B）：线上 100 万+ 分组时这部分真实堆开销
     * 被低估 3~5 倍，槽位估算 102MB 就触发的溢写，实际堆已吃到 250MB+ 每槽；
     * 4 个消费片段 × 3 槽流水线叠在同一 Worker 上 ⇒ 溢写线程里 {@code sortIndices} 的
     * {@code int[]} 分配直接 {@code OutOfMemoryError: Java heap space}（2026-11-19 事故）。</p>
     */
    private long estimateKeyRowMem() { return keyColumns.size() * 16L + 48L; }

    private static long estimateStatesMem(Object[] states) {
        long mem = 16L;
        for (Object s : states) {
            if (s instanceof long[]) mem += 24L;
            else if (s instanceof double[]) mem += 16L + (long) ((double[]) s).length * 8L;
            // MIN/MAX：Object[1] 持有当前极值；变长值（String/byte[]）按实际长度计
            else if (s instanceof Object[] arr) {
                mem += 32L;
                if (arr.length > 0) {
                    if (arr[0] instanceof byte[] b) mem += b.length;
                    else if (arr[0] instanceof String str) mem += 40L + (long) str.length() * 2L;
                    else if (arr[0] != null) mem += 32L;
                }
            }
            // COUNT(DISTINCT)：紧凑状态按实际数组（开放寻址表 / 字节 arena）计量；
            // 其它 Set 实现（历史状态）沿用「条目 + 装箱对象」口径。
            else if (s instanceof com.dojao.core.function.aggregate.DistinctSet ds) mem += ds.estimatedBytes();
            // COLLECT_ORDERED_LIST：两条并行 ArrayList（各 48B 容器 + 32B/条目引用与装箱）
            else if (s instanceof CollectOrderedListAggregateFunction.OrderedState os) {
                mem += 96L + (long) os.size() * 64L;
            }
            // COLLECT_MAP：容器 64B + 每条目（Node 40 + key/value 装箱 ~56）≈ 96B + 追加列表元素 32B。
            else if (s instanceof CollectMapAggregateFunction.MapState ms) {
                mem += 64L + (long) ms.mapSize() * 96L + ms.elementCount() * 32L;
            }
            // Set（collect_set 的 LinkedHashSet / 历史去重状态）：容器 64B + 逐条目
            // （HashMap.Node 32 + 链表 Entry 8 + 装箱值 16 + table 槽位/装载余量 ~24）≈ 80B。
            // 旧口径 24 + 48×n 在「每组 1 个值」时低估 ~2.5 倍（正是线上高基数分组的形状）。
            else if (s instanceof Set<?> set) mem += 64L + (long) set.size() * 80L;
            // COLLECT_LIST：ArrayList 元素只是引用（4B，1.5 倍扩容余量）+ 装箱值 16B ≈ 32B/条目。
            else if (s instanceof java.util.Collection<?> c) mem += 48L + (long) c.size() * 32L;
        }
        return mem;
    }

    /** 按 Field 的真实 Arrow 类型建向量（时间戳 / 日期 / Decimal / 布尔等都要正确实例化）。 */
    private static FieldVector createVector(Field field, BufferAllocator allocator) {
        return field.createVector(allocator);
    }

    /** 写值统一走基类实现（已覆盖小数 / 日期时间 / 布尔 / 二进制 / Decimal）。 */
    private static void setVectorValue(FieldVector vec, int row, Object val) {
        AbstractSpillableOperator.setVectorValue(vec, row, val);
    }
}
