package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.spi.function.CollectAggregateFunction;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 收集-求值型聚合的槽位 —— collect-evaluate 形态（状态全程 Java 侧 Arrow 列）。
 *
 * <p>复用 {@link RowHashTable} 的 key 哈希探测/存储（开放寻址 + 斐波那契散列 + rehash）。
 * 值侧不做聚合状态，而是攒"原始值"：
 * <ul>
 *   <li>每输入值列一个<b>共享累积 Arrow 列</b>（{@link #valueVectors}，每页
 *       {@link RowAccumulator#copyColumnBatch} 追加复制 —— 输入页在 addPage 返回后才被 close，页内复制安全）</li>
 *   <li>每 group 一个<b>行号集合</b>（{@link #groupRows}，指向累积列的行号）</li>
 * </ul>
 * 内存 = keyVectors + 累积列 + 行号集合 —— 完全 Java 可测（{@link #estimateMemoryBytes()}），
 * 溢写判断准确（AbstractSpillableOperator 的 slotBytesThreshold 直接生效）。</p>
 *
 * <p>溢写：{@link #buildSortedSpillRoot()} 按 key 排序、逐 group 提取值行 → [key..., value...]
 * 连续段写 Arrow IPC，与 {@code AbstractSpillableOperator.addToIndex} 的 segment 机制闭环
 * （读回按 key 段直接提取交付）。Phase 2 交付：{@link #sortedSegments()} 按 key 排序返回
 * (key, valueCols, rows)，由算子拼接成 N-key batch 交给 {@link CollectAggregateFunction#evaluateBatch}。</p>
 */
public final class CollectHashTable extends RowHashTable implements Slot {

    /** 一个 key 的行号集合（可变，累积列里的行号）。 */
    public static final class RowSet {
        int[] rows = new int[8];
        int size = 0;

        void add(int r) {
            if (size == rows.length) rows = Arrays.copyOf(rows, rows.length * 2);
            rows[size++] = r;
        }

        int[] toArray() { return Arrays.copyOf(rows, size); }

        int size() { return size; }
    }

    /** 一个 key 的收集段：key 值 + 值列引用 + 值行号集合（供算子拼接交付）。 */
    public record CollectSeg(Object key, FieldVector[] valueCols, int[] rows) {}

    private final List<String> valueColumns;
    private final List<Field> keyFields;
    private final List<Field> valueFields;
    private final List<FieldVector> valueVectors;
    private int valueRowCount;
    private int pageRowBase;
    private final List<RowSet> groupRows;

    public CollectHashTable(BufferAllocator allocator, List<String> keyColumns, List<Field> keyFields,
                            List<String> valueColumns, List<Field> valueFields) {
        super(allocator, keyColumns, keyFields);
        this.valueColumns = valueColumns;
        this.keyFields = keyFields;
        this.valueFields = valueFields;
        this.valueVectors = new ArrayList<>(valueFields.size());
        for (Field f : valueFields) {
            FieldVector v = f.createVector(allocator);
            v.setInitialCapacity(RowHashTable.DEFAULT_VECTOR_CAPACITY);
            v.allocateNew();
            valueVectors.add(v);
        }
        this.valueRowCount = 0;
        this.pageRowBase = 0;
        this.groupRows = new ArrayList<>();
    }

    // ==================== Slot：写入 ====================

    @Override
    public void addRow(Page input, int row) { insertOrAppend(input, row); }

    @Override
    public void addPage(Page input) {
        int n = input.getRowCount();
        if (n == 0) return;
        // 1. 整页值列复制到累积列尾部（输入页在 addPage 返回后才被 close，页内复制安全）
        for (int v = 0; v < valueColumns.size(); v++) {
            RowAccumulator.copyColumnBatch(input.getRoot().getVector(valueColumns.get(v)),
                    valueVectors.get(v), 0, valueRowCount, n);
        }
        int start = valueRowCount;
        valueRowCount += n;
        pageRowBase = start;   // 本页行 → 累积行号 = pageRowBase + 输入行偏移
        // 2. 逐行探测 key（insertGroup/appendToGroup 只记录累积行号，不做聚合）
        for (int row = 0; row < n; row++) insertOrAppend(input, row);
    }

    // ==================== RowHashTable：insertGroup / appendToGroup ====================

    @Override
    protected int insertGroup(Page input, int rowIndex, int bucket, int hash) {
        for (TransferPair tp : keyTransfers) {
            if (tp != null) tp.copyValueSafe(rowIndex, groupCount);
        }
        RowSet rs = new RowSet();
        rs.add(pageRowBase + rowIndex);
        groupRows.add(rs);
        ensureRowHashesCapacity(groupCount);
        rowHashes[groupCount] = hash;
        buckets[bucket] = groupCount;
        groupCount++;
        return groupCount - 1;
    }

    @Override
    protected void appendToGroup(Page input, int rowIndex, int groupIdx) {
        groupRows.get(groupIdx).add(pageRowBase + rowIndex);
    }

    // ==================== 溢写：按 key 排序 → [key..., value...] 连续段 ====================

    @Override
    public VectorSchemaRoot buildSortedSpillRoot() {
        List<Field> fields = new ArrayList<>(keyFields.size() + valueFields.size());
        fields.addAll(keyFields);
        fields.addAll(valueFields);
        List<FieldVector> outCols = new ArrayList<>(fields.size());
        for (Field f : fields) {
            FieldVector v = f.createVector(allocator);
            v.setInitialCapacity(1);
            v.allocateNew();
            outCols.add(v);
        }
        if (groupCount == 0) {
            return new VectorSchemaRoot(fields, outCols, 0);
        }
        Integer[] sorted = sortedGroups();
        int total = 0;
        for (int g : sorted) total += groupRows.get(g).size();
        for (FieldVector v : outCols) {
            v.setInitialCapacity(total);
            v.allocateNew();
        }

        int outRow = 0;
        for (int g : sorted) {
            RowSet rs = groupRows.get(g);
            int sz = rs.size();
            // key 列：该 group 的 key 值重复 sz 行
            for (int i = 0; i < keyColumns.size(); i++) {
                TransferPair tp = keyVectors.get(i).makeTransferPair(outCols.get(i));
                for (int k = 0; k < sz; k++) tp.copyValueSafe(g, outRow + k);
            }
            // 值列：累积列按行号集合提取
            for (int v = 0; v < valueColumns.size(); v++) {
                TransferPair tp = valueVectors.get(v).makeTransferPair(outCols.get(keyColumns.size() + v));
                for (int k = 0; k < sz; k++) tp.copyValueSafe(rs.rows[k], outRow + k);
            }
            outRow += sz;
        }
        for (FieldVector v : outCols) v.setValueCount(total);
        return new VectorSchemaRoot(fields, outCols, total);
    }

    // ==================== 内存路径输出 ====================

    /** 按 key 排序的收集段（供 HashAggregationOperator Phase 2 内存路径拼接交付）。 */
    public List<CollectSeg> sortedSegments() {
        Integer[] sorted = sortedGroups();
        FieldVector[] vcols = valueVectors.toArray(new FieldVector[0]);
        List<CollectSeg> segs = new ArrayList<>(groupCount);
        for (int g : sorted) {
            segs.add(new CollectSeg(extractKey(g), vcols, groupRows.get(g).toArray()));
        }
        return segs;
    }

    // ==================== 交付 batch 拼接（内存/溢写路径共用） ====================

    /** 一个 key 的值片段：引用一组值列 + 行号（内存路径=累积列，溢写路径=已加载溢写 batch 列）。 */
    public record ValueFrag(FieldVector[] valueCols, int[] rows) {
    }

    /** collect 交付片段：key + 值片段列表（同 key 多片段按序拼接 = 跨溢写文件同 key 值连续）。 */
    public record CollectGroup(Object key, List<ValueFrag> frags) {
    }

    /**
     * 把排序的收集片段拼接成 collect 交付 batch：[keyFields..., list<valueFields>...]，<b>每 key 一行</b>。
     *
     * <p>方案A（框架分组、外挂只计算）：切组逻辑全部收敛在引擎侧。key 列每行写一次 key 值；
     * 值列 = Arrow List 列（每行 = 该 key 攒齐的全部值，连续拷入 list child）。外部语言收到已分好
     * 组的结构，逐行 {@code list.value(row)} 即得每 key 值数组 → 纯计算，<b>零切组代码</b>；
     * List 是 Arrow 标准类型，Java/C++/Go/Rust/Python 全部原生解析，天然跨语言。</p>
     *
     * <p>groups 须按 key 升序传入（内存路径= {@link #sortedSegments()} 排序序，溢写路径= sortKeys 序）；
     * 返回新 batch（key/值列均新复制），源（累积列 / 已加载溢写 batch 列）由调用方持有并关闭。</p>
     */
    public static VectorSchemaRoot buildDeliverBatch(BufferAllocator allocator,
                                                    List<Field> keyFields,
                                                    List<Field> valueFields,
                                                    List<CollectGroup> groups) {
        int groupCount = groups.size();
        int[] groupSizes = new int[groupCount];
        int totalValues = 0;
        for (int gi = 0; gi < groupCount; gi++) {
            int sz = 0;
            for (ValueFrag f : groups.get(gi).frags()) sz += f.rows().length;
            groupSizes[gi] = sz;
            totalValues += sz;
        }
        // 列序 = keyFields..., list<valueField>...
        List<Field> fields = new ArrayList<>(keyFields.size() + valueFields.size());
        fields.addAll(keyFields);
        for (Field vf : valueFields) {
            fields.add(new Field(vf.getName(), FieldType.nullable(new ArrowType.List()), List.of(vf)));
        }
        int keyCount = keyFields.size();
        List<FieldVector> cols = new ArrayList<>(fields.size());
        try {
            for (int k = 0; k < keyCount; k++) {   // key 列：每 key 一行
                FieldVector v = keyFields.get(k).createVector(allocator);
                v.setInitialCapacity(Math.max(1, groupCount));
                v.allocateNew();
                cols.add(v);
            }
            // 值列：ListVector（child = valueField 类型）。setInitialCapacity(valueCount, density) 内部按
            // valueCount*density 算 child 容量 —— density 是"每行平均元素数"，传总元素数会爆 OversizedAllocationException
            // （groupCount × totalValues 超 int 上限）。正确：density = totalValues / groupCount → child 容量 ≈ totalValues。
            double density = groupCount > 0 ? (double) totalValues / groupCount : 1.0;
            for (int v = 0; v < valueFields.size(); v++) {
                ListVector lv = (ListVector) fields.get(keyCount + v).createVector(allocator);
                if (RowAccumulator.typeWidthOf(valueFields.get(v).getType()) > 0) {
                    lv.setInitialCapacity(Math.max(1, groupCount), density);
                } else {
                    // 变长 child（VarChar/Binary…）是 DensityAwareVector：两参重载会把 density 再乘一次
                    // （把「每行元素数」当「每值平均字节数」）⇒ totalValues²/groupCount 直接溢出。
                    // 单参只按行数预分配，值写入走 copyValueSafe 的按需 reAlloc。
                    lv.setInitialCapacity(Math.max(1, groupCount));
                }
                lv.allocateNew();
                cols.add(lv);
            }
            if (groupCount == 0) return new VectorSchemaRoot(fields, cols, 0);

            // 值列：startNewValue 置 valid + 传播 offset → endValue 累加 size；child 值按累积位置连续写入
            for (int v = 0; v < valueFields.size(); v++) {
                ListVector lv = (ListVector) cols.get(keyCount + v);
                FieldVector child = lv.getDataVector();
                int dataRow = 0;
                for (int gi = 0; gi < groupCount; gi++) {
                    lv.startNewValue(gi);
                    for (ValueFrag f : groups.get(gi).frags()) {
                        TransferPair tp = f.valueCols()[v].makeTransferPair(child);
                        int len = f.rows().length;
                        for (int k = 0; k < len; k++) tp.copyValueSafe(f.rows()[k], dataRow + k);
                        dataRow += len;
                    }
                    lv.endValue(gi, groupSizes[gi]);
                }
                child.setValueCount(totalValues);
                lv.setValueCount(groupCount);
            }
            // key 列：每 key 一行写一次
            for (int gi = 0; gi < groupCount; gi++) writeKey(cols, keyCount, groups.get(gi).key(), gi);
            for (int k = 0; k < keyCount; k++) cols.get(k).setValueCount(groupCount);
            return new VectorSchemaRoot(fields, cols, groupCount);
        } catch (Throwable t) {
            // 交付构建失败：关闭已分配向量，防 allocator 泄漏告警（deliver 尚未创建，调用方无人兜底）
            for (FieldVector v : cols) {
                try { v.close(); } catch (Exception ignored) {}
            }
            throw t;
        }
    }

    /** 把 key 写进交付 batch 前 keyCount 列，只写第 row 行（每 key 一行）。 */
    private static void writeKey(List<FieldVector> cols, int keyCount, Object key, int row) {
        if (keyCount == 1) {
            setVectorValue(cols.get(0), row, key);
        } else {
            List<?> list = (List<?>) key;
            for (int i = 0; i < keyCount; i++) setVectorValue(cols.get(i), row, list.get(i));
        }
    }

    private static void setVectorValue(FieldVector vec, int row, Object val) {
        if (val == null) { vec.setNull(row); return; }
        switch (vec) {
            case BigIntVector bv -> bv.setSafe(row, ((Number) val).longValue());
            case Float8Vector fv -> fv.setSafe(row, ((Number) val).doubleValue());
            case Float4Vector fv -> fv.setSafe(row, ((Number) val).floatValue());
            case IntVector iv -> iv.setSafe(row, ((Number) val).intValue());
            case VarCharVector vc -> {
                byte[] b = (val instanceof byte[] ba) ? ba
                        : val.toString().getBytes(StandardCharsets.UTF_8);
                vc.setSafe(row, b);
            }
            default -> vec.setNull(row);
        }
    }

    // ==================== 溢写路径：流式交付（峰值 = 交付 batch + 单文件） ====================

    /**
     * 该值列能否走流式交付（定长列 / Utf8 / LargeUtf8 / Binary / LargeBinary）。
     *
     * <p>定长列可乱序写（memcpy）；变长列由 {@link DeliverStream} 直接维护 offset/data buffer，
     * 不依赖 Arrow 的「顺序写入」假设。嵌套类型（List/Struct/Map…）不支持，调用方回退旧路径。</p>
     */
    public static boolean supportsStreaming(Field f) {
        if (RowAccumulator.typeWidthOf(f.getType()) > 0) return true;
        ArrowType t = f.getType();
        return t instanceof ArrowType.Utf8 || t instanceof ArrowType.LargeUtf8
                || t instanceof ArrowType.Binary || t instanceof ArrowType.LargeBinary;
    }

    /**
     * 溢写路径的<b>流式交付构建器</b>：先按 group 尺寸把交付 List 列的行 offset 定好，值段由调用方
     * 按「文件升序」直接写入最终位置。峰值内存 = 交付 batch 本身 + 当前一个溢写文件，
     * 不再把所有溢写文件常驻缓存。
     *
     * <p>定长列乱序 memcpy；变长列按预定 group 起始字节直接写 data/offset buffer
     * （{@code byteCursor[v][g]} 记录组内写入位置），不经过 {@code copyValueSafe} 的顺序约束。</p>
     */
    public static final class DeliverStream implements AutoCloseable {
        private final List<Field> fields;
        private final List<FieldVector> cols;
        private final int keyCount;
        private final int groupCount;
        private final int totalValues;
        private final int[] groupRows;        // groupCount + 1：每组在子列中的起始行

        private final ListVector[] lists;
        private final FieldVector[] children;
        private final boolean[] fixed;
        private final boolean[] varWidth;
        private final boolean[] largeVarWidth;
        private final int[][] rowCursor;      // [v][g] 下一写入行
        private final long[][] byteCursor;    // [v][g] 变长列下一写入字节
        private final long[] totalBytes;      // [v] 变长列总字节
        private boolean finished;
        private boolean closed;

        private DeliverStream(List<Field> fields, List<FieldVector> cols, int keyCount, int groupCount,
                              int totalValues, int[] groupRows, ListVector[] lists,
                              FieldVector[] children, boolean[] fixed, boolean[] varWidth,
                              boolean[] largeVarWidth, int[][] rowCursor, long[][] byteCursor,
                              long[] totalBytes) {
            this.fields = fields;
            this.cols = cols;
            this.keyCount = keyCount;
            this.groupCount = groupCount;
            this.totalValues = totalValues;
            this.groupRows = groupRows;
            this.lists = lists;
            this.children = children;
            this.fixed = fixed;
            this.varWidth = varWidth;
            this.largeVarWidth = largeVarWidth;
            this.rowCursor = rowCursor;
            this.byteCursor = byteCursor;
            this.totalBytes = totalBytes;
        }

        /**
         * 创建交付 batch（[key..., list<value>...]，每 key 一行）。
         *
         * @param keys       按 key 升序的 group key（每 key 一行）
         * @param groupSizes 每组值个数
         * @param groupBytes 变长列的每组字节数（{@code [值列下标][group]}）；无变长列时可为 null
         */
        public static DeliverStream open(BufferAllocator allocator,
                                         List<Field> keyFields,
                                         List<Field> valueFields,
                                         List<Object> keys,
                                         int[] groupSizes,
                                         long[][] groupBytes) {
            int groupCount = keys.size();
            int keyCount = keyFields.size();
            int totalValues = 0;
            for (int sz : groupSizes) totalValues += sz;

            List<Field> fields = new ArrayList<>(keyCount + valueFields.size());
            fields.addAll(keyFields);
            for (Field vf : valueFields) {
                fields.add(new Field(vf.getName(), FieldType.nullable(new ArrowType.List()), List.of(vf)));
            }
            List<FieldVector> cols = new ArrayList<>(fields.size());
            ListVector[] lists = new ListVector[valueFields.size()];
            FieldVector[] children = new FieldVector[valueFields.size()];
            boolean[] fixed = new boolean[valueFields.size()];
            boolean[] varWidth = new boolean[valueFields.size()];
            boolean[] largeVarWidth = new boolean[valueFields.size()];
            int[][] rowCursor = new int[valueFields.size()][groupCount];
            long[][] byteCursor = new long[valueFields.size()][groupCount];
            long[] totalBytes = new long[valueFields.size()];

            try {
                for (int k = 0; k < keyCount; k++) {
                    FieldVector v = keyFields.get(k).createVector(allocator);
                    v.setInitialCapacity(Math.max(1, groupCount));
                    v.allocateNew();
                    cols.add(v);
                }
                int[] groupRows = new int[groupCount + 1];
                for (int g = 0; g < groupCount; g++) groupRows[g + 1] = groupRows[g] + groupSizes[g];

                // ★ 不能用 ListVector.setInitialCapacity(groupCount, density)：变长 child 是
                //   DensityAwareVector，会再乘一次 density（把「每行元素数」误当「每值平均字节数」）
                //   ⇒ 少分组大值域时 data buffer 需求 = totalValues²/groupCount（探针里 240k 值/3 组
                //   直接 19.2e9 字节 OversizedAllocationException）。统一用单参 + 显式 reAlloc。
                for (int v = 0; v < valueFields.size(); v++) {
                    ListVector lv = (ListVector) fields.get(keyCount + v).createVector(allocator);
                    lv.setInitialCapacity(Math.max(1, groupCount));
                    lv.allocateNew();
                    cols.add(lv);
                    lists[v] = lv;
                    FieldVector child = lv.getDataVector();
                    children[v] = child;
                    fixed[v] = RowAccumulator.typeWidthOf(valueFields.get(v).getType()) > 0;
                    varWidth[v] = !fixed[v] && child instanceof BaseVariableWidthVector;
                    largeVarWidth[v] = !fixed[v] && child instanceof BaseLargeVariableWidthVector;
                    if (!fixed[v] && !varWidth[v] && !largeVarWidth[v]) {
                        throw new IllegalStateException("collect 流式交付不支持的值列类型: " + valueFields.get(v));
                    }
                    // 值总数容量：定长/变长都要能容纳整批值（定长乱序 memcpy 不会自行扩容）
                    while (child.getValueCapacity() < totalValues) child.reAlloc();

                    // 组边界行 offset：每 key 一行
                    for (int g = 0; g < groupCount; g++) {
                        lv.startNewValue(g);
                        lv.endValue(g, groupSizes[g]);
                    }
                    for (int g = 0; g < groupCount; g++) rowCursor[v][g] = groupRows[g];

                    if (varWidth[v] || largeVarWidth[v]) {
                        if (groupBytes == null) {
                            throw new IllegalStateException("变长值列缺少 groupBytes 预算: " + valueFields.get(v));
                        }
                        // offset/validity 容量按值总数预扩；data 按字节总数一次分配，避免逐段扩容拷贝
                        while (child.getValueCapacity() < totalValues) child.reAlloc();
                        long tb = 0;
                        for (long b : groupBytes[v]) tb += b;
                        totalBytes[v] = tb;
                        if (tb > 0 && child.getDataBuffer().capacity() < tb) {
                            ensureDataCapacity(child, varWidth[v], tb);
                        }
                        // 每组起始字节：组内首次写入的绝对位置
                        long base = 0;
                        ArrowBuf off = child.getOffsetBuffer();
                        for (int g = 0; g < groupCount; g++) {
                            if (varWidth[v]) off.setInt((long) groupRows[g] * 4, (int) base);
                            else off.setLong((long) groupRows[g] * 8, base);
                            byteCursor[v][g] = base;
                            base += groupBytes[v][g];
                        }
                    }
                }

                DeliverStream ds = new DeliverStream(fields, cols, keyCount, groupCount, totalValues,
                        groupRows, lists, children, fixed, varWidth, largeVarWidth,
                        rowCursor, byteCursor, totalBytes);
                for (int g = 0; g < groupCount; g++) writeKey(cols, keyCount, keys.get(g), g);
                for (int k = 0; k < keyCount; k++) cols.get(k).setValueCount(groupCount);
                return ds;
            } catch (Throwable t) {
                for (FieldVector v : cols) {
                    try { v.close(); } catch (Exception ignored) {}
                }
                throw t;
            }
        }

        /**
         * 把一个值段（{@code src[srcRow, srcRow+count)}）写入第 {@code groupIdx} 组的当前游标处。
         * 同一组内的调用须按文件/段升序传入（组内行号连续推进）。
         */
        public void copy(FieldVector src, int v, int srcRow, int groupIdx, int count) {
            if (count <= 0) return;
            int dstRow = rowCursor[v][groupIdx];
            if (fixed[v]) {
                RowAccumulator.copyColumnBatch(src, children[v], srcRow, dstRow, count);
            } else {
                copyVarWidth(src, v, groupIdx, srcRow, dstRow, count);
            }
            rowCursor[v][groupIdx] = dstRow + count;
        }

        private void copyVarWidth(FieldVector src, int v, int groupIdx, int srcRow, int dstRow, int count) {
            ArrowBuf sData = src.getDataBuffer();
            ArrowBuf sOff = src.getOffsetBuffer();
            FieldVector child = children[v];
            ArrowBuf dOff = child.getOffsetBuffer();
            ArrowBuf dVal = child.getValidityBuffer();
            ArrowBuf dData = child.getDataBuffer();
            boolean large = largeVarWidth[v];
            long bytePos = byteCursor[v][groupIdx];
            for (int k = 0; k < count; k++) {
                int s = srcRow + k, d = dstRow + k;
                boolean isNull = src.isNull(s);
                long so = 0;
                int len = 0;
                if (!isNull) {
                    if (large) {
                        so = sOff.getLong((long) s * 8);
                        len = (int) (sOff.getLong((long) (s + 1) * 8) - so);
                    } else {
                        so = sOff.getInt((long) s * 4);
                        len = sOff.getInt((long) (s + 1) * 4) - (int) so;
                    }
                }
                if (len > 0) {
                    if (dData.capacity() < bytePos + len) {
                        long cap = Math.max(bytePos + len,
                                Math.max(totalBytes[v], dData.capacity() + (dData.capacity() >> 1) + 1024));
                        ensureDataCapacity(child, !large, cap);
                        dData = child.getDataBuffer();
                    }
                    dData.setBytes(bytePos, sData, so, len);
                }
                if (large) dOff.setLong((long) (d + 1) * 8, bytePos + len);
                else dOff.setInt((long) (d + 1) * 4, (int) (bytePos + len));
                if (isNull) BitVectorHelper.unsetBit(dVal, d);
                else BitVectorHelper.setBit(dVal, d);
                bytePos += len;
            }
            byteCursor[v][groupIdx] = bytePos;
        }

        /** 变长 child 的 data buffer 扩到至少 {@code needed} 字节（Large 变体只支持无参 realloc，循环倍增）。 */
        private static void ensureDataCapacity(FieldVector child, boolean varWidth, long needed) {
            if (varWidth) {
                ((BaseVariableWidthVector) child).reallocDataBuffer(needed);
            } else {
                BaseLargeVariableWidthVector lv = (BaseLargeVariableWidthVector) child;
                while (lv.getDataBuffer().capacity() < needed) lv.reallocDataBuffer();
            }
        }

        /** 收尾（设置 valueCount）并返回交付 batch；调用方持有并负责 close。 */
        public VectorSchemaRoot finish() {
            if (!finished) {
                for (int v = 0; v < children.length; v++) {
                    if (varWidth[v]) ((BaseVariableWidthVector) children[v]).setLastSet(totalValues - 1);
                    else if (largeVarWidth[v]) ((BaseLargeVariableWidthVector) children[v]).setLastSet(totalValues - 1);
                    lists[v].setValueCount(groupCount);   // 顺带把 child.setValueCount(offset[G]) 设好
                }
                finished = true;
            }
            return new VectorSchemaRoot(fields, cols, groupCount);
        }

        /** 是否支持走流式交付（全部值列都受支持）。 */
        public static boolean supportsAll(List<Field> valueFields) {
            for (Field f : valueFields) if (!supportsStreaming(f)) return false;
            return true;
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            for (FieldVector v : cols) {
                try { v.close(); } catch (Exception ignored) {}
            }
        }
    }

    /** 取第 groupIdx 个 group 的 key（单 key 列 → 标量；多 key 列 → List）。 */
    public Object extractKey(int groupIdx) {
        if (keyColumns.size() == 1) {
            FieldVector vec = keyVectors.get(0);
            return vec.isNull(groupIdx) ? null : toKeyValue(vec.getObject(groupIdx));
        }
        Object[] values = new Object[keyColumns.size()];
        for (int i = 0; i < keyColumns.size(); i++) {
            FieldVector vec = keyVectors.get(i);
            values[i] = vec.isNull(groupIdx) ? null : toKeyValue(vec.getObject(groupIdx));
        }
        return Arrays.asList(values);
    }

    private static Object toKeyValue(Object raw) {
        if (raw == null) return null;
        if (raw instanceof byte[] b) return new String(b, StandardCharsets.UTF_8);
        return raw;
    }

    // ==================== 排序 & 内存 ====================

    private Integer[] sortedGroups() {
        Integer[] sorted = new Integer[groupCount];
        for (int g = 0; g < groupCount; g++) sorted[g] = g;
        Arrays.sort(sorted, (a, b) -> compareKeys(a, b));
        return sorted;
    }

    private int compareKeys(int a, int b) {
        for (int i = 0; i < keyColumns.size(); i++) {
            FieldVector va = keyVectors.get(i);
            boolean na = va.isNull(a), nb = va.isNull(b);
            if (na && nb) continue;
            if (na) return -1;
            if (nb) return 1;
            Object oa = va.getObject(a), ob = va.getObject(b);
            if (oa instanceof List<?> la && ob instanceof List<?> lb) {
                int cmp = ArrowComparators.compareLists(la, lb);
                if (cmp != 0) return cmp;
            } else if (oa instanceof Comparable ca && ob instanceof Comparable cb) {
                int cmp = ca.compareTo(cb);
                if (cmp != 0) return cmp;
            } else {
                int cmp = oa.toString().compareTo(ob.toString());
                if (cmp != 0) return cmp;
            }
        }
        return 0;
    }

    @Override
    public long estimateMemoryBytes() {
        int rc = valueRowCount;
        long total = 0;
        for (FieldVector v : keyVectors) total += v.getBufferSize();
        // 累积列：valueCount 未设置（copyColumnBatch 直接写 buffer）→ getBufferSize 对 var 列返回 0，
        // 必须像 RowAccumulator 一样直接读 offset buffer（memory trap 修复，否则溢写检查永不触发）。
        for (FieldVector v : valueVectors) {
            total += estimateVectorMem(v, rc) + (rc + 7) / 8;
        }
        for (RowSet rs : groupRows) total += rs.rows.length * 4L + 16L;
        return total;
    }

    private static long estimateVectorMem(FieldVector v, int rc) {
        int tw = RowAccumulator.typeWidthOf(v.getField().getType());
        if (tw > 0) return (long) rc * tw;
        if (v instanceof BaseVariableWidthVector bv) {
            ArrowBuf off = bv.getOffsetBuffer();
            long total = (long) (rc + 1) * 4;
            if (off != null && off.capacity() >= (long) rc * 4 + 4) {
                total += off.getInt((long) rc * 4); // 累积 var 数据字节数
            }
            return total;
        }
        return v.getBufferSize();
    }

    @Override
    public void close() {
        for (FieldVector v : valueVectors) v.close();
        valueVectors.clear();
        groupRows.clear();
        super.close();
    }
}
