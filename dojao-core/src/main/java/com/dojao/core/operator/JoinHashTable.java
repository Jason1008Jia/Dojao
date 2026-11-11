package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Join 专用堆外哈希表 —— 存储 build 侧数据，支持重复 key（一对多匹配）。
 * 继承 {@link RowHashTable} 的开放寻址基础设施。
 *
 * <h3>共享内存（继承自 RowHashTable）</h3>
 * <ol>
 *   <li>{@code buckets[]} —— 哈希桶</li>
 *   <li>{@code rowHashes[]} —— hash 缓存</li>
 *   <li>{@code keyVectors} —— Arrow key 列向量</li>
 * </ol>
 *
 * <h3>Join 特有内存</h3>
 * <ol start="4">
 *   <li>{@code groupHead/groupTail/nextRow} —— 每组一条 build 行号的<b>数组化单链表</b>
 *       （groupHead[g] 首行、nextRow[r] 组内下一行、-1 结束）</li>
 *   <li>{@code buildVectors} —— 存储 build 侧所有列的所有行（按插入顺序）</li>
 * </ol>
 *
 * <p><b>为什么不用 {@code List<List<Integer>>}</b>：高基数 build 键（如唯一 event_id / member_code，
 * 数百万行）时每组一个 {@code ArrayList} + 每行一个装箱 {@code Integer}，实测 2M 行约 200MB
 * <b>Java 堆</b>，而 {@link #estimateMemoryBytes()} 只算 Arrow 向量 ⇒ 预算闸门看不见它，
 * 进程在「堆外才 216/218MB」时先被堆 OOM 打死（线上 2026-11-11 三表 join 回退 Master 的实锤）。
 * 数组化后同样的 2M 行只要 ~24MB，且如实计入预算。</p>
 *
 * <h3>与 AggregationHashTable 的核心区别</h3>
 * <ul>
 *   <li>{@link AggregationHashTable} 对重复 key 做聚合函数累加器更新（group 坍缩）</li>
 *   <li>{@code JoinHashTable} 对重复 key 追加行索引列表（保留所有匹配行）</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/7/17
 */
public class JoinHashTable extends RowHashTable {

    private final Schema buildSchema;
    private final int buildColumnCount;

    // ==================== 行索引（数组化单链表）====================
    /** groupIdx → 该组首个 build 行号（-1 = 空组）。 */
    private int[] groupHead;
    /** groupIdx → 该组末个 build 行号（-1 = 空组），追加时 O(1) 接链。 */
    private int[] groupTail;
    /** build 行号 → 组内下一个 build 行号（-1 = 组结束）。 */
    private int[] nextRow;

    // ==================== Build 侧全量数据 ====================
    private List<FieldVector> buildVectors;
    private List<TransferPair> buildTransfers;
    private int buildRowCount;

    /**
     * @param allocator   Arrow BufferAllocator
     * @param keyColumns  build 侧的 join key 列名列表
     * @param keyFields   build 侧的 join key 列 Field 定义（与 keyColumns 一一对应）
     * @param buildSchema build 侧的完整 schema（所有列）
     */
    public JoinHashTable(BufferAllocator allocator,
                         List<String> keyColumns,
                         List<Field> keyFields,
                         Schema buildSchema) {
        super(allocator, keyColumns, keyFields);
        this.buildSchema = buildSchema;
        this.buildColumnCount = buildSchema.getFields().size();
        this.groupHead = new int[INITIAL_CAPACITY];
        this.groupTail = new int[INITIAL_CAPACITY];
        Arrays.fill(groupHead, -1);
        Arrays.fill(groupTail, -1);
        this.nextRow = new int[DEFAULT_VECTOR_CAPACITY];
        this.buildRowCount = 0;
        this.buildVectors = null;
        this.buildTransfers = null;
    }

    // ==================== 核心操作 ====================

    /**
     * 已有 key 命中：把当前 build 行号追加进该 group 的行索引列表（一对多）。
     * 探测逻辑本身由 {@link RowHashTable#insertOrAppend} 统一处理。
     */
    @Override
    protected void appendToGroup(Page input, int rowIndex, int groupIdx) {
        int row = buildRowCount;   // afterRow 才 ++，这里正是本 build 行号
        ensureNextRowCapacity(row);
        nextRow[row] = -1;
        int tail = groupTail[groupIdx];
        if (tail < 0) {
            groupHead[groupIdx] = row;
        } else {
            nextRow[tail] = row;
        }
        groupTail[groupIdx] = row;
    }

    /**
     * 每行处理完后：把整行所有列深拷贝到 buildVectors。
     * 新 key 和已有 key 都要保留全量行数据，因此放在 afterRow 钩子里统一执行。
     */
    @Override
    protected void afterRow(Page input, int rowIndex) {
        copyRowToBuildVectors(input, rowIndex);
        buildRowCount++;
    }

    // ==================== insertGroup (Join 版本) ====================

    @Override
    protected int insertGroup(Page input, int rowIndex, int bucket, int hash) {
        int newGroupIdx = groupCount;

        // 拷贝 key 值到 keyVectors
        for (int i = 0; i < keyColumns.size(); i++) {
            TransferPair tp = keyTransfers.get(i);
            if (tp != null) tp.copyValueSafe(rowIndex, newGroupIdx);
        }

        // 缓存 hash — rehash 时直接读 rowHashes[g]
        ensureRowHashesCapacity(newGroupIdx);
        rowHashes[newGroupIdx] = hash;

        buckets[bucket] = newGroupIdx;
        groupCount++;

        // 行索引：该组链表首尾都指向当前 build 行（appendToGroup 之后 O(1) 接链）
        ensureGroupCapacity(newGroupIdx);
        ensureNextRowCapacity(buildRowCount);
        nextRow[buildRowCount] = -1;
        groupHead[newGroupIdx] = buildRowCount;
        groupTail[newGroupIdx] = buildRowCount;

        return newGroupIdx;
    }

    // ==================== Probe ====================

    /**
     * 探测（传 probe 页 + 该页里 join 键列的下标数组）。
     *
     * <p><b>必须传全部键列的下标</b>：build 侧是按全部键列建表的
     * （{@link RowHashTable#computeHash}），旧实现只拿首列求 hash 并比较，复合键 join
     * （如 {@code ON a.user_id=b.user_id AND a.dt=b.dt}）会落到不同的桶、静默漏配或错配。</p>
     *
     * <p>{@code keyIndices} 的顺序必须与 build 侧 {@code keyColumns} 一致（规划器两侧按 ON 顺序生成）。</p>
     */
    public int probe(Page input, int[] keyIndices, int rowIndex) {
        if (groupCount == 0) return -1;

        VectorSchemaRoot root = input.getRoot();
        int hash = hashRow(root, keyIndices, rowIndex);
        int bucket = bucketOf(hash, capacity);

        int probes = 0;
        while (probes < capacity) {
            int groupIdx = buckets[bucket];
            if (groupIdx == -1) return -1;
            if (keysEqualAt(root, keyIndices, rowIndex, groupIdx)) return groupIdx;
            bucket = (bucket + 1) % capacity;
            probes++;
        }
        return -1;
    }

    /** 该 group 首个 build 行号（-1 = 该组无行，正常不会出现）。 */
    public int firstMatchingRow(int groupIdx) {
        return groupIdx >= 0 && groupIdx < groupHead.length ? groupHead[groupIdx] : -1;
    }

    /**
     * 组内下一个 build 行号（-1 = 组结束）。
     *
     * <p>与 {@link #firstMatchingRow(int)} 配对遍历，<b>零分配</b> —— probe 热路径逐行调用，
     * 不能返回装箱列表/数组副本。</p>
     */
    public int nextMatchingRow(int buildRow) {
        return nextRow[buildRow];
    }

    // ==================== 行提取 ====================

    public VectorSchemaRoot extractRows(List<Integer> rows) {
        int outCount = rows.size();
        List<FieldVector> outVectors = new ArrayList<>();

        for (int col = 0; col < buildColumnCount; col++) {
            FieldVector src = buildVectors.get(col);
            Field field = buildSchema.getFields().get(col);
            FieldVector dst = field.createVector(allocator);
            dst.setInitialCapacity(outCount); dst.allocateNew();
            TransferPair tp = src.makeTransferPair(dst);
            for (int j = 0; j < outCount; j++) tp.copyValueSafe(rows.get(j), j);
            dst.setValueCount(outCount);
            outVectors.add(dst);
        }
        return new VectorSchemaRoot(buildSchema.getFields(), outVectors, outCount);
    }

    public VectorSchemaRoot extractSingleRow(int buildRow) {
        return extractRows(List.of(buildRow));
    }

    public void copyBuildCell(int buildRow, int buildCol, FieldVector dst, int dstIndex) {
        FieldVector src = buildVectors.get(buildCol);
        if (src.isNull(buildRow)) dst.setNull(dstIndex);
        // 必须用 copyFromSafe：copyFrom（非 safe）不会扩容目标向量，输出向量初始
        // dataBuffer 只有 4096 * DEFAULT_RECORD_BYTE_COUNT(8) = 32KB，超出即越界。
        else dst.copyFromSafe(buildRow, dstIndex, src);
    }

    /** 获取 build 侧列向量（只读，用于批量 TransferPair 创建） */
    public List<FieldVector> getBuildVectors() { return buildVectors; }

    // ==================== 状态查询 ====================

    public int buildRowCount() { return buildRowCount; }

    public Schema getBuildSchema() { return buildSchema; }

    @Override
    public long estimateMemoryBytes() {
        long total = super.estimateMemoryBytes();
        if (buildVectors != null)
            for (FieldVector vec : buildVectors) total += vec.getBufferSize();
        // 行索引是 Java 堆（Arrow 分配器看不见），必须如实计入 —— 否则 build 闸门放行后堆先炸。
        total += (long) groupHead.length * Integer.BYTES;
        total += (long) groupTail.length * Integer.BYTES;
        total += (long) nextRow.length * Integer.BYTES;
        return total;
    }

    // ==================== 生命周期 ====================

    @Override
    public void close() {
        super.close();
        if (buildVectors != null) {
            for (FieldVector vec : buildVectors) {
                if (vec != null) try { vec.close(); } catch (Exception ignored) {}
            }
            buildVectors.clear();
            buildTransfers.clear();
        }
    }

    // ==================== 内部方法 ====================

    /** 组索引数组扩容（扩容后新槽位必须是 -1 = 空组）。 */
    private void ensureGroupCapacity(int groupIdx) {
        if (groupIdx < groupHead.length) return;
        int cap = groupHead.length;
        while (cap <= groupIdx) cap <<= 1;
        int old = groupHead.length;
        groupHead = Arrays.copyOf(groupHead, cap);
        groupTail = Arrays.copyOf(groupTail, cap);
        Arrays.fill(groupHead, old, cap, -1);
        Arrays.fill(groupTail, old, cap, -1);
    }

    /** 行链表数组扩容（新槽位由调用方在写入时置 -1，无需预填）。 */
    private void ensureNextRowCapacity(int buildRow) {
        if (buildRow < nextRow.length) return;
        int cap = nextRow.length;
        while (cap <= buildRow) cap <<= 1;
        nextRow = Arrays.copyOf(nextRow, cap);
    }

    @Override
    protected void ensureTransfers(Page input) {
        VectorSchemaRoot root = input.getRoot();

        if (root != lastInputRoot) {
            keyTransfers.clear();
            for (int i = 0; i < keyColumns.size(); i++) keyTransfers.add(null);
            if (buildTransfers != null) {
                buildTransfers.clear();
                for (int i = 0; i < buildColumnCount; i++) buildTransfers.add(null);
            }
            lastInputRoot = root;
        }

        for (int i = 0; i < keyColumns.size(); i++) {
            if (keyTransfers.get(i) == null) {
                FieldVector src = root.getVector(keyColumns.get(i));
                keyTransfers.set(i, src.makeTransferPair(keyVectors.get(i)));
            }
        }

        // 延迟初始化 buildVectors
        if (buildVectors == null) {
            buildVectors = new ArrayList<>();
            buildTransfers = new ArrayList<>();
            for (Field field : buildSchema.getFields()) {
                FieldVector vec = field.createVector(allocator);
                vec.setInitialCapacity(DEFAULT_VECTOR_CAPACITY);
                vec.allocateNew();
                buildVectors.add(vec);
                buildTransfers.add(null);
            }
        }

        List<Field> schemaFields = buildSchema.getFields();
        for (int i = 0; i < buildColumnCount; i++) {
            if (buildTransfers.get(i) == null) {
                FieldVector src = root.getVector(schemaFields.get(i).getName());
                if (src == null) src = root.getVector(i);
                buildTransfers.set(i, src.makeTransferPair(buildVectors.get(i)));
            }
        }
    }

    private void copyRowToBuildVectors(Page input, int rowIndex) {
        VectorSchemaRoot root = input.getRoot();
        List<Field> schemaFields = buildSchema.getFields();
        for (int col = 0; col < buildColumnCount; col++) {
            TransferPair tp = buildTransfers.get(col);
            if (tp == null) {
                FieldVector src = root.getVector(schemaFields.get(col).getName());
                if (src == null) src = root.getVector(col);
                buildVectors.get(col).copyFromSafe(rowIndex, buildRowCount, src);
            } else {
                tp.copyValueSafe(rowIndex, buildRowCount);
            }
        }
    }

}
