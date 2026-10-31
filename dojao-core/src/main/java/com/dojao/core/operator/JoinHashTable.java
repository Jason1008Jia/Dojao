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
 *   <li>{@code rowIndices} —— rowIndices[groupIdx] = 该 group 的 build 侧行索引列表</li>
 *   <li>{@code buildVectors} —— 存储 build 侧所有列的所有行（按插入顺序）</li>
 * </ol>
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

    // ==================== 行索引 ====================
    // rowIndices[groupIdx] = list of row indices in buildVectors
    private final List<List<Integer>> rowIndices;

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
        this.rowIndices = new ArrayList<>();
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
        rowIndices.get(groupIdx).add(buildRowCount);
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

        // 行索引列表
        List<Integer> indices = new ArrayList<>();
        indices.add(buildRowCount);
        rowIndices.add(indices);

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

    /** 获取某个 group 对应的所有 build 侧行索引列表。 */
    public List<Integer> getMatchingRows(int groupIdx) {
        return rowIndices.get(groupIdx);
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
        rowIndices.clear();
    }

    // ==================== 内部方法 ====================

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
