package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 基于开放寻址的哈希表抽象基类。
 *
 * <h3>共享五块内存</h3>
 * <ol>
 *   <li>{@code buckets[]} —— int 数组，-1 = 空槽，存 groupIdx</li>
 *   <li>{@code rowHashes[]} —— rowHashes[groupIdx] = hash 缓存（rehash 时直接读）</li>
 *   <li>{@code keyVectors} —— Arrow FieldVector，每个唯一 key 一行</li>
 *   <li>④⑤ —— 由子类定义（聚合函数状态 / 行索引列表 + 全量列）</li>
 * </ol>
 *
 * <h3>子类</h3>
 * <ul>
 *   <li>{@link AggregationHashTable} —— GroupBy 聚合（聚合函数累加状态）</li>
 *   <li>{@link JoinHashTable} —— Hash Join build 侧（全量原始列 + 行号索引）</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/8/1
 */
public abstract class RowHashTable implements AutoCloseable {

    protected static final int INITIAL_CAPACITY = 1024;
    protected static final float LOAD_FACTOR = 0.75f;
    protected static final int DEFAULT_VECTOR_CAPACITY = 4096;

    protected final BufferAllocator allocator;
    protected final List<String> keyColumns;

    // ==================== 哈希桶 ====================
    protected int[] buckets;
    protected int capacity;
    /** 唯一 key group 数量（也是下一行可用 row index） */
    protected int groupCount;

    // ==================== 哈希缓存 ====================
    protected int[] rowHashes;

    // ==================== Key 存储 ====================
    protected final List<FieldVector> keyVectors;
    protected final List<TransferPair> keyTransfers;
    protected VectorSchemaRoot lastInputRoot;

    /**
     * 输入 root → key 列下标（按名解析，<b>重名列取第一份</b>）。随输入 root 变化重算。
     *
     * <p><b>为什么不能直接 {@code root.getVector(列名)}</b>：join 输出 = 「左表全列 + 右表全列」，
     * 两侧同名列（如 {@code city}/{@code province}）会一并带出，而
     * {@code VectorSchemaRoot.getVector(name)} 取的是<b>最后</b>一份 —— 与
     * {@code Schema.findField}（分组键字段推导，取第一份）、
     * {@code HashPartitionOperator.resolveKeyIndices}（分区键，取第一份）<b>口径相反</b>。
     * 于是 : 分区分到左表列、分组却绑到右表列 —— LEFT JOIN 中未匹配行右列恒 NULL，
     * 同一个 NULL 便在每个分区里各成一个分组（用户实测 4 行 `NULL NULL`）。
     */
    private int[] keyIndices;
    private VectorSchemaRoot keyIndicesRoot;

    protected volatile boolean closed;

    /**
     * @param allocator  Arrow BufferAllocator
     * @param keyColumns join/group key 列名列表
     * @param keyFields  join/group key 列 Field 定义（与 keyColumns 一一对应）
     */
    protected RowHashTable(BufferAllocator allocator,
                           List<String> keyColumns,
                           List<Field> keyFields) {
        this.allocator = allocator;
        this.keyColumns = keyColumns;
        this.capacity = INITIAL_CAPACITY;
        this.buckets = new int[capacity];
        Arrays.fill(buckets, -1);
        this.rowHashes = new int[DEFAULT_VECTOR_CAPACITY];
        this.groupCount = 0;
        this.closed = false;

        this.keyVectors = new ArrayList<>();
        this.keyTransfers = new ArrayList<>();
        for (Field field : keyFields) {
            FieldVector vec = field.createVector(allocator);
            vec.setInitialCapacity(DEFAULT_VECTOR_CAPACITY);
            vec.allocateNew();
            keyVectors.add(vec);
            keyTransfers.add(null);
        }
    }

    // ==================== 公共的 key 管理 ====================

    /**
     * 确保 key 列的 TransferPair 对应到当前 Page。
     * 子类如果需要额外的 TransferPair（如 buildVectors），应覆盖并先调用 super。
     */
    @SuppressWarnings("unchecked")
    protected void ensureTransfers(Page input) {
        VectorSchemaRoot root = input.getRoot();
        int[] idx = ensureKeyIndices(root);
        if (root != lastInputRoot) {
            keyTransfers.clear();
            for (int i = 0; i < keyColumns.size(); i++) keyTransfers.add(null);
            lastInputRoot = root;
        }
        for (int i = 0; i < keyColumns.size(); i++) {
            if (keyTransfers.get(i) == null) {
                FieldVector src = root.getVector(idx[i]);
                keyTransfers.set(i, src.makeTransferPair(keyVectors.get(i)));
            }
        }
    }

    /**
     * 解析 key 列在当前输入页中的下标（同一 root 只算一次）。重名列取<b>第一份</b> ——
     * 与 {@code Schema.findField}（分组键字段推导）、{@code HashPartitionOperator.resolveKeyIndices}
     * （分区键）同口径，保证「分区用的列」与「分组用的列」是同一列。详见 {@link #keyIndices}。
     *
     * @return key 列下标数组（与 {@link #keyColumns} 一一对应）
     */
    protected int[] ensureKeyIndices(VectorSchemaRoot root) {
        if (root == keyIndicesRoot && keyIndices != null) {
            return keyIndices;
        }
        List<Field> fields = root.getSchema().getFields();
        int[] idx = new int[keyColumns.size()];
        for (int i = 0; i < keyColumns.size(); i++) {
            String name = keyColumns.get(i);
            idx[i] = -1;
            for (int j = 0; j < fields.size(); j++) {
                if (fields.get(j).getName().equals(name)) {
                    idx[i] = j;
                    break;
                }
            }
            if (idx[i] < 0) {
                StringBuilder avail = new StringBuilder();
                for (Field f : fields) {
                    avail.append(avail.isEmpty() ? "" : ", ").append(f.getName());
                }
                throw new UnsupportedOperationException("哈希表 key 列 '" + name
                        + "' 不在输入列中（可用列: " + avail + "）");
            }
        }
        this.keyIndices = idx;
        this.keyIndicesRoot = root;
        return idx;
    }

    // ==================== 公共的 hash 计算 ====================

    protected int computeHash(Page input, int rowIndex) {
        VectorSchemaRoot root = input.getRoot();
        return hashRow(root, ensureKeyIndices(root), rowIndex);
    }

    /**
     * 按<b>显式键列下标</b>计算一行 hash —— 与本表建表时用的公式完全一致。
     *
     * <p>供 join 的 probe 侧使用：probe 侧的键列名（左表）与 build 侧（右表）可以不同名，
     * 但复合键必须按<b>全部</b>键列求 hash，否则两侧桶位不一致，探测会静默漏配/错配。</p>
     */
    protected int hashRow(VectorSchemaRoot root, int[] keyIdx, int rowIndex) {
        int hash = 0;
        for (int i = 0; i < keyIdx.length; i++) {
            FieldVector vec = root.getVector(keyIdx[i]);
            if (vec.isNull(rowIndex)) { hash = 31 * hash; continue; }
            int tw = RowAccumulator.typeWidthOf(vec.getField().getType());
            if (tw == 8) {
                long v = vec.getDataBuffer().getLong((long) rowIndex * 8);
                hash = 31 * hash + (int) (v ^ (v >>> 32));
            } else if (tw == 4) {
                hash = 31 * hash + vec.getDataBuffer().getInt((long) rowIndex * 4);
            } else if (tw == 2) {
                hash = 31 * hash + (int) vec.getDataBuffer().getShort((long) rowIndex * 2);
            } else if (tw == 1) {
                hash = 31 * hash + (int) vec.getDataBuffer().getByte(rowIndex);
            } else {
                hash = 31 * hash + vec.getObject(rowIndex).hashCode();
            }
        }
        return hash;
    }

    // ==================== 公共的 key 比较 ====================

    protected boolean keysEqual(Page input, int inputRow, int groupIdx) {
        VectorSchemaRoot root = input.getRoot();
        return keysEqualAt(root, ensureKeyIndices(root), inputRow, groupIdx);
    }

    /**
     * 按<b>显式键列下标</b>比较一行与已存储的 group 键（全部键列逐一比较）。
     * 与 {@link #hashRow} 配套供 join probe 侧使用。
     */
    protected boolean keysEqualAt(VectorSchemaRoot root, int[] keyIdx, int inputRow, int groupIdx) {
        for (int i = 0; i < keyIdx.length; i++) {
            FieldVector iv = root.getVector(keyIdx[i]);
            FieldVector kv = keyVectors.get(i);
            boolean in = iv.isNull(inputRow), kn = kv.isNull(groupIdx);
            if (in && kn) continue;
            if (in != kn) return false;

            // 定长列直接读 dataBuffer 原始字节比较，避免 Integer/Long/Short/Byte 装箱
            int tw = RowAccumulator.typeWidthOf(iv.getField().getType());
            if (tw == 8) {
                if (iv.getDataBuffer().getLong((long) inputRow * 8)
                        != kv.getDataBuffer().getLong((long) groupIdx * 8)) return false;
            } else if (tw == 4) {
                if (iv.getDataBuffer().getInt((long) inputRow * 4)
                        != kv.getDataBuffer().getInt((long) groupIdx * 4)) return false;
            } else if (tw == 2) {
                if (iv.getDataBuffer().getShort((long) inputRow * 2)
                        != kv.getDataBuffer().getShort((long) groupIdx * 2)) return false;
            } else if (tw == 1) {
                if (iv.getDataBuffer().getByte(inputRow)
                        != kv.getDataBuffer().getByte(groupIdx)) return false;
            } else if (!iv.getObject(inputRow).equals(kv.getObject(groupIdx))) {
                return false;
            }
        }
        return true;
    }

    // ==================== 桶定位：斐波那契（乘法）散列 ====================

    /**
     * 用斐波那契散列（Knuth 乘法散列）把 hash 映射到桶下标。
     *
     * <p>旧实现 {@code (hash & Integer.MAX_VALUE) % capacity} 在 capacity 为 2 的幂时
     * 等价于 {@code hash & (capacity - 1)}，只保留 hash 的「低 k 位」。当 key 的低位存在
     * 结构（顺序自增 id、偶数、id 与 id+2^k 共低位等）时会严重扎堆，线性探测链被拉长到
     * 上千，插入退化为 O(n²)。
     *
     * <p>本实现用黄金比例常数 {@code 0x9E3779B1}（= 2³²/φ）乘以 hash，再取 32 位乘积的
     * 高 k 位。乘积的低 32 位已被 hash 的全部 32 个比特搅动，因此桶位置依赖 key 的每一位，
     * 任意 key 分布都会被均匀打散到全表；探测链长度只由负载因子决定（约 1.4~2.5 次），
     * 与表大小无关，插入总代价保持 O(n)。
     *
     * @param hash     {@link #computeHash} 计算的散列值（可为负，符号位参与混洗）
     * @param capacity 桶数组容量，必须为 2 的幂
     * @return [0, capacity) 内的桶下标
     */
    protected static int bucketOf(int hash, int capacity) {
        int shift = 32 - Integer.numberOfTrailingZeros(capacity);
        // 取低 32 位乘积的高 k 位。切勿用 (hash * M) & (capacity - 1)：M 为奇数，
        // 那等价于只取 hash 低 k 位参与运算，等于没打散。
        return ((int) ((long) hash * 0x9E3779B1L)) >>> shift;
    }

    // ==================== 核心：开放寻址 插入或追加 ====================

    /**
     * 将一行写入哈希表：先按开放寻址探测，新 key → {@link #insertGroup}（插入建组），
     * 已有 key → {@link #appendToGroup}（追加/累积）。两个子类的探测逻辑完全相同，
     * 唯一差异收敛到三个可扩展点里，因此统一收敛到基类。
     *
     * @param input    输入页
     * @param rowIndex 输入行号
     * @return 命中或新建的 group index
     */
    public final int insertOrAppend(Page input, int rowIndex) {
        ensureTransfers(input);
        beforeRow(input);

        int hash = computeHash(input, rowIndex);
        int bucket = bucketOf(hash, capacity);

        int groupIdx;
        while (true) {
            groupIdx = buckets[bucket];
            if (groupIdx == -1) {
                groupIdx = insertGroup(input, rowIndex, bucket, hash);
                if (groupCount > capacity * LOAD_FACTOR) rehash();
                break;
            }
            if (keysEqual(input, rowIndex, groupIdx)) {
                appendToGroup(input, rowIndex, groupIdx);
                break;
            }
            bucket = (bucket + 1) % capacity;
        }

        afterRow(input, rowIndex);
        return groupIdx;
    }

    /**
     * 插入一个新的 key group。子类负责创建 value 存储 + 存 hash 到 rowHashes。
     *
     * @param input    输入页
     * @param rowIndex 输入行号
     * @param bucket   哈希桶位置
     * @param hash     该 key 的 hash 值（用于缓存）
     * @return 新建的 group index
     */
    protected abstract int insertGroup(Page input, int rowIndex, int bucket, int hash);

    /**
     * 已有 key 命中时的追加逻辑。子类实现：
     * <ul>
     *   <li>JoinHashTable: 把当前行的行号追加到 rowIndices[g]（一对多）</li>
     *   <li>AggregationHashTable: 用当前行累积聚合函数状态</li>
     * </ul>
     */
    protected abstract void appendToGroup(Page input, int rowIndex, int groupIdx);

    /** 每行处理前的钩子，默认空。AggregationHashTable 用它解析输入列索引。 */
    protected void beforeRow(Page input) { }

    /** 每行处理后的钩子，默认空。JoinHashTable 用它做全量行深拷贝 + buildRowCount++。 */
    protected void afterRow(Page input, int rowIndex) { }

    // ==================== 公共的扩容缓存 ====================

    protected void ensureRowHashesCapacity(int requiredIdx) {
        if (requiredIdx >= rowHashes.length) {
            rowHashes = Arrays.copyOf(rowHashes, rowHashes.length * 2);
        }
    }

    // ==================== 公共的 rehash ====================

    protected void rehash() {
        int newCapacity = capacity * 2;
        int[] newBuckets = new int[newCapacity];
        Arrays.fill(newBuckets, -1);

        for (int oldBucket = 0; oldBucket < capacity; oldBucket++) {
            int groupIdx = buckets[oldBucket];
            if (groupIdx == -1) continue;

            int hash = rowHashes[groupIdx];
            int newBucket = bucketOf(hash, newCapacity);

            while (newBuckets[newBucket] != -1)
                newBucket = (newBucket + 1) % newCapacity;
            newBuckets[newBucket] = groupIdx;
        }

        this.buckets = newBuckets;
        this.capacity = newCapacity;
    }

    // ==================== 状态查询 ====================

    public int size() { return groupCount; }

    public int rowCount() { return groupCount; }

    public boolean isEmpty() { return groupCount == 0; }

    /** 估算内存占用（字节）。子类应覆盖以包含 value 侧数据。 */
    public long estimateMemoryBytes() {
        long total = 0;
        for (FieldVector vec : keyVectors) total += vec.getBufferSize();
        total += (long) buckets.length * Integer.BYTES;
        total += (long) rowHashes.length * Integer.BYTES;
        return total;
    }

    /** 获取 key 列向量（只读） */
    public List<FieldVector> getKeyVectors() { return keyVectors; }

    // ==================== 生命周期 ====================

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        for (FieldVector vec : keyVectors) {
            if (vec != null) try { vec.close(); } catch (Exception ignored) {}
        }
        keyVectors.clear();
        keyTransfers.clear();
        rowHashes = null;
    }
}
