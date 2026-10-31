package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.dojao.core.operator.AggregateFunction.toDouble;

/**
 * 行累加器 —— Map 分组 + 行存储，实现 {@link AbstractSpillableOperator.Slot}。
 *
 * <p>用于 {@link WindowSortOperator} 的三槽管线，按 PARTITION BY 键分组，
 * 将原始行追加到列向量中存储。溢写时按 key 排序输出全量数据列。</p>
 *
 * <h3>"数据不动，索引动"</h3>
 * <p>物理数据存在 vectors 列存储中（Arrow 列式），分组只维护 key → List&lt;Integer&gt;
 * 的行号映射。排序只排 int[] 行号，不碰 Arrow 列数据。</p>
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/7/23
 */
public class RowAccumulator implements Slot {

    static final Object NULL_KEY = new Object();

    /** 索引每行摊销字节：Object[] 槽位（4）+ 装箱 Integer（16，>127 不走缓存）+ ArrayList 扩容余量。 */
    private static final long ROW_INDEX_BYTES = 24L;

    /** 每个**新增**分组键的摊销字节：HashMap 节点（32）+ key 对象（~40）+ ArrayList 头（24）+ 数组头（~24）。 */
    private static final long KEY_INDEX_BYTES = 120L;

    /**
     * 分组索引（key → 行号列表）的<b>增量</b>内存估算字节。
     *
     * <p>为什么必须计入 {@link #estimateMemoryBytes()}：索引常与列数据同量级甚至更大 ——
     * 高基数 PARTITION BY（如 {@code row_number() OVER (PARTITION BY event_id ...)} 且 event_id 近似唯一）
     * 时每组仅 1 行，9.53M 行就对应 9.53M 个 key，每个 key 一个 map 节点 + key 对象 + ArrayList + 装箱
     * Integer ⇒ 索引约 1–2GB，而此前只用列向量估算（约同量级），导致<b>始终不触发溢写</b>、堆无限膨胀
     * （实测「一涉及开窗函数 worker 就挂」的主因之一）。</p>
     *
     * <p>刻意做成增量累加而非每次遍历：本方法在 {@code accumulatePage} 每页都会被调用，遍历全部 key
     * 会把累积退化成 O(行数 × key 数)。累加值只在 {@link #addPage} / {@link #addRow} 里随写入更新，
     * 读取恒为 O(1)。</p>
     */
    long indexBytes;

    final List<FieldVector> vectors = new ArrayList<>();
    final LinkedHashMap<Object, List<Integer>> keyToRowIndices = new LinkedHashMap<>();
    final List<TransferPair> transfers = new ArrayList<>();
    VectorSchemaRoot lastSrcRoot;
    int rowCount;
    int[] groupKeyIndices;
    boolean isComposite;

    // INT key 快速路径：用 primitive map 避免 Integer 装箱
    transient IntObjectMap intKeyMap;

    // SUM per-key 符号追踪：null=未设置, (byte)1=全非负, (byte)-1=全非正, (byte)0=混合
    transient HashMap<Object, Byte> keySigns;
    transient HashMap<Integer, Byte> intKeySigns;
    int sumColIdx = -1;

    // ==================== Slot 接口 ====================

    @Override
    public void addRow(Page input, int row) {
        VectorSchemaRoot src = input.getRoot();
        for (TransferPair transfer : transfers) {
            transfer.copyValueSafe(row, rowCount);
        }
        Object key = extractGroupKey(src, row);
        List<Integer> list = keyToRowIndices.get(key);
        if (list == null) {
            list = new ArrayList<>(4);
            keyToRowIndices.put(key, list);
            indexBytes += KEY_INDEX_BYTES;
        }
        list.add(rowCount);
        indexBytes += ROW_INDEX_BYTES;
        rowCount++;
    }

    @Override
    public void addPage(Page input) {
        VectorSchemaRoot src = input.getRoot();
        int pageRows = input.getRowCount();
        int dstStart = rowCount;

        // 列批量拷贝：定长列直接 memcpy，变长列出 offset+data buffer 区间拷贝
        for (int col = 0; col < src.getFieldVectors().size(); col++) {
            copyColumnBatch(src.getVector(col), vectors.get(col), 0, dstStart, pageRows);
        }

        // SUM per-key 符号：随数据累积同步更新，no double scan
        FieldVector sumVec = sumColIdx >= 0 ? src.getVector(sumColIdx) : null;

        if (isComposite) {
            for (int r = 0; r < pageRows; r++) {
                Object key = buildCompositeKey(src, r);
                List<Integer> list = keyToRowIndices.get(key);
                if (list == null) {
                    list = new ArrayList<>(4);
                    keyToRowIndices.put(key, list);
                    indexBytes += KEY_INDEX_BYTES;
                }
                list.add(dstStart + r);
                indexBytes += ROW_INDEX_BYTES;
                if (sumVec != null) updateKeySign(key, toDouble(sumVec, r));
            }
        } else if (groupKeyIndices.length == 0) {
            // 全分组键（全窗口，如 ROW_NUMBER() OVER (ORDER BY ...)）
            List<Integer> global = keyToRowIndices.get(NULL_KEY);
            if (global == null) {
                global = new ArrayList<>(pageRows * 2);
                keyToRowIndices.put(NULL_KEY, global);
                indexBytes += KEY_INDEX_BYTES;
            }
            for (int r = 0; r < pageRows; r++) {
                global.add(dstStart + r);
                if (sumVec != null) updateKeySign(NULL_KEY, toDouble(sumVec, r));
            }
            indexBytes += (long) pageRows * ROW_INDEX_BYTES;
        } else {
            int gIdx = groupKeyIndices[0];
            FieldVector keyVec = src.getVector(gIdx);
            int keyTw = typeWidthOf(keyVec.getField().getType());
            if (keyTw > 0) {
                ArrowBuf keyData = keyVec.getDataBuffer();
                if (keyTw == 4) {
                    // INT 键快速路径：primitive map 避免 Integer 装箱
                    if (intKeyMap == null) intKeyMap = new IntObjectMap(pageRows);
                    for (int r = 0; r < pageRows; r++) {
                        int key = keyVec.isNull(r) ? 0 : keyData.getInt((long) r * 4);
                        List<Integer> list = intKeyMap.get(key);
                        if (list == null) {
                            list = new ArrayList<>(4);
                            intKeyMap.put(key, list);
                            indexBytes += KEY_INDEX_BYTES;
                        }
                        list.add(dstStart + r);
                        indexBytes += ROW_INDEX_BYTES;
                        if (sumVec != null) updateIntKeySign(key, toDouble(sumVec, r));
                    }
                } else {
                    for (int r = 0; r < pageRows; r++) {
                        // ★必须按真实位宽读取：以前只区分 8/其它→getInt，TINYINT(tw=1)/SMALLINT(tw=2)
                        //   等键会按 4 字节读 ⇒ 越过 data buffer 末尾抛
                        //   IndexOutOfBoundsException（线上 order_status tinyint 分区的实锤）。
                        //   Decimal(tw=16/32) 也走这里，交给 getObject 保持与 addRow 同型。
                        Object key = readKeyValue(keyVec, r);
                        List<Integer> list = keyToRowIndices.get(key);
                        if (list == null) {
                            list = new ArrayList<>(4);
                            keyToRowIndices.put(key, list);
                            indexBytes += KEY_INDEX_BYTES;
                        }
                        list.add(dstStart + r);
                        indexBytes += ROW_INDEX_BYTES;
                        if (sumVec != null) updateKeySign(key, toDouble(sumVec, r));
                    }
                }
            } else {
                for (int r = 0; r < pageRows; r++) {
                    Object key;
                    if (keyVec.isNull(r)) {
                        key = NULL_KEY;
                    } else {
                        Object val = keyVec.getObject(r);
                        key = (val instanceof byte[] ba) ? new String(ba, StandardCharsets.UTF_8) : val;
                    }
                    List<Integer> list = keyToRowIndices.get(key);
                    if (list == null) {
                        list = new ArrayList<>(4);
                        keyToRowIndices.put(key, list);
                        indexBytes += KEY_INDEX_BYTES;
                    }
                    list.add(dstStart + r);
                    indexBytes += ROW_INDEX_BYTES;
                    if (sumVec != null) updateKeySign(key, toDouble(sumVec, r));
                }
            }
        }

        rowCount += pageRows;
    }

    private void updateKeySign(Object key, double value) {
        if (keySigns == null) keySigns = new HashMap<>();
        byte unit = (byte) (value >= 0 ? 1 : -1);
        Byte cur = keySigns.get(key);
        if (cur == null) keySigns.put(key, unit);
        else if (cur != 0 && cur != unit) keySigns.put(key, (byte) 0);
    }

    private void updateIntKeySign(int key, double value) {
        if (intKeySigns == null) intKeySigns = new HashMap<>();
        byte unit = (byte) (value >= 0 ? 1 : -1);
        Byte cur = intKeySigns.get(key);
        if (cur == null) intKeySigns.put(key, unit);
        else if (cur != 0 && cur != unit) intKeySigns.put(key, (byte) 0);
    }

    /** 按 key 排序后构建溢写根（全量列数据的排序拷贝） */
    @Override
    public VectorSchemaRoot buildSortedSpillRoot() {
        return null; // WindowSortOperator 使用 buildGroupSortedOutput 代替
    }

    @Override
    public long estimateMemoryBytes() {
        int rc = rowCount;
        if (rc == 0) return indexBytes;
        long total = 0;
        for (FieldVector v : vectors) {
            int tw = typeWidthOf(v.getField().getType());
            if (tw > 0) {
                total += (long) rc * tw;
            } else if (v instanceof BaseVariableWidthVector bv) {
                // 变长列：按 Arrow 真实分配的 buffer 容量计（= 已占堆外内存），不能用「逻辑字节数」。
                // Arrow 变长 buffer 按 2 的幂扩容（保存新 buffer 再释放旧 buffer），capacity 可达 used 的 ~2×；
                // 只算逻辑长度会漏掉这部分真实占用 ⇒ estimate 低于实际 ⇒ 单槽实际占用越过阈值才溢写，
                // 多分区叠加时更会先撞分配器硬上限报 OOM（row_number() over(partition by …) 撑挂 Worker）。
                ArrowBuf off = bv.getOffsetBuffer();
                ArrowBuf data = bv.getDataBuffer();
                total += (off != null ? off.capacity() : 0);
                total += (data != null ? data.capacity() : 0);
            } else {
                total += v.getBufferSize();
            }
            total += (rc + 7) / 8;
        }
        // ★ 必须计入分组索引：高基数 PARTITION BY 下索引（map 节点 + key 对象 + 行号列表）常远超列数据，
        //   只算列向量会让「已占用内存」被严重低估 ⇒ 永不触发溢写 ⇒ 堆无限膨胀直至 OOM。
        return total + indexBytes;
    }

    @Override
    public boolean isEmpty() { return rowCount == 0; }

    @Override
    public int rowCount() { return rowCount; }

    @Override
    public void close() { clear(); }

    // ==================== 生命周期 ====================

    void initialize(Schema schema, BufferAllocator allocator, int initialCapacity) {
        vectors.clear();
        keyToRowIndices.clear();
        intKeyMap = null;
        keySigns = null;
        intKeySigns = null;
        transfers.clear();
        lastSrcRoot = null;
        rowCount = 0;
        indexBytes = 0;
        for (Field field : schema.getFields()) {
            FieldVector vec = field.createVector(allocator);
            vec.setInitialCapacity(initialCapacity);
            vec.allocateNew();
            vectors.add(vec);
        }
    }

    void initIfNeeded(Schema schema, BufferAllocator allocator, int initialCapacity) {
        if (vectors.isEmpty()) initialize(schema, allocator, initialCapacity);
    }

    /** 设置 SUM 聚合列索引，启用 per-key 符号累积。 */
    void setSumColumnIndex(int colIdx) { this.sumColIdx = colIdx; }

    /** per-key 符号：for keyToRowIndices（Object keys）。null→未启用；0→混合；±1→全同号。 */
    Byte getKeySign(Object key) { return keySigns != null ? keySigns.get(key) : null; }
    Byte getIntKeySign(int key) { return intKeySigns != null ? intKeySigns.get(key) : null; }

    boolean hasIntKeys() { return intKeyMap != null; }
    int[] sortedIntKeys() { return intKeyMap != null ? intKeyMap.keyArray() : new int[0]; }
    List<Integer> getIntRowIndices(int key) { return intKeyMap != null ? intKeyMap.get(key) : null; }

    void clear() {
        for (FieldVector v : vectors) {
            try { v.close(); } catch (Exception ignored) {}
        }
        vectors.clear();
        keyToRowIndices.clear();
        intKeyMap = null;
        keySigns = null;
        intKeySigns = null;
        transfers.clear();
        lastSrcRoot = null;
        rowCount = 0;
        indexBytes = 0;
    }

    void ensureTransfers(VectorSchemaRoot srcRoot, int[] gKeyIdx, boolean compositeKey) {
        this.groupKeyIndices = gKeyIdx;
        this.isComposite = compositeKey;
        if (srcRoot == lastSrcRoot) return;
        transfers.clear();
        for (int i = 0; i < srcRoot.getFieldVectors().size(); i++) {
            transfers.add(srcRoot.getVector(i).makeTransferPair(vectors.get(i)));
        }
        lastSrcRoot = srcRoot;
    }

    // ==================== Key 提取 ====================

    private Object extractGroupKey(VectorSchemaRoot src, int srcRow) {
        int gIdx = groupKeyIndices != null ? groupKeyIndices[0] : -1;
        if (gIdx < 0) return NULL_KEY;
        FieldVector vec = src.getVector(gIdx);
        if (vec.isNull(srcRow)) return NULL_KEY;
        Object val = vec.getObject(srcRow);
        if (val instanceof byte[] ba) return new String(ba, StandardCharsets.UTF_8);
        return val;
    }

    private Object buildCompositeKey(VectorSchemaRoot src, int row) {
        Object[] vals = new Object[groupKeyIndices.length];
        for (int i = 0; i < groupKeyIndices.length; i++) {
            FieldVector vec = src.getVector(groupKeyIndices[i]);
            vals[i] = readKeyValue(vec, row);
        }
        return new WindowSortOperator.CompositeKey(vals);
    }

    static Object readKeyValue(FieldVector vec, int row) {
        if (vec.isNull(row)) return NULL_KEY;
        int tw = typeWidthOf(vec.getField().getType());
        if (tw == 8) return vec.getDataBuffer().getLong((long) row * 8);
        if (tw == 4) return vec.getDataBuffer().getInt((long) row * 4);
        if (tw == 2) return vec.getDataBuffer().getShort((long) row * 2);
        if (tw == 1) return vec.getDataBuffer().getByte(row);
        Object val = vec.getObject(row);
        return (val instanceof byte[] ba) ? new String(ba, StandardCharsets.UTF_8) : val;
    }

    // ==================== 列批量拷贝 ====================

    /** 跨分配器列拷贝原语（内部 + 交换层 {@code com.dojao.server.exchange.ExchangeFlightProducer.copyPage} 复用）：定长 memcpy / 变长 offset+data 区间拷贝。 */
    public static void copyColumnBatch(FieldVector src, FieldVector dst, int srcOff, int dstOff, int count) {
        if (count == 0) return;
        ArrowType type = src.getField().getType();
        // FixedSizeList（Vector 类型）：不能用 getDataBuffer()（抛 UnsupportedOperationException），
        // 数据在 child vector 中。父级 validity + child 递归拷贝。
        if (type instanceof ArrowType.FixedSizeList fl) {
            int listSize = fl.getListSize();
            // 1. 父级 validity buffer 容量保证 + 位拷贝
            while (dst.getValidityBuffer().capacity() < (dstOff + count + 7) / 8) dst.reAlloc();
            copyValidityBits(src, dst, srcOff, dstOff, count);
            // 2. child 数据 + child validity：偏移/数量 × listSize 递归拷贝
            //    （定长元素走 memcpy，变长元素走 copyValueSafe；child validity 一并复制，
            //     保证 FixedSizeListVector.getObject 逐元素读 child 时不抛 "Value at index is null"）。
            //    long 乘法防溢出：child 元素偏移 = 行偏移 × listSize。
            copyColumnBatch(((FixedSizeListVector) src).getDataVector(),
                    ((FixedSizeListVector) dst).getDataVector(),
                    (int) ((long) srcOff * listSize), (int) ((long) dstOff * listSize),
                    (int) ((long) count * listSize));
            return;
        }
        int tw = typeWidthOf(type);
        if (tw > 0) {
            // 定长列：直接操作 data buffer，一次 setBytes
            ArrowBuf srcBuf = src.getDataBuffer();
            ArrowBuf dstBuf = dst.getDataBuffer();
            long dstOffset = (long) dstOff * tw;
            long bytes = (long) count * tw;
            while (dstBuf.capacity() < dstOffset + bytes) {
                dst.reAlloc();
                dstBuf = dst.getDataBuffer();
            }
            dstBuf.setBytes(dstOffset, srcBuf, (long) srcOff * tw, bytes);
            copyValidityBits(src, dst, srcOff, dstOff, count);
        } else {
            // 变长列（VarChar/VarBinary 等）：TransferPair 逐行拷贝。
            // copyValueSafe 内部推进 lastSet，下游再 setValueCount 时 fillHoles 变 no-op，var 数据安全。
            TransferPair tp = src.makeTransferPair(dst);
            for (int r = 0; r < count; r++) tp.copyValueSafe(srcOff + r, dstOff + r);
        }
    }

    /** 拷贝 validity 位。对齐路径用块拷贝，否则逐位处理。 */
    private static void copyValidityBits(FieldVector src, FieldVector dst, int srcOff, int dstOff, int count) {
        ArrowBuf srcVal = src.getValidityBuffer();
        ArrowBuf dstVal = dst.getValidityBuffer();
        if (srcVal == null || dstVal == null) return;
        if (srcOff % 8 == 0 && dstOff % 8 == 0 && count % 8 == 0) {
            // 完美对齐：直接块拷贝
            dstVal.setBytes(dstOff / 8, srcVal, srcOff / 8, count / 8);
        } else {
            for (int r = 0; r < count; r++) {
                int si = srcOff + r, di = dstOff + r;
                int byteIdx = di / 8;
                int bitMask = 1 << (di % 8);
                if (src.isNull(si)) {
                    dstVal.setByte(byteIdx, (byte) (dstVal.getByte(byteIdx) & ~bitMask));
                } else {
                    dstVal.setByte(byteIdx, (byte) (dstVal.getByte(byteIdx) | bitMask));
                }
            }
        }
    }

    static int typeWidthOf(ArrowType type) {
        if (type instanceof ArrowType.Int i) return i.getBitWidth() / 8;
        if (type instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> 4;
                case DOUBLE -> 8;
                case HALF -> 2;
            };
        }
        if (type instanceof ArrowType.Bool) return 1;
        if (type instanceof ArrowType.Date d) {
            return switch (d.getUnit()) {
                case DAY -> 4;
                case MILLISECOND -> 8;
            };
        }
        if (type instanceof ArrowType.Timestamp) return 8;
        if (type instanceof ArrowType.Time t) {
            return switch (t.getUnit()) {
                case SECOND -> 4;
                case MILLISECOND -> 8;
                case MICROSECOND -> 8;
                case NANOSECOND -> 8;
            };
        }
        if (type instanceof ArrowType.Decimal d) return d.getBitWidth() / 8; // Decimal128=16, Decimal256=32
        return 0; // 变长或未知
    }

    // ==================== INT 键 Primitive Map（避免 Integer 装箱） ====================

    static final class IntObjectMap {
        private static final int EMPTY = Integer.MIN_VALUE;
        private int[] keys;
        private Object[] values;
        private int size;
        private int mask;

        IntObjectMap(int expectedSize) {
            int cap = 16;
            while (cap < expectedSize) cap <<= 1;
            this.keys = new int[cap];
            this.values = new Object[cap];
            this.mask = cap - 1;
            Arrays.fill(keys, EMPTY);
        }

        @SuppressWarnings("unchecked")
        <T> T get(int key) {
            int idx = (key ^ (key >>> 16)) & mask;
            while (keys[idx] != EMPTY) {
                if (keys[idx] == key) return (T) values[idx];
                idx = (idx + 1) & mask;
            }
            return null;
        }

        void put(int key, Object value) {
            if (size >= (keys.length >> 1)) rehash();
            int idx = (key ^ (key >>> 16)) & mask;
            while (keys[idx] != EMPTY && keys[idx] != key) idx = (idx + 1) & mask;
            if (keys[idx] == EMPTY) { keys[idx] = key; size++; }
            values[idx] = value;
        }

        private void rehash() {
            int[] oldKeys = keys;
            Object[] oldValues = values;
            int newCap = keys.length << 1;
            keys = new int[newCap];
            values = new Object[newCap];
            mask = newCap - 1;
            Arrays.fill(keys, EMPTY);
            for (int i = 0; i < oldKeys.length; i++) {
                if (oldKeys[i] != EMPTY) {
                    int idx = (oldKeys[i] ^ (oldKeys[i] >>> 16)) & mask;
                    while (keys[idx] != EMPTY) idx = (idx + 1) & mask;
                    keys[idx] = oldKeys[i];
                    values[idx] = oldValues[i];
                }
            }
        }

        int size() { return size; }

        void removeByKey(int key) {
            int idx = (key ^ (key >>> 16)) & mask;
            while (keys[idx] != EMPTY) {
                if (keys[idx] == key) { keys[idx] = EMPTY; values[idx] = null; size--; return; }
                idx = (idx + 1) & mask;
            }
        }

        int[] keyArray() {
            int[] result = new int[size];
            int pos = 0;
            for (int k : keys) {
                if (k != EMPTY) result[pos++] = k;
            }
            Arrays.sort(result);
            return result;
        }
    }
}
