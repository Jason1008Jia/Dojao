package com.dojao.core.function.aggregate;

import java.nio.charset.StandardCharsets;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * COUNT(DISTINCT) 的精确去重状态 —— 按值类型选择<b>原始类型存储</b>，避免每个值一个装箱对象。
 *
 * <h3>为什么需要它</h3>
 * 旧状态是 {@code LinkedHashSet<Object>}：每条目 = {@code LinkedHashMap.Entry}（约 40B）+ 哈希表槽
 * （约 8B）+ 装箱值（Long 16B / String 对象 + byte[] 约 40B + n），高基数分组（千万级行）时既吃内存
 * 又制造巨量短命对象 ⇒ 长 GC，溢写前后反复编码/解码更慢。本类按列类型只存裸值：
 *
 * <ul>
 *   <li>{@link #addLong}/{@link #addDouble}：开放寻址 {@code long[]} + {@code boolean[]}，装载因子
 *       ≤ 0.5 ⇒ 约 17B/值（旧实现按 48B/值估算都不够）；</li>
 *   <li>{@link #addUtf8}：UTF-8 字节写入分块 arena（64KB 块，不整块复制），旁挂偏移/长度/哈希
 *       数组 ⇒ 约 20B + 字符串字节/值，且<b>不产生任何逐值对象</b>；</li>
 *   <li>类型混杂（极少见）时自动退化为 {@link HashSet} 语义（{@link #kind()} = {@code OBJECT}）。</li>
 * </ul>
 *
 * <p><b>语义完全等价</b>：值仍是 {@link CountDistinctAggregateFunction#normalize} 后的
 * Long / Double / String（即「整型 Number → Long、其余 Number → Double、文本 → String」），
 * 去重口径与旧 {@code LinkedHashSet} 一致；只是不再为每个值建对象。</p>
 *
 * <p><b>为什么实现 {@link Set}</b>：{@code AggregateFunction<Set<Object>>} 的 {@code merge} 用
 * {@code addAll}、{@code evaluate} 用 {@code size}、{@code reset} 用 {@code clear} ——
 * 保持 Set 接口可让这些调用点零改动，同时 {@link #addAll(Collection)} 对同类型状态走批量快路径。</p>
 *
 * <p><b>内存计量</b>：{@link #estimatedBytes()} 按实际数组长度返回，供
 * {@code AggregationHashTable.estimateStatesMem} 精确触发溢写。</p>
 *
 * @author jiaxin
 * @date 2026/10/19
 */
public final class DistinctSet extends AbstractSet<Object> {

    /** 状态种类。EMPTY 尚未收值；LONG/DOUBLE 共用 long 键表（DOUBLE 存 doubleToLongBits）。 */
    private static final byte EMPTY = 0;
    private static final byte LONG = 1;
    private static final byte DOUBLE = 2;
    private static final byte STRING = 3;
    private static final byte OBJECT = 4;

    /** 字符串 arena 的分块大小（单个超长值独占一块）。 */
    private static final int CHUNK_SIZE = 64 * 1024;

    private byte kind = EMPTY;
    private int size;

    // ==================== LONG / DOUBLE：开放寻址 ====================
    private long[] lkeys;
    private boolean[] lused;
    private int lmask;

    // ==================== STRING：分块 arena + 开放寻址槽 ====================
    /** 槽位 → 条目号 + 1（0 = 空槽）。 */
    private int[] slots;
    private int smask;
    /** 条目 → 打包位置（高 32 位 = 块号，低 32 位 = 块内偏移）。 */
    private long[] positions;
    private int[] lengths;
    private int[] hashes;
    private int entryCount;
    private byte[][] chunks;
    /** 当前写入的块号（-1 = 尚无块）。 */
    private int chunkIndex = -1;
    private int chunkFill;
    /**
     * 已分配 arena 的字节总量（含块对象头估算）。
     *
     * <p>★必须增量维护：{@code AggregationHashTable.updateStates} 会<b>逐行</b>调用
     * {@link #estimatedBytes()} 来刷新溢写闸门。若在 estimatedBytes() 里遍历 chunks，单行成本就是
     * O(块数)，而块数随去重值线性增长 ⇒ 整体 O(值数²)。线上 3000 万行 VARCHAR 去重时 chunk 达上万，
     * 表现为 Aggregate 阶段 CPU 打满、几十分钟不前进。</p>
     */
    private long arenaBytes;

    // ==================== OBJECT：罕见混杂类型 ====================
    private HashSet<Object> objectValues;

    // ==================== 写入 ====================

    /** 通用入口：按 {@link CountDistinctAggregateFunction#normalize} 规范化后写入。 */
    @Override
    public boolean add(Object raw) {
        Object v = CountDistinctAggregateFunction.normalize(raw);
        if (v == null) return false;
        return switch (kind) {
            case EMPTY -> {
                if (v instanceof Long l) { kind = LONG; yield addLong(l); }
                if (v instanceof Double d) { kind = DOUBLE; yield addDouble(d); }
                if (v instanceof String s) { kind = STRING; yield addString(s); }
                kind = OBJECT;
                objectValues = new HashSet<>();
                yield objectValues.add(v);
            }
            case LONG -> v instanceof Long l ? addLong(l) : promoteToObject(v);
            case DOUBLE -> v instanceof Double d ? addDouble(d) : promoteToObject(v);
            case STRING -> v instanceof String s ? addString(s) : promoteToObject(v);
            default -> objectValues.add(v);
        };
    }

    /** 整型快路径（调用方已确认非 NULL）。 */
    public boolean addLong(long v) {
        if (kind == EMPTY) kind = LONG;
        if (kind != LONG) return add(Long.valueOf(v));
        if (lkeys == null) initLongTable(64);
        return insertLong(v);
    }

    /** 浮点快路径（调用方已确认非 NULL）。 */
    public boolean addDouble(double v) {
        if (kind == EMPTY) kind = DOUBLE;
        if (kind != DOUBLE) return add(Double.valueOf(v));
        if (lkeys == null) initLongTable(64);
        return insertLong(Double.doubleToLongBits(v));
    }

    /** UTF-8 字节快路径（调用方已确认非 NULL；等价于先转 String 再 add）。 */
    public boolean addUtf8(byte[] src, int off, int len) {
        if (kind == EMPTY) kind = STRING;
        if (kind != STRING) {
            return add(new String(src, off, len, StandardCharsets.UTF_8));
        }
        if (slots == null) initStringTable(64);
        int hash = mix(hashBytes(src, off, len));
        int idx = hash & smask;
        while (slots[idx] != 0) {
            int e = slots[idx] - 1;
            if (hashes[e] == hash && lengths[e] == len && bytesEqual(src, off, e, len)) {
                return false;
            }
            idx = (idx + 1) & smask;
        }
        if (entryCount == positions.length) growEntries();
        int e = entryCount++;
        positions[e] = appendChunk(src, off, len);
        lengths[e] = len;
        hashes[e] = hash;
        slots[idx] = e + 1;
        size++;
        if (entryCount > slots.length / 2) growSlots();
        return true;
    }

    private boolean addString(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        return addUtf8(b, 0, b.length);
    }

    // ==================== LONG/DOUBLE 表 ====================

    private void initLongTable(int capacity) {
        lkeys = new long[capacity];
        lused = new boolean[capacity];
        lmask = capacity - 1;
    }

    private boolean insertLong(long v) {
        int idx = mix(v) & lmask;
        while (lused[idx]) {
            if (lkeys[idx] == v) return false;
            idx = (idx + 1) & lmask;
        }
        lused[idx] = true;
        lkeys[idx] = v;
        size++;
        if (size > lkeys.length / 2) growLongTable();
        return true;
    }

    private void growLongTable() {
        long[] oldKeys = lkeys;
        boolean[] oldUsed = lused;
        int newCap = oldKeys.length << 1;
        if (newCap <= 0) throw new OutOfMemoryError("COUNT(DISTINCT) 去重集合过大: " + size);
        long[] keys = new long[newCap];
        boolean[] used = new boolean[newCap];
        int mask = newCap - 1;
        for (int i = 0; i < oldKeys.length; i++) {
            if (!oldUsed[i]) continue;
            long v = oldKeys[i];
            int idx = mix(v) & mask;
            while (used[idx]) idx = (idx + 1) & mask;
            used[idx] = true;
            keys[idx] = v;
        }
        lkeys = keys;
        lused = used;
        lmask = mask;
    }

    // ==================== STRING 表 ====================

    private void initStringTable(int capacity) {
        slots = new int[capacity];
        smask = capacity - 1;
        positions = new long[capacity / 2];
        lengths = new int[capacity / 2];
        hashes = new int[capacity / 2];
    }

    private void growSlots() {
        int newCap = slots.length << 1;
        if (newCap <= 0) throw new OutOfMemoryError("COUNT(DISTINCT) 去重集合过大: " + size);
        int[] newSlots = new int[newCap];
        int mask = newCap - 1;
        for (int e = 0; e < entryCount; e++) {
            int idx = hashes[e] & mask;
            while (newSlots[idx] != 0) idx = (idx + 1) & mask;
            newSlots[idx] = e + 1;
        }
        slots = newSlots;
        smask = mask;
    }

    private void growEntries() {
        int cap = positions.length << 1;
        if (cap <= 0) throw new OutOfMemoryError("COUNT(DISTINCT) 去重集合过大: " + size);
        positions = Arrays.copyOf(positions, cap);
        lengths = Arrays.copyOf(lengths, cap);
        hashes = Arrays.copyOf(hashes, cap);
    }

    /** 把一段字节追加进 arena，返回打包位置（块号 << 32 | 块内偏移）。 */
    private long appendChunk(byte[] src, int off, int len) {
        if (chunkIndex < 0 || chunkFill + (long) len > chunks[chunkIndex].length) {
            int ci = chunkIndex + 1;
            if (chunks == null) chunks = new byte[4][];
            if (ci == chunks.length) chunks = Arrays.copyOf(chunks, chunks.length << 1);
            chunks[ci] = new byte[Math.max(CHUNK_SIZE, len)];
            arenaBytes += 16L + chunks[ci].length;   // 16B ≈ 数组对象头；增量缓存，estimatedBytes 必须 O(1)
            chunkIndex = ci;
            chunkFill = 0;
        }
        byte[] cur = chunks[chunkIndex];
        System.arraycopy(src, off, cur, chunkFill, len);
        long packed = ((long) chunkIndex << 32) | (chunkFill & 0xffffffffL);
        chunkFill += len;
        return packed;
    }

    private boolean bytesEqual(byte[] src, int off, int entry, int len) {
        long packed = positions[entry];
        byte[] chunk = chunks[(int) (packed >>> 32)];
        int at = (int) packed;
        return Arrays.equals(src, off, off + len, chunk, at, at + len);
    }

    private static int hashBytes(byte[] b, int off, int len) {
        int h = 1;
        for (int i = 0; i < len; i++) h = 31 * h + b[off + i];
        return h;
    }

    /**
     * 64 位混合（murmur3 finalizer）—— 连续 user_id 等低熵键在 2 的幂掩码下也能均匀散开；
     * 直接 {@code v & mask} 会让顺序 id 挤在相邻槽，探测链退化。
     */
    private static int mix(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        z = z ^ (z >>> 33);
        return (int) z;
    }

    // ==================== 类型退化 ====================

    /** 混入与当前存储类型不一致的值：退化为 HashSet（保持与 LinkedHashSet 相同的成员语义）。 */
    private boolean promoteToObject(Object v) {
        HashSet<Object> set = new HashSet<>(Math.max(16, size * 2));
        for (Object each : this) set.add(each);
        boolean added = set.add(v);
        resetStorage();
        kind = OBJECT;
        objectValues = set;
        size = set.size();
        return added;
    }

    // ==================== Set 接口 ====================

    @Override
    public int size() {
        return kind == OBJECT ? objectValues.size() : size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object raw) {
        Object v = CountDistinctAggregateFunction.normalize(raw);
        if (v == null) return false;
        return switch (kind) {
            case LONG -> {
                if (!(v instanceof Long l) || lkeys == null) yield false;
                int idx = mix(l) & lmask;
                while (lused[idx]) {
                    if (lkeys[idx] == l) yield true;
                    idx = (idx + 1) & lmask;
                }
                yield false;
            }
            case DOUBLE -> {
                if (!(v instanceof Double d) || lkeys == null) yield false;
                long bits = Double.doubleToLongBits(d);
                int idx = mix(bits) & lmask;
                while (lused[idx]) {
                    if (lkeys[idx] == bits) yield true;
                    idx = (idx + 1) & lmask;
                }
                yield false;
            }
            case STRING -> {
                if (!(v instanceof String s) || slots == null) yield false;
                byte[] b = s.getBytes(StandardCharsets.UTF_8);
                int hash = mix(hashBytes(b, 0, b.length));
                int idx = hash & smask;
                while (slots[idx] != 0) {
                    int e = slots[idx] - 1;
                    if (hashes[e] == hash && lengths[e] == b.length && bytesEqual(b, 0, e, b.length)) {
                        yield true;
                    }
                    idx = (idx + 1) & smask;
                }
                yield false;
            }
            case OBJECT -> objectValues.contains(v);
            default -> false;
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean addAll(Collection<?> c) {
        if (c == this || c.isEmpty()) return false;
        if (c instanceof DistinctSet other) return mergeFrom(other);
        return super.addAll((Collection<? extends Object>) c);
    }

    /** 同类型状态的批量并集：直接按裸值复制，避免遍历装箱。 */
    private boolean mergeFrom(DistinctSet other) {
        if (other.kind == EMPTY || other.size() == 0) return false;
        if (kind == OBJECT || other.kind == OBJECT || kind != EMPTY && kind != other.kind) {
            // 类型不同（正常不会发生）：退化为通用路径
            boolean changed = false;
            for (Object v : other) changed |= add(v);
            return changed;
        }
        if (kind == EMPTY) kind = other.kind;
        int before = size();
        switch (kind) {
            case LONG, DOUBLE -> {
                if (lkeys == null) initLongTable(64);
                // 直接搬裸值：DOUBLE 的 lkeys 存的是 doubleToLongBits，不能走 addDouble（会再次转换）
                for (int i = 0; i < other.lkeys.length; i++) {
                    if (other.lused[i]) insertLong(other.lkeys[i]);
                }
            }
            case STRING -> {
                for (int e = 0; e < other.entryCount; e++) {
                    long packed = other.positions[e];
                    byte[] chunk = other.chunks[(int) (packed >>> 32)];
                    addUtf8(chunk, (int) packed, other.lengths[e]);
                }
            }
            default -> { }
        }
        return size() != before;
    }

    @Override
    public void clear() {
        resetStorage();
        kind = EMPTY;
    }

    private void resetStorage() {
        lkeys = null;
        lused = null;
        lmask = 0;
        slots = null;
        smask = 0;
        positions = null;
        lengths = null;
        hashes = null;
        entryCount = 0;
        chunks = null;
        chunkIndex = -1;
        chunkFill = 0;
        arenaBytes = 0;
        objectValues = null;
        size = 0;
    }

    @Override
    public Iterator<Object> iterator() {
        return switch (kind) {
            case LONG, DOUBLE -> new Iterator<>() {
                private int i = -1;
                { advance(); }
                private void advance() { while (++i < lkeys.length && !lused[i]) { } }
                @Override public boolean hasNext() { return i < lkeys.length; }
                @Override public Object next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    long raw = lkeys[i];
                    advance();
                    // ★必须用 if：三元表达式里 Long 与 Double 会被二元数值提升成 double 再统一装箱，
                    // 结果恒为 Double（Long 分支反解成 0.0 这类脏值）。
                    if (kind == LONG) return Long.valueOf(raw);
                    return Double.valueOf(Double.longBitsToDouble(raw));
                }
            };
            case STRING -> new Iterator<>() {
                private int e = 0;
                @Override public boolean hasNext() { return e < entryCount; }
                @Override public Object next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    long packed = positions[e];
                    byte[] chunk = chunks[(int) (packed >>> 32)];
                    String s = new String(chunk, (int) packed, lengths[e], StandardCharsets.UTF_8);
                    e++;
                    return s;
                }
            };
            case OBJECT -> objectValues.iterator();
            default -> java.util.Collections.emptyIterator();
        };
    }

    // ==================== 内存计量 ====================

    /**
     * 状态的近似堆占用（供溢写闸门）：按<b>实际数组长度</b>计，而不是「元素数 × 常数」——
     * 开放寻址表的装载因子与 arena 的空闲块都能如实反映。
     */
    public long estimatedBytes() {
        long bytes = 64L;   // 本对象头 + 字段
        switch (kind) {
            case LONG, DOUBLE -> {
                if (lkeys != null) bytes += (long) lkeys.length * 8L + lkeys.length;
            }
            case STRING -> {
                if (slots != null) bytes += (long) slots.length * 4L;
                if (positions != null) {
                    bytes += (long) positions.length * 8L
                            + (long) lengths.length * 4L
                            + (long) hashes.length * 4L;
                }
                // ★O(1)：arenaBytes 在 appendChunk 增量维护。绝不能在这里遍历 chunks ——
                // 该方法被 updateStates 逐行调用，遍历会退化成 O(值数 × 块数) = O(值数²)。
                if (chunks != null) bytes += (long) chunks.length * 8L + arenaBytes;
            }
            case OBJECT -> bytes += 24L + (long) objectValues.size() * 48L;
            default -> { }
        }
        return bytes;
    }

    /** 测试/探针用：当前存储种类（{@code "EMPTY"/"LONG"/"DOUBLE"/"STRING"/"OBJECT"}）。 */
    public String kind() {
        return switch (kind) {
            case LONG -> "LONG";
            case DOUBLE -> "DOUBLE";
            case STRING -> "STRING";
            case OBJECT -> "OBJECT";
            default -> "EMPTY";
        };
    }

    // ==================== 溢写编解码（二进制）====================

    /**
     * 状态 → 二进制（单列 VarChar 的 payload）：
     * <pre>
     *   tag(1B) + count(4B) + 载荷
     *   'L'：count × 8B 原始 long
     *   'D'：count × 8B doubleToLongBits
     *   'S'：count × [len(4B) + UTF-8 字节]
     *   'O'：count × [tag(1B) + 'i'→8B / 'f'→8B / 's'→len(4B)+字节]
     * </pre>
     * 相比旧的「文本 + 长度前缀」：不产生 String/char[] 中间对象（旧路径每个值都 toString +
     * substring），长整数也只占 8B。
     */
    public byte[] encode() {
        switch (kind) {
            case LONG, DOUBLE: {
                int n = size;
                byte[] out = new byte[5 + n * 8];
                out[0] = kind == LONG ? (byte) 'L' : (byte) 'D';
                writeInt(out, 1, n);
                int p = 5;
                for (int i = 0; i < lkeys.length; i++) {
                    if (!lused[i]) continue;
                    writeLong(out, p, lkeys[i]);
                    p += 8;
                }
                return out;
            }
            case STRING: {
                long payload = 0;
                for (int e = 0; e < entryCount; e++) payload += 4L + lengths[e];
                byte[] out = new byte[Math.toIntExact(5 + payload)];
                out[0] = (byte) 'S';
                writeInt(out, 1, entryCount);
                int p = 5;
                for (int e = 0; e < entryCount; e++) {
                    long packed = positions[e];
                    byte[] chunk = chunks[(int) (packed >>> 32)];
                    int at = (int) packed;
                    int len = lengths[e];
                    writeInt(out, p, len);
                    System.arraycopy(chunk, at, out, p + 4, len);
                    p += 4 + len;
                }
                return out;
            }
            default: {
                // EMPTY / OBJECT（含混杂类型）：逐值写带 tag 的规范形态
                Object[] vals = toArray();
                long payload = 0;
                for (Object v : vals) {
                    payload += v instanceof String s ? 5L + utf8Len(s) : 9L;
                }
                byte[] out = new byte[Math.toIntExact(5 + payload)];
                out[0] = (byte) 'O';
                writeInt(out, 1, vals.length);
                int p = 5;
                for (Object v : vals) {
                    if (v instanceof Long l) {
                        out[p++] = (byte) 'i';
                        writeLong(out, p, l);
                        p += 8;
                    } else if (v instanceof Double d) {
                        out[p++] = (byte) 'f';
                        writeLong(out, p, Double.doubleToLongBits(d));
                        p += 8;
                    } else {
                        byte[] b = String.valueOf(v).getBytes(StandardCharsets.UTF_8);
                        out[p++] = (byte) 's';
                        writeInt(out, p, b.length);
                        System.arraycopy(b, 0, out, p + 4, b.length);
                        p += 4 + b.length;
                    }
                }
                return out;
            }
        }
    }

    /** {@link #encode()} 的逆操作；null / 空数组 → 空状态。 */
    public static DistinctSet decode(byte[] bytes) {
        DistinctSet set = new DistinctSet();
        if (bytes == null || bytes.length < 5) return set;
        byte tag = bytes[0];
        int n = readInt(bytes, 1);
        int p = 5;
        switch (tag) {
            case 'L' -> {
                set.kind = LONG;
                set.initLongTable(capacityFor(n));
                for (int i = 0; i < n; i++) {
                    set.addLong(readLong(bytes, p));
                    p += 8;
                }
            }
            case 'D' -> {
                set.kind = DOUBLE;
                set.initLongTable(capacityFor(n));
                for (int i = 0; i < n; i++) {
                    set.addDouble(Double.longBitsToDouble(readLong(bytes, p)));
                    p += 8;
                }
            }
            case 'S' -> {
                set.kind = STRING;
                set.initStringTable(capacityFor(n));
                for (int i = 0; i < n; i++) {
                    int len = readInt(bytes, p);
                    p += 4;
                    set.addUtf8(bytes, p, len);
                    p += len;
                }
            }
            default -> {
                for (int i = 0; i < n; i++) {
                    byte t = bytes[p++];
                    if (t == 'i') {
                        set.addLong(readLong(bytes, p));
                        p += 8;
                    } else if (t == 'f') {
                        set.addDouble(Double.longBitsToDouble(readLong(bytes, p)));
                        p += 8;
                    } else {
                        int len = readInt(bytes, p);
                        p += 4;
                        set.addUtf8(bytes, p, len);
                        p += len;
                    }
                }
            }
        }
        return set;
    }

    private static int capacityFor(int count) {
        long need = Math.max(64L, (long) count * 2L);
        int cap = Integer.highestOneBit((int) Math.min(Integer.MAX_VALUE - 8L, need));
        if (cap < need) cap <<= 1;
        return Math.max(cap, 64);
    }

    private static int utf8Len(String s) {
        // 只用于长度预估；String.getBytes 的实际长度可能更短（代理对），按上界分配足够空间
        return s.length() * 3;
    }

    private static void writeInt(byte[] b, int at, int v) {
        b[at] = (byte) (v >>> 24);
        b[at + 1] = (byte) (v >>> 16);
        b[at + 2] = (byte) (v >>> 8);
        b[at + 3] = (byte) v;
    }

    private static int readInt(byte[] b, int at) {
        return ((b[at] & 0xff) << 24) | ((b[at + 1] & 0xff) << 16)
                | ((b[at + 2] & 0xff) << 8) | (b[at + 3] & 0xff);
    }

    private static void writeLong(byte[] b, int at, long v) {
        for (int i = 7; i >= 0; i--) {
            b[at + i] = (byte) v;
            v >>>= 8;
        }
    }

    private static long readLong(byte[] b, int at) {
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (b[at + i] & 0xffL);
        return v;
    }

    /** 探针用：与旧 {@code LinkedHashSet} 状态逐值比对（只读）。 */
    public static boolean sameMembers(Set<?> a, Set<?> b) {
        if (a.size() != b.size()) return false;
        for (Object v : a) if (!b.contains(v)) return false;
        return true;
    }
}
