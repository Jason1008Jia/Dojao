package com.dojao.core.operator;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

/**
 * Arrow 向量值零分配比较工具 —— 直接从 Arrow buffer 读值，避免 {@code getObject()} 的对象分配。
 *
 * <p>排序热路径中每次比较调用 {@code getObject()} 会创建 Long/Integer/byte[] 等临时对象，
 * 百亿次比较场景下产生严重 GC 压力。本工具直接操作 Arrow 底层 buffer，零对象分配。
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/7/20
 */
public final class ArrowComparators {

    private ArrowComparators() {}

    /**
     * 零分配比较两个 Arrow 向量中的两行值。
     *
     * @return 负值 = a &lt; b, 0 = 相等, 正值 = a &gt; b
     */
    public static int compare(FieldVector vecA, int rowA,
                               FieldVector vecB, int rowB) {
        boolean nullA = vecA.isNull(rowA);
        boolean nullB = vecB.isNull(rowB);
        if (nullA && nullB) return 0;
        // NULL 视为最小：ASC 时排最前、DESC（调用方取反）时排最后，对齐 MySQL 语义
        if (nullA) return -1;
        if (nullB) return 1;

        ArrowType type = vecA.getField().getType();
        int w = typeWidthTag(type);

        // FixedSizeList（ARRAY/向量列）：零分配直接读 child buffer 逐位比较，避免 getObject 建 List+装箱
        if (w == 0 && type instanceof ArrowType.FixedSizeList
                && vecA instanceof FixedSizeListVector && vecB instanceof FixedSizeListVector) {
            return compareFixedSizeList((FixedSizeListVector) vecA, rowA,
                                        (FixedSizeListVector) vecB, rowB);
        }

        switch (w) {
            case 8: // 8-byte: BigInt, Float8, Timestamp, Date64, Duration, Interval
                return Long.compare(
                        vecA.getDataBuffer().getLong((long) rowA * 8),
                        vecB.getDataBuffer().getLong((long) rowB * 8));
            case 4: // 4-byte: Int, Float4, Date32, Time32
                return Integer.compare(
                        vecA.getDataBuffer().getInt((long) rowA * 4),
                        vecB.getDataBuffer().getInt((long) rowB * 4));
            case 2: // 2-byte: Half float, SmallInt
                return Short.compare(
                        vecA.getDataBuffer().getShort((long) rowA * 2),
                        vecB.getDataBuffer().getShort((long) rowB * 2));
            case 1: // 1-byte: Bool, TinyInt
                return Byte.compare(
                        vecA.getDataBuffer().getByte(rowA),
                        vecB.getDataBuffer().getByte(rowB));
            case -1: // VarChar / Binary: 零分配，直接比 buffer 中的字节
                return compareVarChar(vecA, rowA, vecB, rowB);
            default: // Decimal 等 → fallback getObject
                return fallbackCompare(vecA, rowA, vecB, rowB);
        }
    }

    /** 返回定长类型字节宽度标签：-1 = 变长，0 = fallback。 */
    private static int typeWidthTag(ArrowType type) {
        if (type instanceof ArrowType.Int i) return Math.min(i.getBitWidth() / 8, 8);
        if (type instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> 4;
                case DOUBLE -> 8;
                case HALF -> 2;
            };
        }
        if (type instanceof ArrowType.Bool) return 1;
        // Date 有两种物理宽度：DATE 是 Date32（4 字节），DATETIME 才是 Date64（8 字节）。
        // 旧实现一律按 8 读 ⇒ Date32 列每行多读 4 字节（跨到下一行/越过缓冲区尾），排序顺序错乱。
        if (type instanceof ArrowType.Date d) {
            return d.getUnit() == DateUnit.DAY ? 4 : 8;
        }
        if (type instanceof ArrowType.Timestamp) return 8;
        if (type instanceof ArrowType.Time) return 4;
        if (type instanceof ArrowType.Duration) return 8;
        if (type instanceof ArrowType.Interval) return 8;
        if (type instanceof ArrowType.FixedSizeBinary b) return b.getByteWidth();
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.Binary
                || type instanceof ArrowType.LargeUtf8 || type instanceof ArrowType.LargeBinary)
            return -1;
        return 0; // Decimal, Struct, List, Map → fallback
    }

    /** 零分配 VarChar/Binary 比较：直接从 offset buffer + data buffer 逐字节比较。 */
    private static int compareVarChar(FieldVector vecA, int rowA,
                                       FieldVector vecB, int rowB) {
        ArrowBuf offA = vecA.getOffsetBuffer();
        ArrowBuf offB = vecB.getOffsetBuffer();
        ArrowBuf dataA = vecA.getDataBuffer();
        ArrowBuf dataB = vecB.getDataBuffer();

        int startA = offA.getInt((long) rowA * 4);
        int endA = offA.getInt((long) (rowA + 1) * 4);
        int startB = offB.getInt((long) rowB * 4);
        int endB = offB.getInt((long) (rowB + 1) * 4);
        int lenA = endA - startA;
        int lenB = endB - startB;
        int minLen = Math.min(lenA, lenB);

        for (int i = 0; i < minLen; i++) {
            int cmp = (dataA.getByte(startA + i) & 0xFF) - (dataB.getByte(startB + i) & 0xFF);
            if (cmp != 0) return cmp;
        }
        return lenA - lenB;
    }

    /** 零分配 FixedSizeList（ARRAY/向量）比较：child 是 Float4Vector 时直接读数据 buffer 逐位比较。 */
    private static int compareFixedSizeList(FixedSizeListVector vecA, int rowA,
                                            FixedSizeListVector vecB, int rowB) {
        ArrowType.FixedSizeList fl = (ArrowType.FixedSizeList) vecA.getField().getType();
        int n = fl.getListSize();
        FieldVector childA = vecA.getDataVector();
        FieldVector childB = vecB.getDataVector();
        long baseA = (long) rowA * n, baseB = (long) rowB * n;
        if (childA instanceof Float4Vector a && childB instanceof Float4Vector b) {
            ArrowBuf da = a.getDataBuffer(), db = b.getDataBuffer();
            for (int i = 0; i < n; i++) {
                int cmp = Float.compare(da.getFloat((baseA + i) * 4), db.getFloat((baseB + i) * 4));
                if (cmp != 0) return cmp;
            }
            return 0;
        }
        // 其它元素类型（INT/DOUBLE...）逐元素 getObject 比较
        for (int i = 0; i < n; i++) {
            Object va = childA.getObject((int) (baseA + i)), vb = childB.getObject((int) (baseB + i));
            if (va == null && vb == null) continue;
            if (va == null) return -1;
            if (vb == null) return 1;
            @SuppressWarnings({"rawtypes", "unchecked"})
            int cmp = ((Comparable) va).compareTo(vb);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    /** Fallback：Decimal / FixedSizeList(ARRAY,向量) / List / Map 等复杂类型用 getObject。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int fallbackCompare(FieldVector vecA, int rowA,
                                        FieldVector vecB, int rowB) {
        Object a = vecA.getObject(rowA);
        Object b = vecB.getObject(rowB);
        if (a instanceof List<?> la && b instanceof List<?> lb) {
            return compareLists(la, lb);
        }
        if (a instanceof Comparable ca && b instanceof Comparable) {
            return ca.compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }

    /**
     * 逐元素比较两个 List（ARRAY / 向量 / LIST 键）：先比元素个数，再逐位比较（null 元素最小）。
     * <p>向量列排序语义：每一位 float 按 {@link Float#compare} 比较，全相等则相等 —— 对齐 pgvector
     * 对 vector 的"数组逐位排序"，替换原来 {@code toString().compareTo()} 的字符串序（数值序错乱）。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static int compareLists(List<?> a, List<?> b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        int len = Math.min(a.size(), b.size());
        for (int i = 0; i < len; i++) {
            Object va = a.get(i), vb = b.get(i);
            if (va == null && vb == null) continue;
            if (va == null) return -1;
            if (vb == null) return 1;
            if (va instanceof List<?> la && vb instanceof List<?> lb) {
                int cmp = compareLists(la, lb);          // 嵌套 List（LIST<LIST>）递归
                if (cmp != 0) return cmp;
            } else if (va instanceof Comparable ca && vb instanceof Comparable) {
                int cmp = ca.compareTo(vb);
                if (cmp != 0) return cmp;
            } else {
                int cmp = va.toString().compareTo(vb.toString());
                if (cmp != 0) return cmp;
            }
        }
        return Integer.compare(a.size(), b.size());
    }
}
