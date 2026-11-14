package com.dojao.core.function.aggregate;

import org.apache.arrow.vector.util.Text;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * {@code collect_list} / {@code collect_set} 的状态值规范化与去重工具。
 *
 * <p><b>为什么需要规范化</b>：同一个值在内存路径来自 {@code FieldVector.getObject}，在溢写回读路径
 * 来自 Arrow List 列的子向量，两次的 Java 表示必须一致，否则 {@code collect_set} 会把「同一值」
 * 当成两个（{@code Text} vs {@code String}、{@code Integer} vs {@code Long}），合并/溢写后出现重复。</p>
 *
 * <ul>
 *   <li>整数族 → {@code Long}，浮点族 → {@code Double}（同 JOIN 键、MIN/MAX 的口径）；</li>
 *   <li>{@link Text} → {@code String}；</li>
 *   <li>{@code byte[]} → {@link ByteBuffer}：{@code byte[]} 的 equals 是引用比较，无法去重；
 *       ByteBuffer 的 equals/hashCode 按剩余字节内容比较。写回 Arrow 前由 {@link #export} 拆回
 *       {@code byte[]}（{@code VarBinaryVector} 只认 byte[]）。</li>
 *   <li>其余（BigDecimal / LocalDateTime / Boolean / Duration / List…）原样保留。</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/11/14
 */
public final class CollectSupport {

    private CollectSupport() {
    }

    /** 规范化一个原始取值，供状态存储与去重使用。 */
    public static Object normalize(Object v) {
        if (v == null) return null;
        if (v instanceof Text t) return t.toString();
        if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long) {
            return ((Number) v).longValue();
        }
        if (v instanceof Float || v instanceof Double) {
            return ((Number) v).doubleValue();
        }
        if (v instanceof byte[] b) return ByteBuffer.wrap(b);
        return v;
    }

    /** 写回 Arrow 向量时使用：{@link ByteBuffer}（byte[] 去重键）拆回 {@code byte[]}。 */
    public static Object export(Object v) {
        if (v instanceof ByteBuffer buf) {
            ByteBuffer dup = buf.duplicate();
            byte[] out = new byte[dup.remaining()];
            dup.get(out);
            return out;
        }
        return v;
    }

    /** 从溢写读回的 List 值重建状态（{@code collect_set} → LinkedHashSet，否则 → ArrayList）。 */
    public static Object fromValues(String functionName, Collection<?> values) {
        if ("collect_set".equals(functionName)) {
            LinkedHashSet<Object> set = new LinkedHashSet<>();
            if (values != null) {
                for (Object v : values) {
                    Object n = normalize(v);
                    if (n != null) set.add(n);
                }
            }
            return set;
        }
        ArrayList<Object> list = new ArrayList<>(values == null ? 0 : values.size());
        if (values != null) {
            for (Object v : values) {
                Object n = normalize(v);
                if (n != null) list.add(n);
            }
        }
        return list;
    }
}
