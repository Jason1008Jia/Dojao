package com.dojao.core.function.scalar;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 向量参数解析工具 —— 把标量函数入参统一转成 {@code float[]}。
 *
 * <p>入参两种形态：</p>
 * <ul>
 *   <li>列引用：FixedSizeList&lt;Float32&gt; 的 {@code getObject(row)} 返回 {@code List<Float>}</li>
 *   <li>字面量：pgvector 风格 {@code '[v1,v2,...]'} 文本</li>
 * </ul>
 *
 * <p>字面量文本带缓存（线程安全），避免逐行重复解析。</p>
 *
 * @author jiaxin
 * @date 2026/8/9
 */
final class VectorArg {

    /** 字面量向量文本 → float[] 缓存（不同字面量数量极少） */
    private static final ConcurrentHashMap<String, float[]> LITERAL_CACHE = new ConcurrentHashMap<>();

    private VectorArg() {}

    /** 取出第 idx 个参数并转为 float[]；参数为 NULL 时返回 null。 */
    static float[] toVector(Object[] inputs, int idx) {
        if (idx >= inputs.length) return null;
        Object o = inputs[idx];
        if (o == null) return null;
        if (o instanceof List<?> list) {
            float[] v = new float[list.size()];
            for (int i = 0; i < v.length; i++) {
                v[i] = ((Number) list.get(i)).floatValue();
            }
            return v;
        }
        if (o instanceof float[] fa) return fa;
        if (o instanceof String s) {
            return LITERAL_CACHE.computeIfAbsent(s, VectorArg::parseVectorText);
        }
        throw new IllegalArgumentException("向量函数入参类型不支持: " + o.getClass().getSimpleName());
    }

    /** 两个向量维度必须一致，否则报错（防静默截断算错相似度）。 */
    static void checkDim(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("向量维度不一致: a=" + a.length + ", b=" + b.length);
        }
    }

    /** 解析 '[v1,v2,...]' 文本为 float[]（容忍无方括号）。 */
    private static float[] parseVectorText(String s) {
        String t = s.trim();
        if (t.startsWith("[") && t.endsWith("]")) {
            t = t.substring(1, t.length() - 1).trim();
        }
        if (t.isEmpty()) return new float[0];
        String[] parts = t.split(",");
        float[] v = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            v[i] = Float.parseFloat(parts[i].trim());
        }
        return v;
    }
}
