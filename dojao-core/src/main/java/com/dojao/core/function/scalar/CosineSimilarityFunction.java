package com.dojao.core.function.scalar;

import com.dojao.spi.function.ScalarFunction;

/**
 * {@code cosine_similarity(a, b)} —— 余弦相似度 (a·b)/(|a|·|b|)，值域 [-1, 1]。
 *
 * <p>a / b 为向量列（FixedSizeList&lt;Float32&gt;）或 {@code '[v1,v2,...]'} 字面量；
 * 任一参数为 NULL 或为零向量时返回 NULL（余弦无定义）。维度不一致报错。</p>
 *
 * @author jiaxin
 * @date 2026/8/9
 */
public final class CosineSimilarityFunction implements ScalarFunction {

    public static final CosineSimilarityFunction INSTANCE = new CosineSimilarityFunction();

    @Override public String functionName() { return "cosine_similarity"; }

    @Override public String display() { return "余弦相似度 a·b/(|a|·|b|)，值域 [-1, 1]"; }

    @Override public Object evaluate(Object[] inputs) {
        float[] a = VectorArg.toVector(inputs, 0);
        float[] b = VectorArg.toVector(inputs, 1);
        if (a == null || b == null) return null;
        VectorArg.checkDim(a, b);

        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.length; i++) {
            double av = a[i], bv = b[i];
            dot += av * bv;
            na += av * av;
            nb += bv * bv;
        }
        if (na == 0 || nb == 0) return null;   // 零向量 → 余弦无定义
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }
}
