package com.dojao.core.function.scalar;

import com.dojao.spi.function.ScalarFunction;

/**
 * {@code l2_distance(a, b)} —— 欧氏距离 √Σ(aᵢ−bᵢ)²。
 *
 * <p>a / b 为向量列（FixedSizeList&lt;Float32&gt;）或 {@code '[v1,v2,...]'} 字面量；
 * 任一参数为 NULL 时返回 NULL。维度不一致报错。</p>
 *
 * @author jiaxin
 * @date 2026/8/9
 */
public final class L2DistanceFunction implements ScalarFunction {

    public static final L2DistanceFunction INSTANCE = new L2DistanceFunction();

    @Override public String functionName() { return "l2_distance"; }

    @Override public String display() { return "欧氏距离 √Σ(aᵢ−bᵢ)²，非负值"; }

    @Override public Object evaluate(Object[] inputs) {
        float[] a = VectorArg.toVector(inputs, 0);
        float[] b = VectorArg.toVector(inputs, 1);
        if (a == null || b == null) return null;
        VectorArg.checkDim(a, b);

        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            sum += d * d;
        }
        return Math.sqrt(sum);
    }
}
