package com.dojao.core.function.scalar;

import com.dojao.spi.function.ScalarFunction;

/**
 * {@code inner_product(a, b)} —— 内积 Σ(aᵢ·bᵢ)。
 *
 * <p>a / b 为向量列（FixedSizeList&lt;Float32&gt;）或 {@code '[v1,v2,...]'} 字面量；
 * 任一参数为 NULL 时返回 NULL。维度不一致报错。</p>
 *
 * @author jiaxin
 * @date 2026/8/9
 */
public final class InnerProductFunction implements ScalarFunction {

    public static final InnerProductFunction INSTANCE = new InnerProductFunction();

    @Override public String functionName() { return "inner_product"; }

    @Override public String display() { return "内积 Σ(aᵢ·bᵢ)"; }

    @Override public Object evaluate(Object[] inputs) {
        float[] a = VectorArg.toVector(inputs, 0);
        float[] b = VectorArg.toVector(inputs, 1);
        if (a == null || b == null) return null;
        VectorArg.checkDim(a, b);

        double dot = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
        }
        return dot;
    }
}
