package com.dojao.core.function.scalar;

import com.dojao.spi.function.ScalarFunction;

/**
 * IF 标量函数 —— {@code IF(cond, a, b)}：cond 为真返回 a，否则返回 b。
 *
 * <p>与 {@code CASE WHEN cond THEN a ELSE b END} 语义等价。条件支持三类输入（由
 * {@link ScalarEvaluator} 预处理）：布尔列 / 布尔字面量（直接判真）、比较参数
 * {@code col op literal}（逐行求值成 Boolean）、数值（非零为真）。结果为任意类型
 * （数值 / 字符串 / 布尔），输出列类型由 {@link ScalarEvaluator} 按首个非空结果推断。</p>
 *
 * @author jiaxin
 * @date 2026/8/22
 */
public final class IfFunction implements ScalarFunction {

    public static final IfFunction INSTANCE = new IfFunction();

    private IfFunction() {
    }

    @Override
    public String functionName() {
        return "if";
    }

    @Override
    public String display() {
        return "IF(cond, a, b)：cond 真返回 a，否则 b（cond 可为 col op 字面量比较）";
    }

    @Override
    public Object evaluate(Object[] inputs) {
        return truthy(inputs[0]) ? inputs[1] : inputs[2];
    }

    /** 真值判定：null → 假；布尔 → 值；数值 → 非零为真；其余（非空字符串等）→ 真。 */
    private static boolean truthy(Object o) {
        return switch (o) {
            case null -> false;
            case Boolean b -> b;
            case Number n -> n.doubleValue() != 0;
            default -> true;
        };
    }
}
