package com.dojao.spi.function;

/**
 * 标量函数接口 —— 单行输入 → 单行输出（1→1）。
 *
 * <h3>典型场景</h3>
 * <ul>
 *   <li>数学运算：abs(x), round(x, 2), ceil(x)</li>
 *   <li>字符串处理：upper(s), concat(a, b), substr(s, 1, 3)</li>
 *   <li>日期转换：date_format(d, 'yyyy-MM-dd'), year(d)</li>
 *   <li>条件判断：if(cond, a, b), coalesce(a, b, c)</li>
 * </ul>
 *
 * @author jiaxin
 * @since 2.0
 */
public interface ScalarFunction extends Function {

    /** 对单行输入求值，返回标量结果 */
    Object evaluate(Object[] inputs);
}
