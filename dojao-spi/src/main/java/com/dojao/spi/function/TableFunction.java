package com.dojao.spi.function;

import java.util.Iterator;

/**
 * 表函数接口 —— 单行输入 → 多行输出（1→N）。
 *
 * <h3>典型场景</h3>
 * <ul>
 *   <li>explode(Array) → 多行（每个元素一行）</li>
 *   <li>posexplode(Array) → 多行（位置 + 元素）</li>
 *   <li>split_to_rows(s, delimiter) → 多行（每个 token 一行）</li>
 *   <li>json_each(json) → 多行（每个 key-value 一行）</li>
 * </ul>
 *
 * <h3>与 {@link ScalarFunction} 的区别</h3>
 * <p>ScalarFunction：1 行 in → 1 行 out（横向扩展列）。</p>
 * <p>TableFunction：1 行 in → N 行 out（纵向扩展行），输出时需复制其余列。</p>
 *
 * @author jiaxin
 * @since 2.0
 */
public interface TableFunction extends Function {

    /** 对单行输入求值，返回多行结果的迭代器 */
    Iterator<Object[]> evaluate(Object[] inputs);
}
