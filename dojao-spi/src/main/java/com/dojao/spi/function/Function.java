package com.dojao.spi.function;

/**
 * Dojao 函数体系顶层接口 —— 所有函数的公共父类型。
 *
 * <h3>函数分类体系</h3>
 * <pre>
 *   Function
 *   ├── AggregateFunction      (聚合函数，GROUP BY 归约)
 *   │     └── CollectAggregateFunction  (Arrow 原生聚合形态：框架攒值，实现方只写 evaluate)
 *   ├── WindowFunction         (窗口函数，OVER 窗口)
 *   ├── ScalarFunction        (标量函数 1→1)
 *   └── TableFunction          (表函数 1→N)
 * </pre>
 *
 * <p>无论系统内置函数还是用户自定义函数，都在这四类体系下实现对应接口，
 * 不再区分 UDF / UDAF / UDTF / UDWF 等别名——一个函数要么是标量、要么是聚合、
 * 要么是表函数、要么是窗口函数。</p>
 *
 * <p>聚合分两种执行形态：<b>增量四阶段</b>（{@link AggregateFunction}，createState/accumulate/
 * merge/evaluate，状态在实现方）与 <b>collect-evaluate</b>（{@link CollectAggregateFunction}，
 * Arrow 原生传输、状态全引擎侧、实现方只写最后一步，主力推广形态）。</p>
 *
 * @author jiaxin
 * @since 2.0
 */
public interface Function {

    /** 函数名（SQL 中引用的名称），全局唯一 */
    String functionName();

    /**
     * 函数功能描述 —— 说明这个函数是做什么的，供函数管理 / 文档 / CLI 展示。
     * 例如 {@code "余弦相似度 a·b/(|a|·|b|)，值域 [-1, 1]"}。
     */
    String display();
}
