package com.dojao.core.operator;

import java.util.List;

/**
 * 可解释算子 —— 暴露 EXPLAIN 需要、但不出现在节点显示名里的内部结构。
 *
 * <p>典型用途：分布式算子对 Master 侧只是一个叶子节点（如
 * {@code DistributedJoin[3 joins, ...]} / {@code DistributedScan[...]}），其内部
 * 「扫哪张表 / 每层 join 用什么算法与连接键 / 聚合按什么键分组」只有算子自己知道。
 * EXPLAIN 打印器会把 {@link #explainLines()} 逐条渲染在节点行下方。</p>
 *
 * <p>约定：只读、纯内存、不得触发任何 IO（EXPLAIN 对数据源零副作用）。返回空列表 = 无附加信息。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/17
 */
public interface Explainable {

    /** 节点内部结构说明（每行一条，打印器统一加缩进与缩进线）。 */
    List<String> explainLines();
}
