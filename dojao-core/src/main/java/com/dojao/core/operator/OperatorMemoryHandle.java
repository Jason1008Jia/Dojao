package com.dojao.core.operator;

/**
 * 可溢写算子的内存句柄 —— 供上层（{@code QueryMemoryManager} /
 * {@code WorkerMemoryManager}）统一管理内存与溢写。
 *
 * <h3>设计原则</h3>
 * <ul>
 *   <li>算子自己最清楚如何释放内存 —— 句柄只发出"请释放 targetBytes"的请求，
 *       具体释放策略由算子内部决定（哪个哈希表、哪个缓冲区、哪个分区）。</li>
 *   <li>{@link #setMemoryBudget(long)} 支持动态增减，算子会根据新预算调整
 *       溢写阈值和缓冲策略。</li>
 *   <li>所有方法都是线程安全的（算子在后台溢写时，上层可能并发查询状态）。</li>
 * </ul>
 *
 * <h3>实现类</h3>
 * <ul>
 *   <li>{@link HashAggregationOperator} —— 哈希表溢写（所有 groups 全量 dump）</li>
 *   <li>{@link WindowSortOperator} —— 排序缓冲区溢写（选择最大分区触发 sort+spill）</li>
 *   <li>{@link HashJoinOperator} —— build 侧哈希表溢写（子分区 + 递归）</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/19
 */
public interface OperatorMemoryHandle {

    // ==================== 内存查询 ====================

    /** 算子当前实际使用的内存字节数（Arrow 堆外向量的 buffer 总和 + Java 堆关键结构）。 */
    long getCurrentMemoryBytes();

    /** 算子自启动以来累计溢写到磁盘的总字节数。 */
    long getSpilledBytes();

    /** 算子当前的内存预算（字节）。溢写阈值、缓冲区大小等受此约束。 */
    long getMemoryBudget();

    // ==================== 内存控制 ====================

    /**
     * 动态调整内存预算（字节）。上层在以下场景调用：
     * <ul>
     *   <li>Query 总预算变化时，重新分配各算子预算</li>
     *   <li>其他 Query 释放内存后，将富余资源分配给活跃算子</li>
     *   <li>集群内存紧张时，压缩某些算子的预算</li>
     * </ul>
     * 预算调小时，算子应主动触发射序／溢写以满足新预算。
     */
    void setMemoryBudget(long newBudget);

    /**
     * 请求算子释放至少 {@code targetBytes} 字节内存。
     *
     * <p>算子根据自身状态选择最佳溢写策略：
     * <ul>
     *   <li>哈希聚合：将当前哈希表全部溢写（{@link AggregationHashTable#spillAll()}）</li>
     *   <li>窗口排序：选择累积最多的一个/多个分区，排序并溢写其缓冲区</li>
     *   <li>哈希连接：选择 build 侧最大的子分区溢写</li>
     * </ul>
     *
     * <p>可能释放超过 {@code targetBytes}（全量溢写不可拆分），
     * 也可能释放不足（没有更多可溢写数据）。
     *
     * @param targetBytes 期望释放的最小字节数，0 表示"能溢写多少就溢写多少"
     * @return 溢写结果，包含实际释放字节数和完整性标志
     */
    SpillResult spill(long targetBytes);

    // ==================== 状态 ====================

    /** 算子是否正在执行溢写操作（后台线程中）。 */
    boolean isSpilling();
}
