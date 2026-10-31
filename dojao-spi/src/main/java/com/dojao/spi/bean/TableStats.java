package com.dojao.spi.bean;

import java.util.List;

/**
 * 表统计信息 —— 供 Master 查询优化（CBO）与<b>并行度决策</b>使用。
 *
 * <p>{@code splitCount} = <b>连接器自己的分区策略</b>在不设并行度上限时能切出的天然分区数
 * （{@code -1} = 未知 / 无天然上限）。这是「分区规划下推到连接器做第一次规划」的载体：
 * 文件分片型数据源（Hive 一个文件一片）能直接报出文件数，引擎据此按
 * {@code Min(MaxPara, Max(ScanA, ScanB, ...))} 决定最终并行度 —— 于是「8 个 87MB 文件」
 * 不再因「总量 0.82G ÷ 1G 槽位 = 1」被压成单线程串扫。</p>
 *
 * <p>JDBC 系连接器（MySQL / PostgreSQL：主键范围 / ctid / LIMIT-OFFSET 兜底，切几片由引擎要多少
 * 决定）没有天然分片数，保持 {@code -1} —— 此时引擎退回「体积 / 行数 ÷ 槽位」公式估算。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/11
 */
public record TableStats(
    String catalog,
    String schema,
    String table,
    long rowCount,
    long byteSize,              // 数据文件 / 表占用空间（字节），-1 表示未知
    List<ColumnStats> columnStats,
    long splitCount             // 天然分区数（文件分片型数据源 = 文件数），-1 表示未知
) {

    /**
     * 不报告天然分区数的连接器（JDBC 系：拆几片由引擎定）走这里 —— {@code splitCount = -1}，
     * 并行度退回「体积 / 行数 ÷ 槽位」公式。
     */
    public TableStats(String catalog, String schema, String table,
                      long rowCount, long byteSize, List<ColumnStats> columnStats) {
        this(catalog, schema, table, rowCount, byteSize, columnStats, -1L);
    }
}
