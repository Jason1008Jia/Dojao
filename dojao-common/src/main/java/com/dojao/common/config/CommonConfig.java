package com.dojao.common.config;

/**
 * Dojao 全局公共配置。
 * 集中管理所有可由用户自定义的配置项。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/11
 */
public final class CommonConfig {

    private CommonConfig() {}

    /** JDBC / ORC → Arrow 转换时每批次读取的行数。 */
    public static final int DEFAULT_BATCH_SIZE = 10000;

    /**
     * JDBC 写批次行数：一次 {@code executeBatch} 折叠成的多行 INSERT 的行数上限。
     * 默认 3 万行（当前压测值）；生产环境可按需调大（-DJDBCWriteBatchSize=50000），行数
     * 越大单条多行 INSERT 越大，MySQL 解析成本越高，需结合 max_allowed_packet 权衡。
     */
    public static final int JDBC_WRITE_BATCH_SIZE = Integer.getInteger("JDBCWriteBatchSize", 30000);

    // ==================== 并行度 ====================

    /**
     * 传统关系型数据库（MySQL / PostgreSQL / SQL Server 等）的默认并行读取线程数。
     * 事务型数据库对并发查询敏感，默认保守值为 4。
     * 各 Connector 可根据自身情况（表大小、索引策略）动态调整，此值为保底下限。
     */
    public static final int JDBC_DEFAULT_PARALLELISM = 4;

    /**
     * 分析型 OLAP 引擎（Hive / ClickHouse / Doris / Iceberg / Hudi 等）的默认并行读取线程数。
     * OLAP 引擎本身为大规模扫描设计，可以承受更高的并发度，默认值为 8。
     * 各 Connector 可根据集群负载和数据分区数动态调整。
     */
    public static final int OLAP_DEFAULT_PARALLELISM = 8;

    /** 小于此行数不启用并行（走单线程）。 */
    public static final long PARALLEL_SMALL_TABLE_THRESHOLD = 10_000L;

    // ==================== 内存预算 ====================

    /** 开窗排序每个分区的排序缓冲区内存预算（字节）。默认 512MB。资源充裕可调大。 */
    public static final long WINDOW_SORT_BATCH_BYTES = 512L * 1024 * 1024;

    /** Hash 聚合算子的内存上限（字节）。默认 256MB。 */
    public static final long AGGREGATION_MEMORY_LIMIT_BYTES = 256L * 1024 * 1024;

    /** Hash Join 算子的内存上限（字节）。默认 256MB。 */
    public static final long JOIN_MEMORY_LIMIT_BYTES = 256L * 1024 * 1024;

    /**
     * 算子内存预算（字节）：各溢写算子（WindowSort / Aggregation / Join 等）共用，
     * {@code AbstractSpillableOperator} 在预算 ×0.8 时触发溢写。
     * 默认 2GB，可 {@code -Ddojao.operatorMemoryMb=N}（单位 MB）覆盖。
     */
    public static final long OPERATOR_MEMORY_LIMIT_BYTES =
            Long.getLong("dojao.operatorMemoryMb", 2048) * 1024L * 1024L;
}
