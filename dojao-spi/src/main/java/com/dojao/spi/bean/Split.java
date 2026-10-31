package com.dojao.spi.bean;

/**
 * 一个扫描切片（split）——「整表扫描」被切成若干互不重叠、合起来恰好覆盖全表的数据段，
 * 每一段交给一个 Worker 任务处理。
 *
 * <p><b>为什么需要它。</b>并行扫描有两种规划方式：</p>
 * <ol>
 *   <li><b>谓词表达</b>——切片只靠 {@link ReadRequest#filterExpression()} 描述（如
 *       {@code "id >= 100 AND id < 200"}、{@code "ctid >= '(0,1)' AND ctid < '(0,1000)'"}）。
 *       内置 MySQL / PostgreSQL 走这条路，对 JDBC 数据源天然成立。</li>
 *   <li><b>不透明载荷</b>——切片无法用 SQL 谓词表达，只能由连接器自己在规划期算出、读的时候
 *       再交还给自己（如 Hive：一个分区的若干 HDFS 文件路径、ORC 的 stripe 边界）。
 *       JVM 之间只能传字节，所以统一装进 {@link #payload() byte[]}。</li>
 * </ol>
 *
 * <p>两种方式对本 SPI 是同一个抽象：<b>Split 是连接器的私事</b>，引擎只负责规划期拿到一个
 * 列表（{@code planSplits}）、派发时原样搬运（{@code split_payload}）、读取时原样交还
 * （{@code openSplit}），既不解析也不理解里面是什么。默认实现把切片编码成 {@link ReadRequest}
 * 的字节形式（见 {@code SplitPayload}），于是走方式 1 的连接器零改动。</p>
 *
 * <p>{@link #WHOLE} 表示「不切分，整表一个片」——{@code payload} 为 {@code null}，读取时直接用
 * 调用方传入的原 {@link ReadRequest}。语法上没有分区的数据源（小表、分页查询、子查询派生）
 * 都用它。</p>
 *
 * @param payload 连接器私有字节；{@code null} = 整表不分片（见 {@link #WHOLE}）
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/20
 */
public record Split(byte[] payload) {

    /** 整表不分片 —— 读取时直接使用原 {@link ReadRequest}，不叠加任何切片信息。 */
    public static final Split WHOLE = new Split(null);

    /** 是否携带连接器私有载荷（{@code false} = 整表扫描）。 */
    public boolean hasPayload() {
        return payload != null && payload.length > 0;
    }

    /** 构造一个带载荷的切片。 */
    public static Split of(byte[] payload) {
        return payload == null || payload.length == 0 ? WHOLE : new Split(payload);
    }
}
