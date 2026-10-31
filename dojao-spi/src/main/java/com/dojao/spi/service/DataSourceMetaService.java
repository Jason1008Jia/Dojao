package com.dojao.spi.service;

import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.ColumnStats;
import com.dojao.spi.bean.Schema;
import com.dojao.spi.bean.Table;
import com.dojao.spi.bean.TableStats;

import java.util.List;
import java.util.Optional;

/**
 * <b>数据源元数据服务</b> —— 面向「目标数据源」的元数据查询与统计探查接口。
 *
 * <p>与 {@link DojaoMetaService} 的分工：本接口问的是<b>连接器背后的数据源</b>
 * （MySQL / PostgreSQL / Hive ……）里有什么、有多大；{@link DojaoMetaService} 问的是
 * <b>Dojao 自身</b>注册过哪些 catalog、落库过哪些函数。两者互不相干。</p>
 *
 * <p>两类能力：</p>
 * <ul>
 *   <li><b>元数据查询</b>（catalog / schema / table / column 的存在性、清单、结构）
 *       —— 供 SQL 语义校验、列裁剪、CTAS 类型推导使用；</li>
 *   <li><b>统计探查</b>（{@link #getTableStats} / {@link #getColumnStats} / {@link #estimateRows}）
 *       —— 供框架估算并行度 / CBO 决定分区数使用。</li>
 * </ul>
 *
 * <p>所有数据源探查实现（{@link DojaoConnector} 及其各连接器）都实现本接口，框架只调本接口、
 * 不关心具体实现。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27 22:08
 */
public interface DataSourceMetaService {

    // ========== Catalog 级别 ==========

    /**
     * 检查 Catalog 是否存在
     */
    boolean catalogExists(String catalog);

    /**
     * 获取 Catalog 信息
     */
    Optional<Catalog> getCatalog(String catalog);

    /**
     * 列出所有 Catalog
     */
    List<String> listCatalogs();

    // ========== Schema 级别 ==========

    /**
     * 检查 Schema 是否存在
     */
    boolean schemaExists(String catalog, String schema);

    /**
     * 获取 Schema 信息
     */
    Optional<Schema> getSchema(String catalog, String schema);

    /**
     * 列出某个 Catalog 下的所有 Schema
     */
    List<String> listSchemas(String catalog);

    // ========== Table 级别 ==========

    /**
     * 检查 Table 是否存在
     */
    boolean tableExists(String catalog, String schema, String table);

    /**
     * 获取 Table 信息
     */
    Optional<Table> getTable(String catalog, String schema, String table);

    /**
     * 获取表的列信息
     */
    List<Column> getTableColumns(String catalog, String schema, String table);

    /**
     * 列出某个 Schema 下的所有 Table
     */
    List<String> listTables(String catalog, String schema);

    /**
     * 表的分区列名清单（保序，与数据源分区定义一致）。<b>非分区表 / 不支持分区探查的连接器返回空列表。</b>
     *
     * <p><b>这是分区裁剪的连接器契约。</b>一个方法同时回答两问：<b>是不是分区表</b>（返回非空即分区表）
     * 与<b>分区字段有哪些</b>（清单本身）。</p>
     *
     * <p><b>谁来做裁剪</b>：{@code com.dojao.common.util.PartitionPruner} 是引擎侧的通用能力，
     * <b>任何有分区概念、能列举分区的数据源都能接，不需要改 SPI / 协议</b>。连接器在自己的
     * {@link DojaoConnector#planSplits(com.dojao.spi.bean.ReadRequest, int)} 里做三步：
     * ① {@code extract(request.filterExpression(), 分区列+声明类型)} 解析谓词 →
     * ② {@code prune(分区清单, 取值器, 约束)} 过滤 →
     * ③ <b>只对保留下来的分区列举文件 / 开 reader</b>。第 ③ 步必须落在「列举扫描单元之前」，
     * 落到读数据时才过滤就只是白读一遍，省不下扫描量。范围谓词按<b>声明类型</b>比（数值列按数值、
     * {@code date}/{@code timestamp} 按时间），所以本方法虽只回名字，连接器裁剪时要自带声明类型；
     * 拿不准一律不剪（多扫只是慢，剪错就是丢数据）。</p>
     *
     * <p><b>默认空 ▸ 覆盖时机</b>：文件分片型数据源（Hive 等）覆盖为返回 HMS / 目录元数据里的分区列；
     * JDBC 系（MySQL / PostgreSQL）保持默认空 —— 它们把谓词编进发往数据库的 SQL，由数据库自己裁剪
     * 原生分区，引擎侧不需要再剪。</p>
     *
     * <p>返回空只表示「本连接器不提供分区列信息」，<b>不影响正确性</b>：没有裁剪最多是多扫几个分区。</p>
     *
     * @return 分区列名（保序）；非分区表 / 不支持返回空列表
     */
    default List<String> getPartitionColumns(String catalog, String schema, String table) {
        return List.of();
    }

    // ========== Column 级别 ==========

    /**
     * 检查列是否存在
     */
    boolean columnExists(String catalog, String schema, String table, String column);

    /**
     * 获取列信息
     */
    Optional<Column> getColumn(String catalog, String schema, String table, String column);

    // ========== 统计探查（并行度 / CBO） ==========

    /**
     * 探查表的数据量与体积（行数 + 字节数），供框架做并行度估算 / CBO。
     *
     * <p><b>这是元数据探查的抽象接口</b>：每个连接器按自己数据源的探查方式实现，框架只调本接口、
     * 不关心具体实现 —— MySQL 查 information_schema.TABLES、PostgreSQL 查 pg_catalog、
     * Hive 对接 MetaStore 服务、SQL Server 查系统目录视图，各不相同。</p>
     *
     * <p><b>契约（所有连接器实现必须遵守）</b>：</p>
     * <ul>
     *   <li>{@link TableStats#rowCount()} / {@link TableStats#byteSize()} 是<b>估算值</b>
     *       （元数据快照，非精确 COUNT/精确体积），框架不得当精确值用；</li>
     *   <li>无法探查（表不存在 / 权限不足 / 数据源不支持）时返回 <b>{@code -1}</b> 表示未知，
     *       框架据此回退默认并行度，不得返回 0（0 会被当成"空表"）；</li>
     *   <li>{@code byteSize} 应尽量反映物理存储总量（数据 + 索引 / 文件总字节）；</li>
     *   <li>{@code splitCount} 是<b>本连接器分区策略的天然分区数</b>（文件分片型数据源 = 文件数），
     *       供引擎按 {@code Min(MaxPara, Max(ScanA, ScanB, ...))} 决定并行度；没有天然分片数的
     *       连接器（JDBC 系：拆几片由引擎要多少决定）留 {@code -1}，引擎退回槽位公式。
     *       <b>该值只影响并行度，不影响正确性</b> —— 报少了只是慢，报多了引擎仍受集群容量与
     *       每 query 上限封顶；</li>
     *   <li>元数据探查失败允许抛异常，由调用方决定 fail-fast 或回退默认。</li>
     * </ul>
     */
    TableStats getTableStats(String catalog, String schema, String table);

    /**
     * 探查各列的统计信息（NDV / 空值率 / 最值等），供 CBO 估算选择率。
     */
    List<ColumnStats> getColumnStats(String catalog, String schema, String table);

    /**
     * <b>带过滤条件的</b>行数估算 —— {@link #getTableStats} 的「谓词感知」版，供监控的 Scan 进度条
     * 取<b>分母</b>：下推谓词把整表百万行压到个位数时，分母须跟着变小，否则进度条永远停在 0。
     *
     * <p>默认实现<b>忽略过滤条件</b>，退化为整表 {@code rowCount}（元数据探查）—— 对无估算能力的
     * 数据源，进度条分母退化为整表量（语义正确、只是粒度粗）。有估算能力的连接器应覆写：
     * MySQL 对 {@code EXPLAIN SELECT * FROM t WHERE filter} 取 {@code rows × filtered%}、
     * PostgreSQL 走 {@code EXPLAIN}、Hive 走 MetaStore 分区统计等。</p>
     *
     * @param filter 已下推的过滤谓词（连接器方言 SQL 表达式；{@code null}/空 = 无过滤 → 整表估计）
     * @return 过滤后行数估计；无法估算返回 <b>{@code -1}</b>（未知，调用方退化为整表估计或按任务数算进度）
     */
    default long estimateRows(String catalog, String schema, String table, String filter) {
        try {
            return getTableStats(catalog, schema, table).rowCount();
        } catch (Exception e) {
            return -1L;
        }
    }

    /**
     * <b>带过滤条件的字节估算</b> —— {@link #estimateRows} 的「字节版」，供 join 算法选型
     * （{@code JoinDecider}）测<b>过滤后</b>的单侧体量。
     *
     * <p><b>为什么需要它</b>：选型的判据是体量而非行数，而「按行数比例缩小体量」这条路
     * （{@code byteSize × filteredRows/rowCount}）<b>要求连接器同时报得出过滤前后的行数</b>。
     * 只要行数缺失（Hive 分区表未做 {@code ANALYZE} 就没有 {@code numRows}，
     * 但 {@code totalSize} 常常在），比例就算不出来 → 体量按整表算 → 「只读 1/30 分区」的查询
     * 被误判成大表，进而选到与预期不符的算法。字节本身能按分区裁剪求和，比行数比例更可靠。</p>
     *
     * <p>默认实现返回 {@code -1}（未知），调用方退回「整表 {@code byteSize} × 行数比例」的既有口径。</p>
     *
     * @param filter 已下推的过滤谓词（{@code null}/空 = 无过滤 → 整表估计）
     * @return 过滤后字节估计；无法估算返回 <b>{@code -1}</b>
     */
    default long estimateBytes(String catalog, String schema, String table, String filter) {
        return -1L;
    }

    /**
     * <b>解压膨胀系数</b> —— {@link #getTableStats} 报出的 {@code byteSize} 换算到「join build 侧驻留内存」
     * 时的乘数。
     *
     * <p>为什么需要它：{@code byteSize} 是<b>物理存储量</b>（对文件型数据源 = 压缩后的文件长度），而 join 的
     * build 侧在内存里是<b>解压后的</b> Arrow 列式向量，两者不是一个量。压缩比高的数据源（ORC / Parquet +
     * SNAPPY 常达 3~5 倍）若不换算，引擎会按「压缩后」的小体积把大表误判成中表，进而在运行时撞内存上限
     * （{@code HashJoinOperator} 超预算直接 fail-fast，不静默降级）。</p>
     *
     * <p><b>默认 {@code 1.0}</b>（不膨胀）：JDBC 系（MySQL / PostgreSQL）报的是数据库内部页/数据文件长度，
     * 与解压后内存量级接近，保持默认即可。文件型数据源（Hive 等）应覆写为实测系数。</p>
     *
     * <p>该系数<b>只影响 join 算法选型</b>（见 {@code com.dojao.core.resource.JoinDecider}），不影响正确性：
     * 估高了最多多走一次可溢写的排序归并，估低了才可能撞内存。</p>
     *
     * @return ≥ 1.0 的乘数；无压缩特性的数据源保持默认 1.0
     */
    default double joinSizeExpandRatio() {
        return 1.0;
    }

    /**
     * <b>解压膨胀系数（表级）</b> —— 与 {@link #joinSizeExpandRatio()} 同义，但按<b>具体表</b>给值：
     * 同一连接器下不同表的「存储格式 + 压缩算法」可能不同（如 ORC+Snappy 与 ORC+Zlib），压缩比差很多。
     *
     * <p>文件型数据源（Hive 等）应覆写本方法，按表的压缩特性查系数；JDBC 系数据源用默认实现即可
     * （委托给无参版本，保持「不膨胀」）。</p>
     *
     * <p>该系数<b>只影响 join 算法选型</b>，不影响正确性：估高了最多多走一次可溢写的排序归并，
     * 估低了才可能撞内存。</p>
     *
     * @return ≥ 1.0 的乘数；默认委托 {@link #joinSizeExpandRatio()}（向后兼容，老连接器不感知表级差异）
     */
    default double joinSizeExpandRatio(String catalog, String schema, String table) {
        return joinSizeExpandRatio();
    }
}
