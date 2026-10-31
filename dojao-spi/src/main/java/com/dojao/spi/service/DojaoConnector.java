package com.dojao.spi.service;

import com.dojao.spi.bean.*;
import com.dojao.spi.util.DojaoTypeMapper;
import com.dojao.spi.util.VarcharMapping;
import com.dojao.common.data.Page;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Dojao connector unified interface. All data source connectors must implement this.
 * <p>
 * Extends {@link DataSourceMetaService}（数据源元数据查询 + 统计探查），并加上数据面：
 * 数据读、数据写、建表/删表、列裁剪与谓词下推、并行扫描分区规划。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/11
 */
public interface DojaoConnector extends DataSourceMetaService {

    /**
     * 分区元数据合成列：{@link #scanPartitionMetadata} 产出页里的行数列（BIGINT，精确值）。
     *
     * <p>仅引擎内部使用，不对用户暴露；列名带 {@code __dojao_} 前缀与其它内部列（{@code __jk_} /
     * {@code __win_key_}）同一约定。</p>
     */
    String PARTITION_ROW_COUNT_COLUMN = "__dojao_partition_rows__";

    // ==================== 实例复制 ====================

    /**
     * 返回一个可供<b>另一线程同时使用</b>的连接器副本。引擎在并行扫描时会为每个分区线程要一份。
     *
     * <p><b>什么时候需要覆写。</b>只要连接器的读取游标状态挂在<b>连接器实例</b>上（即
     * {@link #read(ReadRequest)} 的「连续调用接着上次读」语义），一个实例就只能服务一个分区，
     * 必须覆写本方法返回新实例（如 {@code new MysqlConnector(...)}）。</p>
     *
     * <p><b>什么时候不用管。</b>若连接器把游标放进 {@link ScanReader}（即覆写了
     * {@link #planSplits} / {@link #openSplit}，每次开 reader 自带状态），
     * 实例本身就是无状态可共享的，用默认实现即可。</p>
     *
     * <p>默认返回 {@code this}（复用共享实例）。这是<b>有意的安全兜底而非理想实现</b>：
     * 不覆写就意味着放弃多实例并行，但绝不会因此读错数据（错的是慢，不是对）。</p>
     */
    default DojaoConnector copy() {
        return this;
    }

    // ==================== 类型映射 ====================

    /**
     * 返回指定表的列名 → Dojao 标准类型映射。
     * 各 Connector 根据自身数据源的类型体系做映射，
     * 保证进入算子流水线的列类型一致。
     *
     * @param schema 数据源的 schema 名称（可为 null）
     * @param table  表名
     * @return 列名 → DojaoType 映射
     */
    default Map<String, DojaoType> getColumnTypes(String schema, String table) {
        return Map.of();
    }

    /**
     * 返回表扫描会产出的指定列 Arrow {@link Field} 清单（读方向类型映射，保序）。
     *
     * <p><b>这是 CTAS 的类型映射扩展点</b>：Master 在派发前需要知道"扫描将产出的列"的精确
     * 类型（int 位宽 / decimal(p,s) / 字符串上限），才能按此建出能装下数据的目标表 —— 分布式
     * 写数据不回流，不能用运行期第一页 schema 兜底。</p>
     *
     * <p>连接器应<b>覆写</b>本方法按自身元数据精确实现（MySQL 走 information_schema 或 JDBC
     * 元数据、PostgreSQL 走 pg_catalog……），保证与 {@link #read(ReadRequest)} 读出的列类型一致；
     * 默认实现退化为 {@link #getColumnTypes}（Dojao 类型，无精度）→ {@link DojaoTypeMapper}
     * 宽安全默认（DECIMAL 一律 Decimal(38,10)，只宽不窄、不截断）。</p>
     *
     * @param columns 需要的列（保序）；null/空 = 表全部列（按表定义顺序）
     * @return 各列的 Field 清单（列序同 columns）；未知列被忽略
     */
    default List<Field> sourceFields(String catalog, String schema, String table,
                                     List<String> columns) {
        List<Column> tableCols = getTableColumns(catalog, schema, table);
        Map<String, String> nativeByName = new LinkedHashMap<>();
        for (Column c : tableCols) {
            nativeByName.put(c.name(), c.dataType());
        }
        Map<String, DojaoType> types = getColumnTypes(schema, table);
        // 列名可能来自 ReadRequest.columns()（已按方言转义）：查元数据前先剥引号，
        // 否则带引号的名字查不到 → 该列被当成未知列丢弃（调用方按位置取值时类型/列数全错）。
        List<String> names = (columns == null || columns.isEmpty())
                ? tableCols.stream().map(Column::name).toList()
                : com.dojao.spi.util.SqlIdentifiers.unquoteAll(columns);
        List<Field> fields = new ArrayList<>();
        for (String name : names) {
            if (nativeByName.isEmpty() && types.isEmpty()) {
                break; // 数据源未提供任何元数据 → 无法推导，返回空让调用方走其它回退
            }
            DojaoType t = types.get(name);
            if (t == null) {
                t = guessDojaoType(nativeByName.get(name));
            }
            if (t != null) {
                fields.add(DojaoTypeMapper.toField(name, t));
            }
        }
        return fields;
    }

    /** 连接器未给 getColumnTypes / dataType 无对应 Dojao 映射时的常见 SQL 类型启发式兜底。 */
    static DojaoType guessDojaoType(String nativeType) {
        if (nativeType == null) {
            return null;
        }
        String t = nativeType.toLowerCase(Locale.ROOT).trim();
        if (t.startsWith("tinyint")) return DojaoType.TINYINT;
        if (t.startsWith("smallint") || t.equals("year")) return DojaoType.SMALLINT;
        if (t.startsWith("mediumint") || t.equals("int") || t.startsWith("int(")
                || t.equals("integer") || t.startsWith("integer(")) return DojaoType.INT;
        if (t.startsWith("bigint")) return DojaoType.BIGINT;
        if (t.equals("bit") || t.equals("bool") || t.equals("boolean")) return DojaoType.BOOLEAN;
        if (t.startsWith("decimal") || t.startsWith("numeric") || t.startsWith("number")
                || t.equals("dec") || t.equals("fixed")) return DojaoType.DECIMAL;
        if (t.startsWith("double")) return DojaoType.DOUBLE;
        if (t.startsWith("real")) return DojaoType.DOUBLE;
        if (t.startsWith("float")) return DojaoType.FLOAT;
        if (t.equals("date")) return DojaoType.DATE;
        if (t.startsWith("datetime")) return DojaoType.DATETIME;
        if (t.startsWith("timestamp")) return DojaoType.TIMESTAMP;
        if (t.startsWith("time")) return DojaoType.STRING;
        if (t.startsWith("json")) return DojaoType.JSON;
        // char / varchar / text / blob / enum / set / 未知 → STRING
        return DojaoType.STRING;
    }

    // ==================== Data read ====================

    /**
     * 从游标读取下一页数据。连续调用同一 ReadRequest 会继续从上次位置读取。
     *
     * <p><b>新连接器一般不必实现本方法。</b>引擎实际只调 {@link #openSplit}（见其 javadoc 的
     * 「引擎唯一调用的读取入口」），而 {@code openSplit} 的默认实现会桥接到本方法 —— 所以
     * 「只实现 read」的既有连接器零改动仍然工作，覆写 {@code openSplit} 的连接器则可以完全不碰
     * 本方法。</p>
     *
     * <p>默认实现直接抛 {@link UnsupportedOperationException}：既不实现 {@code read}、也不覆写
     * {@code openSplit} 的连接器读不出任何数据，这种「误用」应当当场暴露（fail-loud），
     * 而不是静默返回空数据让用户以为表是空的。</p>
     */
    default Page read(ReadRequest request) {
        throw new UnsupportedOperationException(
                "连接器既未实现 read(ReadRequest) 也未覆写 openSplit，无法读取数据: "
                        + getClass().getName() + "（新连接器请实现 openSplit(request, split)）");
    }

    // ==================== 分区元数据扫描 ====================

    /**
     * <b>分区元数据扫描</b> —— 不读数据文件，只返回「<b>一行为一个非空分区</b>」的合成页。
     *
     * <p>用途：当查询的聚合输入<b>全部是分区列</b>时（{@code min(dt) / max(dt) /
     * count(distinct dt) / count(*)}），逐行扫描数据毫无必要 —— 分区列的值来自目录名，行数来自
     * 文件 footer。引擎规划期调用本方法拿到这张「分区清单页」，在其上跑普通聚合，结果与全表扫描
     * <b>精确等价</b>。</p>
     *
     * <p><b>页结构</b>：全部分区列（按数据源声明类型，常量列语义 = 该分区的值；{@code NULL} 分区
     * 如实置 NULL）+ {@link #PARTITION_ROW_COUNT_COLUMN}（BIGINT，该分区的<b>精确</b>行数）。
     * 空分区（没有任何数据行的分区）<b>不出现</b>在页里 —— 它对任何聚合都不贡献行。</p>
     *
     * <p><b>精确性契约（最重要）</b>：行数必须是真实行数，<b>不允许</b>用元数据估算值（如
     * Hive 的 {@code numRows} 统计）冒充 —— {@code count(*)} 会因此算错。只要<b>任一分区</b>的
     * 精确行数拿不到（格式无 footer / 事务表行级合并 / footer 读失败 / 分区未登记），
     * 必须整页返回 {@code null}，由调用方回退真实扫描。<b>本方法永远不返回「部分精确」的结果。</b></p>
     *
     * <p>{@code request.filterExpression()} 按 {@link #planSplits} 的同一口径做分区裁剪
     * （只列命中的分区；未命中的分区连 footer 都不读）。谓词由引擎在合成页上照常再求值一次，
     * 两者都是保守裁剪，语义一致。</p>
     *
     * <p><b>默认 {@code null} = 不支持</b>：只有实现了分区目录 → 精确行数的连接器（Hive）才覆写。
     * 返回页的 buffer 由调用方拥有并负责 {@link Page#close()}。</p>
     *
     * @param request 读请求（catalog/schema/table + 可选分区谓词）
     * @return 一行为一个非空分区的合成页；不支持或行数不精确时 {@code null}
     */
    default Page scanPartitionMetadata(ReadRequest request) {
        return null;
    }

    /**
     * 本连接器对某张表是否支持 {@link #scanPartitionMetadata}（<b>结构性</b>判定，不做数据 I/O）。
     *
     * <p>规划期用它决定「要不要走分区元数据路径」：只有返回 {@code true}，规划器才会把
     * {@code count(*)} 改写成 {@code sum(分区行数列)} 并换用分区元数据算子。运行期
     * {@link #scanPartitionMetadata} 仍可能因 HDFS 故障返回 {@code null} —— 那种情况说明数据
     * 本身也读不了，直接报错而不是静默换个算法（换算法会让 {@code count(*)} 的改写失去依据）。</p>
     *
     * <p>默认 {@code false}（不支持）。</p>
     */
    default boolean supportsPartitionMetadataScan(String catalog, String schema, String table) {
        return false;
    }

    // ==================== Data write ====================

    void write(WriteRequest request);

    /**
     * 建表 —— CTAS（CREATE TABLE AS SELECT）用。根据 Arrow {@link Field} 清单
     * （列名 + 类型）生成建表 DDL 并执行。
     *
     * <p>默认抛出 {@link UnsupportedOperationException}，支持建表的连接器需覆写。</p>
     *
     * @param catalog     目标 catalog
     * @param schema      目标 schema
     * @param table       目标表名
     * @param fields      Arrow schema 字段（列名 + 类型）
     * @param ifNotExists 为 true 时使用 IF NOT EXISTS 语义（表已存在则跳过建表）
     * @return 是否实际创建了表：true 表新建；false 表已存在且 ifNotExists 跳过（调用方须跳过写入）
     */
    default boolean createTable(String catalog, String schema, String table,
                                List<Field> fields, boolean ifNotExists) {
        throw new UnsupportedOperationException(
                "CREATE TABLE not supported by connector: " + catalog + "." + schema + "." + table);
    }

    /**
     * 获取目标 schema 的默认字符集（CTAS 建表时新表继承该字符集，用于推算字符串列
     * VARCHAR 上限）。返回 empty → 连接器未知 / 不支持，调用方回退 TEXT。
     *
     * @param schema 目标 schema
     * @return 字符集名（如 MySQL 的 "utf8mb4"）；未知返回 empty
     */
    default Optional<String> getDefaultCharset(String schema) {
        return Optional.empty();
    }

    /**
     * 删表 —— CTAS 失败清理用（新建目标表全成或全不成：建表后任一分区失败 → 删掉刚建的表，
     * 不留半成品新表）。支持建表的连接器应同时实现本方法。
     *
     * <p>默认抛出 {@link UnsupportedOperationException}；调用方仅在清理场景调用，失败可吞掉
     * （不掩盖主错误）。</p>
     */
    default void dropTable(String catalog, String schema, String table) {
        throw new UnsupportedOperationException(
                "DROP TABLE not supported by connector: " + catalog + "." + schema + "." + table);
    }

    /**
     * 清空表数据、保留表结构（{@code TRUNCATE TABLE}）。
     *
     * <p>Dojao <b>不保证</b>目标数据源账号有清空权限 —— 权限不足、表不存在等由实现方原样抛异常，
     * 调用方（Master）据此把错误返回给客户端；执行成功则返回（无副作用即成功）。</p>
     *
     * <p>默认抛出 {@link UnsupportedOperationException}；支持建表 / 删表的连接器应同时实现本方法。</p>
     */
    default void truncateTable(String catalog, String schema, String table) {
        throw new UnsupportedOperationException(
                "TRUNCATE TABLE not supported by connector: " + catalog + "." + schema + "." + table);
    }

    /**
     * 本连接器的字符串列 DDL 类型映射（CTAS 建表用）。
     *
     * <p>默认全 TEXT 兜底；自定义连接器可覆写以自定义默认写入的数据类型，
     * 各连接器按自身目标库的字符集规则推算 VARCHAR 上限。</p>
     */
    default VarcharMapping varcharMapping() {
        return VarcharMapping.fallback();
    }

    // ==================== 列裁剪 & 谓词下推 ====================

    /**
     * 列裁剪：把读取范围限制为指定列集。
     *
     * <p>引擎把本次查询真正需要的列清单交给连接器，连接器据此生成裁剪后的
     * {@link ReadRequest}，其 {@link #read(ReadRequest)} 实现按此列集生成 SELECT，
     * 让数据源只拉取必要列（减少网络传输、内存占用与下游拷贝）。
     *
     * <p>默认实现直接把列集写入请求；子类可覆写做方言级处理，
     * 如标识符转义（MySQL 反引号、Postgres 双引号）、列名校验、类型改写等。
     *
     * @param request 原始读请求
     * @param columns 本次查询实际需要的列（顺序即输出列序）；null/空表示读取所有列
     * @return 裁剪后的读请求
     */
    default ReadRequest pruneColumns(ReadRequest request, List<String> columns) {
        if (columns == null || columns.isEmpty()) return request;
        return request.withColumns(new ArrayList<>(columns));
    }

    /**
     * 谓词下推：把过滤条件交给数据源执行。
     *
     * <p>引擎把 WHERE 中可下推的谓词（连接器方言表达式）交给连接器，连接器据此生成带
     * 过滤条件的 {@link ReadRequest}，让数据源在扫描时直接过滤行（显著减少上游拉取量）。
     *
     * <p>默认实现把谓词以 {@code AND} 合入请求的 {@link ReadRequest#filterExpression()}；
     * 子类可覆写做方言翻译、谓词改写（如函数改写）或合法性校验。
     *
     * @param request   原始读请求
     * @param predicate 待下推的谓词（连接器方言 SQL 表达式，如 {@code "status = 'A' AND amount > 10"}）
     * @return 带过滤条件的读请求
     */
    default ReadRequest pushdownPredicate(ReadRequest request, String predicate) {
        if (predicate == null || predicate.isBlank()) return request;
        String merged = request.filterExpression()
                .map(f -> "(" + f + ") AND (" + predicate + ")")
                .orElse(predicate);
        return request.withFilter(merged);
    }

    /**
     * 本连接器是否<b>兑现</b>谓词下推契约 —— 即读取侧确实按
     * {@link ReadRequest#filterExpression()} 过滤行。
     *
     * <p><b>为什么这个声明是必须的。</b>规划器把可下推的 WHERE 谓词交给连接器之后，<b>会把引擎侧的
     * Filter 算子消掉</b>（见 {@code Planner} 各处下推点）。连接器若拿到了过滤条件却不理它，查询
     * <b>不会报错</b>，只会<b>静默多返回行</b> —— 这类错误没有异常、没有日志，只有对不上的行数。
     * 因此「能不能过滤」必须由连接器显式声明，引擎据此在「下推给你执行」与「我自己过滤」之间选择。</p>
     *
     * <p><b>默认 {@code false}（保守）。</b>漏声明的代价只是少一次下推优化：引擎在本侧保留一层
     * {@code Filter} 算子，结果仍然正确，只是过滤在引擎侧做、从数据源拉取的行数偏大。反过来若默认
     * {@code true}，漏声明就是静默多返回行 —— 两种错法不对称，故选保守。</p>
     *
     * <p><b>覆写为 {@code true} 的条件</b>：读取实现（{@link #read} 或 {@link #openSplit}）会读
     * {@code request.filterExpression()} 并据此过滤。把过滤编译进数据源 SQL 的连接器（MySQL /
     * PostgreSQL 等 JDBC 系）覆写为 {@code true}；只按 {@code request} 里的分片信息读文件、
     * 不做谓词求值的连接器保持 {@code false}。</p>
     *
     * @return true = 读取侧会按 {@code filterExpression} 过滤（引擎可放心不下推就不过滤）；
     *         false = 引擎必须自己保留过滤（下推也就不必了）
     */
    default boolean supportsPredicatePushdown() {
        return false;
    }

    /**
     * 本次扫描里，哪些合取项<b>已被分区裁剪兑现</b> —— 即对<b>每个保留下来的分区的每一行</b>都必然成立。
     * 引擎据此免于在扫描之上再求值这些项（{@code ConnectorScanOperator.openWithEngineFilter}）。
     *
     * <p><b>为什么需要这一声明。</b>谓词在本引擎里承担两件事：① 连接器按它<b>决定读哪些分区</b>
     * （{@link #planSplits} 里的分区裁剪）；② 不兑现下推的连接器（{@link #supportsPredicatePushdown()} =
     * false，如 Hive）还会被引擎在扫描节点补一层 {@code Filter} 算子再算一遍。对①已保证成立的项，
     * ②纯属重复 —— 分区裁剪越成功，这层重复过滤的开销越大（大分区上逐行重物化整页），且还会把它引用的
     * 分区列拉进扫描投影。</p>
     *
     * <p><b>安全契约：只许返回「对保留下来的每一个分区的每一行都必然为真」的项。</b>返回多了 =
     * <b>静默多返回行</b>（无异常、无日志，最难查）；返回少了只是少省一点，最坏退化为「引擎照旧自己
     * 过滤一遍」。拿不准一律<b>不要</b>返回。默认返回空列表 = 不声明（引擎照常自行求值），任何连接器
     * 不覆写都保持既有行为。</p>
     *
     * <p>典型实现：解析 {@code request.filterExpression()} 得到分区列约束，把它作用到自己的分区清单上，
     * 再问「对保留下来的每个分区取值，哪些合取项确定成立」——见 {@code com.dojao.common.util.PartitionPruner}
     * 的 {@code enforcedConjuncts}，Hive 连接器即如此接入。</p>
     *
     * @param request   本次扫描的基础读请求（其 {@link ReadRequest#filterExpression()} 即下发谓词）
     * @param conjuncts {@code filterExpression()} 的顶层 AND 合取项（原文，保序）
     * @return {@code conjuncts} 中已被分区裁剪兑现的那些（原文，须 ∈ {@code conjuncts}）；默认空
     */
    default List<String> partitionEnforcedConjuncts(ReadRequest request, List<String> conjuncts) {
        return List.of();
    }

    // ==================== 连接性探针（建 catalog 时的一次真实连接） ====================

    /**
     * 用本连接器当前持有的配置<b>真的连一次数据源</b>，连不上就抛异常。
     *
     * <p><b>为什么必须有这一关。</b>「构造连接器对象」与「连得上数据源」是两件事：JDBC 系连接器的
     * 构造只是把 url/账号存进字段（{@code new MysqlConnector(...)} 不会发起任何网络 I/O），
     * 于是<b>一个错误的 url / 密码也能注册成功</b> —— 坏配置被推迟到首次查询（或 UI 展开目录树）
     * 才炸，那时早已落库，用户看到的是「catalog 建好了但加载失败」，真因与创建时机脱节。
     * 引擎在 {@code CREATE CATALOG} 落库<b>之前</b>调本方法，就是为了把「连不上」挡在入库之前：
     * 连不上 → 异常上抛 → <b>什么都不留</b>。</p>
     *
     * <p><b>默认实现 = 通用探针</b>：调 {@link #listSchemas(String)} —— 任何实现了元数据探查的连接器
     * 都会因此发起一次真实往返（JDBC 建连接、HMS 开 Thrift 会话、读文件数据源列举目录……），
     * 无需覆写即可获得「连一次」的效果。</p>
     *
     * <p><b>什么时候覆写。</b>当 {@code listSchemas} 走的是比「建立连接」更重的路径、或它的失败
     * 语义会掩盖连接失败时（如 MySQL 的 {@code listSchemas} 会真的枚举库、可能因权限不足而对一个
     * 连得上的数据源报错）—— 此时覆写成一次<b>纯粹的连接握手</b>（如
     * {@code try (Connection c = getConnection()) { c.isValid(5); }}），语义更准、也更便宜。</p>
     *
     * <p><b>本方法只用于「创建 catalog」这一条路径。</b>启动期从存储重装载（
     * {@code reloadCatalogsFromStorage}）<b>不</b>调它：那时数据源短暂不可用不该让整个 Master
     * 起不来，也不该把已注册的 catalog 判死 —— 连不上的代价是「本次该 catalog 不可用」，
     * 而不是「注册信息被删」。</p>
     *
     * @param catalog 待验证的 catalog 名（仅用于异常文案）
     * @throws RuntimeException 连不上数据源（消息里应带上地址与真因，供 UI 直接展示）
     */
    default void probeConnectivity(String catalog) {
        listSchemas(catalog);
    }

    // ==================== 分区探针（并行 TableScan） ====================

    /**
     * 获取可用于范围分区的单列数值型主键列名。
     * 默认返回空（不支持范围分区），连接器可按需覆盖。
     */
    default Optional<String> getPartitionKey(String schema, String table) {
        return Optional.empty();
    }

    /**
     * 精确行数（SELECT COUNT(*)）。默认返回 0。
     */
    default long getExactRowCount(String schema, String table) {
        return 0;
    }

    /**
     * 主键列最小值/最大值。默认返回 [0, 0]。
     */
    default long[] getPartitionKeyRange(String schema, String table, String pkColumn) {
        return new long[]{0, 0};
    }

    // ==================== 并行扫描分区规划 ====================

    /**
     * 规划并行扫描分区，返回每个分区的独立读请求（已带分区谓词）。
     * 返回空列表 → 无法分区，调用方退化为单分区整表扫描。
     *
     * <p>默认实现：单列数值主键 MIN/MAX 范围均分；无主键 / 非数值主键返回空。
     * 各连接器覆写，在主键分区失败时链式追加方言兜底，确保大表无主键也能并行扫：</p>
     * <ul>
     *   <li>PostgreSQL → ctid 物理行号范围分区（零索引依赖，总 I/O 恰 1 倍）</li>
     *   <li>MySQL → 数值列范围分区，再无数值列退 LIMIT/OFFSET 均分</li>
     * </ul>
     *
     * @param request     已列裁剪 / 谓词下推的基础读请求
     * @param parallelism 期望并行分区数（&lt;=1 或请求自带 limit/offset 时返回空，不分区）
     * @return 分区读请求列表；空表示无法分区（调用方单分区整表扫描）
     */
    default List<ReadRequest> planParallelScan(ReadRequest request, int parallelism) {
        // limit/offset 分页语义与分区互斥：分页请求不做并行拆分
        if (parallelism <= 1 || request.limit() > 0 || request.offset() > 0) return List.of();
        Optional<String> pk = getPartitionKey(request.schema(), request.table());
        if (pk.isEmpty()) return List.of();
        long[] range = getPartitionKeyRange(request.schema(), request.table(), pk.get());
        if (range[0] >= range[1]) return List.of();
        return splitRangePartitions(request, pk.get(), range[0], range[1], parallelism);
    }

    /**
     * 把 [min, max] 均分成 {@code parallelism} 段，每段生成一个带 {@code key >= a AND key <= b}
     * 范围谓词的读请求（与请求原有过滤条件以 AND 合入）。连接器兜底方案可复用。
     */
    static List<ReadRequest> splitRangePartitions(ReadRequest request, String key,
                                                  long min, long max, int parallelism) {
        long span = max - min + 1;
        long step = span / parallelism + (span % parallelism == 0 ? 0 : 1);
        if (step <= 0) step = 1;
        List<ReadRequest> parts = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            long start = min + (long) i * step;
            long end = (i == parallelism - 1) ? max : (min + (long) (i + 1) * step - 1);
            if (start > max) break;
            String rangeFilter = key + " >= " + start + " AND " + key + " <= " + end;
            String merged = request.filterExpression()
                    .map(f -> "(" + f + ") AND (" + rangeFilter + ")")
                    .orElse(rangeFilter);
            parts.add(new ReadRequest(request.catalog(), request.schema(), request.table(),
                    request.columns(), Optional.of(merged), request.offset(), request.limit()));
        }
        return parts;
    }

    // ==================== 切片扫描（Split / ScanReader） ====================

    /**
     * 规划并行扫描切片。<b>这是引擎唯一调用的分片规划入口</b>，替代了直接调用
     * {@link #planParallelScan} —— 后者仍是「谓词型」分片的钩子，被本方法的默认实现桥接进来。
     *
     * <p><b>为什么要有这一层。</b>{@link #planParallelScan} 只能返回 {@code List<ReadRequest>}，
     * 即切片必须能用谓词表达。JDBC 数据源（MySQL 主键范围、PG ctid 范围）成立，但读文件的数据源
     * 不成立 —— Hive 一个分片 = 若干 HDFS 文件，没法写成 SQL 谓词。历史上只能用
     * {@code __dojao_files__} 这类魔法串偷偷塞进 {@link ReadRequest#filterExpression()}，
     * 结果是连接器私有约定污染公共字段、还得在 {@code read()} 里反向解析。本方法把这件事正规化：
     * 切片 = {@link ScanSlice}（公共 {@link ReadRequest} + 私有 {@link Split} 字节），引擎只搬运。</p>
     *
     * <p><b>契约。</b>返回的切片必须<b>互不重叠且合起来恰好覆盖整表</b>（丢一半 = 静默少数据，
     * 重叠 = 静默重复行，两者都不报错，最伤）。返回空列表 = 无法分区，调用方退化为单切片整表扫描。
     * 分页请求（{@code limit}/{@code offset} > 0）因语义互斥不做拆分，返回空。</p>
     *
     * <p><b>默认实现桥接 {@link #planParallelScan}</b>：把每个分区请求包成
     * {@link Split#WHOLE}（无私有信息）的切片。谓词型连接器覆写 {@code planParallelScan} 即可，
     * 无需感知 {@code Split}；载荷型连接器（如 Hive）直接覆写本方法。</p>
     *
     * <p><b>★分区裁剪落在这里。</b>本方法是「决定要读哪些扫描单元」的唯一入口，故有分区概念的数据源
     * 都在这一步按 {@code request.filterExpression()} 把不匹配的分区整个剔掉 —— 被剔掉的分区既不会
     * 出现在切片里、也不会被列目录 / 开文件，扫描量才真的省下来。做法见
     * {@code com.dojao.common.util.PartitionPruner}（引擎侧通用能力，无需改 SPI / 协议），
     * 分区列清单见 {@link DataSourceMetaService#getPartitionColumns}。</p>
     *
     * @param request 已列裁剪 / 谓词下推的基础读请求
     * @param parallelism 切片数<b>上限</b>（&lt;=1 或分页请求时不做拆分）；实际切片数由连接器按自己的
     *                    分区策略定，可以少于该值 —— 连接器把天然分区数报在
     *                    {@link com.dojao.spi.bean.TableStats#splitCount()} 里供引擎先做整体决策
     * @return 切片列表；空 = 无法分区（调用方单切片整表扫描）
     */
    default List<ScanSlice> planSplits(ReadRequest request, int parallelism) {
        List<ReadRequest> parts = planParallelScan(request, parallelism);
        if (parts.isEmpty()) return List.of();
        List<ScanSlice> out = new ArrayList<>(parts.size());
        for (ReadRequest p : parts) {
            out.add(ScanSlice.whole(p));
        }
        return out;
    }

    /**
     * 带<b>截断下推</b>信息的切片规划 —— 与 {@link #planSplits(ReadRequest, int)} 同一件事，多一个
     * 「引擎只需要前 {@code fetchLimit} 行」的提示。
     *
     * <p><b>为什么单独给一个参数，而不是把它写进 {@code request.limit()}。</b>
     * {@link ReadRequest#limit()}/{@link ReadRequest#offset()} 是<b>数据源必须精确兑现的分页语义</b>
     * （MySQL 无主键兜底切的 {@code LIMIT step OFFSET i*step} 就是靠它）；而 {@code fetchLimit} 是
     * 引擎的<b>「够用了，可以早停」提示</b>（由顶层 {@code LIMIT} 在「扫描行与输出一一对应」的单表
     * 直通链上推下来，上方 {@code LimitOperator} 仍照常兜底）。两者混成一个字段，会让「连接器自己
     * 生成的分页切片」与「引擎要的前缀」无法区分。</p>
     *
     * <p><b>连接器可以无视它</b>（返回什么都不会错，最多多读几行）：默认实现即忽略之，直接走
     * {@link #planSplits(ReadRequest, int)}。文件分片型连接器（Hive）则据此只列举/只读「够前缀的
     * 那几个文件」—— 一个切片搞定，不去列举整表文件。</p>
     *
     * <p><b>这个提示的前提：读到的行就是输出的行</b>（行数、行序都不变）。引擎只在「单表直通 +
     * 谓词由数据源执行（{@link #supportsPredicatePushdown()}）或无谓词」时才给非 0 值 —— 若引擎
     * 还要在扫描之上求值 WHERE，截断「前 N 行」拿到的是「前 N 行里命中的那几行」，而不是「命中的
     * 前 N 行」，会静默少返回行。因此连接器若发现自己不会执行 {@code request.filterExpression()}
     * （如 Hive），就<b>必须无视这个提示</b>：按全量列举 —— 早停交给引擎（顶层 {@code LimitOperator}
     * 满行即关上游）。</p>
     *
     * @param fetchLimit 引擎只需前多少行（0 = 无截断下推，等同
     *                   {@link #planSplits(ReadRequest, int)}）
     * @return 切片列表；空 = 无法分区（调用方单切片整表扫描）
     */
    default List<ScanSlice> planSplits(ReadRequest request, int parallelism, long fetchLimit) {
        return planSplits(request, parallelism);
    }

    /**
     * 打开一个切片的读取游标。<b>这是引擎唯一调用的读取入口</b>，替代了直接调用
     * {@link #read(ReadRequest)} —— Worker 任务线程持有返回的 {@link ScanReader}，反复
     * {@code next()} 到 EOF，最后 {@code close()}。
     *
     * <p><b>游标状态从连接器对象搬进了 reader。</b>{@link #read(ReadRequest)} 的「连续调用接着
     * 上次读」语义把「读到哪儿」挂在连接器实例上，一个实例同时只能服务一个切片；跨 JVM 并行扫描
     * 时引擎必须为每个分区复制一份连接器实例（见 {@link #copy()}）。{@link ScanReader}
     * 让每个切片自带状态，连接器实例可被多线程安全共享。</p>
     *
     * <p>默认实现桥接 {@link #read(ReadRequest)}：返回一个首次读到 EOF 即停止转发、{@code close()}
     * 转 {@link #closeRead()} 的 reader。因此<b>所有既有连接器零改动</b>即可满足本接口。若连接器
     * 覆写了 {@link #planSplits} 并产出带载荷的 {@link Split}，则必须同时覆写本方法 —— 默认实现
     * 不认识私有载荷，会直接抛 {@link UnsupportedOperationException}（fail-loud，不静默读错数据）。</p>
     *
     * @param request 本切片的读请求（与规划期 {@link ScanSlice#request()} 一致，跨 JVM 后重建）
     * @param split   本切片的私有定位信息；{@link Split#WHOLE} = 无（用 {@code request} 直接读）
     * @return 本切片的读取游标
     */
    default ScanReader openSplit(ReadRequest request, Split split) {
        if (split != null && split.hasPayload()) {
            throw new UnsupportedOperationException(
                    "连接器产出了带载荷的 Split，但未覆写 openSplit 来解读它: "
                            + getClass().getName());
        }
        return new ScanReader() {
            private boolean done;

            @Override
            public Page next() {
                if (done) return Page.eof();
                Page page = read(request);
                if (page == null || page.isEOF()) {
                    done = true;
                    return Page.eof();
                }
                return page;
            }

            @Override
            public void close() {
                closeRead();
            }
        };
    }

    // ==================== 游标 / 写会话清理 ====================

    /** 关闭当前读取游标。默认空实现。 */
    default void closeRead() {}

    /**
     * 关闭本连接器持有的全部写会话（持久 Connection + PreparedStatement）。
     *
     * <p>并行写通道（TableSink）在写线程内复用连接与语句，写语句结束（{@code doClose}）时必须
     * 释放，否则连接泄露。默认空实现；有写会话的连接器（如 MySQL/PG 内置连接器）须重写。
     */
    default void closeWrite() {}

    /**
     * 写语句整体收尾：写线程都已停之后由 Master（{@code TableSinkOperator.doClose}）调用，
     * 把本连接器里<b>所有线程</b>的写会话一并收掉。
     *
     * <p>与 {@link #closeWrite()} 的分工：Worker 片段各写各的（并发兄弟片段不能互收，
     * 见 {@code FragmentExecutor.runAndSink}）走 {@code closeWrite()}；Master 本地写路径的
     * 多个写线程已在 {@code doClose} 前 join 完毕，此时才可以「全都收」。默认退回
     * {@link #closeWrite()}（只收当前线程），对无跨线程会话的连接器无差别。</p>
     */
    default void finishWrite() {
        closeWrite();
    }
}
