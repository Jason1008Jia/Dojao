package com.dojao.ui.ai;

import com.dojao.ai.AiConfig;
import com.dojao.ai.AiTool;
import com.dojao.ai.ReActAgent;
import com.dojao.ai.ReActListener;
import com.dojao.common.sql.SqlSplitter;
import com.dojao.metastore.MetaStore;
import com.dojao.spi.bean.Column;
import com.dojao.ui.auth.Role;
import com.dojao.ui.log.LogViewer;
import com.dojao.ui.monitor.MonitorCluster;
import com.dojao.ui.monitor.MonitorColumn;
import com.dojao.ui.monitor.MonitorFunction;
import com.dojao.ui.monitor.MonitorJob;
import com.dojao.ui.monitor.MonitorOperator;
import com.dojao.ui.monitor.MonitorQuery;
import com.dojao.ui.monitor.MonitorResult;
import com.dojao.ui.monitor.MonitorSubmit;
import com.dojao.ui.monitor.MonitorTask;
import com.dojao.ui.monitor.MonitorViews;
import com.dojao.ui.monitor.MonitorWorker;
import com.dojao.ui.monitor.QueryMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.StreamingChatModel;
import dev.langchain4j.model.chat.request.json.JsonArraySchema;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.request.json.JsonStringSchema;
import dev.langchain4j.model.openai.OpenAiStreamingChatModel;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Dojao 专属的 ReAct Agent 服务 —— 把「通用 ReAct 内核」（{@link ReActAgent}，在 dojao-ai）
 * 与「Dojao 数据能力」拼起来：提供 13 个工具（只读 SQL / DDL / 图表渲染 / 文件导出 / 查目录元数据 /
 * 函数详情 / 集群感知 / 节点日志 / 引擎与查询诊断），
 * 并把「引擎能力边界 + 当前可见的全部函数清单」前置写进系统提示词，每个用户各自隔离。
 *
 * <p>分层：{@link ReActAgent} 在 dojao-ai，零 dojao-metastore/ui 依赖；本类在 dojao-ui，
 * 注入 {@link QueryMonitor}（SQL 执行）+ {@link MetaStore}（元数据），据当前登录用户（viewer）构造工具。</p>
 *
 * <p><b>读写约束</b>：{@code execute_sql} 仅放行 SELECT/SHOW/DESCRIBE/EXPLAIN（只读）；
 * DDL（CREATE/DROP/ALTER/TRUNCATE）走独立工具 {@code execute_ddl}，且<b>执行前必须经
 * {@link DdlConfirm} 回调由用户在前端确认</b>，避免模型擅自改结构。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/13
 */
public final class AiAgentService {

    /**
     * 只读语句的首关键字白名单。
     *
     * <p>{@code WITH} 允许：文法里只有 {@code query} 可以以 WITH 开头（{@code insertStatement}
     * 必以 INSERT 开头），故 WITH 开头的语句一定是 CTE 查询，不是写操作。</p>
     */
    private static final Set<String> READ_ONLY_KEYWORDS =
            Set.of("SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN");

    /** DDL 语句的首关键字白名单（execute_ddl 专用）。 */
    private static final Set<String> DDL_KEYWORDS =
            Set.of("CREATE", "DROP", "ALTER", "TRUNCATE");

    /**
     * DDL 执行前的用户确认回调：返回 true = 用户同意执行，false = 拒绝。
     *
     * <p>由上层（UI 流式端点）实现 —— 把待执行的 DDL 推给前端、阻塞等待用户点「确认 / 取消」。
     * 为 {@code null} 时视为「不可用」，{@code execute_ddl} 一律拒绝执行。</p>
     */
    @FunctionalInterface
    public interface DdlConfirm {
        boolean confirm(String sql);
    }

    /**
     * 图表输出回调：{@code render_chart} 工具把一份图表规格（JSON 文本）推给前端渲染。
     *
     * <p>由上层（UI 流式端点）实现 —— 把 spec 作为一条 {@code chart} 事件写进 NDJSON 流。
     * 为 {@code null} 时视为「不可用」，{@code render_chart} 一律拒绝。</p>
     */
    @FunctionalInterface
    public interface ChartSink {
        void emit(String specJson);
    }

    /**
     * 文件输出回调：{@code export_file} 工具生成可下载文件后，把文件元数据（id / 文件名 / 格式 /
     * 大小 / 下载地址）推给前端。
     *
     * <p>由上层（UI 流式端点）实现 —— 作为一条 {@code file} 事件写进 NDJSON 流，并随本轮落库。
     * 为 {@code null} 时视为「不可用」，{@code export_file} 一律拒绝。</p>
     */
    @FunctionalInterface
    public interface FileSink {
        void emit(GeneratedFile file);
    }

    /** 已生成文件的可展示元数据（id 用于 {@code GET /api/ai/files/{id}} 下载）。 */
    public record GeneratedFile(String id, String name, String format, long size, String url) {
    }

    /** export_file 支持的格式（同时也是参数枚举白名单）。 */
    private static final List<String> EXPORT_FORMATS = AiExportFormats.FORMATS;

    /**
     * 「Dojao 只支持这些图表」的统一口径 —— 系统提示词与后端报错文案共用，保证模型看到的约束
     * 与实际能力逐字一致（模型越界时用户看到的话术也一致）。
     */
    static final String CHART_SUPPORT_SENTENCE =
            "Dojao 目前只支持柱状图、折线图、面积图、饼图、散点图、漏斗图、矩形树图、树图、热力图、关系图和数据集表格";

    /**
     * render_chart 支持的图表类型（同时也是参数枚举白名单）：
     * 常规直角坐标（bar / line / area / scatter）、占比（pie / funnel）、
     * 层级（treemap / tree）、二维分布（heatmap）、关系（graph），以及数据集表格（dataset）。
     */
    private static final List<String> CHART_TYPES = List.of(
            "bar", "line", "area", "pie", "scatter",
            "funnel", "treemap", "tree", "heatmap", "graph", "dataset");

    /**
     * png / jpg / pdf 图表导出支持的图形 —— 受 {@link AiChartRenderer}（Java2D）能力限制，
     * 目前只有直角坐标与饼图；新增类型不要直接加进来，除非渲染器同步实现。
     */
    private static final List<String> EXPORT_CHART_TYPES = List.of("bar", "line", "area", "pie", "scatter");

    /**
     * render_chart 中「单条数值序列」可用的图形（混合图：同一张图里每条序列各自成图，如柱状 + 折线）。
     * 不含 pie —— 饼图不参与多序列混合（各序列各自成扇区没有意义）。
     */
    private static final List<String> SERIES_CHART_TYPES = List.of("bar", "line", "area", "scatter");

    /** 本服务提供的全部工具名（也是 Skill 绑定工具时的可选项白名单）。 */
    public static final List<String> AVAILABLE_TOOLS = List.of(
            "execute_sql", "execute_ddl", "render_chart", "export_file", "list_catalogs", "list_tables",
            "describe_table", "describe_function", "cluster_status", "read_logs",
            "list_queries", "query_status", "operator_status");

    /**
     * 仅开发者 / 管理员可用的集群运维类工具 —— 集群状态、节点日志、查询与算子诊断。
     * 分析师角色的工具集会被剔除这几项（提示词同时明确拒绝回答相关话题）。
     */
    public static final Set<String> DEVELOPER_TOOLS = Set.of(
            "cluster_status", "read_logs", "list_queries", "query_status", "operator_status");

    /** read_logs 单次返回给模型的最大行数（比界面默认更小，避免日志撑爆上下文）。 */
    static final int AI_LOG_TAIL_MAX = 1000;

    /** read_logs 单次回给模型的字符上限（超出按尾部截断并说明）。 */
    private static final int AI_LOG_MAX_CHARS = 120_000;

    /** 某角色可用的工具名（Skill 表单的可选项、服务端入参校验共用）。 */
    public static List<String> toolsForRole(Role role) {
        if (role == null || role.canUseClusterTools()) {
            return AVAILABLE_TOOLS;
        }
        return AVAILABLE_TOOLS.stream().filter(n -> !DEVELOPER_TOOLS.contains(n)).toList();
    }

    /**
     * 一次推理所使用的「技能」：附加系统提示词 + 允许的工具子集。
     *
     * <p>由上层（UI 流式端点）按用户选中的 Skill 构造。{@code prompt} 追加到基础系统提示词之后；
     * {@code tools} 为空 = 不限制（用全部工具），否则只启用其中的工具。</p>
     */
    public record Skill(String name, String prompt, List<String> tools) {
    }

    /** 图表取数的最大行数（图表可读性 + spec 体积双重考虑）。 */
    private static final int CHART_MAX_ROWS = 200;

    /** 每用户缓存的结果集份数上限（供 {@code render_chart} 用 {@code data="last"/"<id>"} 复用）。 */
    private static final int RESULT_CACHE_MAX = 10;

    /** execute_sql 结果回给模型的预览参数。 */
    private static final int PREVIEW_ROWS = 50;

    /**
     * AI 提交的只读查询结果行数<b>硬上限</b>：由 {@link #applyAiRowLimit(String)} 以 LIMIT 形式写进 SQL，
     * 防止模型提交无界查询把大结果集拉回 Web 界面（控制台 / 图表）导致卡死。
     */
    static final int AI_MAX_ROWS = 500;

    /** ReAct 最大推理步数（用尽后内核会强制模型收尾作答，绝不缺答案就停）。 */
    private static final int MAX_ITERATIONS = 16;

    // ==================== 集群感知：忙闲判据阈值 ====================
    // 说明：数据源为 monitor.cluster()。忙闲只统计「worker」节点——
    // MonitorWorker.runningQueries 对 worker 是该节点运行中任务数、对 master 是活动查询数（单位不同）。

    /** 运行中任务数 ≥ 槽位数 × 本比例 → 判定为「繁忙」。 */
    private static final double CLUSTER_BUSY_SLOT_RATIO = 0.9;
    /** 运行中任务数 ≥ 槽位数 × 本比例 → 判定为「适中」（未达繁忙时）。 */
    private static final double CLUSTER_MODERATE_SLOT_RATIO = 0.6;
    /** 全节点平均 CPU ≥ 本百分比 → 判定为「繁忙」。 */
    private static final int CLUSTER_BUSY_CPU_PCT = 80;
    /** 全节点平均 CPU ≥ 本百分比 → 判定为「适中」（未达繁忙时）。 */
    private static final int CLUSTER_MODERATE_CPU_PCT = 50;

    /**
     * Dojao SQL 方言约束 —— 随系统提示词一起提交给模型，避免模型按 MySQL/PostgreSQL 习惯写出
     * 引擎无法解析的 SQL（这类错误占执行失败的多数）。事实来源：{@code dojao-grammar/.../SqlBase.g4}
     * （语法）与引擎已有语义校验（窗口/HAVING 等）。
     *
     * <p><b>★维护约定</b>：Dojao SQL 能力发生变化时（新增/修改语法定式、数据类型、内置函数、
     * 语义校验规则等），应同步更新本段方言描述 —— 否则模型会按旧口径/他方言写 SQL。新增能力
     * 一般补一条「之前不支持、现在怎么写」的说明即可（与 {@code SqlBase.g4} 对齐）。</p>
     */
    static final String DOJAO_SQL_DIALECT = """
            Dojao SQL 方言约定（Dojao 是自研分布式引擎，语法与 MySQL / PostgreSQL 不同；请严格按此写，不要照搬其他方言的习惯）：
            - 字符串只能用单引号：'abc'（内含单引号写作 ''）。双引号 "x" 与反引号 `x` 都是「标识符」引用，不是字符串。
            - 表名完整形式为三段 catalog.schema.table（以 list_tables / describe_table 的结果为准）；不要臆造 catalog / 表名 / 列名。
            - 保留字不能裸用作列名或别名（如 date / time / timestamp / interval / rows / partition / over / filter / row / current / case / end / string 等）；确实冲突时用 "date" 或 `date` 引用。
            - 数据语句只有 SELECT 与 INSERT，没有 UPDATE / DELETE；用户要求改数据时如实说明无法用这两条语句。
            - 没有 || 字符串拼接：用 concat(a, b) 或 concat_ws(sep, a, b)。
            - 没有 DATE '2026-01-01' 这种「类型前缀字面量」：用 CAST('2026-01-01' AS DATE)，或直接与字符串比较。
            - LIMIT 不支持 LIMIT m, n 写法：分页请写 LIMIT n OFFSET m。
            - 去重计数用 COUNT(DISTINCT 列)，例如 count(distinct user_id)；**没有 count_distinct(...) 这种函数写法**，别把「去重计数」写成函数调用。
            - ★CASE 必须一律写成搜索式：CASE WHEN <条件表达式> THEN <值> [WHEN <条件表达式> THEN <值> ...] [ELSE <值>] END。
              严禁写简单式 CASE <列或表达式> WHEN <值> THEN ... END（引擎语法会直接报错，例如 CASE province WHEN '广东' THEN 1 END 非法）。
              简单式必须改写成「条件 + 等值比较」：CASE WHEN <表达式> = <值> THEN <结果> WHEN <表达式> = <值2> THEN <结果2> ELSE <默认值> END
              （例：CASE province WHEN '广东' THEN 1 ELSE 0 END 要写成 CASE WHEN province = '广东' THEN 1 ELSE 0 END）。
              每个 WHEN 后面必须是一个完整的布尔条件（比较、AND / OR、IN、BETWEEN、IS NULL、函数调用等），不能只写列名或字面量。
            - 连接必须带 ON：A [INNER] JOIN B ON 条件，另有 LEFT / RIGHT / FULL [OUTER] JOIN B ON 条件；不支持 USING(col)、NATURAL JOIN。
            - ★SEMI JOIN（LEFT / RIGHT SEMI JOIN）与（非 UNNEST 的）CROSS JOIN 目前暂不支持（计划在后续版本支持），不要生成这两种写法：
              「存在匹配」语义请改写成 EXISTS / IN 子查询；笛卡尔积请改写为带明确 ON 条件的 JOIN。
              唯一例外是 `CROSS JOIN UNNEST(...)` —— 它等价于逗号写法，见下一条。
            - ★JSON 与数组展开：取路径用 get_json_object(json, '$.a.b[0]')（返回裸文本，字符串不带引号）或
              json_extract(json, '$.a')（返回合法 JSON 文本，字符串带引号；多路径返回 JSON 数组）；
              把 JSON 数组展开成多行（一行变多行）写成
              FROM 表 t, UNNEST(json_extract(t.payload, '$.items')) AS u(item)
              —— 可加 `WITH ORDINALITY AS u(item, ord)` 取从 1 起的序号列；元素列 / 序号列名不能与左侧列重名；
              UNNEST 关系不能作为 JOIN 的一侧，也不能出现在 ON 里。输入不是 JSON 数组会直接报错。
            - 类型：BOOLEAN / TINYINT / SMALLINT / INT / BIGINT / FLOAT / DOUBLE / DECIMAL(p,s) / STRING / VARCHAR(n) / CHAR(n) / VARBINARY / JSON / DATE / TIME / TIMESTAMP / ARRAY<T> / MAP<K,V> / ROW(...) / UUID；没有 TEXT / DATETIME / BLOB / NUMBER 等别名。
            - 窗口函数：f() OVER (PARTITION BY ... ORDER BY ... [ROWS/RANGE 帧])。★窗口的 PARTITION BY / ORDER BY / 参数里不得出现聚合函数（例如 OVER (ORDER BY sum(x)) 非法）——需先把聚合放到子查询里物化成一列再用。有 GROUP BY 时，窗口的 PARTITION BY / ORDER BY 只能引用分组键或常量。
            - 多维分组：GROUP BY CUBE(a, b) / ROLLUP(a, b) / GROUPING SETS((a), (b), ())。
            - ★DDL 能力（第一版）：**只支持** ① `CREATE TABLE c.s.t AS SELECT ...`（CTAS，表结构由 SELECT 输出类型推导，可用 `CREATE TABLE IF NOT EXISTS`）；
              ② `CREATE CATALOG c USING 连接器 WITH (k = v)` / `DROP CATALOG c`；③ `DROP TABLE c.s.t`；④ `TRUNCATE TABLE c.s.t`；
              ⑤ `CREATE FUNCTION` / `DROP FUNCTION`。
              **不支持** `CREATE TABLE t (列定义 ...)`（显式列定义 / COMMENT / NOT NULL / DEFAULT）、`CREATE SCHEMA` / `DROP SCHEMA`（建库 / 删库）、
              `ALTER TABLE`（ADD / DROP / RENAME COLUMN）、`CREATE VIEW` / `DROP VIEW` ——
              这些语句虽然能被语法解析，但执行会报 `UnsupportedOperationException: 暂不支持执行的语句类型`。
              用户要求建库 / 改表结构时，如实说明第一版不支持：建表只能用 CTAS；改结构只能 `DROP TABLE` + 重新 CTAS（先备份数据）。
            - 写完后先按以上方言自查一遍（尤其：引号、LIMIT、CASE、窗口聚合、类型名），确认无误再调用 execute_sql；若报错，按返回的语法 / 校验信息定位改写，不要重复提交同一条错误 SQL。
            """;

    /**
     * Dojao 引擎能力 / 边界 —— 随系统提示词一起提交给模型，让它对「Dojao 能干什么、不能干什么」有正确认知，
     * 不必靠反复试探与重试来摸索（省 token、更快出结果）。
     *
     * <p><b>★维护约定</b>：Dojao 引擎能力发生变化（新增/移除算子、连接器、语句类型、事务/存储过程等）时，
     * 应同步更新本段 —— 与 {@link #DOJAO_SQL_DIALECT}（语法层）互补，本段讲<b>能力层</b>。</p>
     */
    static final String DOJAO_CAPABILITIES = """
            Dojao 引擎能力与边界（先建立正确认知，避免无用试探）：
            - 定位：Dojao 是自研的**分布式** SQL 分析引擎，面向海量数据的稳定执行。
            - ★强项：即使数据量大、内存吃紧（有 OOM 风险），引擎也会**自动磁盘溢写兜底**把任务稳定跑完。
              因此不要因为「担心数据量太大」就擅自把用户要的分析缩小或跳过 —— 该跑什么就跑什么；
              只需遵守下面的集群感知规则（重查询先看负载）。
            - 联邦查询：一条 SQL 里可以 JOIN 多个不同数据源（catalog）的表（如 Hive / MySQL / TiDB / Doris / ClickHouse / 本地文件）。
            - ★跨库对数（数据校验 / 对账）：借助联邦查询，可以在同一条 SQL 里直接核对不同数据源的表 ——
              行数 / 汇总值比对、主键级明细差异（FULL JOIN 找「仅左有 / 仅右有 / 值不一致」）、
              存在性校验（EXISTS / IN，跨源走批量广播半连接，无需把整表搬到一处）。
            - 自动优化：连接算法（shuffle / sort_merge / broadcast）、聚合与窗口是否溢写等由引擎自动选择，你无需手工干预；
              EXPLAIN 的每一层 join 都会显式打印 `algo=` 与 `spill=`，可直接据此解释「为什么慢 / 会不会溢写」。
            - 支持的语句：SELECT（JOIN / 子查询 / CTE / 窗口函数 / GROUP BY（含 CUBE、ROLLUP、GROUPING SETS）/ HAVING / UNION / LIMIT）、INSERT。
            - ★DDL（第一版）只有这些可用：`CREATE TABLE ... AS SELECT`（CTAS）、`CREATE / DROP CATALOG`、
              `CREATE / DROP FUNCTION`、`DROP TABLE`、`TRUNCATE TABLE`。
              **`CREATE TABLE` 显式列定义、`CREATE / DROP SCHEMA`、`ALTER TABLE`、`CREATE / DROP VIEW` 未实现**（执行会报「暂不支持执行的语句类型」）——
              用户提到建库 / 改表结构时要如实说明不支持，不要生成这几类语句。
            - 边界：数据语句只有 SELECT 与 INSERT，**没有 UPDATE / DELETE**；不支持事务、存储过程、触发器；
              每次 execute_sql / execute_ddl 只执行**单条**语句。语法细节见下方「Dojao SQL 方言约定」。
            - 图表：只支持 11 种二维图表（bar / line / area / pie / scatter / funnel / treemap / tree / heatmap /
              graph / dataset）；radar / sankey / gauge / map / 3D 等**不支持**，遇到时按下方「图表能力边界」的固定话术致歉，
              不要尝试渲染或用别的类型顶替。
            - ★目前暂不支持、计划在后续版本支持：SEMI JOIN（LEFT / RIGHT SEMI JOIN）与（非 UNNEST 的）CROSS JOIN。
              不要尝试这两种写法 —— 半连接语义用 EXISTS / IN 子查询替代，笛卡尔积改写为带明确 ON 条件的 JOIN；
              `CROSS JOIN UNNEST(...)` 例外，可用。
            - ★JSON 与数组展开：get_json_object / json_extract 取 JSON 路径值；UNNEST(json数组表达式) 把数组展开成多行
              （FROM 表, UNNEST(...) AS u(item) [WITH ORDINALITY]），适合拆嵌套 JSON / 数组结构。
            - 集合运算：支持 UNION / UNION ALL；**不支持 INTERSECT / EXCEPT / MINUS**（规划期会报错）。
              对数里的「取差集」请用 FULL JOIN + IS NULL，或 UNION ALL + GROUP BY + COUNT 实现。
            - 函数：下方「可用 SQL 函数」列出了你能用的**全部**函数；清单里没有的函数不要臆造。
              需要某个函数的详情或调用示例时，用 describe_function 查询。
            """;

    /**
     * Dojao Web 控制台 / Master 对外提供的 HTTP 接口清单 —— 随系统提示词提交给模型。
     *
     * <p>用途：用户询问「有哪些接口 / 怎么用接口拿引擎状态 / 能不能用 curl 拉查询列表」时，
     * 模型能按真实路由、真实参数如实作答（并可给出示例），而不是臆造 URL 或假装调用过。</p>
     *
     * <p><b>★维护约定</b>：{@code DojaoUiApp} 的路由发生变化（新增 / 改名 / 改参数 / 改返回）时，
     * 必须同步更新本段，否则模型会照着过时清单回答。</p>
     */
    static final String DOJAO_HTTP_API = """
            Dojao HTTP 接口清单（Web 控制台与 Master 同源；用于回答「有哪些接口 / 怎么调用」类问题）：
            - ★边界：你自身**不能发起 HTTP 请求**，也不要假装调用过。查数据 / 查状态请用上面的工具
              （它们与下列接口同源、返回内容等价）；本清单只用于如实告诉用户「调哪个接口、参数是什么、返回什么」，
              必要时给出 curl 示例。**不要编造清单之外的 URL**。
            - 鉴权：除登录接口外，所有 /api/* 都需要登录会话（HttpOnly cookie）；
              未登录返回 401 {"error":"未登录或会话已过期"}。先 POST /api/auth/login 获取 cookie。
              未绑定执行引擎（独立预览模式）时，查询 / 监控类接口返回 503。
            - 认证：POST /api/auth/login（body {"username","password"} → 用户信息 + Set-Cookie）、
              POST /api/auth/logout、GET /api/auth/me（当前用户）。
            - SQL 执行：
              · POST /api/query —— 异步提交，body {"sql":"..."}，返回受理的 queryId（多条语句可用分号分隔，串行执行）；
              · POST /api/query/{id}/cancel —— 取消进行中的查询；
              · GET /api/query/{id}/rows?offset=&limit= —— 分页拉取结果预览（limit 默认 200、上限 1000）；
              · POST /api/explain —— body {"sql":"..."}，返回逻辑计划 DAG（只规划不执行）。
            - 引擎可见性（与 Web 监控页同源）：
              · GET /api/engine/status?state=&limit= —— 引擎总览：sampledAt、totalQueries、matchedQueries、
                queryStates{RUNNING,SUBMITTED,FINISHED,FAILED,CANCELED,…}、cluster（Master 与各 Worker 的 CPU / 内存 /
                堆 / 磁盘 / 线程 / 直接内存 / 槽位 / 运行任务 + 活动与最近查询）、queries[]；limit 默认 20、上限 200；
              · GET /api/queries?state=&limit= —— 查询列表（按提交时间倒序）；state 支持 all / active / running /
                submitted / finished / failed / canceled 及中文别名，可逗号组合；limit 默认 0=不限、上限 1000；
                返回 queries[] + total（过滤后条数）+ totalQueries（全部条数）+ state；
              · GET /api/queries/{id} —— 单查询完整快照（含算子 DAG 与任务进度；未知 id 返回 state=UNKNOWN 占位）；
              · GET /api/queries/{id}/operators —— 该查询全部算子（含 children / tasks）+ operatorFailures；
                查询不存在 → 404；
              · GET /api/queries/{id}/operators/{op} —— 单个算子全部指标 + 每任务明细；
                op 可为算子序号 "3" / "#3" 或 operatorId（大小写不敏感）；
              · GET /api/cluster —— 当前集群资源快照（各节点 CPU / 内存 / 堆 / 磁盘 / 线程 / 槽位 / 运行任务）；
              · GET /api/cluster/history?max=N —— 各节点资源时间序列（max 默认 180、上限 1000）；
              · GET /api/logs?node=&tail= —— 只读查看日志尾部；node 默认 master，可填 local / Worker 的 workerId
                （支持唯一前缀）或主机名；tail 默认 200、上限 5000；读取远端 Worker 需 Master 到 Worker 的免密 ssh。
            - 元数据：GET /api/metadata/catalogs；
              GET /api/metadata/catalogs/{catalog}/schemas；
              GET /api/metadata/catalogs/{catalog}/schemas/{schema}/tables；
              GET /api/metadata/catalogs/{catalog}/schemas/{schema}/tables/{table}/columns；
              GET /api/functions（内置 + 当前登录用户可见的自定义函数）。
            - SQL 脚本（每个用户只能看到自己的）：GET /api/scripts、POST /api/scripts（body {"name","content"}，同名覆盖）、
              GET /api/scripts/{id}、DELETE /api/scripts/{id}。
            - AI 对话：GET /api/ai/config、POST /api/ai/config（配置；密钥不回传，POST 留空 = 保留旧值）、
              POST /api/ai/chat（单轮，body {"message"} → {"reply"}）、
              POST /api/ai/chat/stream（NDJSON 流式：text / thinking / tool_call / tool_result / ddl_confirm /
              chart / file / done / error）、POST /api/ai/confirm（{"streamId","approve"}）、
              POST /api/ai/stop（{"streamId"}）、POST /api/ai/files/parse（multipart 字段 file=参考文件，返回解析文本）、
              GET /api/ai/files/{id}（下载导出文件）、GET /api/ai/sessions、POST /api/ai/sessions、
              GET /api/ai/sessions/{id}、DELETE /api/ai/sessions/{id}、
              GET /api/ai/skills、POST /api/ai/skills、GET /api/ai/skills/{id}、
              POST /api/ai/skills/{id}、DELETE /api/ai/skills/{id}。
            - 状态口径：SUBMITTED=排队中、RUNNING=运行中、FINISHED=已完成、FAILED=失败、CANCELED=已取消；
              task 状态另有 RUNNING / FINISHED / FAILED / LOST（失联）。
            """;

    /**
     * 角色边界段 —— 追加在基础系统提示词之后，且<b>优先级高于上文关于集群工具的说明</b>。
     * 管理员 = 开发者 + 分析师能力的并集；开发者可用集群运维类工具；分析师不行。
     */
    static final String ROLE_PROMPT_ADMIN = """
            【当前账号角色：管理员（ADMIN）】
            - 你拥有「开发者」与「分析师」的全部能力：数据探查 / 报表 / 分析与挖掘，以及集群测试、压测、
              日志分析、查询与算子诊断等运维能力（cluster_status / read_logs / list_queries / query_status /
              operator_status）；上述能力对应的工具你都可以使用。压测通过提交查询并结合这些工具观察吞吐与资源。
            - 账号的新建 / 删除 / 角色调整由管理员在 Web 控制台的「用户管理」界面完成。
              你只需指引用户到该界面操作，不要声称自己能直接创建、删除账号或修改口令。
            """;

    static final String ROLE_PROMPT_DEVELOPER = """
            【当前账号角色：开发者（DEVELOPER）】
            - 你可以：数据探查 / 报表 / 数据分析与挖掘；集群测试与压测；日志分析；集群、查询、算子层面的
              诊断（cluster_status / read_logs / list_queries / query_status / operator_status）。
            - 用户询问集群状态、压测结果、日志、某条查询为什么慢 / 失败时，正常调用上述工具回答；
              需要看原始日志时用 read_logs（按节点读日志尾部，再归纳异常 / 报错 / 告警）。
            - 压测没有独立入口：用 execute_sql 提交目标查询，再用 cluster_status / query_status /
              operator_status 观察吞吐、溢写、内存与各 Worker 任务，给出瓶颈判断与调参建议。
            - 账号管理（创建 / 删除用户）仍属于管理员职责：如实说明自己不能操作，并指引到「用户管理」。
            """;

    static final String ROLE_PROMPT_ANALYST = """
            【当前账号角色：分析师（ANALYST）】
            - 你可以：数据探查、编写 SQL 取数、出报表 / 图表、做数据分析与挖掘、解释 SQL 报错并协助修正、
              导入导出文本（附件 / export_file）。
            - 你【没有】开发者 / 运维能力：不得回答或操作与集群测试、压测、日志分析、集群部署与调参、
              节点 / 进程管理、查询与算子内部诊断相关的话题。
            - ★角色边界优先于上文的任何工具说明：上文中关于 cluster_status / read_logs / list_queries /
              query_status / operator_status 的用法对当前角色不适用 —— 不要调用这些工具，也不要猜测集群状态、
              任务队列、节点日志或算子指标。当用户问到开发者范畴的问题时，不要尝试绕道查询，只回下面这句固定话术（不要追加解释）：
              「当前账号是「分析师」角色，只能进行数据探查、报表与数据分析；集群测试、压测、日志分析等开发者能力请联系管理员或使用开发者账号。」
            - 例外（属于你的职责）：用户问「这条 SQL 为什么报错 / 结果不对 / 怎么写」时，正常分析 SQL 与数据并给出
              修正后的 SQL；这与集群运维诊断无关。
            """;

    static final String SYSTEM_PROMPT = """
            你是 Dojao 分布式 SQL 引擎的数据分析助手。你可以调用以下工具：
            - list_catalogs：列出所有数据源（catalog）。
            - list_tables：列出某 catalog / schema 下的表。
            - describe_table：查看某张表的列定义（列名 / 类型 / 注释）。
            - describe_function：查看某个函数的详细信息（来源 / 类别 / 实现语言 / 说明）。你能用的函数清单已列在下方「可用 SQL 函数」中。
            - execute_sql：执行一条只读 SQL（SELECT / SHOW / DESCRIBE / EXPLAIN）并返回结果预览。
              需要评估性能（如对比优化前后、判断某条 SQL 是否过慢）时传 includeTiming=true，结果会附带本次执行的引擎墙钟耗时、输出行数与吞吐。
            - execute_ddl：执行一条 DDL 来变更库表结构。第一版可用：`CREATE TABLE ... AS SELECT`（CTAS）、CREATE / DROP CATALOG、
              CREATE / DROP FUNCTION、DROP TABLE、TRUNCATE TABLE；**CREATE TABLE 显式列定义 / CREATE SCHEMA / ALTER TABLE 未实现**，不要生成。
            - render_chart：把一条只读 SQL 的查询结果渲染成网页图表（柱状 / 折线 / 面积 / 饼图 / 散点 / 漏斗 / 矩形树图 / 树图 / 热力图 / 关系图 / 数据集表格），直接显示在回答里。
            - export_file：把内容、查询结果或图表导出成**可下载的文件**（md / txt / html / csv / excel / png / jpg / pdf），
              生成后回答下方会出现下载按钮。
              文本类（md / txt / html）用 content 传正文：html 可传 Markdown（自动转换），也可直接传完整 HTML 文档
              （含 <style> / <script>，会原样输出，适合带 ECharts 交互图表的报告）；
              表格类（csv / excel）用 data 传结果集引用（如 "rs3" / "last"，优先）或结果集 JSON。
              ★图片（png / jpg）用来保存图表：除 data 外还要给图表参数 type（bar / line / area / pie / scatter）、
              category（X 轴列名）、values（数值列名数组），可用 seriesTypes / seriesAxes 做混合图 / 双 Y 轴。
              ★pdf 可导出「图表页 + 正文 / 表格页」（图表参数可选），也可只导出正文或表格。
              用户要求「下载 / 导出 / 生成报告 / 保存成文件 / 把图表保存成图片或 PDF」时必须用它，而不是只在回答里贴文本。
            - cluster_status：查看 Dojao 集群当前资源与任务负载（各节点 CPU / 内存 / 堆 / 磁盘 / 线程 / 运行任务 / 槽位、活动与最近查询、以及一句「空闲 / 适中 / 繁忙」结论）。提交重型查询前先调用它判断是否繁忙。
            - read_logs：只读查看 Master 或某个 Worker 节点的日志尾部（node 缺省 master，可填 local / workerId（支持唯一前缀）/ 主机名；
              tail 缺省 200、上限 1000）。用于排查异常、报错、溢写 / OOM 告警、任务失败原因与集群启动问题；
              拿到日志后请归纳关键异常与时间线，不要整段回贴。
            - list_queries：查看引擎中的查询任务列表（可按状态筛选：active / running / submitted / finished / failed / canceled，可逗号组合），
              按提交时间倒序，返回 queryId / 状态 / 耗时 / 输出行数 / SQL 摘要。用于回答「现在有哪些查询在跑」「刚才那条成功了吗」「最近有哪些失败」。
            - query_status：查看某条查询的完整执行状态：状态 / 耗时 / 输出行数 / 失败原因 / 执行 DAG 中每个算子
              （类型、状态、并行度、行数与字节吞吐、内存 / 峰值 / 溢写、CPU、耗时、速率、上游算子）。
            - operator_status：查看某条查询的算子明细。省略 operator 时列出全部算子（含任务数汇总）；
              指定 operator（算子序号如 "3" / "#3" 或 operatorId）时返回该算子全部指标 + 每个 Worker 任务
              （状态 / 处理行数 / 耗时 / 失败原因 / 扫描源表）。
            工作方式：先探查有哪些表和字段，再写 SQL 查询，根据查询结果用中文回答用户的问题。
            注意：
            - ★回答边界（必须严格执行）：你的职责是处理与 Dojao、数据库、SQL、数据分析、库表/字段/函数、集群运行状况相关的数据类问题。
              · 问候与寒暄（如「你好」「hi」「hello」「在吗」「谢谢」「好的」「再见」），以及询问「你是谁 / 你能做什么」
                这类 Dojao 自身的问题：**属于职责范围，正常友好地回应**（一两句话即可，可顺带说明你能帮用户
                查数、对数、看表、出图、管库表、看集群），
                **不要套用下面的拒绝模板**，也不必调用工具。
              · 与数据无关的实质性问题（天气、娱乐、写代码、通用知识、情感聊天等）：
                **不要调用任何工具、不要展开解释**，只用下面这句模板作答（把「比如」后的内容换成本轮上下文中
                用户可能感兴趣的一个具体数据问题，不得保留占位符，也不要追加其他话）：
                「Dojao 您这个问题 Dojao 还不太明白，请您找一个其他问题吧，比如<结合上文推测的具体数据问题>」
                示例：上下文刚查过 orders 表 →「……比如查询 orders 表最近 7 天的订单量趋势」；
                上下文是城市销售数据 →「……比如统计各城市的销售额排名」。
                即使历史里聊过数据，也不能把闲聊当数据问题；只回这一句，不再追问。
            - execute_sql 为只读，禁止 INSERT / UPDATE / DELETE / DDL；如需变更结构，改用 execute_ddl。
            - execute_ddl 会在执行前暂停，等待用户在界面上点击「确认执行」；用户可能拒绝，此时应尊重结果并说明。
            - ★行数上限：execute_sql / render_chart 提交的 SELECT / WITH 查询会被**自动加上 LIMIT 500**
              （你写的更小 LIMIT 保持，更大的会被收紧到 500），避免大结果集撑爆网页。
              所以不要写超大 LIMIT；需要精确总行数请单独用 COUNT(*)。
            - 若查询报错，分析原因并改正后重试。若用户只是提问（未要求改结构），不要主动调用 execute_ddl。
            - ★收敛与作答（重要）：拿到足够信息后立即给出最终回答，不要无止境探索；一般 3~6 次工具调用内就应产出结论。
              绝不重复执行同一条（或等价）SQL；同一目的已查过就不再查。用 render_chart 出图后不要对同一份数据反复出图。
              探索完成后必须输出一段完整的 Markdown 结论（哪怕数据有限），不要只留下工具调用轨迹而不作答。
            - 自定义函数（UDF）：你能用的**函数清单**（含内置与当前你可见的用户自定义 UDF）已列在下方「可用 SQL 函数」里，
              不必再去检索。当用户的需求涉及内置函数无法直接完成的能力（例如从图片/文本中提取关键词、调用某个外部模型等），
              先看清单里的「自定义函数（UDF）」段；若某条说明疑似匹配，用 describe_function 确认它的用法，再像普通函数一样在 SQL 里调用。
              你只能使用清单里列出的函数；其他用户创建的私有函数不会出现，也**不得臆造函数名**。
            - 图表：当数据适合可视化（占比用 pie / funnel、趋势用 line/area、对比用 bar、相关关系用 scatter、
              二维分布用 heatmap（另给 yCategory）、层级占比用 treemap / tree、关系网络用 graph（另给 target，
              category=源节点、values[0]=边权重），或用户想要图表时，用 render_chart 出图；
              只想把数据以表格形式展示时用 type=dataset（不需要 category / values）。
              数据来源二选一：sql=只读查询（去引擎取数，须先用 execute_sql 跑通并确认列名，再原样传给 sql），
              或 data=结果集引用/JSON。★execute_sql 每次会返回一个「结果集引用」id（形如 rs3）；
              若你已经用 execute_sql 拿到这份数据，画图时请**直接用该 id 或 "last" 作为 data**（如 data="rs3" / data="last"）——
              render_chart 会复用那份已算好的结果，**既不重算、也不用你回传数据**，比再传一遍 sql 或 JSON 都更省更准。
              （也支持直接传 JSON：{"columns":[...],"rows":[[...],...]} 或行对象数组 [{...},...]。）
              category 填作为 X 轴 / 饼图扇区名的列，values 填数值列的列名数组。同一轮不必对同一份数据反复出图。
              ★混合图：需要「柱状 + 折线」等不同图形叠加时，用 seriesTypes 按 values 顺序指定每条序列的图形
              （可选，缺省用 type）；两条序列量纲差异大（如数量 vs 比率）时用 seriesAxes 按 values 顺序指定
              挂主轴 y 还是副轴 y2（缺省 y），让它们各自用合适的刻度。
            - ★图表能力边界（必须严格执行）：Dojao 只支持以下 11 种图表 —— bar（柱状图）、line（折线图）、
              area（面积图）、pie（饼图）、scatter（散点图）、funnel（漏斗图）、treemap（矩形树图）、tree（树图）、
              heatmap（热力图）、graph（关系图）、dataset（数据集表格），**没有其他图表类型**。
              radar（雷达图）、sankey（桑基图）、gauge（仪表盘）、map（地图）、3D 图表，以及任何未列出的类型都**不支持**。
              当用户点名要一个不支持的图表时：**不要调用 render_chart、不要传非法 type、也不要用其他类型顶替**，
              只回一句固定话术（把「<图表名>」替换成用户要的图表名）：
              「抱歉，Dojao 目前只支持柱状图、折线图、面积图、饼图、散点图、漏斗图、矩形树图、树图、热力图、关系图和数据集表格，暂不支持<图表名>。」
              如确有必要，可在这句后补一句最接近的支持类型作为替代建议（例如「占比可以用饼图或漏斗图展示」），
              但不得实际尝试渲染不支持的图表。不要把 export_file 的图片 / PDF 导出当作绕过限制的手段。
            - ★集群感知：Dojao 是分布式引擎，提交查询会占用集群资源。当查询较重（大表扫描 / 无过滤 / 聚合 / 多表连接）时，
              先调用 cluster_status 查看集群负载；若结论为「繁忙」，本服务会自动拒绝本次提交并提示稍后重试——
              此时应如实告知用户当前集群繁忙、建议稍后再试，或改用更轻量的查询（更窄的过滤条件 / LIMIT），
              不要反复提交同一条重查询。
            - ★引擎可见性与诊断：你可以查看引擎与查询的实时状态（与 Web 监控页同源）——
              · 系统层面（CPU / 磁盘 / 内存 / 堆 / 线程 / 槽位）：cluster_status；
              · 节点原始日志（异常 / 报错 / 告警 / 启动与部署问题）：read_logs(node, tail)；
              · 查询层面（运行中 / 已完成 / 失败 / 已取消 + 提交时间、耗时、输出行数）：list_queries(state=...);
              · 单查询详情（状态、失败原因、每个算子的行/字节/内存/溢写/CPU/速率）：query_status(queryId);
              · 单个算子详情（各 Worker 任务进度、失败原因、扫描源表）：operator_status(queryId, operator)。
              当用户问「系统/集群现在什么状态」「有哪些查询在跑 / 失败了」「这条查询卡在哪 / 为什么慢 / 为什么失败」时：
              先用 list_queries 拿到 queryId，再 query_status 看算子级指标，必要时 operator_status 下钻到具体 Worker 任务；
              回答要给可操作的判断（如哪个算子溢写或内存高、哪个任务失败在哪台 Worker、是否是数据倾斜/排队），
              不要臆测未观测到的数据。查不到 queryId 时如实说明并列出最近的查询供用户选择。
            - ★跨库对数（数据校验 / 对账）：Dojao 支持在一个 SQL 里直接比对不同数据源（catalog）的表。
              用户提到「对数 / 对账 / 跨库校验 / 数据一致性 / 两边数据对不对得上」时，按下面的层次推进：
              · 对齐口径（先问清或从上下文确认）：核对哪两（几）张表、主键 / 关联键、时间范围与过滤条件、
                金额 / 计数的精度口径；两侧过滤条件必须等价，否则先说明差异来自口径而非数据。
              · ① 总量级：两侧分别 COUNT(*)，或一条 SQL 用 UNION ALL + 来源标记列分组对比；
                指标类再比 SUM / 分组汇总，避免拉明细（SELECT 会被自动加 LIMIT 500，总量必须用聚合）。
              · ② 明细级：按主键 FULL JOIN，用 CASE 把行分类为「仅左有 / 仅右有 / 值不一致 / 一致」后 GROUP BY 计数，
                再抽样列出差异行；键可能重复时先去重或先聚合，避免一条键放大成多行造成假差异。
              · ③ 存在性 / 范围校验：EXISTS / IN 子查询（支持跨源），检查某侧是否缺少对方的主键集合。
              · ④ 结论：给出「一致 / 不一致」判断、差异条数与占比、按维度的差异分布、典型差异样例，
                并区分「真实数据差异」与「口径 / NULL / 类型精度 / 重复键 / 时区导致的假差异」。
              边界：不支持 INTERSECT / EXCEPT / MINUS —— 差集用 FULL JOIN + IS NULL 或 UNION ALL + GROUP BY 实现；
              跨源大表先 cluster_status 看负载；只读 SELECT，不要尝试 UPDATE / DELETE。
              若「可用 SQL 函数」里有 md5 / sha2 / hash，可用「按主键分组取摘要」做汇总指纹比对，
              但最终差异仍要落到具体键上。

            """ + "\n\n" + DOJAO_HTTP_API + "\n\n" + DOJAO_SQL_DIALECT + """

            输出格式（必须严格遵守）：
            - 最终回答必须是规范的 Markdown（GFM）文本：用 `##` / `###` 标题分段，用有序或无序列表罗列要点，
              用 Markdown 表格展示对比或汇总数据，用 ```sql 代码块包裹关键 SQL。
            - 直接输出 Markdown 正文本身；不要用 JSON 包裹，不要输出 HTML 标签，也不要用代码围栏包住整段回答。
            - 先用一两句话给出结论，再按需展开；只讲结论与必要说明，不要复述工具调用的原始过程。
            """;

    private final AiConfig config;
    private final QueryMonitor monitor;   // 可为 null（独立预览模式）
    private final MetaStore metaStore;
    private final ObjectMapper mapper = new ObjectMapper();

    /** {@code export_file} 生成文件的临时存储（按 viewer 隔离，下载路由与工具共用同一实例）。 */
    private final AiFileStore fileStore;

    private volatile StreamingChatModel model;

    /**
     * 每用户（viewer）最近若干次只读查询结果的缓存：{@code execute_sql} 跑完把 (列, 行) 存一份并回一个引用 id，
     * {@code render_chart} 用 {@code data="<id>"} / {@code data="last"} 直接复用，**模型无需回吐数据、也不重算**。
     * 按 viewer 隔离；每 viewer 保留最近 {@link #RESULT_CACHE_MAX} 份。
     */
    private final Map<String, ViewerResults> resultCache = new ConcurrentHashMap<>();

    /** 独立预览 / 探针用：自建一个临时文件存储。 */
    public AiAgentService(AiConfig config, QueryMonitor monitor, MetaStore metaStore) {
        this(config, monitor, metaStore, new AiFileStore());
    }

    /**
     * @param fileStore 导出文件存储（由 UI 持有，下载路由按同一个实例取文件）；为 null 时自建一个
     */
    public AiAgentService(AiConfig config, QueryMonitor monitor, MetaStore metaStore, AiFileStore fileStore) {
        this.config = config;
        this.monitor = monitor;
        this.metaStore = metaStore;
        this.fileStore = fileStore == null ? new AiFileStore() : fileStore;
    }

    /** 关闭导出文件存储（应用停止时调用）。 */
    public void close() {
        fileStore.close();
    }

    /**
     * 跑一轮 ReAct 推理（阻塞），流式事件回调给 {@code listener}。
     *
     * @param message  本轮用户提问
     * @param history  历史会话消息（UserMessage / AiMessage 交替），可为 null / 空 = 单轮
     * @param viewer   当前用户（用于 catalog / 函数隔离）
     * @param listener 流式事件接收器
     * @param confirm  DDL 执行前的用户确认回调（可为 null = 不允许执行 DDL）
     * @param chartSink 图表输出回调（可为 null = 不允许渲染图表）
     */
    public void run(String message, List<ChatMessage> history, String viewer,
                    ReActListener listener, DdlConfirm confirm, ChartSink chartSink) {
        run(message, history, viewer, listener, confirm, chartSink, null, null);
    }

    /** 带文件导出回调（无技能）的重载。 */
    public void run(String message, List<ChatMessage> history, String viewer,
                    ReActListener listener, DdlConfirm confirm, ChartSink chartSink, FileSink fileSink) {
        run(message, history, viewer, listener, confirm, chartSink, fileSink, null);
    }

    /**
     * 跑一轮 ReAct 推理（阻塞，带可选 {@link Skill}），流式事件回调给 {@code listener}。
     *
     * <p>相比不带 skill 的重载：系统提示词追加 Skill 的自定义指令，工具集按 Skill 的白名单过滤。</p>
     *
     * @param fileSink 导出文件回调（可为 null = 不允许导出文件）
     * @param skill 本次使用的技能（可为 null = 无技能，用基础提示词与全部工具）
     */
    public void run(String message, List<ChatMessage> history, String viewer,
                    ReActListener listener, DdlConfirm confirm, ChartSink chartSink, FileSink fileSink,
                    Skill skill) {
        run(message, history, viewer, listener, confirm, chartSink, fileSink, skill, Role.ADMIN);
    }

    /**
     * 跑一轮 ReAct 推理（带技能与账号角色）。
     *
     * <p>角色决定两件事：①系统提示词里的角色边界段（分析师拒绝集群开发类问题）；
     * ②工具集 —— 分析师会被剔除 {@link #DEVELOPER_TOOLS}，即使技能白名单里写了也不会下发。
     * 技能白名单只能在此之上进一步收窄。</p>
     */
    public void run(String message, List<ChatMessage> history, String viewer,
                    ReActListener listener, DdlConfirm confirm, ChartSink chartSink, FileSink fileSink,
                    Skill skill, Role role) {
        Role effective = role == null ? Role.ANALYST : role;
        List<AiTool> tools = filterToolsForRole(buildTools(viewer, confirm, chartSink, fileSink), effective);
        tools = filterTools(tools, skill == null ? null : skill.tools());
        // 前置告知「引擎能力 + 全部可用函数」——让模型一次性建立认知，避免反复试探/重试浪费 token。
        List<MonitorFunction> funcs = monitor == null ? List.of() : monitor.listFunctions(viewer);
        String base = composeBasePrompt(SYSTEM_PROMPT, funcs, effective);
        ReActAgent agent = new ReActAgent(model(), tools, composeSystemPrompt(base, skill), MAX_ITERATIONS);
        agent.run(history, message, listener);
    }

    /**
     * 合成本轮系统提示词（纯函数，<b>离线可测</b>）：基础提示词 + 技能段。
     * 技能为空或其提示词为空 → 原样返回基础提示词。
     */
    static String composeSystemPrompt(String base, Skill skill) {
        if (skill == null || skill.prompt() == null || skill.prompt().isBlank()) {
            return base;
        }
        String name = skill.name() == null ? "" : skill.name().strip();
        String header = name.isEmpty() ? "【当前技能】" : "【当前技能：" + name + "】";
        return base + "\n\n" + header + "\n" + skill.prompt().strip();
    }

    /**
     * 在基础系统提示词后追加「引擎能力 / 边界」与「当前可见的全部 SQL 函数清单」两段（纯函数，<b>离线可测</b>）。
     *
     * <p>动机：让模型一次性知道 Dojao 能干什么、支持哪些函数，避免靠反复调用工具 / 反复试错来摸索（耗 token、慢）。
     * {@code funcs} 应由调用方提供<b>已按用户隔离</b>的结果（{@code monitor.listFunctions(viewer)}）；为 null/空（如独立预览模式）
     * 时只追加能力段，不追加空的函数清单。</p>
     */
    static String composeBasePrompt(String systemPrompt, List<MonitorFunction> funcs) {
        return composeBasePrompt(systemPrompt, funcs, Role.ADMIN);
    }

    /**
     * 在基础系统提示词后追加「角色边界 + 引擎能力 + 函数清单」（纯函数，<b>离线可测</b>）。
     *
     * <p>角色段紧随基础提示词，且措辞为「角色边界优先于上文的工具说明」—— 分析师因此会拒绝
     * cluster_status 一类开发话题，即使基础提示词里写着这些工具怎么用。</p>
     */
    static String composeBasePrompt(String systemPrompt, List<MonitorFunction> funcs, Role role) {
        String base = systemPrompt + "\n\n" + rolePrompt(role) + "\n\n" + DOJAO_CAPABILITIES;
        String catalog = buildFunctionCatalog(funcs);
        return catalog.isEmpty() ? base : base + "\n\n" + catalog;
    }

    /** 角色边界段（未知 / null 按分析师处理，最小权限）。 */
    static String rolePrompt(Role role) {
        Role effective = role == null ? Role.ANALYST : role;
        return switch (effective) {
            case ADMIN -> ROLE_PROMPT_ADMIN;
            case DEVELOPER -> ROLE_PROMPT_DEVELOPER;
            case ANALYST -> ROLE_PROMPT_ANALYST;
        };
    }

    /**
     * 按角色过滤工具集（纯函数，<b>离线可测</b>）：分析师剔除 {@link #DEVELOPER_TOOLS}，
     * 管理员 / 开发者原样返回。
     */
    static List<AiTool> filterToolsForRole(List<AiTool> tools, Role role) {
        if (tools == null || role == null || role.canUseClusterTools()) {
            return tools;
        }
        List<AiTool> out = new ArrayList<>();
        for (AiTool t : tools) {
            if (!DEVELOPER_TOOLS.contains(t.name())) {
                out.add(t);
            }
        }
        return out;
    }

    /**
     * 按技能的工具白名单过滤工具集（纯函数，<b>离线可测</b>）。
     * 白名单为空 / null → 返回全部工具；白名单与现有工具无交集（配置异常）→ 也返回全部工具，
     * 绝不产出空工具集（空集会让模型无从下手）。
     */
    static List<AiTool> filterTools(List<AiTool> tools, List<String> names) {
        if (tools == null || names == null || names.isEmpty()) {
            return tools;
        }
        Set<String> allow = new HashSet<>(names);
        List<AiTool> out = new ArrayList<>();
        for (AiTool t : tools) {
            if (allow.contains(t.name())) {
                out.add(t);
            }
        }
        return out.isEmpty() ? tools : out;
    }

    /** 惰性构造流式模型（首次调用时建，之后复用）。 */
    private StreamingChatModel model() {
        StreamingChatModel m = model;
        if (m == null) {
            synchronized (this) {
                m = model;
                if (m == null) {
                    m = OpenAiStreamingChatModel.builder()
                            .baseUrl(config.baseUrl())
                            .apiKey(config.apiKey())
                            .modelName(config.model())
                            .temperature(config.temperature())
                            .timeout(Duration.ofSeconds(120))
                            .build();
                    model = m;
                }
            }
        }
        return m;
    }

    // ==================== 工具 ====================

    private List<AiTool> buildTools(String viewer, DdlConfirm confirm, ChartSink chartSink,
                                    FileSink fileSink) {
        AiTool executeSql = new AiTool("execute_sql",
                "执行一条只读 SQL（SELECT / SHOW / DESCRIBE / EXPLAIN），返回列名与前若干行结果。"
                        + "禁止 INSERT / UPDATE / DELETE / DDL。"
                        + "需要做性能评估 / 优化前后对比时传 includeTiming=true，结果会附带本次执行的"
                        + "墙钟耗时、输出行数与吞吐。",
                JsonObjectSchema.builder()
                        .addStringProperty("sql", "要执行的只读 SQL")
                        .addBooleanProperty("includeTiming",
                                "是否在结果中附带本次 SQL 的执行耗时（引擎墙钟毫秒 + 输出行数 + 行/秒），"
                                        + "用于性能评估 / 优化对比；默认 false，普通取数不必开启")
                        .required("sql").build(),
                args -> executeSql(arg(args, "sql"), argBoolean(args, "includeTiming"), viewer));

        AiTool executeDdl = new AiTool("execute_ddl",
                "执行一条 DDL，用于变更库表结构。第一版支持：CREATE TABLE ... AS SELECT（CTAS）、"
                        + "CREATE / DROP CATALOG、CREATE / DROP FUNCTION、DROP TABLE、TRUNCATE TABLE。"
                        + "不支持 CREATE TABLE 显式列定义 / CREATE·DROP SCHEMA / ALTER TABLE / CREATE·DROP VIEW（会报暂不支持）。"
                        + "执行前会请用户在界面上确认。",
                JsonObjectSchema.builder()
                        .addStringProperty("sql", "要执行的 DDL 语句")
                        .required("sql").build(),
                args -> executeDdl(arg(args, "sql"), viewer, confirm));

        AiTool renderChart = new AiTool("render_chart",
                "把查询结果（或已算好的结果集）渲染成网页图表，直接显示在回答里（不改变数据、只做可视化）。"
                        + "适用：占比用 pie / funnel，趋势用 line / area，对比用 bar，相关关系用 scatter，"
                        + "二维分布用 heatmap（另需 yCategory），层级占比用 treemap / tree，"
                        + "关系网络用 graph（另需 target），只想以表格查看数据用 dataset。"
                        + "数据来源二选一：sql=一条只读查询（会提交到引擎取数）；"
                        + "或 data=结果集引用/JSON —— ★优先用 execute_sql 返回的「结果集引用」id（如 \"rs3\"）或 \"last\""
                        + "（最新一次结果）来复用已算好的数据，**这样既不重算、也不用回传数据**；"
                        + "也可直接给 JSON（{\"columns\":[\"a\",\"b\"],\"rows\":[[\"x\",\"1\"],[\"y\",\"2\"]]} 或行对象数组 [{...},...]）。"
                        + "参数：type=图表类型、category=作为 X 轴（或扇区/节点源）的列名、"
                        + "values=作为数值序列的列名数组（pie / funnel / treemap / tree / heatmap / graph 只用第一个）、"
                        + "title=图表标题（可选）。heatmap 用 yCategory 指定 Y 轴分类列（二维网格）；"
                        + "graph 用 target 指定边的目标节点列（category 为源节点，values[0] 为边权重）。"
                        + "dataset 不需要 category / values，会把结果集直接以表格卡片展示。"
                        + "★混合图：若要把多条序列画成不同图形（例如柱状 + 折线叠加），用 seriesTypes 按 values 顺序"
                        + "逐个指定每条的图形（可选，取值 bar / line / area / scatter，缺省用 type）；"
                        + "若两条序列量纲差异大（如「数量」与「百分比/比率」同图），用 seriesAxes 按 values 顺序指定"
                        + "每条挂主轴还是副轴（可选，y=主 Y 轴、y2=副 Y 轴，缺省 y）。"
                        + "★只支持上述 11 种二维图表；radar / sankey / gauge / map / 3D 等不支持 —— "
                        + "用户点名不支持的图表时不要调用本工具，按系统提示词的固定话术致歉。"
                        + "用 sql 时务必先用 execute_sql 验证 sql 可正确执行并确认列名。",
                JsonObjectSchema.builder()
                        .addEnumProperty("type", CHART_TYPES, "图表类型（也是各序列未单独指定时的默认图形）")
                        .addStringProperty("sql", "只读 SQL 查询（SELECT / SHOW / DESCRIBE / EXPLAIN）；与 data 二选一")
                        .addStringProperty("data", "结果集引用或 JSON（与 sql 二选一）："
                                + "★推荐用 execute_sql 返回的引用 id（如 \"rs3\"）或 \"last\" 复用已算好的数据（免重算、免回传）；"
                                + "也可直接给 {\"columns\":[...],\"rows\":[[...],...]} 或行对象数组 [{...},...]")
                        .addStringProperty("category", "作为 X 轴 / 扇区名 / 节点源（graph）的列名；dataset 可省略")
                        .addStringProperty("yCategory", "heatmap 专用：作为 Y 轴分类的列名（与 category 组成二维网格）；"
                                + "其他类型可省略")
                        .addStringProperty("target", "graph 专用：关系边的目标节点列名（category 为源节点）；其他类型可省略")
                        .addProperty("values", JsonArraySchema.builder()
                                .description("作为数值序列的列名数组（pie / funnel / treemap / tree / heatmap / graph 只用第一个）；"
                                        + "dataset 可省略")
                                .items(JsonStringSchema.builder().build())
                                .build())
                        .addProperty("seriesTypes", JsonArraySchema.builder()
                                .description("每条数值序列各自的图形，按 values 顺序一一对应（可选；取值 "
                                        + String.join(" / ", SERIES_CHART_TYPES) + "，缺省用 type；"
                                        + "仅 bar / line / area / scatter 支持逐条混合）")
                                .items(JsonStringSchema.builder().build())
                                .build())
                        .addProperty("seriesAxes", JsonArraySchema.builder()
                                .description("每条数值序列挂哪个 Y 轴，按 values 顺序一一对应（可选；y=主 Y 轴、y2=副 Y 轴，缺省 y）")
                                .items(JsonStringSchema.builder().build())
                                .build())
                        .addStringProperty("title", "图表标题（可选）")
                        .required("type").build(),
                args -> renderChart(arg(args, "type"), arg(args, "sql"), arg(args, "data"), arg(args, "category"),
                        argList(args, "values"), argList(args, "seriesTypes"), argList(args, "seriesAxes"),
                        arg(args, "yCategory"), arg(args, "target"),
                        arg(args, "title"), viewer, chartSink));

        AiTool exportFile = new AiTool("export_file",
                "把内容、查询结果或**图表**导出成可下载的文件，生成后回答下方会出现下载按钮，用户点击即可保存到本地。"
                        + "支持 md / txt / html / csv / excel / png / jpg / pdf。"
                        + "文本类（md / txt / html）用 content 传正文：html 可传 Markdown（自动转成网页），"
                        + "也可直接传完整 HTML 文档（含 <style> / <script>，会原样输出，适合带 ECharts 图表的报告）；"
                        + "表格类（csv / excel）用 data 传表格数据。"
                        + "★图片（png / jpg）：把图表保存成图片，需要图表参数 type（bar / line / area / pie / scatter）、"
                        + "category（X 轴列名）、values（数值列名数组），数据同样用 data 提供；"
                        + "seriesTypes / seriesAxes 可选（同 render_chart，用于混合图 / 双 Y 轴）。"
                        + "★PDF：可导出「图表页 + 正文/表格页」，也可只导出正文或表格。"
                        + "data 一律 ★优先用 execute_sql 返回的结果集引用（如 \"rs3\"）或 \"last\"，也可传结果集 JSON。"
                        + "用户说「下载 / 导出 / 生成报告 / 保存成文件 / 把图表保存成图片或 PDF」时必须调用本工具，"
                        + "不要只在回答里贴文本。",
                JsonObjectSchema.builder()
                        .addEnumProperty("format", EXPORT_FORMATS,
                                "文件格式：md=Markdown、txt=纯文本、html=网页、csv=逗号分隔表、excel=Excel(.xlsx)、"
                                        + "png / jpg=图表图片、pdf=PDF 文档（图表 + 正文/表格）")
                        .addStringProperty("filename", "文件名（不含扩展名，可留空；留空时用 title 或 dojao-export）")
                        .addStringProperty("title", "文档标题（可选；md/html/pdf 用作标题，也参与默认文件名）")
                        .addStringProperty("content", "文本正文（md / txt / html 用；html 也可直接给完整 HTML 文档，"
                                + "含 <style>/<script> 时原样输出；csv 在无 data 时可直接给 CSV 文本；"
                                + "pdf 可选，与图表一起排版）")
                        .addStringProperty("data", "表格 / 图表数据："
                                + "★推荐 execute_sql 返回的结果集引用 id（如 \"rs3\"）或 \"last\"；"
                                + "也可给 {\"columns\":[...],\"rows\":[[...],...]} 或行对象数组 [{...},...]")
                        .addEnumProperty("type", EXPORT_CHART_TYPES,
                                "仅 png / jpg / pdf 图表用：图表类型（bar / line / area / pie / scatter；缺省 bar；"
                                        + "漏斗 / 热力图 / 树图 / 关系图等暂仅支持网页渲染）")
                        .addStringProperty("category", "仅 png / jpg / pdf 图表用：作为 X 轴 / 饼图扇区名的列名")
                        .addProperty("values", JsonArraySchema.builder()
                                .description("仅 png / jpg / pdf 图表用：作为数值序列的列名数组（pie 只用第一个）")
                                .items(JsonStringSchema.builder().build())
                                .build())
                        .addProperty("seriesTypes", JsonArraySchema.builder()
                                .description("仅图表用：每条数值序列各自的图形，按 values 顺序一一对应（可选；"
                                        + String.join(" / ", SERIES_CHART_TYPES) + "，缺省用 type）")
                                .items(JsonStringSchema.builder().build())
                                .build())
                        .addProperty("seriesAxes", JsonArraySchema.builder()
                                .description("仅图表用：每条数值序列挂哪个 Y 轴，按 values 顺序一一对应（可选；y 主 / y2 副，缺省 y）")
                                .items(JsonStringSchema.builder().build())
                                .build())
                        .required("format").build(),
                args -> exportFile(arg(args, "format"), arg(args, "filename"), arg(args, "title"),
                        arg(args, "content"), arg(args, "data"), arg(args, "type"), arg(args, "category"),
                        argList(args, "values"), argList(args, "seriesTypes"), argList(args, "seriesAxes"),
                        viewer, fileSink));

        AiTool listCatalogs = new AiTool("list_catalogs",
                "列出所有可用的数据源（catalog）名称。",
                JsonObjectSchema.builder().build(),
                args -> joinOrNone(listCatalogs(viewer)));

        AiTool listTables = new AiTool("list_tables",
                "列出指定 catalog（及可选 schema）下的所有表名。",
                JsonObjectSchema.builder()
                        .addStringProperty("catalog", "catalog 名")
                        .addStringProperty("schema", "schema 名，可留空")
                        .required("catalog").build(),
                args -> joinOrNone(listTables(arg(args, "catalog"), arg(args, "schema"))));

        AiTool describeTable = new AiTool("describe_table",
                "查看指定表的列定义（列名 / 类型 / 注释）。",
                JsonObjectSchema.builder()
                        .addStringProperty("catalog", "catalog 名")
                        .addStringProperty("schema", "schema 名，可留空")
                        .addStringProperty("table", "表名")
                        .required("catalog", "table").build(),
                args -> describeTable(arg(args, "catalog"), arg(args, "schema"), arg(args, "table")));

        AiTool describeFunction = new AiTool("describe_function",
                "查看某个函数的详细信息（来源 / 类别 / 实现语言 / 说明）。只能查看你有权限使用的函数，"
                        + "其他用户的私有函数会被拒绝。你能用的全部函数清单已在系统提示词中列出。",
                JsonObjectSchema.builder()
                        .addStringProperty("name", "函数名")
                        .required("name").build(),
                args -> describeFunction(arg(args, "name"), viewer));

        AiTool clusterStatus = new AiTool("cluster_status",
                "查看 Dojao 集群当前资源与任务负载：各节点 CPU / 内存 / 堆 / 磁盘 / 线程 / 运行任务 / 槽位，"
                        + "活动与最近查询，以及一句「空闲 / 适中 / 繁忙」结论。提交重型查询前先调用它判断是否繁忙。",
                JsonObjectSchema.builder().build(),
                args -> clusterStatus());

        AiTool readLogs = new AiTool("read_logs",
                "只读查看 Dojao 节点日志尾部（dojao.out），用于排查异常、报错、溢写 / OOM 告警、任务失败与集群启动问题。"
                        + "node 缺省 master，可填 local / Worker 的 workerId（支持唯一前缀）/ 主机名；"
                        + "tail 为返回行数（默认 200，上限 1000）。远端 Worker 通过 Master 的免密 ssh 读取。",
                JsonObjectSchema.builder()
                        .addStringProperty("node", "节点：master（缺省）/ local / Worker 的 workerId（支持唯一前缀）/ 主机名")
                        .addStringProperty("tail", "返回最后多少行（可选，默认 200，上限 1000）")
                        .build(),
                args -> readLogs(arg(args, "node"), arg(args, "tail")));

        AiTool listQueries = new AiTool("list_queries",
                "查看 Dojao 引擎中的查询任务列表（运行中 / 排队中 / 已完成 / 失败 / 已取消），按提交时间倒序，"
                        + "返回 queryId / 状态 / 耗时 / 输出行数 / SQL 摘要。可按状态筛选。"
                        + "拿到 queryId 后可再用 query_status / operator_status 查看算子级详情。",
                JsonObjectSchema.builder()
                        .addStringProperty("state",
                                "状态筛选（可选，默认 all）：all=全部、active=活跃（排队+运行）、running=运行中、"
                                        + "submitted=排队中、finished=已完成、failed=失败、canceled=已取消；"
                                        + "也可用逗号组合多个（如 failed,canceled）")
                        .addStringProperty("limit", "最多返回条数（可选，默认 20，上限 100）")
                        .build(),
                args -> listQueries(arg(args, "state"), arg(args, "limit")));

        AiTool queryStatus = new AiTool("query_status",
                "查看某条查询的完整执行状态：状态 / 墙钟耗时 / 输出行数 / 失败原因 / 执行 DAG 中每个算子的"
                        + "类型、状态、并行度、行数与字节吞吐、内存（峰值）、溢写、CPU、耗时、速率与上游算子。"
                        + "用于诊断查询卡在哪、哪个算子溢写或内存高、失败根因。",
                JsonObjectSchema.builder()
                        .addStringProperty("queryId", "查询 id（来自 list_queries，或 execute_sql 返回的信息）")
                        .required("queryId").build(),
                args -> queryStatus(arg(args, "queryId")));

        AiTool operatorStatus = new AiTool("operator_status",
                "查看某条查询的算子执行明细。省略 operator 时列出全部算子（含每个算子的任务数汇总）；"
                        + "指定 operator 时返回该算子的全部指标与每个分布式任务（Worker、状态、处理行数、"
                        + "耗时、失败原因、扫描源表、预计行数）。",
                JsonObjectSchema.builder()
                        .addStringProperty("queryId", "查询 id")
                        .addStringProperty("operator",
                                "算子序号（如 \"3\" / \"#3\"）或 operatorId；省略则列出全部算子")
                        .addStringProperty("tasks",
                                "是否包含分布式任务明细（指定 operator 时生效，默认 true：true / false）")
                        .required("queryId").build(),
                args -> operatorStatus(arg(args, "queryId"), arg(args, "operator"),
                        argBooleanDefault(args, "tasks", true)));

        return List.of(executeSql, executeDdl, renderChart, exportFile, listCatalogs, listTables,
                describeTable, describeFunction, clusterStatus, readLogs,
                listQueries, queryStatus, operatorStatus);
    }

    /** 查看查询列表：读 {@code monitor.listQueries()} 并按状态过滤 / 截断。 */
    private String listQueries(String state, String limitRaw) {
        if (monitor == null) {
            return "独立预览模式不支持查看查询列表（未连接集群）。";
        }
        int limit = MonitorViews.clampLimit(limitRaw, 20, 100);
        return formatQueryList(state, monitor.listQueries(), limit);
    }

    /** 查看单查询状态：读 {@code monitor.snapshot(queryId)} 并格式化成给模型的文本。 */
    private String queryStatus(String queryId) {
        if (monitor == null) {
            return "独立预览模式不支持查看查询状态（未连接集群）。";
        }
        if (queryId == null || queryId.isBlank()) {
            return "queryId 为空。请先用 list_queries 查看最近的查询 id。";
        }
        return formatQueryStatus(monitor.snapshot(queryId.strip()));
    }

    /** 查看算子明细：省略 operator → 全部算子概览；指定 → 单个算子的全部指标与任务。 */
    private String operatorStatus(String queryId, String operator, boolean includeTasks) {
        if (monitor == null) {
            return "独立预览模式不支持查看算子状态（未连接集群）。";
        }
        if (queryId == null || queryId.isBlank()) {
            return "queryId 为空。请先用 list_queries 查看最近的查询 id。";
        }
        MonitorQuery q = monitor.snapshot(queryId.strip());
        if (!MonitorViews.exists(q)) {
            return formatQueryStatus(q);
        }
        if (q.operators() == null || q.operators().isEmpty()) {
            return "查询 " + q.queryId() + " 暂无算子明细（可能尚在提交/规划阶段，或是 EXPLAIN、极早取消的查询）。"
                    + "可稍后用 query_status 再看一次。";
        }
        if (operator == null || operator.isBlank()) {
            return formatOperatorList(q);
        }
        MonitorOperator op = MonitorViews.findOperator(q, operator);
        if (op == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("未找到算子 \"").append(operator).append("\"。该查询可用的算子：\n");
            for (MonitorOperator o : q.operators()) {
                sb.append("  #").append(o.idx()).append(' ').append(o.type())
                        .append(" [").append(stateLabel(o.state())).append("]\n");
            }
            sb.append("提示：operator 可填算子序号（如 \"3\" / \"#3\"）或 operatorId。");
            return sb.toString();
        }
        return formatOperatorDetail(q, op, includeTasks);
    }

    /** 查看集群状态：读 {@code monitor.cluster()} 并格式化成给模型的文本。 */
    private String clusterStatus() {
        if (monitor == null) {
            return "独立预览模式不支持查看集群状态（未连接集群）。";
        }
        return formatClusterStatus(monitor.cluster());
    }

    /**
     * 查看节点日志尾部（开发者 / 管理员工具）。
     *
     * <p>节点只能从集群快照里挑（{@link LogViewer#resolve}），与 {@code GET /api/logs} 同一套
     * 解析与读取口径；本机直接读文件，远端 Worker 走 Master→Worker 免密 ssh。返回给模型的
     * 行数上限比界面更小（{@link #AI_LOG_TAIL_MAX}），超长按尾部截断，避免撑爆上下文。</p>
     */
    private String readLogs(String node, String tailRaw) {
        if (monitor == null) {
            return "独立预览模式不支持查看节点日志（未连接集群）。";
        }
        LogViewer.Target target = LogViewer.resolve(monitor.cluster(), node);
        if (target == null) {
            return "未找到节点「" + (node == null || node.isBlank() ? "master" : node.trim())
                    + "」：可用 master / local / Worker 的 workerId（支持唯一前缀）/ 主机名。";
        }
        int tail = MonitorViews.clampLimit(tailRaw, LogViewer.DEFAULT_TAIL, AI_LOG_TAIL_MAX);
        Path path = LogViewer.logPath();
        List<String> lines;
        try {
            lines = target.local()
                    ? LogViewer.tailLocal(path, tail)
                    : LogViewer.tailRemote(target.host(), path, tail);
        } catch (IOException e) {
            return "读取节点 " + target + " 的日志失败（路径 " + path + "）：" + e.getMessage();
        } catch (RuntimeException e) {
            return "读取节点 " + target + " 的日志失败：" + e.getMessage();
        }
        if (lines.isEmpty()) {
            return "节点 " + target + " 的日志为空（路径 " + path + "）。";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("节点 ").append(target).append(" 日志尾部 ").append(lines.size())
                .append(" 行（").append(path).append("）：\n");
        String body = String.join("\n", lines);
        if (body.length() > AI_LOG_MAX_CHARS) {
            body = "…（前部已截断，仅保留最近 " + AI_LOG_MAX_CHARS + " 字符）\n"
                    + body.substring(body.length() - AI_LOG_MAX_CHARS);
        }
        sb.append(body);
        return sb.toString();
    }

    /** 查看单个函数详情（限定在 {@code viewer} 可见范围内）。 */
    private String describeFunction(String name, String viewer) {
        if (monitor == null) {
            return "独立预览模式不支持查看函数（未连接集群）。";
        }
        return describeFunctionFrom(monitor.listFunctions(viewer), name);
    }

    /**
     * 执行只读 SQL：先过只读白名单，再提交执行。
     *
     * @param includeTiming 为 true 时在结果末尾附上本次执行耗时（供 AI 做性能评估）
     */
    String executeSql(String sql, boolean includeTiming, String viewer) {
        if (sql == null || sql.isBlank()) {
            return "SQL 为空。";
        }
        if (!isReadOnlySql(sql)) {
            return "只读模式：仅允许 SELECT / WITH / SHOW / DESCRIBE / EXPLAIN，已拒绝执行。";
        }
        return runSql(applyAiRowLimit(sql), viewer, includeTiming);
    }

    /**
     * 执行 DDL：先过 DDL 白名单，再请用户确认（{@link DdlConfirm}），确认后才提交执行。
     */
    private String executeDdl(String sql, String viewer, DdlConfirm confirm) {
        if (sql == null || sql.isBlank()) {
            return "DDL 为空。";
        }
        if (!isDdlSql(sql)) {
            return "execute_ddl 仅支持 DDL（CREATE / DROP / ALTER / TRUNCATE）；请改用 execute_sql 执行查询。";
        }
        if (confirm == null) {
            return "当前环境不支持执行 DDL（无法请求用户确认）。";
        }
        if (!confirm.confirm(sql)) {
            return "用户未确认执行该 DDL，已取消。";
        }
        return runSql(sql, viewer, false);
    }

    /**
     * 渲染图表：数据来源二选一 —— ①{@code data}（已算好的结果集 JSON，直接复用、不重算）；
     * ②{@code sql}（提交只读查询取数，SQL 先经 {@link #applyAiRowLimit} 加 LIMIT 500，
     * 结果再按 {@link #CHART_MAX_ROWS} 截断）。随后构造 spec 推给前端。
     */
    String renderChart(String type, String sql, String data, String category, List<String> values,
                       List<String> seriesTypes, List<String> seriesAxes, String title,
                       String viewer, ChartSink sink) {
        return renderChart(type, sql, data, category, values, seriesTypes, seriesAxes,
                null, null, title, viewer, sink);
    }

    /**
     * 渲染图表（完整参数）：在基础参数之外，{@code yCategory} 供 heatmap 指定 Y 轴分类列，
     * {@code target} 供 graph 指定边的目标节点列。
     */
    String renderChart(String type, String sql, String data, String category, List<String> values,
                       List<String> seriesTypes, List<String> seriesAxes,
                       String yCategory, String target, String title,
                       String viewer, ChartSink sink) {
        if (sink == null) {
            return "当前环境不支持渲染图表。";
        }
        List<String> columns;
        List<List<String>> rows;
        boolean truncated;
        long totalRows;
        boolean fromData = data != null && !data.isBlank();
        if (fromData) {
            if (looksLikeRef(data)) {
                CachedResult c = lookupResultRef(viewer, data);
                if (c == null) {
                    return "结果集引用「" + data.trim() + "」未找到（可能已被清理）：请改用 sql 重算，或传入完整 data JSON。";
                }
                columns = c.columns();
                rows = c.rows();
            } else {
                ResultSet ds = parseDataSet(data);
                if (ds == null) {
                    return "data 既不是结果集引用（last / rs<编号>），也不是有效的结果集 JSON："
                            + "应为 {\"columns\":[...],\"rows\":[[...],...]} 或行对象数组 [{...},...]。";
                }
                columns = ds.columns();
                rows = ds.rows();
            }
            truncated = false;
            totalRows = rows.size();
        } else {
            if (sql == null || sql.isBlank()) {
                return "需要提供 sql 或 data 之一：sql=只读查询（去引擎取数），data=结果集引用（last / rs<编号>）或结果集 JSON。";
            }
            if (!isReadOnlySql(sql)) {
                return "render_chart 仅支持只读查询（SELECT / WITH / SHOW / DESCRIBE / EXPLAIN）。";
            }
            if (monitor == null) {
                return "独立预览模式不支持渲染图表（未连接集群）。";
            }
            MonitorResult r;
            try {
                r = submitAndPoll(applyAiRowLimit(sql), viewer, CHART_MAX_ROWS);
            } catch (IllegalStateException e) {
                return e.getMessage();
            }
            columns = r.columns();
            rows = r.rows();
            truncated = r.truncated();
            totalRows = r.totalRows();
            cacheResult(viewer, columns, rows);   // 顺手缓存，便于换图形时复用同一份数据
        }
        String spec;
        try {
            spec = buildChartSpec(type, title, columns, rows, category, values,
                    seriesTypes, seriesAxes, yCategory, target, truncated, totalRows);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            return msg != null && msg.startsWith("抱歉") ? msg : "图表参数有误: " + msg;
        }
        sink.emit(spec);
        String kind = (seriesTypes != null && !seriesTypes.isEmpty())
                ? "混合图表" : chartTypeLabel(normalizeChartType(type));
        return "已渲染 " + kind + "（数据 " + rows.size() + " 行"
                + (fromData ? "，来自传入结果集" : truncated ? "，已截断，实际 " + totalRows + " 行" : "") + "）。";
    }

    /**
     * 导出文件（无图表）：文本正文（md / txt / html）、结果集（csv / excel）或 PDF 文档。
     */
    String exportFile(String format, String fileName, String title, String content, String data,
                      String viewer, FileSink sink) {
        return exportFile(format, fileName, title, content, data, null, null, null, null, null,
                viewer, sink);
    }

    /**
     * 导出文件：把文本正文（md / txt / html）、结果集（csv / excel）、图表（png / jpg）
     * 或「图表 + 正文 / 表格」（pdf）生成为可下载文件，落进 {@link #fileStore}（按 viewer 隔离）
     * 后经 {@link FileSink} 推给前端。
     *
     * <p>{@code data} 与 {@code render_chart} 同口径：结果集引用（{@code rs3} / {@code last}）优先，
     * 也接受结果集 JSON —— 这样模型不必把整份数据再回传一遍。
     * {@code category + values} 非空即视为「图表导出」，{@code type} 缺省按柱状图。</p>
     */
    String exportFile(String format, String fileName, String title, String content, String data,
                      String chartType, String category, List<String> values,
                      List<String> seriesTypes, List<String> seriesAxes,
                      String viewer, FileSink sink) {
        if (sink == null) {
            return "当前环境不支持导出文件。";
        }
        String fmt = AiExportFormats.normalizeFormat(format);
        if (fmt == null) {
            return "不支持的文件格式「" + format + "」：可选 " + String.join(" / ", EXPORT_FORMATS) + "。";
        }
        boolean imageFormat = "png".equals(fmt) || "jpg".equals(fmt);
        boolean chartRequested = category != null && !category.isBlank()
                && values != null && !values.isEmpty();
        if (imageFormat && !chartRequested) {
            return "导出 " + fmt.toUpperCase(Locale.ROOT) + " 需要图表参数：type（图表类型）、"
                    + "category（X 轴列名）、values（数值列名数组），并用 data 提供数据。";
        }
        List<String> columns = null;
        List<List<String>> rows = null;
        boolean hasData = data != null && !data.isBlank();
        if (hasData) {
            if (looksLikeRef(data)) {
                CachedResult c = lookupResultRef(viewer, data);
                if (c == null) {
                    return "结果集引用「" + data.trim() + "」未找到（可能已被清理）："
                            + "请先用 execute_sql 重新取数，或传入完整 data JSON。";
                }
                columns = c.columns();
                rows = c.rows();
            } else {
                ResultSet ds = parseDataSet(data);
                if (ds == null) {
                    return "data 既不是结果集引用（last / rs<编号>），也不是有效的结果集 JSON："
                            + "应为 {\"columns\":[...],\"rows\":[[...],...]} 或行对象数组 [{...},...]。";
                }
                columns = ds.columns();
                rows = ds.rows();
            }
        }
        boolean noContent = content == null || content.isBlank();
        boolean noTable = columns == null || columns.isEmpty();
        if (noContent && noTable) {
            if (imageFormat) {
                return "导出图片需要数据：请先 execute_sql 取数，再用 data=\"rs<编号>\"（或 \"last\"），"
                        + "并给出 category / values。";
            }
            return switch (fmt) {
                case "excel" -> "导出 Excel 需要表格数据：请先 execute_sql 取数，再用 data=\"rs<编号>\"（或 \"last\"）。";
                case "csv" -> "导出 CSV 需要 data（查询结果引用 / JSON）或 content（CSV 文本）。";
                case "pdf" -> chartRequested
                        ? "导出 PDF 图表需要数据：请先 execute_sql 取数，再用 data=\"rs<编号>\"（或 \"last\"）。"
                        : "导出 PDF 需要 content（正文）/ data（表格数据），或图表参数（type / category / values）与数据。";
                default -> "导出 " + fmt + " 需要 content（正文）或 data（表格数据）。";
            };
        }
        AiExportFormats.ChartData chart = null;
        if (chartRequested) {
            if (noTable) {
                return "导出图表需要数据：请先 execute_sql 取数，再用 data=\"rs<编号>\"（或 \"last\"）。";
            }
            try {
                chart = buildChartData(chartType, title, columns, rows, category, values,
                        seriesTypes, seriesAxes);
            } catch (IllegalArgumentException e) {
                String msg = e.getMessage();
                return msg != null && msg.startsWith("抱歉") ? msg : "图表参数有误: " + msg;
            }
        }
        String name = AiExportFormats.fileName(fmt, fileName, title);
        byte[] bytes;
        try {
            bytes = AiExportFormats.generate(fmt, title, content, columns, rows, chart);
        } catch (IllegalArgumentException e) {
            return "导出失败: " + e.getMessage();
        }
        try {
            AiFileStore.StoredFile stored = fileStore.save(viewer, name, fmt, bytes);
            GeneratedFile file = new GeneratedFile(stored.id(), stored.name(), stored.format(),
                    stored.size(), "/api/ai/files/" + stored.id());
            sink.emit(file);
            return "已生成文件：" + stored.name() + "（" + fmt + "，" + humanBytes(stored.size())
                    + (rows == null ? "" : "，" + rows.size() + " 行") + "）。"
                    + "回答下方已出现下载按钮，用户点击即可保存到本地。";
        } catch (RuntimeException e) {
            return "导出失败: " + e.getMessage();
        }
    }

    /** 结果集（列名 + 行）——{@link #parseDataSet} 产物。 */
    record ResultSet(List<String> columns, List<List<String>> rows) {
    }

    /**
     * 解析结果集 JSON（纯函数，离线可测）。接受两种形态：
     * <ul>
     *   <li>对象 {@code {"columns":["a","b"], "rows":[["1","2"], ...]}}；</li>
     *   <li>行对象数组 {@code [{"a":"1","b":"2"}, ...]}（列取首对象的键顺序）。</li>
     * </ul>
     * 缺列 / 结构不符 → {@code null}（调用方提示模型改格式）。单元格缺失或 null → 空串。
     */
    ResultSet parseDataSet(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            JsonNode n = mapper.readTree(json);
            if (n.isObject()) {
                JsonNode cols = n.get("columns");
                JsonNode rows = n.get("rows");
                if (cols == null || !cols.isArray() || rows == null || !rows.isArray()) {
                    return null;
                }
                List<String> columns = new ArrayList<>();
                for (JsonNode c : cols) {
                    columns.add(c == null || c.isNull() ? "" : c.asText());
                }
                if (columns.isEmpty()) {
                    return null;
                }
                List<List<String>> out = new ArrayList<>();
                for (JsonNode row : rows) {
                    if (row == null || !row.isArray()) {
                        return null;
                    }
                    out.add(cellsFor(row, columns));
                }
                return new ResultSet(columns, out);
            }
            if (n.isArray()) {
                if (n.size() == 0) {
                    return null;
                }
                JsonNode first = n.get(0);
                if (first == null || !first.isObject()) {
                    return null;
                }
                List<String> columns = new ArrayList<>();
                first.fieldNames().forEachRemaining(columns::add);
                if (columns.isEmpty()) {
                    return null;
                }
                List<List<String>> out = new ArrayList<>();
                for (JsonNode row : n) {
                    if (row == null || !row.isObject()) {
                        return null;
                    }
                    List<String> cells = new ArrayList<>(columns.size());
                    for (String c : columns) {
                        JsonNode cell = row.get(c);
                        cells.add(cell == null || cell.isNull() ? "" : cell.asText());
                    }
                    out.add(cells);
                }
                return new ResultSet(columns, out);
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /** 按列名从「行数组」取值（越界 / null → 空串）。 */
    private static List<String> cellsFor(JsonNode rowArray, List<String> columns) {
        List<String> cells = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            JsonNode cell = i < rowArray.size() ? rowArray.get(i) : null;
            cells.add(cell == null || cell.isNull() ? "" : cell.asText());
        }
        return cells;
    }

    // ==================== 结果集缓存（供 render_chart 复用，免重算 / 免重传）====================

    /** 一份缓存结果：引用 id（形如 {@code rs3}）+ 列名 + 行。 */
    record CachedResult(String id, List<String> columns, List<List<String>> rows) {
    }

    /** 单用户的最近结果集（插入序，队尾最新）+ 引用编号计数器。 */
    private static final class ViewerResults {
        final Deque<CachedResult> recent = new ArrayDeque<>();
        int seq = 0;
    }

    /**
     * 缓存一次只读查询结果，返回引用 id（形如 {@code rs3}）。空列（如 DDL 结果）不缓存、返回 {@code null}。
     * 按 {@code viewer} 隔离，每 viewer 只保留最近 {@link #RESULT_CACHE_MAX} 份。
     */
    String cacheResult(String viewer, List<String> columns, List<List<String>> rows) {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        ViewerResults vr = resultCache.computeIfAbsent(viewer == null ? "" : viewer, k -> new ViewerResults());
        synchronized (vr) {
            String id = "rs" + (++vr.seq);
            vr.recent.addLast(new CachedResult(id, List.copyOf(columns),
                    rows == null ? List.of() : List.copyOf(rows)));
            while (vr.recent.size() > RESULT_CACHE_MAX) {
                vr.recent.removeFirst();
            }
            return id;
        }
    }

    /** {@code data} 是否形如结果集引用（{@code last} / {@code previous} / {@code rs<编号>}，可带前导 #）。 */
    static boolean looksLikeRef(String data) {
        String t = data == null ? "" : data.trim();
        if (t.startsWith("#")) {
            t = t.substring(1).trim();
        }
        return t.equalsIgnoreCase("last") || t.equalsIgnoreCase("previous") || t.matches("(?i)rs\\d+");
    }

    /** 查该 viewer 的缓存引用（{@code last}/{@code previous} 取最近一份，{@code rs<编号>} 精确匹配）；未命中 null。 */
    CachedResult lookupResultRef(String viewer, String ref) {
        if (!looksLikeRef(ref)) {
            return null;
        }
        ViewerResults vr = resultCache.get(viewer == null ? "" : viewer);
        if (vr == null) {
            return null;
        }
        String t = ref.trim();
        if (t.startsWith("#")) {
            t = t.substring(1).trim();
        }
        synchronized (vr) {
            if (t.equalsIgnoreCase("last") || t.equalsIgnoreCase("previous")) {
                return vr.recent.peekLast();
            }
            for (CachedResult c : vr.recent) {
                if (c.id().equalsIgnoreCase(t)) {
                    return c;
                }
            }
            return null;
        }
    }

    /** 追加到 {@code execute_sql} 结果后的「结果集引用」提示（无引用时返回空串）。 */
    private static String resultRefHint(String ref, int rowCount) {
        if (ref == null) {
            return "";
        }
        return "\n★结果集引用: " + ref + "（共 " + rowCount + " 行）。如需把这份数据画图，"
                + "直接调用 render_chart(..., data=\"" + ref + "\") 复用；如需导出成 CSV / Excel 文件，"
                + "调用 export_file(format=\"csv\"/\"excel\", data=\"" + ref + "\")；"
                + "如需把图表保存成图片或 PDF，调用 export_file(format=\"png\"/\"jpg\"/\"pdf\", data=\""
                + ref + "\", type=..., category=..., values=[...])。"
                + "不要重传数据或 SQL；也可用 data=\"last\" 引用最新一次结果。";
    }

    /**
     * 由查询结果构造图片 / PDF 导出用的 {@link AiExportFormats.ChartData}（纯函数，<b>离线可测</b>）。
     *
     * <p>校验口径与 {@link #buildChartSpec} 一致：图表类型、category / values 列存在性、
     * seriesTypes / seriesAxes 数量与取值；非数值单元格保留为 {@code null}（该点跳过）。
     * 非法参数 → {@link IllegalArgumentException}（调用方转成给模型的文案）。</p>
     */
    static AiExportFormats.ChartData buildChartData(String type, String title, List<String> columns,
                                                    List<List<String>> rows, String category,
                                                    List<String> values, List<String> seriesTypes,
                                                    List<String> seriesAxes) {
        String t = normalizeChartType(type);
        if (t == null) {
            throw new IllegalArgumentException(unsupportedChartType(type));
        }
        if (!EXPORT_CHART_TYPES.contains(t)) {
            throw new IllegalArgumentException("图片 / PDF 导出暂不支持图表类型: " + type
                    + "（可选 " + String.join(" / ", EXPORT_CHART_TYPES) + "；"
                    + "漏斗 / 热力图 / 树图 / 关系图等请用 render_chart 在网页中渲染）");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("查询没有返回任何列。");
        }
        int ci = indexOfColumn(columns, category);
        if (ci < 0) {
            throw new IllegalArgumentException("category 列不存在: " + category
                    + "；可用列: " + String.join(", ", columns));
        }
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("values 不能为空。");
        }
        List<String> seriesTypeList = resolveSeriesTypes(t, values, seriesTypes);
        List<String> seriesAxisList = resolveSeriesAxes(values, seriesAxes);
        List<Integer> valueIdx = new ArrayList<>();
        for (String v : values) {
            int idx = indexOfColumn(columns, v);
            if (idx < 0) {
                throw new IllegalArgumentException("values 列不存在: " + v
                        + "；可用列: " + String.join(", ", columns));
            }
            valueIdx.add(idx);
        }

        List<List<String>> safeRows = rows == null ? List.of() : rows;
        List<String> categories = new ArrayList<>(safeRows.size());
        for (List<String> row : safeRows) {
            categories.add(cell(row, ci));
        }
        List<AiExportFormats.ChartData.Series> series = new ArrayList<>(valueIdx.size());
        boolean dualAxis = false;
        for (int k = 0; k < valueIdx.size(); k++) {
            boolean y2 = "y2".equals(seriesAxisList.get(k));
            dualAxis |= y2;
            List<Double> data = new ArrayList<>(safeRows.size());
            for (List<String> row : safeRows) {
                data.add(parseNum(cell(row, valueIdx.get(k))));
            }
            series.add(new AiExportFormats.ChartData.Series(values.get(k),
                    seriesTypeList.get(k), data, y2));
        }
        return new AiExportFormats.ChartData(t,
                title == null || title.isBlank() ? null : title.strip(),
                categories, series, dualAxis);
    }

    /**
     * 由查询结果构造图表规格 JSON（纯函数，<b>离线可测</b>）—— 单类型便捷重载，
     * 等价于「所有序列都用 {@code type}、全部挂主 Y 轴」。
     */
    String buildChartSpec(String type, String title, List<String> columns, List<List<String>> rows,
                          String category, List<String> values, boolean truncated, long totalRows) {
        return buildChartSpec(type, title, columns, rows, category, values, null, null, truncated, totalRows);
    }

    /**
     * 由查询结果构造图表规格 JSON（纯函数，<b>离线可测</b>）：
     * {@code {type, title?, categories:[…], series:[{name, type, data:[…], axis?}], dualAxis?, truncated, totalRows}}。
     *
     * <p><b>混合图</b>：{@code seriesTypes} / {@code seriesAxes} 按 {@code values} 顺序一一对应，
     * 让每条序列各自成图（如柱状 + 折线）并使用主 / 副 Y 轴。二者可缺省（缺省 = 全部用 {@code type}、全挂主轴，
     * 与旧单类型行为一致）。{@code series} 每项恒带归一化后的 {@code type}（前端缺省回退顶层 {@code type}，
     * 故旧规格仍可渲染）；仅当某序列挂副轴时写 {@code axis:"y2"}，并在顶层置 {@code dualAxis:true}。</p>
     *
     * <p>列不存在 / 类型非法 / 数组长度与 values 不一致 → {@link IllegalArgumentException}（调用方转成给模型的文案）。</p>
     */
    String buildChartSpec(String type, String title, List<String> columns, List<List<String>> rows,
                          String category, List<String> values, List<String> seriesTypes,
                          List<String> seriesAxes, boolean truncated, long totalRows) {
        return buildChartSpec(type, title, columns, rows, category, values, seriesTypes, seriesAxes,
                null, null, truncated, totalRows);
    }

    /**
     * 构造图表规格 JSON（纯函数，<b>离线可测</b>）。除常规直角坐标 / 饼图外，还支持：
     * <ul>
     *   <li><b>funnel / treemap / tree</b>：复用 category + values[0]（分类名 + 数值）；</li>
     *   <li><b>heatmap</b>：category = X 轴分类列、{@code yCategory} = Y 轴分类列、values[0] = 数值，
     *       输出 {@code categories + yCategories + series[0].data = [[xIdx, yIdx, v], ...]}；</li>
     *   <li><b>graph</b>：category = 源节点列、{@code target} = 目标节点列、values[0] 可选作边权重，
     *       输出 {@code nodes + links}；</li>
     *   <li><b>dataset</b>：不需要 category / values，输出原始 {@code columns + rows} 供前端表格卡片。</li>
     * </ul>
     */
    String buildChartSpec(String type, String title, List<String> columns, List<List<String>> rows,
                          String category, List<String> values, List<String> seriesTypes,
                          List<String> seriesAxes, String yCategory, String target,
                          boolean truncated, long totalRows) {
        String t = normalizeChartType(type);
        if (t == null) {
            throw new IllegalArgumentException(unsupportedChartType(type));
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("查询没有返回任何列。");
        }
        List<List<String>> safeRows = rows == null ? List.of() : rows;
        ObjectNode root = mapper.createObjectNode();
        root.put("type", t);
        if (title != null && !title.isBlank()) {
            root.put("title", title);
        }

        // 数据集：不做坐标映射，直接把原始结果集交给前端表格卡片。
        if ("dataset".equals(t)) {
            ArrayNode cols = root.putArray("columns");
            for (String c : columns) {
                cols.add(c);
            }
            ArrayNode data = root.putArray("rows");
            for (List<String> row : safeRows) {
                ArrayNode r = data.addArray();
                for (int i = 0; i < columns.size(); i++) {
                    r.add(cell(row, i));
                }
            }
            root.put("truncated", truncated);
            root.put("totalRows", totalRows);
            return writeSpec(root);
        }

        if (category == null || category.isBlank()) {
            throw new IllegalArgumentException("图表类型 " + t + " 需要 category（分类 / X 轴列名）。");
        }
        int ci = indexOfColumn(columns, category);
        if (ci < 0) {
            throw new IllegalArgumentException("category 列不存在: " + category + "；可用列: " + String.join(", ", columns));
        }
        if ((values == null || values.isEmpty()) && !"graph".equals(t)) {
            throw new IllegalArgumentException("图表类型 " + t + " 需要 values（数值列名数组）。");
        }

        if ("graph".equals(t)) {
            return buildGraphSpec(root, columns, safeRows, ci, values == null ? List.of() : values,
                    target, truncated, totalRows);
        }
        if ("heatmap".equals(t)) {
            return buildHeatmapSpec(root, columns, safeRows, ci, values, yCategory, truncated, totalRows);
        }

        // 每条序列各自的图形（缺省回退基础类型）与 Y 轴（缺省主 Y 轴）；均按 values 顺序一一对应。
        List<String> seriesTypeList = resolveSeriesTypes(t, values, seriesTypes);
        List<String> seriesAxisList = resolveSeriesAxes(values, seriesAxes);
        boolean cartesian = SERIES_CHART_TYPES.contains(t);
        boolean dualAxis = cartesian && seriesAxisList.contains("y2");
        List<Integer> valueIdx = new ArrayList<>();
        for (String v : values) {
            int idx = indexOfColumn(columns, v);
            if (idx < 0) {
                throw new IllegalArgumentException("values 列不存在: " + v + "；可用列: " + String.join(", ", columns));
            }
            valueIdx.add(idx);
        }

        ArrayNode categories = root.putArray("categories");
        for (List<String> row : safeRows) {
            categories.add(cell(row, ci));
        }
        ArrayNode series = root.putArray("series");
        for (int k = 0; k < valueIdx.size(); k++) {
            ObjectNode ser = series.addObject();
            ser.put("name", values.get(k));
            ser.put("type", seriesTypeList.get(k));
            if ("y2".equals(seriesAxisList.get(k))) {
                ser.put("axis", "y2");
            }
            ArrayNode data = ser.putArray("data");
            int idx = valueIdx.get(k);
            for (List<String> row : safeRows) {
                String raw = cell(row, idx);
                Double num = parseNum(raw);
                if (num != null) {
                    data.add(num);
                } else {
                    data.add(raw);
                }
            }
        }
        if (dualAxis) {
            root.put("dualAxis", true);
        }
        root.put("truncated", truncated);
        root.put("totalRows", totalRows);
        return writeSpec(root);
    }

    /** 热力图：X/Y 各一列分类 + 一个数值列 → {@code [[xIdx, yIdx, value], ...]}（非数值行跳过）。 */
    private String buildHeatmapSpec(ObjectNode root, List<String> columns, List<List<String>> rows,
                                    int ci, List<String> values, String yCategory,
                                    boolean truncated, long totalRows) {
        if (yCategory == null || yCategory.isBlank()) {
            throw new IllegalArgumentException("heatmap 需要 yCategory（Y 轴分类列名），"
                    + "与 category（X 轴分类列名）组成二维网格。");
        }
        int yi = indexOfColumn(columns, yCategory);
        if (yi < 0) {
            throw new IllegalArgumentException("yCategory 列不存在: " + yCategory
                    + "；可用列: " + String.join(", ", columns));
        }
        int vi = indexOfColumn(columns, values.get(0));
        if (vi < 0) {
            throw new IllegalArgumentException("values 列不存在: " + values.get(0)
                    + "；可用列: " + String.join(", ", columns));
        }
        List<String> xCats = distinctValues(rows, ci);
        List<String> yCats = distinctValues(rows, yi);
        ArrayNode xa = root.putArray("categories");
        for (String c : xCats) {
            xa.add(c);
        }
        ArrayNode ya = root.putArray("yCategories");
        for (String c : yCats) {
            ya.add(c);
        }
        ArrayNode series = root.putArray("series");
        ObjectNode ser = series.addObject();
        ser.put("name", values.get(0));
        ser.put("type", "heatmap");
        ArrayNode data = ser.putArray("data");
        for (List<String> row : rows) {
            Double v = parseNum(cell(row, vi));
            if (v == null) {
                continue;
            }
            String x = cell(row, ci);
            String y = cell(row, yi);
            Integer xi = lookupIndex(xCats, x);
            Integer yiIdx = lookupIndex(yCats, y);
            if (xi == null || yiIdx == null) {
                continue;
            }
            ArrayNode p = data.addArray();
            p.add(xi);
            p.add(yiIdx);
            p.add(v);
        }
        root.put("truncated", truncated);
        root.put("totalRows", totalRows);
        return writeSpec(root);
    }

    /** 关系图：category = 源节点、target = 目标节点、values[0] 可选 = 边权重 → {@code nodes + links}。 */
    private String buildGraphSpec(ObjectNode root, List<String> columns, List<List<String>> rows,
                                  int ci, List<String> values, String target,
                                  boolean truncated, long totalRows) {
        if (target == null || target.isBlank()) {
            throw new IllegalArgumentException("graph 需要 target（边的目标节点列名），category 作为源节点列。");
        }
        int ti = indexOfColumn(columns, target);
        if (ti < 0) {
            throw new IllegalArgumentException("target 列不存在: " + target
                    + "；可用列: " + String.join(", ", columns));
        }
        int vi = -1;
        if (values != null && !values.isEmpty()) {
            vi = indexOfColumn(columns, values.get(0));
            if (vi < 0) {
                throw new IllegalArgumentException("values 列不存在: " + values.get(0)
                        + "；可用列: " + String.join(", ", columns));
            }
        }
        Set<String> names = new java.util.LinkedHashSet<>();
        for (List<String> row : rows) {
            names.add(cell(row, ci));
            names.add(cell(row, ti));
        }
        ArrayNode nodes = root.putArray("nodes");
        for (String n : names) {
            nodes.addObject().put("name", n);
        }
        ArrayNode links = root.putArray("links");
        for (List<String> row : rows) {
            ObjectNode link = links.addObject();
            link.put("source", cell(row, ci));
            link.put("target", cell(row, ti));
            Double w = vi < 0 ? null : parseNum(cell(row, vi));
            link.put("value", w == null ? 1.0 : w);
        }
        root.put("truncated", truncated);
        root.put("totalRows", totalRows);
        return writeSpec(root);
    }

    private static List<String> distinctValues(List<List<String>> rows, int idx) {
        java.util.LinkedHashSet<String> seen = new java.util.LinkedHashSet<>();
        for (List<String> row : rows) {
            seen.add(cell(row, idx));
        }
        return new ArrayList<>(seen);
    }

    private static Integer lookupIndex(List<String> values, String v) {
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i).equals(v)) {
                return i;
            }
        }
        return null;
    }

    private String writeSpec(ObjectNode root) {
        try {
            return mapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new IllegalStateException("图表规格序列化失败: " + e.getMessage(), e);
        }
    }

    /** 归一化每条序列的图形：显式 {@code seriesTypes} 优先（须与 values 等长），缺省则全部回退基础类型。 */
    private static List<String> resolveSeriesTypes(String base, List<String> values, List<String> seriesTypes) {
        List<String> out = new ArrayList<>(values.size());
        if (seriesTypes == null || seriesTypes.isEmpty()) {
            for (int i = 0; i < values.size(); i++) {
                out.add(base);
            }
            return out;
        }
        // 逐序列混合图形只对直角坐标类型有意义（饼图/漏斗/热力图等整图一个图形）。
        if (!SERIES_CHART_TYPES.contains(base)) {
            throw new IllegalArgumentException("图表类型 " + base + " 不支持 seriesTypes 逐序列混合图形"
                    + "（仅 " + String.join(" / ", SERIES_CHART_TYPES) + " 支持）");
        }
        if (seriesTypes.size() != values.size()) {
            throw new IllegalArgumentException("seriesTypes 数量(" + seriesTypes.size()
                    + ")须与 values(" + values.size() + ")一致。");
        }
        for (String s : seriesTypes) {
            String nt = normalizeChartType(s);
            if (nt == null || !SERIES_CHART_TYPES.contains(nt)) {
                throw new IllegalArgumentException("不支持的序列图形: " + s
                        + "（可选 " + String.join(" / ", SERIES_CHART_TYPES) + "）");
            }
            out.add(nt);
        }
        return out;
    }

    /** 归一化每条序列的 Y 轴：显式 {@code seriesAxes} 优先（须与 values 等长），缺省则全部主轴 y。 */
    private static List<String> resolveSeriesAxes(List<String> values, List<String> seriesAxes) {
        List<String> out = new ArrayList<>(values.size());
        if (seriesAxes == null || seriesAxes.isEmpty()) {
            for (int i = 0; i < values.size(); i++) {
                out.add("y");
            }
            return out;
        }
        if (seriesAxes.size() != values.size()) {
            throw new IllegalArgumentException("seriesAxes 数量(" + seriesAxes.size()
                    + ")须与 values(" + values.size() + ")一致。");
        }
        for (String a : seriesAxes) {
            String na = normalizeSeriesAxis(a);
            if (na == null) {
                throw new IllegalArgumentException("不支持的 Y 轴: " + a + "（可选 y / y2）");
            }
            out.add(na);
        }
        return out;
    }

    /** Y 轴归一化：y（主）/ y2（副）；空白 → 主轴；未知 → null。 */
    static String normalizeSeriesAxis(String axis) {
        if (axis == null || axis.isBlank()) {
            return "y";
        }
        return switch (axis.trim().toLowerCase(Locale.ROOT)) {
            case "y", "left", "primary", "main" -> "y";
            case "y2", "right", "secondary" -> "y2";
            default -> null;
        };
    }

    /** 图表类型的中文名（用于给模型的回执文案）。 */
    private static String chartTypeLabel(String type) {
        if (type == null) {
            return "图表";
        }
        return switch (type) {
            case "bar" -> "柱状图";
            case "line" -> "折线图";
            case "area" -> "面积图";
            case "pie" -> "饼图";
            case "scatter" -> "散点图";
            case "funnel" -> "漏斗图";
            case "treemap" -> "矩形树图";
            case "tree" -> "树图";
            case "heatmap" -> "热力图";
            case "graph" -> "关系图";
            case "dataset" -> "数据集表格";
            default -> type + " 图";
        };
    }

    /**
     * 越界图表类型的统一致歉文案（与系统提示词里的「图表能力边界」逐字一致）。
     * 模型若绕过提示词直接传非法 type，工具结果也会给出同一句话术。
     */
    static String unsupportedChartType(String type) {
        String name = type == null || type.isBlank() ? "该图表" : type.strip();
        return "抱歉，" + CHART_SUPPORT_SENTENCE + "，暂不支持 " + name + "。";
    }

    /** 图表类型归一化（小写 + 别名）；未知或空白返回 null。 */
    static String normalizeChartType(String type) {
        if (type == null || type.isBlank()) {
            return "bar";
        }
        return switch (type.trim().toLowerCase(Locale.ROOT)) {
            case "bar", "column" -> "bar";
            case "line" -> "line";
            case "area" -> "area";
            case "pie", "donut", "doughnut" -> "pie";
            case "scatter" -> "scatter";
            case "funnel" -> "funnel";
            case "treemap", "tree-map", "矩形树图" -> "treemap";
            case "tree", "树图" -> "tree";
            case "heatmap", "heat-map", "热力图" -> "heatmap";
            case "graph", "network", "关系图" -> "graph";
            case "dataset", "table", "数据集" -> "dataset";
            default -> null;
        };
    }

    /** 列名匹配（忽略大小写与首尾空白），返回下标；找不到返回 -1。 */
    private static int indexOfColumn(List<String> columns, String name) {
        if (columns == null || name == null) {
            return -1;
        }
        String target = name.trim();
        for (int i = 0; i < columns.size(); i++) {
            String c = columns.get(i);
            if (c != null && c.trim().equalsIgnoreCase(target)) {
                return i;
            }
        }
        return -1;
    }

    private static String cell(List<String> row, int idx) {
        return row != null && idx >= 0 && idx < row.size() && row.get(idx) != null ? row.get(idx) : "";
    }

    /** 数值解析：可解析为有限 double → 该值；否则 null（保持原文本）。 */
    private static Double parseNum(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        try {
            double d = Double.parseDouble(s.trim());
            return Double.isFinite(d) ? d : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 提交一条 SQL 到集群并轮询到终态，返回结果（文本预览与图表共用）。
     *
     * <p><b>不设执行时限</b>：Dojao 的核心优势是把可能 OOM 的重任务通过**磁盘溢写兜底**稳定跑出来，
     * 因此这里一直轮询到终态（FINISHED / FAILED / CANCELED），**本服务不因耗时主动取消**。
     * （用户可从前端手动取消；引擎侧仍有「结果回传看门狗」fail-fast 保护链路故障。）
     *
     * @throws IllegalStateException 提交失败 / 执行失败 / 被取消（message 即可直接展示的文案）
     */
    private MonitorResult submitAndPoll(String sql, String viewer, int maxRows) {
        // 集群感知护栏：集群繁忙时拒绝提交（确定性，不依赖模型自觉）。
        String busy = clusterBusyAdvisory(monitor.cluster());
        if (busy != null) {
            throw new IllegalStateException(busy);
        }
        MonitorSubmit submit = monitor.submit(sql, viewer);
        if (submit == null) {
            throw new IllegalStateException("提交失败：无响应。");
        }
        if ("FAILED".equalsIgnoreCase(submit.state()) || "REJECTED".equalsIgnoreCase(submit.state())) {
            throw new IllegalStateException("执行失败: " + submit.message());
        }
        String queryId = submit.queryId();
        try {
            while (true) {
                MonitorResult r = monitor.result(queryId, 0, maxRows);
                if (r != null) {
                    String st = r.state();
                    if ("FINISHED".equals(st)) {
                        return r;
                    }
                    if ("FAILED".equals(st)) {
                        throw new IllegalStateException("执行失败: " + (r.error() == null ? "(未知错误)" : r.error()));
                    }
                    if ("CANCELED".equals(st)) {
                        throw new IllegalStateException("查询已取消。");
                    }
                }
                Thread.sleep(400);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("查询被中断。");
        }
    }

    /**
     * 提交一条 SQL 到集群执行并返回文本预览（只读 / DDL 共用）；只读结果会顺手缓存并回一个引用 id。
     *
     * @param includeTiming 为 true 时在结果末尾追加本次执行的耗时信息（AI 性能评估用）
     */
    private String runSql(String sql, String viewer, boolean includeTiming) {
        if (monitor == null) {
            return "独立预览模式不支持执行 SQL（未连接集群）。";
        }
        try {
            MonitorResult r = submitAndPoll(sql, viewer, PREVIEW_ROWS);
            String ref = cacheResult(viewer, r.columns(), r.rows());   // DDL（空列）不缓存
            return formatResult(r) + resultRefHint(ref, r.rows().size())
                    + (includeTiming ? timingHint(r.wallMs(), r.totalRows()) : "");
        } catch (IllegalStateException e) {
            return e.getMessage();
        }
    }

    /**
     * {@code execute_sql(includeTiming=true)} 追加到结果末尾的耗时摘要。
     *
     * <p>耗时取自引擎侧墙钟（{@link MonitorResult#wallMs()}，不含提交排队与前端 400ms 轮询间隔），
     * 配合输出总行数给出吞吐，供模型对比优化前后的性能。取不到耗时（0）时明确说明而不是显示 0ms。</p>
     */
    static String timingHint(long wallMs, long totalRows) {
        if (wallMs <= 0L) {
            return "\n★本次 SQL 耗时: 不可用（引擎未返回执行墙钟）。";
        }
        long rowsPerSec = Math.round(totalRows * 1000.0 / wallMs);
        return "\n★本次 SQL 耗时: " + String.format("%,d", wallMs) + " ms"
                + "（输出 " + String.format("%,d", totalRows) + " 行，约 "
                + String.format("%,d", rowsPerSec) + " 行/秒）。";
    }

    private List<String> listCatalogs(String viewer) {
        return monitor != null ? monitor.listCatalogs(viewer) : metaStore.listRegisteredCatalogs(viewer);
    }

    private List<String> listTables(String catalog, String schema) {
        if (catalog == null || catalog.isBlank()) {
            return List.of("需要提供 catalog 名。");
        }
        String schemaName = schema == null ? "" : schema;
        return monitor != null
                ? monitor.listTables(catalog, schemaName)
                : metaStore.listAllTables(catalog, schemaName);
    }

    private String describeTable(String catalog, String schema, String table) {
        if (catalog == null || table == null) {
            return "需要提供 catalog 与 table。";
        }
        String schemaName = schema == null ? "" : schema;
        List<String> lines = new ArrayList<>();
        if (monitor != null) {
            for (MonitorColumn c : monitor.listColumns(catalog, schemaName, table)) {
                lines.add(c.name() + " " + c.dataType() + (c.comment() == null ? "" : "  -- " + c.comment()));
            }
        } else {
            for (Column c : metaStore.getColumns(catalog, schemaName, table)) {
                lines.add(c.name() + " " + c.dataType() + (c.comment() == null ? "" : "  -- " + c.comment()));
            }
        }
        return lines.isEmpty() ? "(未找到该表或列信息)" : String.join("\n", lines);
    }

    /**
     * 把「对当前用户可见的全部函数」格式化成分组清单，注入系统提示词（纯函数，<b>离线可测</b>）。
     *
     * <p>{@code funcs} 应由调用方提供<b>已按用户隔离</b>的结果（{@code monitor.listFunctions(viewer)}），
     * 本方法不再做权限判断 —— 隔离语义在 server 侧（{@code FunctionManager.definitions(viewer)}）保证。</p>
     *
     * <p>内置函数按 {@code kind}（scalar / aggregate / window）分组，只列名字（保持紧凑）；自定义 UDF 用
     * {@link #functionLine} 逐条列出（带实现语言与说明——UDF 名对模型无意义，说明才是关键信号）。
     * 无任何函数（如独立预览模式）→ 返回空串（不往提示词里塞空段）。</p>
     */
    static String buildFunctionCatalog(List<MonitorFunction> funcs) {
        if (funcs == null || funcs.isEmpty()) {
            return "";
        }
        List<MonitorFunction> scalar = new ArrayList<>();
        List<MonitorFunction> aggregate = new ArrayList<>();
        List<MonitorFunction> window = new ArrayList<>();
        List<MonitorFunction> custom = new ArrayList<>();
        for (MonitorFunction f : funcs) {
            if ("custom".equalsIgnoreCase(f.source())) {
                custom.add(f);
                continue;
            }
            String kind = f.kind() == null ? "" : f.kind().toLowerCase(Locale.ROOT);
            switch (kind) {
                case "aggregate" -> aggregate.add(f);
                case "window" -> window.add(f);
                default -> scalar.add(f);
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("## 可用 SQL 函数（你只能在 SQL 里调用下列函数，不得臆造清单外的函数名）\n");
        appendBuiltinGroup(sb, "标量函数", scalar);
        appendBuiltinGroup(sb, "聚合函数", aggregate);
        appendBuiltinGroup(sb, "窗口函数", window);
        if (!custom.isEmpty()) {
            sb.append("- 自定义函数（UDF，").append(custom.size()).append("）：\n");
            for (MonitorFunction f : custom) {
                sb.append("  - ").append(functionLine(f)).append('\n');
            }
        }
        return sb.toString().stripTrailing();
    }

    /**
     * 目录里展示名修正：内置聚合的内部键名与「真实 SQL 调用式」可能不同 ——
     * {@code count_distinct} 是引擎内部聚合键（源自 {@code COUNT(DISTINCT x)} 语法），
     * <b>不能当成函数名</b>，否则模型会照抄写出 {@code count_distinct(x)}（引擎不认识）。这里改写成真实写法。
     */
    private static final Map<String, String> CATALOG_DISPLAY_NAMES = Map.of("count_distinct", "count(distinct x)");

    /** 追加一组内置函数：{@code - 标签（N）：name1, name2, ...}；空组不追加。 */
    private static void appendBuiltinGroup(StringBuilder sb, String label, List<MonitorFunction> group) {
        if (group.isEmpty()) {
            return;
        }
        sb.append("- ").append(label).append("（").append(group.size()).append("）：");
        sb.append(group.stream().map(f -> CATALOG_DISPLAY_NAMES.getOrDefault(
                f.name() == null ? "" : f.name().toLowerCase(Locale.ROOT), f.name()))
                .collect(Collectors.joining(", ")));
        sb.append('\n');
    }

    /**
     * 在「对当前用户可见的函数」里按名查详情（纯函数，<b>离线可测</b>）。
     *
     * <p>找不到 → 明确告知「无此函数或为他人私有函数，无权使用」，绝不尝试越权查询（{@code funcs}
     * 已是隔离后的可见集，未命中即代表不可见或不存在）。</p>
     */
    static String describeFunctionFrom(List<MonitorFunction> funcs, String name) {
        if (name == null || name.strip().isEmpty()) {
            return "需要提供函数名。";
        }
        String target = name.strip();
        if (funcs != null) {
            for (MonitorFunction f : funcs) {
                if (f.name() != null && f.name().strip().equalsIgnoreCase(target)) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("函数名：").append(f.name()).append('\n');
                    sb.append("来源：").append("custom".equalsIgnoreCase(f.source()) ? "自定义（用户注册的 UDF）" : "内置")
                            .append('\n');
                    if (f.kind() != null && !f.kind().isBlank()) {
                        sb.append("类别：").append(f.kind()).append('\n');
                    }
                    if (f.backend() != null && !f.backend().isBlank()) {
                        sb.append("实现：").append(f.backend()).append('\n');
                    }
                    sb.append("说明：").append(f.detail() == null || f.detail().isBlank() ? "(无)" : f.detail()).append('\n');
                    sb.append("用法：可在 SQL 中按名直接调用，例如 SELECT ").append(f.name()).append("(...) ...");
                    return sb.toString();
                }
            }
        }
        return "未找到函数「" + target + "」，或该函数为其他用户的私有函数，你无权使用。"
                + "你能用的函数清单已列在系统提示词的「可用 SQL 函数」中，请从清单里挑选。";
    }

    /** 单条函数的一行描述：{@code name [source/kind/backend]：说明}。 */
    private static String functionLine(MonitorFunction f) {
        StringBuilder b = new StringBuilder(f.name());
        b.append(" [").append(f.source());
        if (f.kind() != null && !f.kind().isBlank()) {
            b.append('/').append(f.kind());
        }
        if (f.backend() != null && !f.backend().isBlank()) {
            b.append('/').append(f.backend());
        }
        b.append(']');
        if (f.detail() != null && !f.detail().isBlank()) {
            b.append("：").append(f.detail());
        }
        return b.toString();
    }

    // ==================== 集群感知（cluster_status 工具 + 忙闲护栏）====================

    /**
     * 把集群总览格式化成给模型的文本（纯函数，<b>离线可测</b>）。
     *
     * <p>覆盖：各节点卡片（CPU / 内存 / 堆 / 磁盘 / 线程 / 运行任务 / 槽位）+ 集群汇总
     * （健康 worker 数、总槽位、运行中任务与占用率、平均 CPU）+ 活动 / 最近查询 + 一句负载结论。</p>
     */
    static String formatClusterStatus(MonitorCluster c) {
        if (c == null) {
            return "(未获取到集群状态)";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Dojao 集群状态：\n");

        List<MonitorWorker> nodes = new ArrayList<>();
        if (c.master() != null) {
            nodes.add(c.master());
        }
        if (c.workers() != null) {
            nodes.addAll(c.workers());
        }
        if (nodes.isEmpty()) {
            sb.append("- 节点：无（集群尚无活跃节点）\n");
        } else {
            sb.append("- 节点（").append(nodes.size()).append("）：\n");
            for (MonitorWorker w : nodes) {
                sb.append("  ").append(workerLine(w)).append('\n');
            }
        }

        int slots = totalSlots(c);
        int running = totalRunningTasks(c);
        int cpu = avgCpuPct(c);
        sb.append("- 汇总：健康 worker ").append(healthyWorkerCount(c)).append('/').append(workerCount(c))
                .append("；总槽位 ").append(slots).append("；运行中任务 ").append(running);
        if (slots > 0) {
            sb.append("（占用率 ").append(Math.round(100.0 * running / slots)).append("%）");
        }
        sb.append("；平均 CPU ").append(cpu < 0 ? "未知" : cpu + "%").append('\n');

        List<MonitorJob> active = c.activeQueries() == null ? List.of() : c.activeQueries();
        List<MonitorJob> recent = c.recentQueries() == null ? List.of() : c.recentQueries();
        int runningQ = 0;
        int submittedQ = 0;
        for (MonitorJob j : active) {
            if ("RUNNING".equalsIgnoreCase(j.state())) {
                runningQ++;
            } else if ("SUBMITTED".equalsIgnoreCase(j.state())) {
                submittedQ++;
            }
        }
        sb.append("- 活动查询：").append(active.size())
                .append("（运行中 ").append(runningQ).append("、排队/提交 ").append(submittedQ).append("）")
                .append("；最近完成 ").append(recent.size()).append('\n');
        int shown = 0;
        for (MonitorJob j : active) {
            if (shown++ >= 8) {
                break;
            }
            sb.append("  · ").append(j.queryId()).append(" [").append(j.state()).append("] ")
                    .append(j.wallMs()).append("ms  ").append(snippet(j.sql(), 120)).append('\n');
        }

        sb.append("- 负载结论：").append(clusterLoadLabel(c));
        return sb.toString();
    }

    /** 单个节点的一行摘要（MonitorWorker → 文本）。 */
    private static String workerLine(MonitorWorker w) {
        StringBuilder b = new StringBuilder();
        b.append('[').append(w.role() == null ? "?" : w.role()).append("] ");
        b.append(w.host() == null ? "?" : w.host());
        if (w.port() > 0) {
            b.append(':').append(w.port());
        }
        b.append(' ').append(w.state()).append(w.healthy() ? "(healthy)" : "(unhealthy)");
        b.append(" CPU ").append(w.cpuUsagePct() < 0 ? "未知" : w.cpuUsagePct() + "%");
        b.append(" 内存 ").append(humanBytes(w.usedMemoryBytes())).append('/').append(humanBytes(w.totalMemoryBytes()));
        b.append(" 堆 ").append(humanBytes(w.heapUsedBytes())).append('/').append(humanBytes(w.heapMaxBytes()));
        b.append(" 磁盘空闲 ").append(humanBytes(w.freeDiskBytes()));
        b.append(" 线程 ").append(w.threadCount());
        if ("worker".equalsIgnoreCase(w.role())) {
            b.append(" 任务 ").append(w.runningQueries()).append('/').append(w.maxConcurrency());
        } else {
            b.append(" 活动查询 ").append(w.runningQueries());
        }
        return b.toString();
    }

    /**
     * 集群负载分级（纯函数，<b>离线可测</b>）：0 = 空闲，1 = 适中，2 = 繁忙。
     *
     * <p>判据 = 运行中任务 / 总槽位 的占用率，与全节点平均 CPU 取「更严重者」：
     * 达到繁忙阈值（{@link #CLUSTER_BUSY_SLOT_RATIO} / {@link #CLUSTER_BUSY_CPU_PCT}）→ 2；
     * 否则达到适中阈值 → 1；否则 0。CPU 未知（-1）不参与判定。</p>
     */
    static int clusterLoadLevel(MonitorCluster c) {
        if (c == null) {
            return 0;
        }
        int slots = totalSlots(c);
        int running = totalRunningTasks(c);
        double slotRatio = slots > 0 ? (double) running / slots : 0.0;
        int cpu = avgCpuPct(c);
        if (slotRatio >= CLUSTER_BUSY_SLOT_RATIO || cpu >= CLUSTER_BUSY_CPU_PCT) {
            return 2;
        }
        if (slotRatio >= CLUSTER_MODERATE_SLOT_RATIO || cpu >= CLUSTER_MODERATE_CPU_PCT) {
            return 1;
        }
        return 0;
    }

    /** 负载结论文案：空闲 / 适中 / 繁忙。 */
    static String clusterLoadLabel(MonitorCluster c) {
        return switch (clusterLoadLevel(c)) {
            case 2 -> "繁忙";
            case 1 -> "适中";
            default -> "空闲";
        };
    }

    /** 集群是否繁忙（= 负载分级达到 2）。 */
    static boolean isClusterBusy(MonitorCluster c) {
        return clusterLoadLevel(c) >= 2;
    }

    /**
     * 提交护栏（纯函数，<b>离线可测</b>）：集群不忙返回 {@code null}；繁忙则返回「拒绝提交」的提示文案。
     *
     * <p>由 {@code submitAndPoll} 在真正 {@code monitor.submit} 之前调用 —— 忙则直接抛
     * {@link IllegalStateException}（文案即展示内容），<b>不提交</b>。</p>
     */
    static String clusterBusyAdvisory(MonitorCluster c) {
        if (!isClusterBusy(c)) {
            return null;
        }
        int slots = totalSlots(c);
        int running = totalRunningTasks(c);
        int cpu = avgCpuPct(c);
        return "集群当前繁忙（运行中任务 " + running + "/" + slots + " 槽位，平均 CPU "
                + (cpu < 0 ? "未知" : cpu + "%") + "），为避免挤占资源，本次暂不提交。"
                + "请稍后重试，或改用更轻量的查询（更窄过滤 / LIMIT）。";
    }

    /** 全部 worker 的槽位总数（Σ maxConcurrency；不含 master）。 */
    private static int totalSlots(MonitorCluster c) {
        if (c == null || c.workers() == null) {
            return 0;
        }
        int sum = 0;
        for (MonitorWorker w : c.workers()) {
            sum += Math.max(0, w.maxConcurrency());
        }
        return sum;
    }

    /** 全部 worker 的运行中任务总数（Σ runningQueries；不含 master —— 其口径为活动查询数）。 */
    private static int totalRunningTasks(MonitorCluster c) {
        if (c == null || c.workers() == null) {
            return 0;
        }
        int sum = 0;
        for (MonitorWorker w : c.workers()) {
            sum += Math.max(0, w.runningQueries());
        }
        return sum;
    }

    /** 全节点（master + workers）平均 CPU 百分比；无有效采样（均 < 0）→ -1。 */
    private static int avgCpuPct(MonitorCluster c) {
        if (c == null) {
            return -1;
        }
        long sum = 0;
        int n = 0;
        if (c.master() != null && c.master().cpuUsagePct() >= 0) {
            sum += c.master().cpuUsagePct();
            n++;
        }
        if (c.workers() != null) {
            for (MonitorWorker w : c.workers()) {
                if (w.cpuUsagePct() >= 0) {
                    sum += w.cpuUsagePct();
                    n++;
                }
            }
        }
        return n == 0 ? -1 : (int) Math.round((double) sum / n);
    }

    private static int workerCount(MonitorCluster c) {
        return c == null || c.workers() == null ? 0 : c.workers().size();
    }

    private static int healthyWorkerCount(MonitorCluster c) {
        if (c == null || c.workers() == null) {
            return 0;
        }
        int n = 0;
        for (MonitorWorker w : c.workers()) {
            if (w.healthy()) {
                n++;
            }
        }
        return n;
    }

    /** 字节数 → 紧凑人类可读串（B / K / M / G）；负数视为未知。 */
    private static String humanBytes(long b) {
        if (b < 0) {
            return "未知";
        }
        if (b < 1024L) {
            return b + "B";
        }
        double kb = b / 1024.0;
        if (kb < 1024.0) {
            return String.format(Locale.ROOT, "%.0fK", kb);
        }
        double mb = kb / 1024.0;
        if (mb < 1024.0) {
            return String.format(Locale.ROOT, "%.0fM", mb);
        }
        return String.format(Locale.ROOT, "%.1fG", mb / 1024.0);
    }

    /** 折成一行并截断（避免多行 SQL 撑爆摘要）。 */
    private static String snippet(String s, int max) {
        if (s == null) {
            return "";
        }
        String one = s.replaceAll("\\s+", " ").strip();
        return one.length() <= max ? one : one.substring(0, max) + "…";
    }

    // ==================== 引擎可见性（list_queries / query_status / operator_status）====================

    /** 千分位整数（监控数字可读性；负数原样）。 */
    private static String num(long n) {
        return String.format(Locale.ROOT, "%,d", n);
    }

    /** 查询 / 任务状态 → 中文标签（未知状态原样返回）。 */
    static String stateLabel(String state) {
        if (state == null || state.isBlank()) {
            return "未知";
        }
        return switch (state.toUpperCase(Locale.ROOT)) {
            case "RUNNING" -> "运行中";
            case "SUBMITTED" -> "排队中";
            case "FINISHED" -> "已完成";
            case "FAILED" -> "失败";
            case "CANCELED" -> "已取消";
            case "UNKNOWN" -> "未知";
            case "REJECTED" -> "已拒绝";
            case "LOST" -> "失联";
            default -> state;
        };
    }

    /**
     * 把查询列表格式化成给模型的文本（纯函数，<b>离线可测</b>）。
     *
     * <p>覆盖：筛选口径与命中条数、每条查询的 queryId / 状态 / 耗时 / 输出行数 / SQL 摘要，
     * 以及下钻提示。{@code limit ≤ 0} → 不截断。</p>
     */
    static String formatQueryList(String stateSpec, List<MonitorJob> allJobs, int limit) {
        List<MonitorJob> all = allJobs == null ? List.of() : allJobs;
        List<MonitorJob> jobs = MonitorViews.filterJobs(all, stateSpec);
        int shown = limit <= 0 ? jobs.size() : Math.min(limit, jobs.size());
        StringBuilder sb = new StringBuilder();
        sb.append("Dojao 查询列表（筛选：")
                .append(stateSpec == null || stateSpec.isBlank() ? "全部" : stateSpec.strip())
                .append("）：共 ").append(all.size()).append(" 条，命中 ").append(jobs.size()).append(" 条");
        if (shown < jobs.size()) {
            sb.append("，显示最近 ").append(shown).append(" 条");
        }
        sb.append('\n');
        if (jobs.isEmpty()) {
            sb.append("(没有符合条件的查询)\n");
            return sb.toString();
        }
        for (int i = 0; i < shown; i++) {
            MonitorJob j = jobs.get(i);
            sb.append("- ").append(j.queryId())
                    .append(" [").append(stateLabel(j.state())).append(" / ").append(j.state()).append(']')
                    .append(' ').append(num(j.wallMs())).append("ms")
                    .append("  输出 ").append(num(j.totalRowsOut())).append(" 行")
                    .append("  ").append(snippet(j.sql(), 160)).append('\n');
        }
        sb.append("提示：用 query_status(queryId) 查看某条查询的算子明细，"
                + "用 operator_status(queryId, operator) 下钻单个算子。");
        return sb.toString();
    }

    /**
     * 把单查询快照格式化成给模型的文本（纯函数，<b>离线可测</b>）。
     *
     * <p>未知 queryId → 明确的「未找到」文案并引导用 list_queries；否则输出状态、耗时、
     * 输出行数、失败原因、全部算子的紧凑指标行与失败算子映射。算子行最多展示 40 个。</p>
     */
    static String formatQueryStatus(MonitorQuery q) {
        if (q == null) {
            return "(未获取到查询状态)";
        }
        if (!MonitorViews.exists(q)) {
            return "未找到查询 " + q.queryId() + "（可能已过期、已被清理，或 queryId 有误）。"
                    + "请用 list_queries 查看最近的查询 id。";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("查询 ").append(q.queryId()).append("：")
                .append(stateLabel(q.state())).append("（").append(q.state()).append("）\n");
        sb.append("- 耗时：").append(num(q.wallMs())).append(" ms");
        if ("RUNNING".equalsIgnoreCase(q.state()) || "SUBMITTED".equalsIgnoreCase(q.state())) {
            sb.append("（仍在进行，为已耗时）");
        }
        sb.append("；输出行数：").append(num(q.totalRowsOut())).append('\n');
        sb.append("- SQL：").append(snippet(q.sql(), 500)).append('\n');
        if (q.failureMessage() != null && !q.failureMessage().isBlank()) {
            sb.append("- 失败原因：").append(snippet(q.failureMessage(), 800)).append('\n');
        }
        List<MonitorOperator> ops = q.operators() == null ? List.of() : q.operators();
        sb.append("- 算子：").append(ops.size())
                .append(" 个（#序号，0 = 最下游；「上游」为 DAG 子节点序号）\n");
        int max = 40;
        for (int i = 0; i < Math.min(ops.size(), max); i++) {
            sb.append("  ").append(operatorLine(ops.get(i))).append('\n');
        }
        if (ops.size() > max) {
            sb.append("  … 其余 ").append(ops.size() - max)
                    .append(" 个算子省略（可对具体算子用 operator_status 下钻）\n");
        }
        Map<String, String> fails = q.operatorFailures() == null ? Map.of() : q.operatorFailures();
        if (!fails.isEmpty()) {
            sb.append("- 失败算子：\n");
            for (Map.Entry<String, String> e : fails.entrySet()) {
                sb.append("  · ").append(e.getKey()).append(": ")
                        .append(snippet(e.getValue(), 400)).append('\n');
            }
        }
        sb.append("提示：用 operator_status(queryId, operator=\"#序号\") 查看某个算子的全部指标与各 Worker 任务明细。");
        return sb.toString();
    }

    /** 单个算子的紧凑指标行（query_status / operator_status 列表共用）。 */
    static String operatorLine(MonitorOperator op) {
        StringBuilder b = new StringBuilder();
        b.append('#').append(op.idx()).append(' ').append(op.type())
                .append(" [").append(stateLabel(op.state())).append(']')
                .append(" 并行 ").append(op.parallelism())
                .append("｜行 ").append(num(op.rowsIn())).append("→").append(num(op.rowsOut()));
        if (op.inputBytes() > 0 || op.outputBytes() > 0) {
            b.append("｜字节 ").append(humanBytes(op.inputBytes()))
                    .append("→").append(humanBytes(op.outputBytes()));
        }
        b.append("｜内存 ").append(humanBytes(op.currentMemBytes()));
        if (op.peakMemBytes() > 0) {
            b.append("(峰值 ").append(humanBytes(op.peakMemBytes())).append(')');
        }
        if (op.spilledBytes() > 0) {
            b.append("｜溢写 ").append(humanBytes(op.spilledBytes()));
        }
        b.append("｜CPU ").append(num(op.cpuMs())).append("ms")
                .append("｜耗时 ").append(num(op.elapsedMs())).append("ms");
        if (op.instantRowsPerSec() > 0 || op.avgRowsPerSec() > 0) {
            b.append("｜").append(num(op.instantRowsPerSec())).append('/')
                    .append(num(op.avgRowsPerSec())).append(" 行/s");
        }
        if (op.children() != null && !op.children().isEmpty()) {
            b.append("｜上游 ").append(op.children().stream()
                    .map(c -> "#" + c).collect(Collectors.joining(",")));
        }
        if (op.failureMessage() != null && !op.failureMessage().isBlank()) {
            b.append("｜失败: ").append(snippet(op.failureMessage(), 200));
        }
        return b.toString();
    }

    /** 任务状态汇总（空任务列表 → 空串）。 */
    static String taskSummary(List<MonitorTask> tasks) {
        if (tasks == null || tasks.isEmpty()) {
            return "";
        }
        int running = 0;
        int finished = 0;
        int failed = 0;
        int lost = 0;
        for (MonitorTask t : tasks) {
            String s = t.state() == null ? "" : t.state().toUpperCase(Locale.ROOT);
            switch (s) {
                case "RUNNING" -> running++;
                case "FINISHED" -> finished++;
                case "FAILED" -> failed++;
                case "LOST" -> lost++;
                default -> { }
            }
        }
        StringBuilder b = new StringBuilder();
        b.append("（运行 ").append(running).append(" / 完成 ").append(finished)
                .append(" / 失败 ").append(failed);
        if (lost > 0) {
            b.append(" / 失联 ").append(lost);
        }
        return b.append('）').toString();
    }

    /** 全部算子概览（含每个算子的任务数汇总），算子最多展示 60 个。 */
    static String formatOperatorList(MonitorQuery q) {
        List<MonitorOperator> ops = q.operators() == null ? List.of() : q.operators();
        StringBuilder sb = new StringBuilder();
        sb.append("查询 ").append(q.queryId()).append(" [")
                .append(stateLabel(q.state())).append("] 的算子（").append(ops.size()).append(" 个）：\n");
        int max = 60;
        for (int i = 0; i < Math.min(ops.size(), max); i++) {
            MonitorOperator op = ops.get(i);
            sb.append("  ").append(operatorLine(op));
            List<MonitorTask> tasks = op.tasks() == null ? List.of() : op.tasks();
            if (!tasks.isEmpty()) {
                sb.append("｜任务 ").append(tasks.size()).append(taskSummary(tasks));
            }
            sb.append('\n');
        }
        if (ops.size() > max) {
            sb.append("  … 其余 ").append(ops.size() - max).append(" 个算子省略\n");
        }
        sb.append("提示：operator_status(queryId, operator=\"#序号\") 查看单个算子的全部指标与各 Worker 任务明细。");
        return sb.toString();
    }

    /**
     * 单个算子的全部指标 + 每个分布式任务明细（任务最多展示 24 个）。
     * {@code includeTasks=false} 时跳过任务段。
     */
    static String formatOperatorDetail(MonitorQuery q, MonitorOperator op, boolean includeTasks) {
        StringBuilder sb = new StringBuilder();
        sb.append("查询 ").append(q.queryId()).append(" [").append(stateLabel(q.state()))
                .append("] 的算子 #").append(op.idx()).append(' ').append(op.type()).append('\n');
        sb.append("- operatorId：").append(op.operatorId() == null ? "-" : op.operatorId()).append('\n');
        sb.append("- 状态：").append(stateLabel(op.state())).append("（").append(op.state()).append("）")
                .append("；并行度：").append(op.parallelism()).append('\n');
        if (op.children() != null && !op.children().isEmpty()) {
            sb.append("- 上游算子（DAG 子节点）：").append(op.children().stream()
                    .map(c -> "#" + c).collect(Collectors.joining(", "))).append('\n');
        }
        sb.append("- 行数：输入 ").append(num(op.rowsIn()))
                .append(" / 输出 ").append(num(op.rowsOut())).append('\n');
        sb.append("- 字节：输入 ").append(humanBytes(op.inputBytes()))
                .append(" / 输出 ").append(humanBytes(op.outputBytes()));
        if (op.instantBytesPerSec() > 0 || op.avgBytesPerSec() > 0) {
            sb.append("（瞬时 ").append(humanBytes(op.instantBytesPerSec())).append("/s")
                    .append(" / 平均 ").append(humanBytes(op.avgBytesPerSec())).append("/s）");
        }
        sb.append('\n');
        sb.append("- 内存：当前 ").append(humanBytes(op.currentMemBytes()));
        if (op.peakMemBytes() > 0) {
            sb.append("（峰值 ").append(humanBytes(op.peakMemBytes())).append('）');
        }
        sb.append("；溢写：").append(humanBytes(op.spilledBytes())).append('\n');
        sb.append("- CPU：").append(num(op.cpuMs())).append(" ms")
                .append("；耗时：").append(num(op.elapsedMs())).append(" ms\n");
        sb.append("- 速率：瞬时 ").append(num(op.instantRowsPerSec())).append(" 行/秒")
                .append("；平均 ").append(num(op.avgRowsPerSec())).append(" 行/秒\n");
        if (op.failureMessage() != null && !op.failureMessage().isBlank()) {
            sb.append("- 失败原因：").append(snippet(op.failureMessage(), 800)).append('\n');
        }
        List<MonitorTask> tasks = op.tasks() == null ? List.of() : op.tasks();
        if (includeTasks) {
            sb.append("- 任务：").append(tasks.size()).append(" 个").append(taskSummary(tasks)).append('\n');
            int max = 24;
            for (int i = 0; i < Math.min(tasks.size(), max); i++) {
                MonitorTask t = tasks.get(i);
                sb.append("  · ").append(t.taskId()).append(" @ ").append(t.workerId())
                        .append(" [").append(stateLabel(t.state())).append(']')
                        .append(t.producer() ? " producer" : "")
                        .append(" 行=").append(num(t.rows()))
                        .append(" 耗时=").append(num(t.elapsedMs())).append("ms");
                if (t.source() != null && !t.source().isBlank()) {
                    sb.append(" 源=").append(t.source());
                }
                if (t.expectedRows() > 0) {
                    sb.append("（预计 ").append(num(t.expectedRows())).append(" 行）");
                }
                if (t.failure() != null && !t.failure().isBlank()) {
                    sb.append(" 失败: ").append(snippet(t.failure(), 200));
                }
                sb.append('\n');
            }
            if (tasks.size() > max) {
                sb.append("  … 其余 ").append(tasks.size() - max).append(" 个任务省略\n");
            }
        } else {
            sb.append("- 任务：").append(tasks.size()).append(" 个").append(taskSummary(tasks))
                    .append("（明细已省略）\n");
        }
        return sb.toString();
    }

    private static String formatResult(MonitorResult r) {
        StringBuilder sb = new StringBuilder();
        sb.append("列: ").append(String.join(", ", r.columns())).append('\n');
        sb.append("返回行数: ").append(r.rows().size());
        if (r.truncated()) {
            sb.append("（已截断，实际总数 ").append(r.totalRows()).append("）");
        }
        sb.append('\n');
        for (List<String> row : r.rows()) {
            sb.append(String.join(" | ", row)).append('\n');
        }
        return sb.toString();
    }

    private static String joinOrNone(List<String> names) {
        return names == null || names.isEmpty() ? "(无)" : String.join("\n", names);
    }

    /** 从工具参数 JSON 里取一个字符串数组字段（非数组 / 缺失 → 空列表）。 */
    private List<String> argList(String argsJson, String field) {
        if (argsJson == null || argsJson.isBlank()) {
            return List.of();
        }
        try {
            JsonNode n = mapper.readTree(argsJson).get(field);
            if (n == null || !n.isArray()) {
                return List.of();
            }
            List<String> out = new ArrayList<>();
            for (JsonNode e : n) {
                if (e != null && !e.isNull()) {
                    out.add(e.asText());
                }
            }
            return out;
        } catch (Exception e) {
            return List.of();
        }
    }

    /** 从工具参数 JSON 里取一个布尔字段（缺失 / 非布尔 / 解析失败 → false）。 */
    private boolean argBoolean(String argsJson, String field) {
        return argBooleanDefault(argsJson, field, false);
    }

    /** 从工具参数 JSON 里取一个布尔字段；缺失 / 解析失败 → {@code def}。 */
    private boolean argBooleanDefault(String argsJson, String field, boolean def) {
        if (argsJson == null || argsJson.isBlank()) {
            return def;
        }
        try {
            JsonNode n = mapper.readTree(argsJson).get(field);
            if (n == null || n.isNull()) {
                return def;
            }
            return n.isBoolean() ? n.booleanValue() : "true".equalsIgnoreCase(n.asText());
        } catch (Exception e) {
            return def;
        }
    }

    /** 从工具参数 JSON 里取一个字符串字段。 */
    private String arg(String argsJson, String field) {
        if (argsJson == null || argsJson.isBlank()) {
            return null;
        }
        try {
            JsonNode n = mapper.readTree(argsJson).get(field);
            return n == null || n.isNull() ? null : n.asText();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 只读判据：<b>恰好一条</b>语句，且剥掉行首注释/空白后首关键字在白名单内。
     *
     * <p>★拒绝多语句（以顶层分号切分）：Web 控制台支持批量提交后，若这里只看首个关键字，
     * 一条 {@code select 1; drop table t} 就能绕过只读守卫执行写操作。AI 工具按约定每次只提交一条。</p>
     */
    static boolean isReadOnlySql(String sql) {
        List<String> statements = SqlSplitter.executableStatements(sql);
        if (statements.size() != 1) {
            return false;
        }
        String s = stripLeadingComments(statements.get(0)).trim();
        if (s.isEmpty()) {
            return false;
        }
        String first = s.split("\\s+", 2)[0].toUpperCase(Locale.ROOT);
        return READ_ONLY_KEYWORDS.contains(first);
    }

    /** DDL 判据：<b>恰好一条</b>语句，且剥掉行首注释/空白后首关键字在 DDL 白名单内。 */
    static boolean isDdlSql(String sql) {
        List<String> statements = SqlSplitter.executableStatements(sql);
        if (statements.size() != 1) {
            return false;
        }
        String s = stripLeadingComments(statements.get(0)).trim();
        if (s.isEmpty()) {
            return false;
        }
        String first = s.split("\\s+", 2)[0].toUpperCase(Locale.ROOT);
        return DDL_KEYWORDS.contains(first);
    }

    /**
     * 给 AI 提交的只读 SQL 加/收紧到 {@link #AI_MAX_ROWS} 行的 LIMIT（纯函数，<b>离线可测</b>）。
     *
     * <ul>
     *   <li>只处理单条 SELECT / WITH 查询；SHOW / DESCRIBE / EXPLAIN / DDL / INSERT 原样返回
     *       （元数据语句结果本就小，且 LIMIT 语法未必适用）；</li>
     *   <li>顶层没有 LIMIT → 末尾追加 {@code LIMIT 500}；</li>
     *   <li>已有 LIMIT n：n &lt;= 500 保持原样；n &gt; 500 把数字改成 500（OFFSET 等后缀保留）；</li>
     *   <li>LIMIT 只在<b>括号外</b>识别 —— 子查询里的 LIMIT 不算外层限制；字符串 / 注释里的
     *       {@code limit} 也不算；</li>
     *   <li>多条语句（AI 工具本应拒绝）原样返回，绝不改写以免破坏语句。</li>
     * </ul>
     */
    static String applyAiRowLimit(String sql) {
        List<String> statements = SqlSplitter.executableStatements(sql);
        if (statements.size() != 1) {
            return sql == null ? "" : sql.trim();
        }
        String s = statements.get(0);
        String first = firstKeyword(s);
        if (!"SELECT".equals(first) && !"WITH".equals(first)) {
            return s;
        }
        int[] hit = findTopLevelLimit(s);   // [数字起点, 数字终点)；没有数字时终点为 -1
        if (hit == null) {
            return s + " LIMIT " + AI_MAX_ROWS;
        }
        int numStart = hit[0];
        int numEnd = hit[1];
        if (numEnd < 0) {
            return s;   // LIMIT 后不是数字（异常写法）—— 不改写，避免破坏语句
        }
        long n;
        try {
            n = Long.parseLong(s.substring(numStart, numEnd));
        } catch (NumberFormatException e) {
            n = Long.MAX_VALUE;   // 超长数字必然超过上限
        }
        return n > AI_MAX_ROWS
                ? s.substring(0, numStart) + AI_MAX_ROWS + s.substring(numEnd)
                : s;
    }

    /** 首个关键字（大写，跳过行首空白 / 注释）；空 SQL → 空串。 */
    private static String firstKeyword(String sql) {
        String s = stripLeadingComments(sql).trim();
        if (s.isEmpty()) {
            return "";
        }
        return s.split("\\s+", 2)[0].toUpperCase(Locale.ROOT);
    }

    /**
     * 找顶层（括号外、字符串 / 注释外）的 LIMIT 关键字。
     *
     * @return {@code null} = 没有 LIMIT；否则 {@code [数字起点, 数字终点)}，LIMIT 后不是数字时终点为 -1
     */
    private static int[] findTopLevelLimit(String s) {
        int i = 0;
        int n = s.length();
        int depth = 0;
        while (i < n) {
            char c = s.charAt(i);
            // 字符串 / 标识符（含 '' 转义）
            if (c == '\'' || c == '"' || c == '`') {
                char quote = c;
                i++;
                while (i < n) {
                    char d = s.charAt(i);
                    i++;
                    if (d == quote) {
                        if (i < n && s.charAt(i) == quote) {
                            i++;
                        } else {
                            break;
                        }
                    }
                }
                continue;
            }
            if (c == '-' && i + 1 < n && s.charAt(i + 1) == '-') {
                while (i < n && s.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }
            if (c == '/' && i + 1 < n && s.charAt(i + 1) == '*') {
                i += 2;
                while (i + 1 < n && !(s.charAt(i) == '*' && s.charAt(i + 1) == '/')) {
                    i++;
                }
                i = Math.min(n, i + 2);
                continue;
            }
            if (c == '(') {
                depth++;
                i++;
                continue;
            }
            if (c == ')') {
                depth = Math.max(0, depth - 1);
                i++;
                continue;
            }
            if (depth == 0 && (Character.isLetter(c) || c == '_')) {
                int start = i;
                while (i < n && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) {
                    i++;
                }
                if (i - start == 5 && s.regionMatches(true, start, "LIMIT", 0, 5)) {
                    int j = i;
                    while (j < n && Character.isWhitespace(s.charAt(j))) {
                        j++;
                    }
                    int numStart = j;
                    while (j < n && Character.isDigit(s.charAt(j))) {
                        j++;
                    }
                    return new int[]{numStart, j > numStart ? j : -1};
                }
                continue;
            }
            i++;
        }
        return null;
    }

    /** 剥掉行首的空白与 {@code --} 行注释、块注释（斜杠星号形式）。 */
    private static String stripLeadingComments(String sql) {
        String s = sql == null ? "" : sql;
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (s.startsWith("--", i)) {
                int nl = s.indexOf('\n', i);
                if (nl < 0) {
                    return "";
                }
                i = nl + 1;
            } else if (s.startsWith("/*", i)) {
                int end = s.indexOf("*/", i + 2);
                if (end < 0) {
                    return "";
                }
                i = end + 2;
            } else {
                break;
            }
        }
        return s.substring(i);
    }
}
