package com.dojao.ui;

import com.dojao.ai.AiChatService;
import com.dojao.ai.AiConfig;
import com.dojao.ai.ReActListener;
import com.dojao.grammar.analyzer.SemanticAnalyzer;
import com.dojao.grammar.analyzer.exception.SemanticException;
import com.dojao.grammar.analyzer.plan.LogicalPlan;
import com.dojao.grammar.optimize.Optimizer;
import com.dojao.metastore.MetaStore;
import com.dojao.metastore.MetaStoreType;
import com.dojao.ui.ai.AiAgentService;
import com.dojao.ui.ai.AiConfigStore;
import com.dojao.ui.ai.AiExportFormats;
import com.dojao.ui.ai.AiFileStore;
import com.dojao.ui.ai.AiSessionStore;
import com.dojao.ui.ai.FileParser;
import com.dojao.ui.ai.AiTurns;
import com.dojao.ui.ai.SkillStore;
import com.dojao.ui.auth.AuthStore;
import com.dojao.ui.auth.Role;
import com.dojao.ui.auth.User;
import com.dojao.ui.catalog.CatalogCreateSupport;
import com.dojao.ui.log.LogViewer;
import com.dojao.ui.script.SqlScriptStore;
import com.dojao.ui.monitor.MonitorCluster;
import com.dojao.ui.monitor.MonitorColumn;
import com.dojao.ui.monitor.MonitorEngineStatus;
import com.dojao.ui.monitor.MonitorFunction;
import com.dojao.ui.monitor.MonitorJob;
import com.dojao.ui.monitor.MonitorOperator;
import com.dojao.ui.monitor.MonitorQuery;
import com.dojao.ui.monitor.MonitorQueryOperators;
import com.dojao.ui.monitor.MonitorResult;
import com.dojao.ui.monitor.MonitorSubmit;
import com.dojao.ui.monitor.MonitorViews;
import com.dojao.ui.monitor.QueryMonitor;
import com.dojao.common.util.Errors;
import com.dojao.spi.bean.Column;
import com.dojao.spi.service.ConnectorFactory;
import com.dojao.spi.service.ConnectorRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import io.javalin.Javalin;
import io.javalin.config.SizeUnit;
import io.javalin.http.Context;
import io.javalin.http.UploadedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Dojao UI 应用入口 —— 启动 Web 服务，提供 SQL 监控台 + 执行计划 DAG 可视化。
 *
 * <p>页面资源挂在 classpath {@code /public}（index.html + js + css），REST 接口三类：</p>
 * <ul>
 *   <li>{@code /api/auth/*}：登录 / 登出 / 当前用户 —— 账号与会话落在独立 SQLite
 *       （见 {@link AuthStore}），全局 {@code before} 过滤器要求其余请求带有效会话
 *       （接口 401、页面跳 {@code /login.html}）；</li>
 *   <li>{@code /api/explain}：本地逻辑计划 → DAG 预览（无需集群）；</li>
 *   <li>{@code /api/query*、/api/queries}：查询监控 / 异步 SQL 执行，
 *       背后是注入的 {@link QueryMonitor}（Master 实时信息）。未注入 monitor 时返回 503；
 *       {@code /api/queries/{id}/operators[/{op}]} 下钻到算子级（含各 Worker 任务明细）；</li>
 *   <li>{@code /api/engine/status}：引擎状态总览（系统 CPU / 内存 / 磁盘 / 线程 / 槽位 +
 *       查询按状态计数 + 过滤后的查询列表），供外部系统与 Dojao AI 诊断使用；
 *       {@code /api/cluster[/history]}：集群总览与资源时间序列；</li>
 *   <li>{@code /api/logs}：只读查看各节点 {@code logs/dojao.out} 尾部（见 {@link LogViewer}）。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/5
 */
public final class DojaoUiApp {

    private static final Logger log = LoggerFactory.getLogger(DojaoUiApp.class);

    /** 登录态 cookie 名（HttpOnly，见 {@link #login}）。 */
    private static final String COOKIE = "dojao_ui_token";

    /** 请求属性名：当前登录用户 —— 过滤器写入，REST 处理器读取。 */
    private static final String ATTR_USER = "dojaoUser";

    private final SemanticAnalyzer analyzer;
    private final Optimizer optimizer;
    /** 查询监控实现（可为 null = 独立预览模式，监控接口 503）。 */
    private final QueryMonitor monitor;
    /**
     * 元数据库：负责 {@code /api/explain} 的语义解析，并在未绑定 monitor 时充当目录树数据源。
     *
     * <p><b>集群模式下由 Master 注入引擎自己的 MetaStore</b>（SQLite，与 SQL 执行同源）；
     * 只有独立预览模式（{@link #DojaoUiApp()}）才自建一个<b>内存</b>演示库。UI 绝不自建
     * 持久化元数据库 —— 那会写出与引擎注册表分叉的 catalog（且演示用的假 {@code hive}
     * 类型没有对应工厂，每次启动刷一条误导 ERROR）。</p>
     */
    private final MetaStore metaStore;
    private final ObjectMapper json = new ObjectMapper();
    private Javalin app;

    /** 控制台账号库（{@link #start} 打开、{@link #stop} 关闭）。 */
    private AuthStore authStore;
    /** 控制台 SQL 脚本库（同一 db 文件，{@link #start} 打开、{@link #stop} 关闭）。 */
    private SqlScriptStore scriptStore;
    /** AI 对话配置库（同一 db 文件，{@link #start} 打开、{@link #stop} 关闭）。 */
    private AiConfigStore aiConfigStore;
    /** AI 长对话（session）库（同一 db 文件，{@link #start} 打开、{@link #stop} 关闭）。 */
    private AiSessionStore aiSessionStore;
    /** AI 技能（Skill）库（同一 db 文件，{@link #start} 打开、{@link #stop} 关闭）。 */
    private SkillStore aiSkillStore;
    /**
     * 按登录账号缓存的 AI 服务：每个账号使用自己的模型配置（一账号一个模型）。
     * 配置未就绪（没有 API Key）时 {@code agent()/chat()} 为 null，聊天接口返回 503。
     * 账号保存新配置或删除账号时移除对应条目，下次请求按新配置重建。
     */
    private final Map<String, UserAiServices> aiServices = new ConcurrentHashMap<>();

    /** 一个账号的 AI 服务组（配置 + 单轮对话 + ReAct Agent）。 */
    private record UserAiServices(AiConfig config, AiChatService chat, AiAgentService agent) {
    }
    /**
     * AI 导出文件存储（{@code export_file} 工具写、{@code GET /api/ai/files/{id}} 读）。
     * 在 {@link #start} 创建、{@link #stop} 关闭；重建 AI 服务时复用同一实例，已生成文件不丢。
     */
    private AiFileStore aiFileStore;
    /** AI 流式推理的后台执行器（daemon，避免阻塞请求线程池）。 */
    private ExecutorService aiExecutor;
    /** 待确认的 DDL：streamId → 用户决策队列（由 {@code POST /api/ai/confirm} 投递 true/false）。 */
    private final Map<String, BlockingQueue<Boolean>> aiConfirmations = new ConcurrentHashMap<>();

    /** 正在跑的推理线程：streamId → 工作线程（{@code POST /api/ai/stop} 用户强制停止时 interrupt）。 */
    private final Map<String, Thread> aiRunning = new ConcurrentHashMap<>();
    private long sessionTtlMs;

    /** 独立预览模式：仅 SQL 计划 DAG，无集群监控。元数据用内存演示库，不落盘。 */
    public DojaoUiApp() {
        this(null, previewMetaStore());
    }

    /**
     * 注入查询监控实现（dojao-server 的 UiQueryMonitor）+ 引擎的元数据库，启用监控 / SQL 执行接口。
     *
     * <p>元数据库必须是<b>引擎正在用的那个实例</b>（由 {@code MasterServer} 注入）：UI 只读它，
     * 不注册任何 catalog —— 注册表唯一权威是引擎，UI 另起一套必然分叉。</p>
     */
    public DojaoUiApp(QueryMonitor monitor, MetaStore metaStore) {
        this.metaStore = metaStore;
        this.analyzer = new SemanticAnalyzer(metaStore);
        this.optimizer = new Optimizer();
        this.monitor = monitor;
    }

    /**
     * 独立预览模式（{@link #main}）用的内存演示元数据库。
     *
     * <p><b>刻意用 {@link MetaStoreType#LOCAL}（内存，不持久化）</b>：演示用的假 {@code hive}
     * catalog 没有对应的 ConnectorFactory，一旦落到持久化存储，下次任何进程以该存储构造
     * MetaStore 都会在启动时刷一行 <code>Catalog 'hive' 装载失败，本次跳过</code>。内存库
     * 随进程消失，不会有这个尾巴。</p>
     *
     * <p><b>自包含 demo 连接器</b>：预览模式直接挂 {@link DemoInMemoryConnector}（内存假数据），
     * <b>不连本机 MySQL / Hive</b> —— 也就让 dojao-ui 主代码零连接器依赖（连接器已插件化）。
     * 走 3 参 {@code registerConnector} 重载：绕过「落库前连一次」的探针，连不上更不会抛。</p>
     */
    private static MetaStore previewMetaStore() {
        MetaStore store = new MetaStore(MetaStoreType.LOCAL);

        // 演示 SQL（test.sql）里引用的两张 MySQL 侧假表 = mysql.ball.nba_team_rs_rank / nba_team_info。
        store.registerConnector("mysql", new DemoInMemoryConnector("mysql", "mysql", List.of(
            new DemoInMemoryConnector.DemoTable("ball", "nba_team_rs_rank", List.of(
                new Column("team_code", "VARCHAR(20)", "球队编码", null),
                new Column("team_name", "VARCHAR(100)", "球队名称", null),
                new Column("conference", "VARCHAR(20)", "分区", null),
                new Column("win_rate", "DECIMAL(5,4)", "胜率", null),
                new Column("win_or_lose", "VARCHAR(2)", "胜/负", null),
                new Column("season", "VARCHAR(10)", "赛季", null))),
            new DemoInMemoryConnector.DemoTable("ball", "nba_team_info", List.of(
                new Column("team_code", "VARCHAR(20)", "球队编码", null),
                new Column("team_name", "VARCHAR(100)", "球队名称", null),
                new Column("conference", "VARCHAR(20)", "分区", null),
                new Column("division", "VARCHAR(50)", "赛区", null),
                new Column("city_name", "VARCHAR(50)", "城市", null),
                new Column("flag", "INT", "有效标志", null))))),
            Map.of("type", "mysql"));

        // 演示用「Hive」catalog：内存假表 hive.ball.nba_player_stats，不连真实 Hive。
        store.registerConnector("hive", new DemoInMemoryConnector("hive", "hive", List.of(
            new DemoInMemoryConnector.DemoTable("ball", "nba_player_stats", List.of(
                new Column("id", "BIGINT", "主键", null),
                new Column("player_name", "VARCHAR(100)", "球员名称", null),
                new Column("team_code", "VARCHAR(20)", "球队编码", null),
                new Column("city", "VARCHAR(50)", "城市", null),
                new Column("season", "VARCHAR(10)", "赛季", null),
                new Column("points", "DECIMAL(5,1)", "场均得分", null),
                new Column("assists", "DECIMAL(5,1)", "场均助攻", null),
                new Column("rebounds", "DECIMAL(5,1)", "场均篮板", null),
                new Column("games_played", "INT", "出场次数", null),
                new Column("create_time", "DATETIME", "创建时间", null))))),
            Map.of("type", "hive"));

        return store;
    }

    public void start(int port) {
        // 控制台账号库：独立 SQLite 文件（默认 ./data/dojao-ui.db），重启后账号与会话仍在。
        // 账号表为空时创建首个管理员 admin（默认口令 Admin@123456，-Dsystem.ui.admin.password 覆盖）。
        sessionTtlMs = AuthStore.defaultSessionTtlMs();
        authStore = AuthStore.open(AuthStore.defaultDbPath(), sessionTtlMs);
        authStore.ensureBootstrapAdmin(System.getProperty("system.ui.admin.password"));
        // SQL 脚本库：与账号库同文件（同一 db），独立连接 + 独立表 ui_sql_script。
        scriptStore = SqlScriptStore.open(AuthStore.defaultDbPath());
        // AI 对话配置库：同一 db，独立表 ui_ai_config_user（一账号一模型；无自有配置时沿用管理员配置）。
        aiConfigStore = AiConfigStore.open(AuthStore.defaultDbPath());
        // AI 长对话库：同一 db，独立表 ui_ai_session（按登录用户隔离）。
        aiSessionStore = AiSessionStore.open(AuthStore.defaultDbPath());
        // AI 技能库：同一 db，独立表 ui_ai_skill（按登录用户隔离）。
        aiSkillStore = SkillStore.open(AuthStore.defaultDbPath());
        // AI 导出文件存储：系统临时目录（按登录用户隔离、TTL 过期清理）。
        aiFileStore = new AiFileStore();
        // AI 流式推理后台执行器（daemon：随 JVM 退出，不拖住关闭）。
        aiExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "dojao-ai-agent");
            t.setDaemon(true);
            return t;
        });

        app = Javalin.create(cfg -> {
            cfg.staticFiles.add("/public");
            cfg.http.defaultContentType = "application/json";
            // AI 对话附件上传（txt / md / docx / xlsx…）：默认请求体仅 1MB、multipart 单文件
            // 不限但全落磁盘，这里显式放开到与 FileParser 的配额一致（单文件 10MB、请求 16MB）。
            cfg.http.maxRequestSize = 16L * 1024 * 1024;
            cfg.jetty.multipartConfig.maxFileSize(FileParser.MAX_FILE_BYTES, SizeUnit.BYTES);
            cfg.jetty.multipartConfig.maxInMemoryFileSize(FileParser.MAX_FILE_BYTES, SizeUnit.BYTES);
            cfg.jetty.multipartConfig.maxTotalRequestSize(16, SizeUnit.MB);
        }).start(port);

        // 鉴权：除登录页及其静态资源、登录接口外，一律要求有效会话。
        // 静态文件由 Javalin 在 endpoint 阶段（BEFORE 之后）经 resourceHandler 提供，
        // 故这里能拦住页面请求并整页跳登录，而不是只让接口 401。
        app.before(this::authenticate);

        // 首页
        app.get("/", this::indexPage);

        // API: 控制台登录态
        app.post("/api/auth/login", this::login);
        app.post("/api/auth/logout", this::logout);
        app.get("/api/auth/me", this::me);

        // API: 用户管理（仅管理员）：账号列表 / 新建 / 修改角色与口令 / 删除
        app.get("/api/users", this::listUsers);
        app.post("/api/users", this::createUser);
        app.post("/api/users/{username}", this::updateUser);
        app.delete("/api/users/{username}", this::deleteUser);

        // API: 分析 SQL 并返回 DAG（本地逻辑计划预览，无需 monitor）
        app.post("/api/explain", this::explain);

        // API: 查询监控（任务列表 / 详情 DAG / 算子明细 / 集群总览 / 引擎状态）
        app.get("/api/queries", this::listQueries);
        app.get("/api/queries/{id}", this::querySnapshot);
        app.get("/api/queries/{id}/operators", this::queryOperators);
        app.get("/api/queries/{id}/operators/{op}", this::queryOperator);
        app.get("/api/engine/status", this::engineStatus);
        app.get("/api/cluster", this::cluster);
        app.get("/api/cluster/history", this::clusterHistory);
        app.get("/api/logs", this::logs);

        // API: 新建 catalog 向导（连接器卡片清单 / 测试连接 / 执行 CREATE CATALOG）
        app.get("/api/connectors", this::listConnectors);
        app.post("/api/catalog/test", this::testCatalogConnection);
        app.post("/api/catalog", this::createCatalog);

        // API: 元数据目录树（catalog → schema → table，首页左侧）
        app.get("/api/metadata/catalogs", this::listCatalogs);
        app.get("/api/metadata/catalogs/{catalog}/schemas", this::listSchemas);
        app.get("/api/metadata/catalogs/{catalog}/schemas/{schema}/tables", this::listTables);
        app.get("/api/metadata/catalogs/{catalog}/schemas/{schema}/tables/{table}/columns",
                this::listColumns);

        // API: 函数目录（内置 + 自定义），SQL 编辑器补全用
        app.get("/api/functions", this::listFunctions);

        // API: SQL 脚本（当前登录用户私有：保存 / 列表 / 加载 / 删除）
        app.get("/api/scripts", this::listScripts);
        app.post("/api/scripts", this::saveScript);
        app.get("/api/scripts/{id}", this::getScript);
        app.delete("/api/scripts/{id}", this::deleteScript);

        // API: SQL 异步执行（提交 / 取消 / 结果预览）
        app.post("/api/query", this::submitQuery);
        app.post("/api/query/{id}/cancel", this::cancelQuery);
        app.get("/api/query/{id}/rows", this::queryRows);

        // API: AI 对话（配置读写 + 单轮提问 + ReAct 流式推理；配置持久化到 ui_ai_config）
        app.get("/api/ai/config", this::aiConfigGet);
        app.post("/api/ai/config", this::aiConfigSave);
        app.post("/api/ai/chat", this::aiChat);
        app.post("/api/ai/chat/stream", this::aiChatStream);
        app.post("/api/ai/confirm", this::aiConfirmDdl);
        app.post("/api/ai/stop", this::aiStop);

        // API: AI 附件上传解析（multipart；统一走 FileParser，解析出的文本随对话请求回传）
        app.post("/api/ai/files/parse", this::aiFileParse);

        // API: AI 导出文件下载（按登录用户隔离；export_file 工具生成）
        app.get("/api/ai/files/{id}", this::aiFileDownload);

        // API: AI 长对话会话（按登录用户隔离持久化到 ui_ai_session）
        app.get("/api/ai/sessions", this::aiSessionList);
        app.post("/api/ai/sessions", this::aiSessionCreate);
        app.get("/api/ai/sessions/{id}", this::aiSessionGet);
        app.delete("/api/ai/sessions/{id}", this::aiSessionDelete);

        // API: AI 技能（Skill：提示词 + 工具白名单，按登录用户隔离持久化到 ui_ai_skill）
        app.get("/api/ai/skills", this::aiSkillList);
        app.post("/api/ai/skills", this::aiSkillCreate);
        app.get("/api/ai/skills/{id}", this::aiSkillGet);
        app.post("/api/ai/skills/{id}", this::aiSkillUpdate);
        app.delete("/api/ai/skills/{id}", this::aiSkillDelete);

        System.out.println("Dojao UI started at http://localhost:" + port);
    }

    /** 停止 Javalin 服务器并关闭账号库。 */
    public void stop() {
        if (app != null) {
            app.stop();
            System.out.println("Dojao UI stopped");
        }
        if (authStore != null) {
            authStore.close();
            authStore = null;
        }
        if (scriptStore != null) {
            scriptStore.close();
            scriptStore = null;
        }
        if (aiConfigStore != null) {
            aiConfigStore.close();
            aiConfigStore = null;
        }
        if (aiSessionStore != null) {
            aiSessionStore.close();
            aiSessionStore = null;
        }
        if (aiSkillStore != null) {
            aiSkillStore.close();
            aiSkillStore = null;
        }
        if (aiExecutor != null) {
            aiExecutor.shutdownNow();
            aiExecutor = null;
        }
        aiServices.clear();
        if (aiFileStore != null) {
            aiFileStore.close();
            aiFileStore = null;
        }
    }

    private void indexPage(Context ctx) {
        ctx.redirect("/index.html");
    }

    // ==================== 鉴权 REST 处理器 ====================

    /**
     * 免登录路径 —— 登录页本身及其静态资源（否则登录页连 css/js 都拿不到）。
     * 这些资源不含任何数据，放行无风险；真正的数据全在 {@code /api/*} 之后。
     */
    private static boolean isPublicPath(String path) {
        return "/login.html".equals(path)
                || "/favicon.ico".equals(path)
                || path.startsWith("/css/")
                || path.startsWith("/js/")
                || path.startsWith("/img/")
                || path.startsWith("/vendor/");
    }

    /**
     * 全局鉴权过滤器（BEFORE 阶段）：无有效会话时，接口返 401、页面整页跳登录页。
     * 通过后把 {@link User} 挂到请求属性，供 {@link #me} 使用。
     */
    private void authenticate(Context ctx) {
        String path = ctx.path();
        if (isPublicPath(path) || "/api/auth/login".equals(path)) {
            return;
        }
        User user = authStore.resolve(ctx.cookie(COOKIE)).orElse(null);
        if (user == null) {
            if (path.startsWith("/api/")) {
                ctx.status(401).json(Map.of("error", "未登录或会话已过期"));
            } else {
                ctx.redirect("/login.html");
            }
            ctx.skipRemainingHandlers();
            return;
        }
        ctx.attribute(ATTR_USER, user);
    }

    /** POST /api/auth/login —— 校验口令并下发 HttpOnly 会话 cookie。 */
    private void login(Context ctx) {
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            String username = body == null ? null : (String) body.get("username");
            String password = body == null ? null : (String) body.get("password");
            Optional<User> found = authStore.authenticate(username, password);
            if (found.isEmpty()) {
                // 故意不区分「用户不存在」与「口令错误」：避免账号枚举
                ctx.status(401).json(Map.of("error", "用户名或口令不正确"));
                return;
            }
            User user = found.get();
            String token = authStore.newSession(user.username());
            ctx.header("Set-Cookie", COOKIE + "=" + token
                    + "; Path=/; Max-Age=" + (sessionTtlMs / 1000)
                    + "; HttpOnly; SameSite=Lax");
            ctx.json(Map.of("username", user.username(),
                    "role", user.role().name(),
                    "displayName", displayNameOf(user)));
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad request: " + e.getMessage()));
        }
    }

    /** POST /api/auth/logout —— 删除服务端会话并清 cookie。 */
    private void logout(Context ctx) {
        authStore.invalidate(ctx.cookie(COOKIE));
        ctx.header("Set-Cookie", COOKIE + "=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax");
        ctx.json(Map.of("ok", true));
    }

    /** GET /api/auth/me —— 当前登录用户（前端据此过滤横向菜单）。 */
    private void me(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        ctx.json(Map.of("username", user.username(),
                "role", user.role().name(),
                "displayName", displayNameOf(user)));
    }

    private static String displayNameOf(User user) {
        return user.displayName() == null || user.displayName().isBlank()
                ? user.username() : user.displayName();
    }

    /**
     * 当前视角用户名，用于 catalog / 自定义函数的 {@code is_private} 隔离：
     * admin（或未登录）→ {@code null} = 看全部（且创建的私有资源视为全局）；其余 → 登录名。
     */
    private static String viewerOf(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        return user == null || user.isAdmin() ? null : user.username();
    }

    // ==================== 用户管理 REST 处理器（仅管理员）====================

    /** 管理员闸门；非管理员写 403 并返回 false。 */
    private boolean requireAdmin(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        if (user == null || !user.isAdmin()) {
            ctx.status(403).json(Map.of("error", "仅管理员可以管理用户"));
            return false;
        }
        return true;
    }

    /** GET /api/users —— 账号列表 + 当前账号 + 可选角色。 */
    private void listUsers(Context ctx) {
        if (!requireAdmin(ctx)) {
            return;
        }
        User current = ctx.attribute(ATTR_USER);
        List<Map<String, Object>> users = new ArrayList<>();
        for (AuthStore.UserEntry e : authStore.listUsers()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("username", e.username());
            m.put("role", e.role().name());
            m.put("displayName", e.displayName() == null ? "" : e.displayName());
            m.put("createdAt", e.createdAt());
            m.put("updatedAt", e.updatedAt());
            users.add(m);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("users", users);
        resp.put("currentUser", current.username());
        resp.put("roles", List.of(Role.ADMIN.name(), Role.DEVELOPER.name(), Role.ANALYST.name()));
        ctx.json(resp);
    }

    /**
     * POST /api/users —— 新建账号，body {@code {username,password,role,displayName}}。
     * 口令最短 6 位；用户名不可重复。
     */
    private void createUser(Context ctx) {
        if (!requireAdmin(ctx)) {
            return;
        }
        Map body;
        try {
            body = json.readValue(ctx.body(), Map.class);
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad request: " + e.getMessage()));
            return;
        }
        String username = trimOrNull(str(body == null ? null : body.get("username")));
        String password = str(body == null ? null : body.get("password"));
        String displayName = str(body == null ? null : body.get("displayName"));
        Role role;
        try {
            role = Role.parse(str(body == null ? null : body.get("role")));
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(Map.of("error", e.getMessage()));
            return;
        }
        String invalid = validateUsername(username);
        if (invalid != null) {
            ctx.status(400).json(Map.of("error", invalid));
            return;
        }
        if (password == null || password.isBlank()) {
            ctx.status(400).json(Map.of("error", "口令不能为空"));
            return;
        }
        if (password.length() < 6) {
            ctx.status(400).json(Map.of("error", "口令至少 6 位"));
            return;
        }
        if (authStore.findUser(username).isPresent()) {
            ctx.status(409).json(Map.of("error", "用户已存在: " + username));
            return;
        }
        authStore.createUser(username, password, role, blankToNull(displayName));
        log.info("用户管理：新建账号 {}（角色 {}）", username, role);
        ctx.json(Map.of("ok", true, "username", username, "role", role.name()));
    }

    /**
     * POST /api/users/{username} —— 修改角色 / 显示名 / 口令，body
     * {@code {role,displayName,password?}}；{@code password} 留空 = 不改口令。
     */
    private void updateUser(Context ctx) {
        if (!requireAdmin(ctx)) {
            return;
        }
        String username = ctx.pathParam("username");
        Optional<User> found = authStore.findUser(username);
        if (found.isEmpty()) {
            ctx.status(404).json(Map.of("error", "用户不存在: " + username));
            return;
        }
        Map body;
        try {
            body = json.readValue(ctx.body(), Map.class);
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad request: " + e.getMessage()));
            return;
        }
        Role role;
        try {
            role = Role.parse(str(body == null ? null : body.get("role")));
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(Map.of("error", e.getMessage()));
            return;
        }
        String password = str(body == null ? null : body.get("password"));
        if (password != null && !password.isBlank() && password.length() < 6) {
            ctx.status(400).json(Map.of("error", "口令至少 6 位"));
            return;
        }
        if (found.get().isAdmin() && role != Role.ADMIN && authStore.countAdmins() <= 1) {
            ctx.status(400).json(Map.of("error", "至少保留一个管理员账号"));
            return;
        }
        String displayName = str(body == null ? null : body.get("displayName"));
        authStore.updateUser(username, role, blankToNull(displayName), password);
        log.info("用户管理：更新账号 {} → 角色 {}", username, role);
        ctx.json(Map.of("ok", true, "username", username, "role", role.name()));
    }

    /**
     * DELETE /api/users/{username} —— 删除账号，并清理其脚本 / AI 会话 / 技能 / 模型配置。
     * 保护：不能删除自己；不能删除最后一个管理员。
     */
    private void deleteUser(Context ctx) {
        if (!requireAdmin(ctx)) {
            return;
        }
        User current = ctx.attribute(ATTR_USER);
        String username = ctx.pathParam("username");
        if (current != null && current.username().equals(username)) {
            ctx.status(400).json(Map.of("error", "不能删除当前登录账号"));
            return;
        }
        Optional<User> found = authStore.findUser(username);
        if (found.isEmpty()) {
            ctx.status(404).json(Map.of("error", "用户不存在: " + username));
            return;
        }
        if (found.get().isAdmin() && authStore.countAdmins() <= 1) {
            ctx.status(400).json(Map.of("error", "至少保留一个管理员账号"));
            return;
        }
        authStore.deleteUser(username);
        aiConfigStore.delete(username);
        aiSessionStore.deleteAll(username);
        aiSkillStore.deleteAll(username);
        scriptStore.deleteAll(username);
        aiServices.remove(username);
        log.info("用户管理：删除账号 {}（含脚本 / AI 会话 / 技能 / 模型配置）", username);
        ctx.json(Map.of("ok", true, "username", username));
    }

    private static String trimOrNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s.trim();
    }

    /** 用户名规则：非空、≤64 字符、不含空白。返回错误文案（合法返回 null）。 */
    private static String validateUsername(String username) {
        if (username == null) {
            return "用户名不能为空";
        }
        if (username.length() > 64) {
            return "用户名不能超过 64 个字符";
        }
        for (int i = 0; i < username.length(); i++) {
            if (Character.isWhitespace(username.charAt(i))) {
                return "用户名不能包含空白字符";
            }
        }
        return null;
    }

    private void explain(Context ctx) {
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            String sql = (String) body.get("sql");
            if (sql == null || sql.isBlank()) {
                ctx.status(400).json(Map.of("error", "SQL is required"));
                return;
            }

            LogicalPlan plan = analyzer.analyze(sql);
            LogicalPlan optimized = optimizer.optimize(plan);

            // 如果 EXPLAIN 包装，取内部计划展示
            LogicalPlan toShow = optimized instanceof LogicalPlan.Explain e
                ? e.statement() : optimized;

            DagData dag = PlanToDagConverter.convert(toShow);
            ctx.json(dag);
        } catch (SemanticException e) {
            ctx.status(400).json(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", "Internal error: " + e.getMessage()));
        }
    }

    // ==================== 查询监控 REST 处理器 ====================

    /** 当前 QueryMonitor；为 null 时监控接口返回 503。 */
    private QueryMonitor requireMonitor(Context ctx) {
        if (monitor == null) {
            ctx.status(503).json(Map.of("error", "monitor not bound (standalone preview mode)"));
            return null;
        }
        return monitor;
    }

    /**
     * GET /api/queries?state=&limit= —— 任务列表（含最近历史，倒序）。
     *
     * <p>{@code state}：all（默认）/ active / running / submitted / finished / failed / canceled，
     * 可逗号组合（别名见 {@link MonitorViews#statesOf(String)}）；
     * {@code limit}：最多返回条数（默认 0 = 不限，上限 {@value MonitorViews#MAX_LIST_QUERIES}）。
     * 响应额外带 {@code total}（过滤后条数）与 {@code totalQueries}（全部条数），
     * 原 {@code queries} 字段口径不变（前端向后兼容）。</p>
     */
    private void listQueries(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        List<MonitorJob> all = m.listQueries();
        String state = ctx.queryParam("state");
        List<MonitorJob> jobs = MonitorViews.filterJobs(all, state);
        int total = jobs.size();
        int limit = MonitorViews.clampLimit(ctx.queryParam("limit"), 0, MonitorViews.MAX_LIST_QUERIES);
        if (limit > 0) {
            jobs = MonitorViews.head(jobs, limit);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("queries", jobs);
        body.put("total", total);
        body.put("totalQueries", all.size());
        body.put("state", state == null || state.isBlank() ? "all" : state);
        ctx.json(body);
    }

    /** GET /api/queries/{id} —— 单查询完整快照（算子 DAG + 任务进度）。 */
    private void querySnapshot(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        String id = ctx.pathParam("id");
        MonitorQuery q = m.snapshot(id);
        ctx.json(q);
    }

    /**
     * GET /api/queries/{id}/operators —— 单查询的算子执行明细（查询概要 + 全部算子 + 失败映射）。
     * 查询不存在 → 404（区别于 {@link #querySnapshot} 的 UNKNOWN 占位快照）。
     */
    private void queryOperators(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        String id = ctx.pathParam("id");
        MonitorQueryOperators dto = MonitorViews.toQueryOperators(m.snapshot(id));
        if (!dto.found()) {
            ctx.status(404).json(Map.of("error", "no such query: " + id));
            return;
        }
        ctx.json(dto);
    }

    /**
     * GET /api/queries/{id}/operators/{op} —— 单个算子的完整指标与分布式任务明细。
     *
     * <p>{@code op} 可为执行 DAG 的节点下标（{@code "3"} / {@code "#3"}）或 {@code operatorId}。
     * 查询或算子不存在 → 404（算子不存在时附带该查询可用的算子下标列表）。</p>
     */
    private void queryOperator(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        String id = ctx.pathParam("id");
        String key = ctx.pathParam("op");
        MonitorQuery q = m.snapshot(id);
        if (!MonitorViews.exists(q)) {
            ctx.status(404).json(Map.of("error", "no such query: " + id));
            return;
        }
        MonitorOperator op = MonitorViews.findOperator(q, key);
        if (op == null) {
            ctx.status(404).json(Map.of(
                    "error", "operator not found: " + key,
                    "operators", q.operators() == null ? List.of() : q.operators().stream()
                            .map(o -> "#" + o.idx() + " " + o.type()).toList()));
            return;
        }
        ctx.json(op);
    }

    /**
     * GET /api/engine/status?state=&limit= —— 引擎状态总览：系统层面（Master + Worker 的
     * CPU / 内存 / 堆 / 磁盘 / 线程 / 受管直接内存 / 槽位与运行任务）+ 查询层面（按状态计数、
     * 过滤后的查询列表）。{@code limit} 默认 {@value MonitorViews#DEFAULT_ENGINE_QUERIES}、
     * 上限 {@value MonitorViews#MAX_ENGINE_QUERIES}。
     */
    private void engineStatus(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        List<MonitorJob> all = m.listQueries();
        String state = ctx.queryParam("state");
        int limit = MonitorViews.clampLimit(ctx.queryParam("limit"),
                MonitorViews.DEFAULT_ENGINE_QUERIES, MonitorViews.MAX_ENGINE_QUERIES);
        MonitorEngineStatus status = MonitorViews.engineStatus(
                all, m.cluster(), state, limit, System.currentTimeMillis());
        ctx.json(status);
    }

    /** GET /api/cluster —— 集群总览（Master + 健康 Worker + 活动/最近查询）。 */
    private void cluster(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        MonitorCluster c = m.cluster();
        ctx.json(c);
    }

    /** GET /api/cluster/history?max=N —— 各节点资源时间序列（监控页折线图）。 */
    private void clusterHistory(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        int max = Math.min(1000, Math.max(1, ctx.queryParamAsClass("max", Integer.class).getOrDefault(180)));
        ctx.json(m.history(max));
    }

    /**
     * GET /api/logs?node=&tail= —— 只读查看集群日志（各机 {@code logs/dojao.out} 尾部）。
     *
     * <p>{@code node}：{@code master}（默认）/ {@code local} / Worker 的 {@code workerId}
     * （支持唯一前缀）或主机名；{@code tail}：返回行数，默认 200、上限 5000。</p>
     *
     * <p>节点只能从当前集群快照里挑（{@link LogViewer#resolve}），主机名与路径都不来自请求参数；
     * Master 同机直接读文件，其余 Worker 走 Master → Worker 的免密 ssh（见 {@link LogViewer}）。</p>
     */
    private void logs(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        int tail;
        String rawTail = ctx.queryParam("tail");
        if (rawTail == null || rawTail.isBlank()) {
            tail = LogViewer.DEFAULT_TAIL;
        } else {
            try {
                tail = Integer.parseInt(rawTail.trim());
            } catch (NumberFormatException e) {
                ctx.status(400).json(Map.of("error", "tail 必须是整数: " + rawTail));
                return;
            }
            tail = Math.min(LogViewer.MAX_TAIL, Math.max(1, tail));
        }

        LogViewer.Target target = LogViewer.resolve(m.cluster(), ctx.queryParam("node"));
        if (target == null) {
            ctx.status(404).json(Map.of("error",
                    "未知节点: " + ctx.queryParam("node")
                            + "（可选 master，或 /api/cluster 里的 workerId / 主机名）"));
            return;
        }

        java.nio.file.Path path = LogViewer.logPath();
        List<String> lines;
        try {
            lines = target.local() ? LogViewer.tailLocal(path, tail)
                    : LogViewer.tailRemote(target.host(), path, tail);
        } catch (NoSuchFileException e) {
            ctx.status(404).json(Map.of("error",
                    "日志文件不存在: " + path + "（可用 -Dsystem.ui.log.path=... 指定）"));
            return;
        } catch (Exception e) {
            log.warn("读取节点日志失败 [{}] {}", target, e.getMessage());
            ctx.status(502).json(Map.of("error", "读取节点日志失败: " + e.getMessage()));
            return;
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("node", target.node());
        body.put("host", target.host());
        body.put("local", target.local());
        body.put("path", path.toString());
        body.put("tail", tail);
        body.put("lines", lines);
        ctx.json(body);
    }

    // ==================== SQL 异步执行 REST 处理器 ====================

    /** POST /api/query —— 异步提交 SQL，body {"sql": "..."}，返回受理 queryId。 */
    private void submitQuery(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            String sql = body == null ? null : (String) body.get("sql");
            if (sql == null || sql.isBlank()) {
                ctx.status(400).json(Map.of("error", "SQL is required"));
                return;
            }
            MonitorSubmit r = m.submit(sql, viewerOf(ctx));
            ctx.json(r);
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad request: " + e.getMessage()));
        }
    }

    /** POST /api/query/{id}/cancel —— 取消进行中的查询。 */
    private void cancelQuery(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        String id = ctx.pathParam("id");
        try {
            m.cancel(id);
            ctx.json(Map.of("queryId", id, "state", "CANCELED"));
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", "cancel failed: " + e.getMessage()));
        }
    }

    /** GET /api/query/{id}/rows?offset=&limit= —— 分页拉取结果预览。 */
    private void queryRows(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        String id = ctx.pathParam("id");
        int offset = Math.max(0, ctx.queryParamAsClass("offset", Integer.class).getOrDefault(0));
        int limit = Math.min(1000, Math.max(1, ctx.queryParamAsClass("limit", Integer.class).getOrDefault(200)));
        MonitorResult r = m.result(id, offset, limit);
        ctx.json(r);
    }

    // ==================== 新建 Catalog 向导 REST 处理器 ====================
    // 首页「Catalog 浏览」标题左侧（刷新按钮左边）的【新建 catalog】：连接器卡片 → 表单 → 测试连接 / 创建。
    // 「创建」不在 UI 进程直接写元数据，而是拼出 CREATE CATALOG 走正常 SQL 执行线 ——
    // 与手写 DDL 同一套权限、工厂校验、连通性探针和落库顺序，不会出现两份语义。

    /** 建 catalog 的 DDL 等待上限（连接探针 + 落库）；超时把 queryId 回给用户自行查看。 */
    private static final long CATALOG_CREATE_TIMEOUT_MS = 120_000L;
    private static final long CATALOG_CREATE_POLL_MS = 300L;

    /** GET /api/connectors —— 新建向导的连接器卡片清单（含表单字段与加载失败记录）。 */
    private void listConnectors(Context ctx) {
        try {
            ConnectorRegistry registry = MetaStore.connectorRegistry();
            List<CatalogCreateSupport.CatalogConnector> connectors = CatalogCreateSupport.describe(registry);
            ctx.json(Map.of("connectors", connectors, "failures", registry.failures()));
        } catch (Exception e) {
            log.warn("连接器清单加载失败: {}", Errors.describe(e));
            ctx.status(500).json(Map.of("error", "连接器清单加载失败: " + Errors.describe(e)));
        }
    }

    /** POST /api/catalog/test —— body {type, name?, values}；真的连一次数据源，不落库。 */
    private void testCatalogConnection(Context ctx) {
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            String type = CatalogCreateSupport.requireType(asString(body.get("type")));
            String name = asString(body.get("name"));
            ConnectorFactory factory = connectorFactory(type);
            CatalogCreateSupport.CatalogConnector connector =
                    CatalogCreateSupport.describe(factory, MetaStore.connectorRegistry().origins().get(type));
            Map<String, String> values = CatalogCreateSupport.effectiveValues(
                    connector, asStringMap(body.get("values")));
            CatalogCreateSupport.testConnection(factory, name, values);
            log.info("[catalog] 测试连接成功 type={} name={}", type, name == null ? "" : name.trim());
            ctx.json(Map.of("ok", true, "message", "连接成功，可以创建了"));
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.warn("[catalog] 测试连接失败: {}", Errors.describe(e));
            ctx.status(400).json(Map.of("error", Errors.describe(e)));
        }
    }

    /**
     * POST /api/catalog —— body {name, type, values}；生成并执行 {@code CREATE CATALOG}。
     *
     * <p>提交到 {@link QueryMonitor} 后同步轮询到终态（DDL 通常毫秒级；连接探针可能慢一些），
     * 失败把 cause 链原样回给前端。密码只进 SQL 文本，不写日志、不回响应。</p>
     */
    private void createCatalog(Context ctx) {
        QueryMonitor m = requireMonitor(ctx);
        if (m == null) return;
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            String name = CatalogCreateSupport.requireName(asString(body.get("name")));
            String type = CatalogCreateSupport.requireType(asString(body.get("type")));
            ConnectorFactory factory = connectorFactory(type);
            CatalogCreateSupport.CatalogConnector connector =
                    CatalogCreateSupport.describe(factory, MetaStore.connectorRegistry().origins().get(type));
            Map<String, String> values = CatalogCreateSupport.effectiveValues(
                    connector, asStringMap(body.get("values")));

            // 先跑工厂静态校验：缺必填项这类错误直接 400，不占执行队列
            factory.validate(name, values);
            String sql = CatalogCreateSupport.buildCreateSql(name, type, values);

            MonitorSubmit submitted = m.submit(sql, viewerOf(ctx));
            if (!"SUBMITTED".equals(submitted.state())) {
                ctx.status(400).json(Map.of("error",
                        submitted.message() == null ? "提交失败" : submitted.message()));
                return;
            }

            long deadline = System.currentTimeMillis() + CATALOG_CREATE_TIMEOUT_MS;
            MonitorResult result = m.result(submitted.queryId(), 0, 1);
            while (!isTerminalState(result.state()) && System.currentTimeMillis() < deadline) {
                Thread.sleep(CATALOG_CREATE_POLL_MS);
                result = m.result(submitted.queryId(), 0, 1);
            }
            if (!isTerminalState(result.state())) {
                ctx.status(504).json(Map.of(
                        "error", "创建超时（" + (CATALOG_CREATE_TIMEOUT_MS / 1000) + " 秒），"
                                + "可到任务列表查看 queryId=" + submitted.queryId(),
                        "queryId", submitted.queryId(),
                        "state", result.state()));
                return;
            }
            if (!"FINISHED".equals(result.state())) {
                ctx.status(400).json(Map.of(
                        "error", result.error() == null ? ("执行失败: " + result.state()) : result.error(),
                        "queryId", submitted.queryId(),
                        "state", result.state()));
                return;
            }
            log.info("[catalog] 新建 catalog name={} type={} viewer={} queryId={}",
                    name, type, viewerOf(ctx), submitted.queryId());
            ctx.json(Map.of("ok", true, "queryId", submitted.queryId(),
                    "message", "Catalog '" + name + "' 创建成功"));
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(Map.of("error", e.getMessage()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ctx.status(500).json(Map.of("error", "创建过程被中断"));
        } catch (Exception e) {
            log.warn("[catalog] 新建 catalog 失败: {}", Errors.describe(e));
            ctx.status(500).json(Map.of("error", Errors.describe(e)));
        }
    }

    /** 按类型取工厂；没有就给出「当前可用类型」清单。 */
    private static ConnectorFactory connectorFactory(String type) {
        return MetaStore.factoryFor(type).orElseThrow(() -> new IllegalArgumentException(
                "未知的连接器类型: '" + type + "'。当前可用: "
                        + String.join(", ", MetaStore.registeredTypes())));
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    /** JSON object → 字符串 Map（值统一转字符串；null 值按空串）。 */
    private static Map<String, String> asStringMap(Object value) {
        Map<String, String> out = new LinkedHashMap<>();
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (e.getKey() == null) continue;
                out.put(String.valueOf(e.getKey()), e.getValue() == null ? "" : String.valueOf(e.getValue()));
            }
        }
        return out;
    }

    private static boolean isTerminalState(String state) {
        return "FINISHED".equals(state) || "FAILED".equals(state) || "CANCELED".equals(state);
    }

    // ==================== 元数据目录树 REST 处理器 ====================
    // 首页左侧「catalog → schema → table」树。绑定了 monitor（集群模式）时读 server 权威
    // MetaStore；独立预览模式（monitor == null）回退本地演示 MetaStore。连接器不可达时
    // 返回 500 + 原因，前端把该节点显示为空/错误，不影响其余节点。

    /**
     * 目录树分片加载失败的统一出口：<b>Master 日志 + 带 cause 链的错误体</b>。
     *
     * <p>两个都必须有：只回 {@code e.getMessage()} 时，用户拿到的是连接器外壳那句
     * {@code "Failed to list schemas"}，真因（{@code Access denied for user 'root'@'aliyun001'}、
     * {@code Unknown database 'metastore'}、{@code Communications link failure}）埋在 cause 链里没人看得到；
     * 而 UI 跑在 Master 进程内，不打日志则 Master 端一片安静，看起来像「没报错」。见 {@link Errors}。</p>
     */
    private static void failMetadata(Context ctx, String what, String target, Throwable e) {
        String detail = Errors.describe(e);
        log.warn("目录树加载失败 [{} {}]: {}", what, target, detail);
        ctx.status(500).json(Map.of("error", what + " failed: " + detail));
    }

    /** GET /api/metadata/catalogs —— 已注册 catalog 列表。 */
    private void listCatalogs(Context ctx) {
        try {
            List<String> catalogs = monitor != null
                    ? monitor.listCatalogs(viewerOf(ctx)) : metaStore.listRegisteredCatalogs();
            ctx.json(Map.of("catalogs", catalogs));
        } catch (Exception e) {
            failMetadata(ctx, "list catalogs", "-", e);
        }
    }

    /** GET /api/metadata/catalogs/{catalog}/schemas —— 指定 catalog 下的 schema 列表。 */
    private void listSchemas(Context ctx) {
        String catalog = ctx.pathParam("catalog");
        try {
            List<String> schemas = monitor != null
                    ? monitor.listSchemas(catalog) : metaStore.listAllSchemas(catalog);
            ctx.json(Map.of("schemas", schemas));
        } catch (Exception e) {
            failMetadata(ctx, "list schemas", catalog, e);
        }
    }

    /** GET /api/metadata/catalogs/{catalog}/schemas/{schema}/tables —— 指定 schema 下的表列表。 */
    private void listTables(Context ctx) {
        String catalog = ctx.pathParam("catalog");
        String schema = ctx.pathParam("schema");
        try {
            List<String> tables = monitor != null
                    ? monitor.listTables(catalog, schema) : metaStore.listAllTables(catalog, schema);
            ctx.json(Map.of("tables", tables));
        } catch (Exception e) {
            failMetadata(ctx, "list tables", catalog + "." + schema, e);
        }
    }

    /** GET .../tables/{table}/columns —— 指定表的列信息（目录树展开到表级别）。 */
    private void listColumns(Context ctx) {
        String catalog = ctx.pathParam("catalog");
        String schema = ctx.pathParam("schema");
        String table = ctx.pathParam("table");
        try {
            List<MonitorColumn> columns = monitor != null
                    ? monitor.listColumns(catalog, schema, table)
                    : localColumns(catalog, schema, table);
            ctx.json(Map.of("columns", columns));
        } catch (Exception e) {
            failMetadata(ctx, "list columns", catalog + "." + schema + "." + table, e);
        }
    }

    /** 独立预览模式（未绑定 monitor）下，用本地演示 MetaStore 读列信息。 */
    private List<MonitorColumn> localColumns(String catalog, String schema, String table) {
        List<MonitorColumn> out = new java.util.ArrayList<>();
        for (com.dojao.spi.bean.Column c : metaStore.getColumns(catalog, schema, table)) {
            out.add(new MonitorColumn(c.name(), c.dataType(), c.comment(), c.nullable()));
        }
        return out;
    }

    /** GET /api/functions —— 内置 + 自定义函数目录（SQL 编辑器补全）。 */
    private void listFunctions(Context ctx) {
        try {
            List<MonitorFunction> functions = monitor != null
                    ? monitor.listFunctions(viewerOf(ctx)) : localFunctions();
            ctx.json(Map.of("functions", functions));
        } catch (Exception e) {
            failMetadata(ctx, "list functions", "-", e);
        }
    }

    /**
     * 独立预览模式（未绑定 monitor）下只能给出自定义函数 —— 内置目录在 dojao-core，
     * ui 模块不依赖它。与「独立预览本就跑不了查询」一致，补全此时只缺内置函数。
     */
    private List<MonitorFunction> localFunctions() {
        List<MonitorFunction> out = new java.util.ArrayList<>();
        for (com.dojao.spi.bean.FunctionDefinition def : metaStore.listFunctions()) {
            String name = def.name() == null ? "" : def.name().toLowerCase();
            String backend = def.type() == null ? "" : def.type();
            String detail = def.comment() == null ? "" : def.comment();
            out.add(new MonitorFunction(name, "custom", "", backend, detail));
        }
        out.sort(java.util.Comparator.comparing(MonitorFunction::name));
        return out;
    }

    // ==================== SQL 脚本 REST 处理器 ====================
    // 脚本归属 = 当前登录用户（「跟随登录状态」的隔离）：每人只见/只读写自己的脚本。
    // 与函数/catalog 的 is_private 是两条独立的隔离线，这里不涉及 admin 全局视角。

    /** GET /api/scripts —— 当前用户脚本清单（不含正文，按更新时间倒序）。 */
    private void listScripts(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        ctx.json(Map.of("scripts", scriptStore.list(user.username())));
    }

    /** POST /api/scripts —— 保存脚本，body {"name": "...", "content": "..."}；同名覆盖。 */
    private void saveScript(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            String name = body == null ? null : (String) body.get("name");
            String content = body == null ? null : (String) body.get("content");
            if (name == null || name.isBlank()) {
                ctx.status(400).json(Map.of("error", "脚本名不能为空"));
                return;
            }
            ctx.json(scriptStore.save(user.username(), name.trim(), content == null ? "" : content));
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad request: " + e.getMessage()));
        }
    }

    /** GET /api/scripts/{id} —— 单条脚本（含正文），供加载进编辑器。 */
    private void getScript(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        SqlScriptStore.ScriptFull s = scriptStore.get(user.username(), ctx.pathParam("id"));
        if (s == null) {
            ctx.status(404).json(Map.of("error", "脚本不存在"));
            return;
        }
        ctx.json(s);
    }

    /** DELETE /api/scripts/{id} —— 删除本人脚本。 */
    private void deleteScript(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        boolean ok = scriptStore.delete(user.username(), ctx.pathParam("id"));
        if (!ok) {
            ctx.status(404).json(Map.of("error", "脚本不存在"));
            return;
        }
        ctx.json(Map.of("ok", true));
    }

    // ==================== AI 对话 REST 处理器 ====================
    // 配置按登录账号隔离（一账号一个模型）；无自有配置时沿用管理员配置。
    // 密钥不回传，POST 留空 = 保留该账号当前生效的密钥。

    /**
     * 取账号的 AI 服务组：配置未变时复用缓存，变了（或首次访问）按新配置重建。
     * 未配置 API Key → {@code chat()/agent()} 为 null（聊天接口 503）。
     */
    private UserAiServices aiServicesFor(String owner) {
        AiConfig cfg = aiConfigStore.load(owner);
        UserAiServices cur = aiServices.get(owner);
        if (cur != null && cur.config().equals(cfg)) {
            return cur;
        }
        synchronized (aiServices) {
            cur = aiServices.get(owner);
            if (cur != null && cur.config().equals(cfg)) {
                return cur;
            }
            UserAiServices built = new UserAiServices(cfg,
                    cfg.configured() ? new AiChatService(cfg) : null,
                    cfg.configured() ? new AiAgentService(cfg, monitor, metaStore, aiFileStore) : null);
            aiServices.put(owner, built);
            return built;
        }
    }

    /**
     * GET /api/ai/config —— 当前账号的生效配置；apiKey 只回布尔 apiKeySet，绝不含原始密钥。
     * {@code scope}：own = 本账号独立配置；inherited = 沿用管理员配置；unset = 尚无密钥。
     */
    private void aiConfigGet(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        String owner = user == null ? AiConfigStore.ADMIN_OWNER : user.username();
        AiConfig c = aiConfigStore.load(owner);
        boolean own = aiConfigStore.hasOwn(owner);
        boolean configured = c.apiKey() != null && !c.apiKey().isBlank();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("owner", owner);
        resp.put("scope", own ? "own" : (configured ? "inherited" : "unset"));
        resp.put("baseUrl", c.baseUrl());
        resp.put("model", c.model());
        resp.put("temperature", c.temperature());
        resp.put("apiKeySet", configured);
        ctx.json(resp);
    }

    /**
     * POST /api/ai/config —— 保存<b>当前账号</b>的配置，body {"baseUrl","model","temperature","apiKey"}。
     * {@code apiKey} 留空 = 保留当前生效的密钥。
     */
    private void aiConfigSave(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        String owner = user == null ? AiConfigStore.ADMIN_OWNER : user.username();
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            if (body == null) {
                ctx.status(400).json(Map.of("error", "请求体为空"));
                return;
            }
            String baseUrl = str(body.get("baseUrl"));
            if (baseUrl == null || baseUrl.isBlank()) {
                ctx.status(400).json(Map.of("error", "服务地址不能为空"));
                return;
            }
            String model = str(body.get("model"));
            if (model == null || model.isBlank()) {
                ctx.status(400).json(Map.of("error", "模型名不能为空"));
                return;
            }
            String apiKeyIn = str(body.get("apiKey"));
            // 留空 = 沿用当前生效的密钥（可能是管理员配置）
            String apiKey = (apiKeyIn == null || apiKeyIn.isBlank())
                    ? aiConfigStore.load(owner).apiKey()
                    : apiKeyIn;

            double temperature = AiConfigStore.DEFAULT_TEMPERATURE;
            Object tempObj = body.get("temperature");
            if (tempObj instanceof Number n) {
                temperature = n.doubleValue();
            } else if (tempObj != null && !tempObj.toString().isBlank()) {
                temperature = Double.parseDouble(tempObj.toString().trim());
            }

            aiConfigStore.save(owner, new AiConfig(baseUrl.trim(), apiKey, model.trim(), temperature));
            aiServices.remove(owner);   // 下次请求按新配置重建
            ctx.json(Map.of("ok", true, "owner", owner));
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad request: " + e.getMessage()));
        }
    }

    /** POST /api/ai/chat —— 单轮提问，body {"message": "..."}；返回 {"reply": "..."}。 */
    private void aiChat(Context ctx) {
        User aiUser = ctx.attribute(ATTR_USER);
        String who = aiUser == null ? AiConfigStore.ADMIN_OWNER : aiUser.username();
        AiChatService chat = aiServicesFor(who).chat();
        if (chat == null) {
            ctx.status(503).json(Map.of("error", "AI 未配置（请在「AI 模型管理」填写当前账号的服务地址与密钥）"));
            return;
        }
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            String message = body == null ? null : str(body.get("message"));
            if (message == null || message.isBlank()) {
                ctx.status(400).json(Map.of("error", "消息不能为空"));
                return;
            }
            log.info("[AI] 单轮提问 user={} 提问: {}", who, oneLine(message, 300));
            String reply = chat.chat(message);
            log.info("[AI] 单轮回答 user={} 回答{}字", who, reply == null ? 0 : reply.length());
            ctx.json(Map.of("reply", reply == null ? "" : reply));
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", e.getMessage() == null ? e.toString() : e.getMessage()));
        }
    }

    /** 取字符串值（非串类型转 toString；null 原样返回）。 */
    private static String str(Object o) {
        return o == null ? null : o.toString();
    }

    /** 日志用单行摘要：换行/制表压成空格，超长截断（供后台日志观察，避免刷屏）。 */
    private static String oneLine(String s, int max) {
        if (s == null) {
            return "(null)";
        }
        String one = s.replaceAll("\\s+", " ").strip();
        return one.length() <= max ? one : one.substring(0, max) + "…（共 " + one.length() + " 字）";
    }

    // ---------- ReAct 流式推理（NDJSON）----------

    /** 队列哨兵：drain 循环见它即结束（不写回客户端）。 */
    private static final String STREAM_END = "\u0000__END__";

    /** 多轮对话带进模型的历史轮数上限（越早的轮次仍留在库/界面，只是不再喂给模型）。 */
    private static final int HISTORY_TURNS = 10;

    /**
     * 推理进度增量落库的节流间隔（纳秒）：思考 / 半成品回答按此间隔写回 pending 轮次，
     * 工具调用 / 结果 / 图表 / 文件则立即写回（它们本身就是重要节点）。
     */
    private static final long AI_PROGRESS_FLUSH_NANOS = 500_000_000L;

    /**
     * POST /api/ai/chat/stream —— ReAct 推理，流式返回 NDJSON（每行一个事件 JSON）：
     * {@code {"type":"text","text":..}} / {@code {"type":"thinking",..}} /
     * {@code {"type":"tool_call","name":..,"args":..}} / {@code {"type":"tool_result","name":..,"result":..}} /
     * {@code {"type":"ddl_confirm","streamId":..,"sql":..}}（等待用户确认 DDL）/
     * {@code {"type":"chart","spec":..}}（图表）/
     * {@code {"type":"file","file":{id,name,format,size,url}}}（可下载文件）/
     * 终态 {@code {"type":"done"}} 或 {@code {"type":"error","message":..}}。
     *
     * <p>请求体 {@code {"sessionId":..,"streamId":..,"message":..,"attachments":[…]}}：会话历史由服务端
     * 按 sessionId 读出（取最近 {@link #HISTORY_TURNS} 轮）拼进模型上下文；{@code attachments} 为前端经
     * {@code /api/ai/files/parse} 解析好的参考文件（形状见 {@link FileParser#toJson}），会以统一的
     * 「参考文件」块拼进本轮提问并随轮次落库（后续追问仍可引用）。本轮完成后服务端把该轮问答落库。</p>
     *
     * <p>推理在后台线程跑（LangChain4j 流式回调来自其 HTTP 线程），事件经 {@link BlockingQueue} 交给
     * 请求线程逐行写出并 flush。DDL 确认经 {@link #aiConfirmations} 与 {@code POST /api/ai/confirm} 联动。</p>
     */
    private void aiChatStream(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        String owner = user == null ? AiConfigStore.ADMIN_OWNER : user.username();
        UserAiServices ai = aiServicesFor(owner);
        if (ai.agent() == null) {
            ctx.status(503).json(Map.of("error", "AI 未配置（请在「AI 模型管理」填写当前账号的服务地址与密钥）"));
            return;
        }
        String message;
        String sessionId;
        String streamId;
        String skillId;
        JsonNode attachmentsNode;
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            message = body == null ? null : str(body.get("message"));
            sessionId = body == null ? null : str(body.get("sessionId"));
            streamId = body == null ? null : str(body.get("streamId"));
            skillId = body == null ? null : str(body.get("skillId"));
            attachmentsNode = body == null ? null : json.valueToTree(body.get("attachments"));
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad request: " + e.getMessage()));
            return;
        }
        // 附件：前端已通过 POST /api/ai/files/parse 解析成文本，这里只做配额校验 / 截断。
        List<FileParser.ParsedFile> attachments;
        try {
            attachments = FileParser.fromJson(attachmentsNode);
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(Map.of("error", e.getMessage()));
            return;
        }
        if ((message == null || message.isBlank()) && attachments.isEmpty()) {
            ctx.status(400).json(Map.of("error", "消息不能为空"));
            return;
        }
        if (sessionId == null || sessionId.isBlank()) {
            ctx.status(400).json(Map.of("error", "缺少 sessionId"));
            return;
        }
        if (streamId == null || streamId.isBlank()) {
            streamId = UUID.randomUUID().toString();
        }
        // 附件文本以统一的「参考文件」块拼进本轮提问（历史轮次走 buildHistory 的同一格式）。
        final String rawMessage = message == null ? "" : message;
        final String modelMessage = (rawMessage.isBlank() ? "请根据下面的参考文件回答问题。" : rawMessage)
                + FileParser.promptBlock(attachments);
        final JsonNode attachmentsForStore = attachments.isEmpty() ? null : FileParser.toJson(attachments);

        String viewer = viewerOf(ctx);

        AiSessionStore.SessionFull session = aiSessionStore.get(owner, sessionId);
        if (session == null) {
            ctx.status(404).json(Map.of("error", "会话不存在"));
            return;
        }
        List<ChatMessage> history = buildHistory(session.turnsJson());

        // 本轮技能（可选）：按 owner 取，越权 / 不存在视作「无技能」。
        final AiAgentService.Skill skill = loadSkill(owner, skillId);
        final String skillName = skill == null ? null : skill.name();

        // 后台日志（随 Master 控制台输出）：记录一次 AI 会话的开始 / 工具调用 / 结束，便于后台观察。
        final long aiStartNanos = System.nanoTime();
        final int historyTurns = Math.min(history.size() / 2, HISTORY_TURNS);
        log.info("[AI] 会话开始 user={} session={} skill={} 历史轮数={} 附件={} 提问: {}",
                owner, sessionId, skillName == null ? "(无)" : skillName, historyTurns,
                attachments.isEmpty() ? "(无)" : attachmentSummary(attachments), oneLine(rawMessage, 300));

        // ★ 写前落库：先把本次「用户提问」作为一条 pending 轮次落进会话。这样即便用户中途刷新页面 /
        //   断线，提问也不会丢；答案生成结束后再由 finalizeTurn 回填到同一条轮次。
        final String turnId = beginTurn(owner, sessionId, rawMessage, skillName, attachmentsForStore);

        // 该流对应的 DDL 确认队列：execute_ddl 工具阻塞等它，POST /api/ai/confirm 投递用户决策。
        final String sid = streamId;
        BlockingQueue<Boolean> confirmQueue = new LinkedBlockingQueue<>(1);
        aiConfirmations.put(sid, confirmQueue);

        ctx.res().setContentType("application/x-ndjson");
        ctx.res().setCharacterEncoding("UTF-8");
        ctx.res().setHeader("Cache-Control", "no-cache");
        ctx.res().setHeader("X-Accel-Buffering", "no");

        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        StringBuilder answer = new StringBuilder();     // 累积本轮最终回答（落库用）
        List<String> ops = new ArrayList<>();           // 本轮调用的工具名（落库用）
        List<String> charts = new ArrayList<>();        // 本轮渲染的图表规格 JSON（落库用）
        List<String> files = new ArrayList<>();         // 本轮导出文件的元数据 JSON（落库用）
        // ★ 增量进度：思考 / 工具调用 / 结果 / 半成品回答节流写回 pending 轮次，
        //   用户中途刷新页面后仍能看到已经产生的推理链（不再只有「正在生成回答…」）。
        AiTurnProgress progress = new AiTurnProgress(
                owner, sessionId, turnId, answer, charts, files);

        ReActListener listener = new ReActListener() {
            @Override
            public void onText(String text) {
                if (text != null) {
                    answer.append(text);
                }
                progress.text();
                enqueue(queue, Map.of("type", "text", "text", text == null ? "" : text));
            }

            @Override
            public void onThinking(String text) {
                log.debug("[AI] user={} 思考: {}", owner, oneLine(text, 200));
                progress.thinking(text);
                enqueue(queue, Map.of("type", "thinking", "text", text == null ? "" : text));
            }

            @Override
            public void onToolCall(String name, String argumentsJson) {
                if (name != null) {
                    ops.add(name);
                }
                progress.toolCall(name, argumentsJson);
                log.info("[AI] user={} 调用工具 {} 参数: {}", owner, name,
                        oneLine(argumentsJson, 300));
                enqueue(queue, Map.of("type", "tool_call", "name", name == null ? "" : name,
                        "args", argumentsJson == null ? "" : argumentsJson));
            }

            @Override
            public void onToolResult(String name, String resultText) {
                progress.toolResult(name, resultText);
                log.info("[AI] user={} 工具 {} 返回: {}", owner, name, oneLine(resultText, 300));
                enqueue(queue, Map.of("type", "tool_result", "name", name == null ? "" : name,
                        "result", resultText == null ? "" : resultText));
            }

            @Override
            public void onComplete() {
                // 先回填本轮答案再发 done：前端收到 done 后刷新会话列表即可读到本轮。
                progress.finish();
                finalizeTurn(owner, sessionId, turnId, rawMessage, answer.toString(), ops, charts, files,
                        progress.trace(), skillName);
                aiConfirmations.remove(sid);
                long ms = (System.nanoTime() - aiStartNanos) / 1_000_000L;
                log.info("[AI] 会话结束 user={} session={} 耗时={}ms 工具链={} 回答{}字",
                        owner, sessionId, ms,
                        ops.isEmpty() ? "(无)" : String.join(" -> ", ops), answer.length());
                enqueue(queue, Map.of("type", "done"));
                enqueueRaw(queue, STREAM_END);
            }

            @Override
            public void onError(Throwable t) {
                // 出错也回填（保留已产出的部分推理链 / 回答），避免轮次永远停在「生成中」。
                progress.finish();
                finalizeTurn(owner, sessionId, turnId, rawMessage, answer.toString(), ops, charts, files,
                        progress.trace(), skillName);
                aiConfirmations.remove(sid);
                log.warn("[AI] 会话出错 user={} session={}: {}", owner, sessionId,
                        t.getMessage() == null ? t.toString() : t.getMessage());
                enqueue(queue, Map.of("type", "error",
                        "message", t.getMessage() == null ? t.toString() : t.getMessage()));
                enqueueRaw(queue, STREAM_END);
            }
        };

        // execute_ddl 执行前的用户确认：把待执行 DDL 推给前端，阻塞等 /api/ai/confirm 的决策。
        AiAgentService.DdlConfirm confirm = sql -> {
            log.info("[AI] user={} 请求执行 DDL（等待用户确认）: {}", owner, oneLine(sql, 300));
            enqueue(queue, Map.of("type", "ddl_confirm", "streamId", sid, "sql", sql == null ? "" : sql));
            try {
                Boolean ok = confirmQueue.poll(300, TimeUnit.SECONDS);
                log.info("[AI] user={} DDL 用户确认结果: {}", owner, ok != null && ok ? "同意" : "拒绝/超时");
                return ok != null && ok;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        };

        // render_chart 的图表规格：既写进 NDJSON 流实时渲染，也留存本轮以便落库（重开会话可重绘）。
        AiAgentService.ChartSink chartSink = spec -> {
            if (spec == null || spec.isBlank()) {
                return;
            }
            charts.add(spec);
            progress.chart();
            enqueue(queue, Map.of("type", "chart", "spec", spec));
        };

        // export_file 生成的文件：推一条 file 事件供前端显示下载按钮，同时留存元数据以便落库
        // （重开会话仍能下载 —— 文件本身在 AiFileStore 里按 TTL 存活）。
        AiAgentService.FileSink fileSink = file -> {
            if (file == null) {
                return;
            }
            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("id", file.id());
            meta.put("name", file.name());
            meta.put("format", file.format());
            meta.put("size", file.size());
            meta.put("url", file.url());
            try {
                files.add(json.writeValueAsString(meta));
            } catch (Exception e) {
                return;
            }
            log.info("[AI] user={} 生成导出文件 {}（{}，{} 字节）", owner, file.name(), file.format(), file.size());
            progress.file();
            enqueue(queue, Map.of("type", "file", "file", meta));
        };

        aiExecutor.submit(() -> {
            // 登记本流的工作线程：/api/ai/stop 用户强制停止时 interrupt 它。
            aiRunning.put(sid, Thread.currentThread());
            try {
                // 附件文本拼在本轮提问后面一并交给模型（历史轮次的附件由 buildHistory 补齐）。
                ai.agent().run(modelMessage, history, viewer, listener, confirm, chartSink, fileSink,
                        skill, user.role());
            } catch (Throwable t) {
                // run() 内部已捕获异常走 onError；这里兜底（如线程池拒绝前的中断）。
                progress.finish();
                finalizeTurn(owner, sessionId, turnId, rawMessage, answer.toString(), ops, charts, files,
                        progress.trace(), skillName);
                log.warn("[AI] 会话异常终止 user={} session={}: {}", owner, sessionId,
                        t.getMessage() == null ? t.toString() : t.getMessage(), t);
                enqueue(queue, Map.of("type", "error",
                        "message", t.getMessage() == null ? t.toString() : t.getMessage()));
                enqueueRaw(queue, STREAM_END);
            } finally {
                aiRunning.remove(sid);
                aiConfirmations.remove(sid);
            }
        });

        try {
            PrintWriter out = ctx.res().getWriter();
            while (true) {
                String line = queue.poll(300, TimeUnit.SECONDS);
                if (line == null || STREAM_END.equals(line)) {
                    break;
                }
                out.write(line);
                out.write('\n');
                out.flush();
            }
        } catch (Exception e) {
            // 客户端断开或写入失败：结束 drain，后台推理线程自行结束。
            log.debug("AI 流式写出中断: {}", e.getMessage());
        }
    }

    // ---------- AI 长对话会话 + DDL 确认 ----------

    /**
     * 由轮次 JSON（{@code [{user,assistant,ops:[…]}]}）拼出模型历史：取最近 {@link #HISTORY_TURNS} 轮，
     * 每轮压成 UserMessage + AiMessage（空回答轮只留 user）。
     */
    private List<ChatMessage> buildHistory(String turnsJson) {
        List<ChatMessage> out = new ArrayList<>();
        try {
            JsonNode arr = json.readTree(turnsJson);
            if (arr == null || !arr.isArray()) {
                return out;
            }
            int from = Math.max(0, arr.size() - HISTORY_TURNS);
            Map<Integer, String> attachmentBlocks = historyAttachmentBlocks(arr, from);
            for (int i = from; i < arr.size(); i++) {
                JsonNode t = arr.get(i);
                String u = t.path("user").asText("");
                String a = t.path("assistant").asText("");
                String block = attachmentBlocks.getOrDefault(i, "");
                if (!u.isBlank() || !block.isEmpty()) {
                    // 附件随原轮次的提问一起喂回模型：追问「这份文件里…」时模型才不会「忘记」内容。
                    out.add(UserMessage.userMessage(u + block));
                }
                // 生成中的轮次可能已写入「半成品回答」，不能当完整答案喂回模型。
                if (!a.isBlank() && !t.path("pending").asBoolean(false)) {
                    out.add(AiMessage.from(a));
                }
            }
        } catch (Exception e) {
            log.warn("解析 AI 会话历史失败: {}", e.getMessage());
        }
        return out;
    }

    /**
     * 历史轮次的附件提示词块：<b>从最新一轮往前</b>分配总字符预算（{@link FileParser#MAX_TOTAL_CHARS}），
     * 避免多轮附件（每轮可达 20 万字）把模型上下文撑爆；超预算的更早附件截断并注明。
     */
    private Map<Integer, String> historyAttachmentBlocks(JsonNode turns, int from) {
        Map<Integer, String> out = new LinkedHashMap<>();
        int budget = FileParser.MAX_TOTAL_CHARS;
        for (int i = turns.size() - 1; i >= from && budget > 0; i--) {
            JsonNode att = turns.get(i).path("attachments");
            if (!att.isArray() || att.isEmpty()) {
                continue;
            }
            String block = FileParser.promptBlockFromJson(att);
            if (block.isEmpty()) {
                continue;
            }
            if (block.length() > budget) {
                block = block.substring(0, budget) + "\n…（更早轮次的附件内容已省略）\n";
            }
            out.put(i, block);
            budget -= Math.min(budget, block.length());
        }
        return out;
    }

    /** 日志用附件摘要：{@code 报告.docx(1234字) + q3.xlsx(567字)}。 */
    private static String attachmentSummary(List<FileParser.ParsedFile> attachments) {
        List<String> parts = new ArrayList<>();
        for (FileParser.ParsedFile f : attachments) {
            parts.add(f.name() + "(" + f.chars() + "字" + (f.truncated() ? "，已截断" : "") + ")");
        }
        return String.join(" + ", parts);
    }

    /**
     * <b>写前落库</b>：把本次「用户提问」作为一条 pending 轮次先落进会话（assistant 暂空）。
     * 这样即便用户中途刷新页面 / 断线，提问也不会丢；答案生成结束后由
     * {@link #finalizeTurn} 按 {@code turnId} 回填到同一条轮次。首轮据提问生成标题。
     *
     * @param attachments 本轮附件的 JSON 数组（{@code [{name,format,chars,truncated,text}]}），可为 null
     * @return 本次轮次的 {@code turnId}（回填时据此定位）；会话不存在等异常路径返回 {@code null}
     */
    private String beginTurn(String owner, String sessionId, String userMsg, String skillName,
                             JsonNode attachments) {
        String turnId = UUID.randomUUID().toString();
        try {
            AiSessionStore.SessionFull cur = aiSessionStore.get(owner, sessionId);
            if (cur == null) {
                return null;
            }
            boolean firstTurn = AiTurns.isEmpty(cur.turnsJson());
            String turnsJson = AiTurns.appendPending(cur.turnsJson(), turnId, userMsg, skillName, attachments);

            String title = cur.title();
            if (firstTurn || title == null || title.isBlank() || AiSessionStore.DEFAULT_TITLE.equals(title)) {
                title = titleOf(userMsg);
                // 只带附件、不写文字的提问：用首个文件名生成标题，比「新对话」更有辨识度。
                if (AiSessionStore.DEFAULT_TITLE.equals(title) && attachments != null
                        && attachments.isArray() && !attachments.isEmpty()) {
                    String name = attachments.get(0).path("name").asText("");
                    if (!name.isBlank()) {
                        title = "分析文件 " + name;
                    }
                }
            }
            aiSessionStore.save(owner, sessionId, title, turnsJson);
            return turnId;
        } catch (Exception e) {
            log.warn("写入 AI 提问轮次失败: {}", e.getMessage());
            return null;
        }
    }

    /**
     * <b>回填本轮答案</b>：按 {@code turnId} 找到 {@link #beginTurn} 落下的 pending 轮次，
     * 写入 assistant / ops / charts 并去掉 pending 标记（既支持正常完成，也用于出错时保留已产出部分）；
     * 找不到（如 beginTurn 失败）则退化为追加一条完整轮次。
     */
    private void finalizeTurn(String owner, String sessionId, String turnId, String userMsg,
                              String assistant, List<String> ops, List<String> charts,
                              List<String> files, List<String> trace, String skillName) {
        try {
            AiSessionStore.SessionFull cur = aiSessionStore.get(owner, sessionId);
            if (cur == null) {
                return;
            }
            AiTurns.FillResult r = AiTurns.fill(cur.turnsJson(), turnId, userMsg, assistant,
                    ops, charts, files, trace, skillName);

            String title = cur.title();
            if (r.appended() && (title == null || title.isBlank() || AiSessionStore.DEFAULT_TITLE.equals(title))) {
                title = titleOf(userMsg);
            }
            aiSessionStore.save(owner, sessionId, title, r.turnsJson());
        } catch (Exception e) {
            log.warn("回填 AI 会话轮次失败: {}", e.getMessage());
        }
    }

    /**
     * 一轮推理的<b>增量进度</b>：思考 / 工具调用 / 工具结果 / 半成品回答 / 图表 / 文件，
     * 节流写回 {@link #beginTurn} 落下的 pending 轮次。这样用户思考途中刷新页面，
     * 仍能看到已经发生的完整推理链，而不是只剩一句「正在生成回答…」。
     *
     * <p>所有回调都由模型线程串行触发（ReAct 循环），因此无需额外加锁；写库失败只记 debug 日志，
     * 不影响推理本身。</p>
     */
    private final class AiTurnProgress {

        private final String owner;
        private final String sessionId;
        private final String turnId;
        private final StringBuilder answer;
        private final List<String> charts;
        private final List<String> files;
        /** 已封口的推理事件（思考在工具调用 / 结束处封口，避免每个 token 一条事件）。 */
        private final List<String> events = new ArrayList<>();
        /** 正在流式输出的思考文本（每次落库时作为一条 think 事件附加在末尾）。 */
        private final StringBuilder thinking = new StringBuilder();
        private long lastFlushNanos;
        private boolean finished;

        AiTurnProgress(String owner, String sessionId, String turnId, StringBuilder answer,
                       List<String> charts, List<String> files) {
            this.owner = owner;
            this.sessionId = sessionId;
            this.turnId = turnId;
            this.answer = answer;
            this.charts = charts;
            this.files = files;
        }

        void thinking(String text) {
            if (text == null || text.isEmpty()) {
                return;
            }
            thinking.append(text);
            flush(false);
        }

        /** 回答正文追加后调用（半成品回答也写回，刷新后能接着看到已生成的部分）。 */
        void text() {
            flush(false);
        }

        void toolCall(String name, String args) {
            sealThinking();
            ObjectNode ev = json.createObjectNode();
            ev.put("k", "call");
            ev.put("name", name == null ? "" : name);
            ev.put("args", args == null ? "" : args);
            events.add(ev.toString());
            flush(true);
        }

        void toolResult(String name, String result) {
            sealThinking();
            ObjectNode ev = json.createObjectNode();
            ev.put("k", "result");
            ev.put("name", name == null ? "" : name);
            ev.put("t", result == null ? "" : result);
            events.add(ev.toString());
            flush(true);
        }

        void chart() {
            flush(true);
        }

        void file() {
            flush(true);
        }

        /** 结束前的推理事件快照（含尚未封口的思考）。 */
        List<String> trace() {
            List<String> out = new ArrayList<>(events);
            if (thinking.length() > 0) {
                out.add(thinkEvent(thinking.toString()));
            }
            return out;
        }

        /** 收尾：最后一次强制落库，此后不再写（避免覆盖 finalize 的最终答案）。 */
        void finish() {
            if (finished) {
                return;
            }
            flush(true);
            finished = true;
        }

        private void sealThinking() {
            if (thinking.length() > 0) {
                events.add(thinkEvent(thinking.toString()));
                thinking.setLength(0);
            }
        }

        private String thinkEvent(String text) {
            ObjectNode ev = json.createObjectNode();
            ev.put("k", "think");
            ev.put("t", text);
            return ev.toString();
        }

        private void flush(boolean force) {
            if (finished || turnId == null) {
                return;
            }
            long now = System.nanoTime();
            if (!force && now - lastFlushNanos < AI_PROGRESS_FLUSH_NANOS) {
                return;
            }
            lastFlushNanos = now;
            try {
                AiSessionStore.SessionFull cur = aiSessionStore.get(owner, sessionId);
                if (cur == null) {
                    return;
                }
                String turnsJson = AiTurns.progress(cur.turnsJson(), turnId, trace(), charts, files,
                        answer == null ? "" : answer.toString());
                aiSessionStore.save(owner, sessionId, cur.title(), turnsJson);
            } catch (Exception e) {
                log.debug("AI 推理进度落库失败: {}", e.getMessage());
            }
        }
    }

    /** 用首轮提问生成会话标题：取首行、截断到 24 字。 */
    private static String titleOf(String userMsg) {
        if (userMsg == null || userMsg.isBlank()) {
            return AiSessionStore.DEFAULT_TITLE;
        }
        String s = userMsg.strip().split("\\R", 2)[0].strip();
        if (s.length() > 24) {
            s = s.substring(0, 24) + "\u2026";
        }
        return s.isBlank() ? AiSessionStore.DEFAULT_TITLE : s;
    }

    /**
     * POST /api/ai/files/parse —— 上传一个文件并解析成纯文本（multipart/form-data，取第一个文件字段）。
     *
     * <p>解析统一走 {@link FileParser}（txt/md/csv/json/xml/html/docx/xlsx…）。返回
     * {@code {name,format,chars,truncated,text}}；前端把它作为本轮附件随
     * {@code /api/ai/chat/stream} 的 {@code attachments} 回传，由服务端拼进提示词并随轮次落库。
     * 解析失败一律 400 + 可直接展示给用户的文案。</p>
     */
    private void aiFileParse(Context ctx) {
        UploadedFile file;
        try {
            List<UploadedFile> files = ctx.uploadedFiles();
            if (files.isEmpty()) {
                ctx.status(400).json(Map.of("error", "未收到上传文件"));
                return;
            }
            file = files.get(0);
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "读取上传内容失败: " + e.getMessage()));
            return;
        }
        if (file.size() > FileParser.MAX_FILE_BYTES) {
            ctx.status(400).json(Map.of("error", "文件过大（" + file.size() + " 字节），单文件上限 "
                    + (FileParser.MAX_FILE_BYTES / 1024 / 1024) + "MB。"));
            return;
        }
        try (InputStream in = file.content()) {
            byte[] data = in.readAllBytes();
            FileParser.ParsedFile parsed = FileParser.parse(file.filename(), data);
            log.info("[AI] 解析附件 user={} 文件={} 格式={} {}字{}", viewerOf(ctx), parsed.name(),
                    parsed.format(), parsed.chars(), parsed.truncated() ? "（已截断）" : "");
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("name", parsed.name());
            out.put("format", parsed.format());
            out.put("chars", parsed.chars());
            out.put("truncated", parsed.truncated());
            out.put("text", parsed.text());
            ctx.json(out);
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "解析文件失败: " + e.getMessage()));
        }
    }

    /**
     * GET /api/ai/files/{id} —— 下载 export_file 生成的文件。
     *
     * <p>按当前登录视角隔离：文件只对生成它的用户可见（admin 视角与其他 admin 共享）。
     * 过期 / 不存在 / 越权一律 404，不泄露文件是否存在。</p>
     */
    private void aiFileDownload(Context ctx) {
        if (aiFileStore == null) {
            ctx.status(404).json(Map.of("error", "文件不存在或已过期"));
            return;
        }
        AiFileStore.StoredFile f = aiFileStore.find(viewerOf(ctx), ctx.pathParam("id"));
        if (f == null) {
            ctx.status(404).json(Map.of("error", "文件不存在或已过期"));
            return;
        }
        try {
            byte[] data = Files.readAllBytes(f.path());
            ctx.header("Content-Type", AiExportFormats.mimeType(f.format()));
            ctx.header("Content-Disposition", contentDisposition(f.name()));
            ctx.header("Content-Length", String.valueOf(data.length));
            ctx.header("Cache-Control", "no-store");
            ctx.res().getOutputStream().write(data);
        } catch (Exception e) {
            log.debug("AI 导出文件下载失败 id={}: {}", f.id(), e.getMessage());
            ctx.status(404).json(Map.of("error", "文件不存在或已过期"));
        }
    }

    /**
     * Content-Disposition：ASCII 回退名 + RFC 5987 {@code filename*}（UTF-8），保证中文名在各浏览器
     * 都能正确落盘。
     */
    private static String contentDisposition(String name) {
        String n = name == null || name.isBlank() ? "dojao-export" : name;
        String ascii = n.replaceAll("[^\\x20-\\x7E]", "_").replace("\\", "_").replace("\"", "_");
        String encoded = URLEncoder.encode(n, StandardCharsets.UTF_8).replace("+", "%20");
        return "attachment; filename=\"" + ascii + "\"; filename*=UTF-8''" + encoded;
    }

    /** GET /api/ai/sessions —— 当前用户的会话清单（按更新时间倒序）。 */
    private void aiSessionList(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        ctx.json(Map.of("sessions", aiSessionStore.list(user.username())));
    }

    /** POST /api/ai/sessions —— 新建空会话，返回 {@code {id,title,turns:[]}}。 */
    private void aiSessionCreate(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        AiSessionStore.SessionFull s = aiSessionStore.create(user.username());
        ctx.json(Map.of("id", s.id(), "title", s.title(), "turns", json.createArrayNode()));
    }

    /** GET /api/ai/sessions/{id} —— 单会话全文（turns 为解析后的数组）。 */
    private void aiSessionGet(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        AiSessionStore.SessionFull s = aiSessionStore.get(user.username(), ctx.pathParam("id"));
        if (s == null) {
            ctx.status(404).json(Map.of("error", "会话不存在"));
            return;
        }
        JsonNode turns;
        try {
            turns = json.readTree(s.turnsJson());
        } catch (Exception e) {
            turns = json.createArrayNode();
        }
        ctx.json(Map.of("id", s.id(), "title", s.title(), "turns", turns));
    }

    /** DELETE /api/ai/sessions/{id} —— 删除本人会话。 */
    private void aiSessionDelete(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        if (!aiSessionStore.delete(user.username(), ctx.pathParam("id"))) {
            ctx.status(404).json(Map.of("error", "会话不存在"));
            return;
        }
        ctx.json(Map.of("ok", true));
    }

    // ---------- AI 技能（Skill）----------

    /** GET /api/ai/skills —— 当前用户的技能清单 + 可绑定的工具名目录。 */
    private void aiSkillList(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        List<Map<String, Object>> out = new ArrayList<>();
        for (SkillStore.SkillMeta m : aiSkillStore.list(user.username())) {
            out.add(Map.of(
                    "id", m.id(),
                    "name", m.name(),
                    "description", m.description(),
                    "tools", parseTools(m.toolsJson()),
                    "updatedAt", m.updatedAt()));
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("skills", out);
        // 分析师只能绑定数据类工具：集群诊断类工具不下发（服务端 run 时还会再过滤一次）。
        resp.put("availableTools", AiAgentService.toolsForRole(user.role()));
        ctx.json(resp);
    }

    /** POST /api/ai/skills —— 新建技能，body {@code {name,description,prompt,tools[]}}。 */
    private void aiSkillCreate(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            String name = body == null ? null : str(body.get("name"));
            if (name == null || name.isBlank()) {
                ctx.status(400).json(Map.of("error", "技能名称不能为空"));
                return;
            }
            String description = body == null ? "" : str(body.get("description"));
            String prompt = body == null ? "" : str(body.get("prompt"));
            String toolsJson = toolsJsonOf(body == null ? null : body.get("tools"), user.role());
            SkillStore.SkillFull s = aiSkillStore.create(user.username(), name.trim(),
                    description == null ? "" : description, prompt == null ? "" : prompt, toolsJson);
            ctx.json(Map.of("id", s.id()));
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad request: " + e.getMessage()));
        }
    }

    /** GET /api/ai/skills/{id} —— 单技能全文（tools 为解析后的数组）。 */
    private void aiSkillGet(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        SkillStore.SkillFull s = aiSkillStore.get(user.username(), ctx.pathParam("id"));
        if (s == null) {
            ctx.status(404).json(Map.of("error", "技能不存在"));
            return;
        }
        ctx.json(Map.of("id", s.id(), "name", s.name(), "description", s.description(),
                "prompt", s.prompt(), "tools", parseTools(s.toolsJson())));
    }

    /** POST /api/ai/skills/{id} —— 更新本人技能，body {@code {name,description,prompt,tools[]}}。 */
    private void aiSkillUpdate(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        String id = ctx.pathParam("id");
        if (aiSkillStore.get(user.username(), id) == null) {
            ctx.status(404).json(Map.of("error", "技能不存在"));
            return;
        }
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            String name = body == null ? null : str(body.get("name"));
            if (name == null || name.isBlank()) {
                ctx.status(400).json(Map.of("error", "技能名称不能为空"));
                return;
            }
            String description = body == null ? "" : str(body.get("description"));
            String prompt = body == null ? "" : str(body.get("prompt"));
            String toolsJson = toolsJsonOf(body == null ? null : body.get("tools"), user.role());
            aiSkillStore.save(user.username(), id, name.trim(),
                    description == null ? "" : description, prompt == null ? "" : prompt, toolsJson);
            ctx.json(Map.of("ok", true));
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad request: " + e.getMessage()));
        }
    }

    /** DELETE /api/ai/skills/{id} —— 删除本人技能。 */
    private void aiSkillDelete(Context ctx) {
        User user = ctx.attribute(ATTR_USER);
        if (!aiSkillStore.delete(user.username(), ctx.pathParam("id"))) {
            ctx.status(404).json(Map.of("error", "技能不存在"));
            return;
        }
        ctx.json(Map.of("ok", true));
    }

    /**
     * 按 owner 载入技能并转成 {@link AiAgentService.Skill}（越权 / 不存在 / 未配置 → null = 无技能）。
     */
    private AiAgentService.Skill loadSkill(String owner, String skillId) {
        if (skillId == null || skillId.isBlank()) {
            return null;
        }
        SkillStore.SkillFull s = aiSkillStore.get(owner, skillId);
        if (s == null) {
            return null;
        }
        return new AiAgentService.Skill(s.name(), s.prompt(), parseTools(s.toolsJson()));
    }

    /** 解析工具名数组 JSON 文本；损坏 / 非数组 → 空列表。 */
    private List<String> parseTools(String toolsJson) {
        List<String> out = new ArrayList<>();
        try {
            JsonNode n = json.readTree(toolsJson);
            if (n != null && n.isArray()) {
                for (JsonNode e : n) {
                    if (e != null && !e.isNull()) {
                        out.add(e.asText());
                    }
                }
            }
        } catch (Exception ignored) {
            // 空 / 损坏 → 不限制
        }
        return out;
    }

    /**
     * 把前端传来的工具名列表序列化为 JSON 文本（仅保留非空字符串）；非数组 → {@code []}。
     * 同时按当前账号角色裁剪：分析师无法把集群诊断类工具写进技能。
     */
    private String toolsJsonOf(Object tools, Role role) {
        ArrayNode arr = json.createArrayNode();
        List<String> allowed = AiAgentService.toolsForRole(role);
        if (tools instanceof List<?> list) {
            for (Object o : list) {
                if (o != null) {
                    String s = o.toString().trim();
                    if (!s.isEmpty() && allowed.contains(s)) {
                        arr.add(s);
                    }
                }
            }
        }
        return arr.toString();
    }

    /** POST /api/ai/confirm —— 用户对待执行 DDL 的决策，body {@code {"streamId":..,"approve":bool}}。 */
    private void aiConfirmDdl(Context ctx) {
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            String streamId = body == null ? null : str(body.get("streamId"));
            boolean approve = body != null && Boolean.TRUE.equals(body.get("approve"));
            BlockingQueue<Boolean> q = streamId == null ? null : aiConfirmations.get(streamId);
            if (q == null) {
                ctx.status(404).json(Map.of("error", "确认请求不存在或已结束"));
                return;
            }
            q.offer(approve);
            ctx.json(Map.of("ok", true));
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad request: " + e.getMessage()));
        }
    }

    /**
     * POST /api/ai/stop —— 用户强制停止本次推理，body {@code {"streamId":..}}。
     *
     * <p>两件事：①释放可能挂起的 DDL 确认（投 false ⇒ 工具当作「取消」返回，不再阻塞 300s）；
     * ②{@code interrupt} 推理线程（阻塞中的 LLM 调用 / 轮询会抛 InterruptedException 而收敛）。
     * 客户端同时会 abort 掉 fetch，双向收口。</p>
     */
    private void aiStop(Context ctx) {
        try {
            Map body = json.readValue(ctx.body(), Map.class);
            String streamId = body == null ? null : str(body.get("streamId"));
            if (streamId == null || streamId.isBlank()) {
                ctx.status(400).json(Map.of("error", "缺少 streamId"));
                return;
            }
            BlockingQueue<Boolean> q = aiConfirmations.get(streamId);
            if (q != null) {
                q.offer(Boolean.FALSE);
            }
            Thread t = aiRunning.get(streamId);
            if (t != null) {
                t.interrupt();
            }
            ctx.json(Map.of("ok", true));
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad request: " + e.getMessage()));
        }
    }

    /** 把一个事件对象序列化后入队（序列化失败则丢弃该事件）。 */
    private void enqueue(BlockingQueue<String> queue, Map<String, Object> event) {
        try {
            queue.put(json.writeValueAsString(event));
        } catch (Exception ignored) {
            // 忽略：单个事件序列化失败不影响整体流
        }
    }

    private static void enqueueRaw(BlockingQueue<String> queue, String raw) {
        try {
            queue.put(raw);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        new DojaoUiApp().start(2380);
    }
}
