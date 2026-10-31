package com.dojao.server.master;

import com.dojao.common.data.Page;
import com.dojao.core.config.ResourceConfig;
import com.dojao.core.operator.AbstractDistributedOperator;
import com.dojao.core.operator.Operator;
import com.dojao.core.planner.PhysicalPlan;
import com.dojao.core.planner.Planner;
import com.dojao.core.planner.PlanningContext;
import com.dojao.core.planner.SqlTextUtil;
import com.dojao.core.resource.ParallelismBudget;
import com.dojao.core.resource.ParallelismCalculator;
import com.dojao.core.resource.ScanTableExtractor;
import com.dojao.core.resource.TableRef;
import com.dojao.core.rpc.*;
import com.dojao.core.runtime.master.WorkerRegistry;
import com.dojao.grammar.analyzer.exception.SemanticException;
import com.dojao.grammar.analyzer.expression.Expression;
import com.dojao.grammar.analyzer.plan.LogicalPlan;
import com.dojao.metastore.MetaStore;
import com.dojao.server.function.FunctionManager;
import com.dojao.spi.bean.FunctionDefinition;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.util.NestedTypeSupport;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 查询执行服务：接收客户端 SQL（经 {@link MasterQueryFlightProducer} 从 Arrow Flight 解包）→
 * SqlValidator 验证 → Planner 生成物理计划 → 执行算子链返回结果行；EXPLAIN 语句返回物理计划文本。
 *
 * <p>不走物理计划的旁路有三类：EXPLAIN（回计划文本）、函数 DDL（{@code CREATE/DROP FUNCTION}）、
 * 元数据语句（{@code SHOW *} / {@code DESCRIBE} / {@code USE}）—— 后者见 {@link MetadataExecutor}。</p>
 *
 * <p>传输协议 = Arrow Flight（doAction "execute_query"），本类不再感知 gRPC。</p>
 *
 * @author jiaxin
 * @version 1.0
 */
public class QueryServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(QueryServiceImpl.class);

    /** 结果流式回传分块行数：master 每攒满 N 行发一块给客户端（内存有界，默认 10 万行）。 */
    private static final int RESULT_CHUNK_ROWS = Integer.getInteger("DojaoResultChunkRows", 100_000);

    /**
     * 走 Master 本地直读的 LIMIT 上限（默认 5000）。只对「简单单表 + 小 LIMIT」生效：
     * limit 大于此值说明用户要的是大数据量，派发 Worker 反而更合适。
     */
    private static final long LOCAL_SCAN_MAX_LIMIT = Long.getLong("DojaoLocalScanMaxLimit", 5_000L);

    /**
     * 格式化 SQL 执行耗时：<1000ms 显示 ms，1000~60000ms 显示 s，60s~1h 显示 min，否则显示 h，保留两位小数。
     */
    private static String formatDuration(long elapsedMs) {
        if (elapsedMs < 1000) return String.format("%.2f ms", (double) elapsedMs);
        if (elapsedMs < 60_000) return String.format("%.2f s", elapsedMs / 1000.0);
        if (elapsedMs < 3_600_000) return String.format("%.2f min", elapsedMs / 60_000.0);
        return String.format("%.2f h", elapsedMs / 3_600_000.0);
    }

    private final SqlValidator sqlValidator;
    private final Planner planner;
    /** 连接器提供者：非空时启用动态并行度（探查元数据 → 计算 → 准入）；null 为兼容旧构造（沿用默认并行度）。 */
    private final PlanningContext.ConnectorProvider connectorProvider;
    /** 集群并行度预算（准入控制）；容量来源 = 健康 Worker 的 maxConcurrency 之和。 */
    private final ParallelismBudget budget;

    public QueryServiceImpl(SqlValidator sqlValidator, Planner planner) {
        this(sqlValidator, planner, null);
    }

    public QueryServiceImpl(SqlValidator sqlValidator, Planner planner,
                            PlanningContext.ConnectorProvider connectorProvider) {
        this.sqlValidator = sqlValidator;
        this.planner = planner;
        this.connectorProvider = connectorProvider;
        this.budget = new ParallelismBudget(new ParallelismBudget.CapacitySource() {
            @Override
            public int healthyCapacity() {
                return WorkerRegistry.get()
                        .healthy(PlanningContext.DEFAULT_WORKER_HEALTH_TIMEOUT_MS).stream()
                        .mapToInt(r -> r.lastResource() != null ? r.lastResource().maxConcurrency() : 0)
                        .sum();
            }

            @Override
            public int workerCount() {
                return WorkerRegistry.get().healthy(PlanningContext.DEFAULT_WORKER_HEALTH_TIMEOUT_MS).size();
            }
        });
    }

    /**
     * 执行一条 SQL：SELECT 结果按 {@link #RESULT_CHUNK_ROWS} 分块<b>实时</b>回调 {@code chunkSink}
     * （master 内存有界 = 有界结果队列 + 当前一块，客户端逐块消费即背压；不再把全量结果物化成一个
     * 巨型响应），EXPLAIN / DDL / 写语句为单块。异常向上抛，由 Flight producer 映射为错误码。
     * 内部自动生成可排序 queryId（yyyyMMddHHmmss_UUID）。
     */
    public void executeQuery(QueryRequest request, java.util.function.Consumer<QueryResponse> chunkSink)
            throws Exception {
        executeQuery(QueryManager.newQueryId(), request, chunkSink);
    }

    /**
     * 执行一条 SQL，使用调用方<b>预生成的 queryId</b>（如 WebUI 异步提交：提交即知 queryId，
     * 可立即轮询 / 取消）。其余行为与 {@link #executeQuery(QueryRequest, java.util.function.Consumer)}
     * 完全一致。查询失败/取消后 QueryManager 仍在 Registry 最近缓存可查。
     */
    public void executeQuery(String queryId, QueryRequest request,
                             java.util.function.Consumer<QueryResponse> chunkSink) throws Exception {
        executeQuery(queryId, request, chunkSink, null);
    }

    /**
     * 执行一条 SQL（带视角归属）。
     *
     * @param owner 当前视角用户名（Web 控制台登录）；{@code null} = 全局（CLI/JDBC/admin）。
     *              用于 CREATE/DROP FUNCTION / CATALOG 的归属隔离与 SHOW 过滤。
     */
    public void executeQuery(String queryId, QueryRequest request,
                             java.util.function.Consumer<QueryResponse> chunkSink,
                             String owner) throws Exception {
        long startNanos = System.nanoTime();
        String sql = request.getSql();
        log.info("收到查询请求: queryId={}, sql={}", queryId, sql);

        // 查询前懒加载：把 CREATE FUNCTION 已定义但未实例化的函数灌进执行注册表（标量→ScalarFunctionRegistry，
        // 聚合→AggregateFunctionRegistry）。幂等，只在首次调用时真正加载；EXPLAIN 也需注册才能解析函数引用。
        // ★ 必须在 validate 之前：语义分析需查 AggregateFunctionRegistry 识别自定义聚合（GROUP BY 校验放行），
        //   若在 validate 之后灌，自定义聚合会被当成普通列引用而报 "must appear in GROUP BY"。
        FunctionManager.ensureLoaded();
        LogicalPlan plan = sqlValidator.validate(sql);
        if (plan instanceof LogicalPlan.Explain explain) {
            // EXPLAIN：先规划（并行度按元数据决策）；普通 EXPLAIN 到此为止，对数据源零副作用
            int parallelism = resolveQueryParallelism(explain.statement());
            PhysicalPlan physical = planner.withParallelism(parallelism).plan(plan);
            log.info("EXPLAIN 并行度: queryId={}, parallelism={}, analyze={}",
                    queryId, parallelism, explain.analyze());
            if (!explain.analyze()) {
                emit(chunkSink, respondExplain(queryId, physical.explainText(), startNanos));
                return;
            }
            // EXPLAIN ANALYZE：真正执行一次语句，再把各节点实际输出行数回填到计划树。
            // ★只允许只读语句：INSERT / CTAS 会产生副作用，拒绝（EXPLAIN 语义不得写数据）。
            if (explain.statement() instanceof LogicalPlan.Insert
                    || explain.statement() instanceof LogicalPlan.CreateTableAsSelect) {
                throw new UnsupportedOperationException(
                        "EXPLAIN ANALYZE 只支持只读查询（不支持 INSERT / CTAS）");
            }
            Map<String, Long> sourceRows = probeSourceRows(explain.statement());
            QueryManager qm = new QueryManager(queryId, sql, physical, 0, sourceRows);
            QueryManagerRegistry.get().register(qm);
            // 直接消费算子链并把结果丢弃（EXPLAIN ANALYZE 只回传计划文本，不回传数据行）
            respondRows(queryId, physical, qm, startNanos, ignored -> { });
            String analyzed = com.dojao.core.planner.PhysicalPlanPrinter.print(
                    physical.root(), qm.actualRows());
            emit(chunkSink, respondExplain(queryId, analyzed, startNanos));
            return;
        }
        if (plan instanceof LogicalPlan.CreateFunction cf) {
            // CREATE FUNCTION：后端解析 → 冒烟校验（失败 fail-fast 不落库）→ 内存注册 + 持久化
            FunctionDefinition def = toFunctionDefinition(cf, owner);
            FunctionManager.createFunction(def, cf.temporary(), cf.ifNotExists(), owner);
            emit(chunkSink, respondDdl(queryId, "CREATE FUNCTION " + def.name(), startNanos));
            return;
        }
        if (plan instanceof LogicalPlan.DropFunction df) {
            // DROP FUNCTION：内存注册表 + 执行注册表 + 存储移除
            String name = String.join(".", df.qualifiedName());
            FunctionManager.dropFunction(name, df.temporary(), df.ifExists(), owner);
            emit(chunkSink, respondDdl(queryId, "DROP FUNCTION " + name, startNanos));
            return;
        }
        // 元数据 / DDL 语句（SHOW CATALOGS/SCHEMAS/TABLES/COLUMNS/CREATE TABLE、DESCRIBE、USE、
        // CREATE/DROP CATALOG、DROP TABLE、TRUNCATE TABLE）：没有算子 DAG，不进 Planner ——
        // 直接查注册表 / 连接器（DROP / TRUNCATE 在 Master 端执行），产出一个普通结果集。
        // catalog / schema 缺省段取当前 USE 值（见 MetadataExecutor）。
        if (MetadataExecutor.handles(plan)) {
            emit(chunkSink, respondMetadata(queryId, plan, startNanos, owner));
            return;
        }
        // WITH（CTE）顶层计划放行：planner.plan 第一步 inlineCtes 会把 CTE 递归内联成派生表
        // （FROM 的 CTE 引用 → subquery 槽），随后走与裸派生表完全相同的执行路径（本地 or 分布式）。
        // 不允许的话，执行期会在进 planner 前就被拒（EXPLAIN 已走 plan() 因此早已支持）。
        if (!(plan instanceof LogicalPlan.Select)
                && !(plan instanceof LogicalPlan.Insert)
                && !(plan instanceof LogicalPlan.CreateTableAsSelect)
                && !(plan instanceof LogicalPlan.With)
                && !(plan instanceof LogicalPlan.SetOperation)) {
            throw new UnsupportedOperationException(
                    "暂不支持执行的语句类型: " + plan.getClass().getSimpleName());
        }

        // 写语句目标表校验（CTAS）：目标表已存在且未写 IF NOT EXISTS → 立刻拒绝。
        // 放在计划 / 并行度准入之前：一次毫秒级的 information_schema 探查，换掉「整条链路
        // 算完（2048 万行 join+窗口+聚合 ≈ 40s）才在建表处失败」的白跑。EXPLAIN 已在上方提前
        // 返回，不经过这里 —— 保持 EXPLAIN 对数据源零副作用。
        sqlValidator.validateWriteTarget(plan);

        // 两条 LIMIT 捷径（Master 本地直读 / 截断下推）共用同一个前提：扫描读到的行<b>原样</b>流到
        // 最终输出（行数、行序都不变）。谓词由引擎在扫描之上求值时该前提不成立（读 N 行 ≠ 结果前 N
        // 行）—— 判定见 scanRowsAreOneToOne。
        boolean rowsOneToOne = isSimpleSingleTableSelect(plan) && scanRowsAreOneToOne(plan);
        // Master 本地直读优化：小 LIMIT 的简单单表查询不派发 Worker（省一次派发 + 跨进程搬数据），
        // 因而也不占集群并行度额度。判定见 isMasterLocalEligible。
        boolean localScan = rowsOneToOne && isMasterLocalEligible(plan);
        // 顶层 LIMIT 截断下推：简单单表直通链上，可以让数据源「读满即停」（SELECT ... LIMIT n），
        // 而不是等下游关游标。判定见 computeScanFetchRows。
        long scanFetchRows = rowsOneToOne ? computeScanFetchRows(plan) : 0L;
        // 动态并行度：探查扫描表元数据 → 计算初始并行度 → 准入控制（可用 < 应需×80% 时阻塞等待）
        int desired = resolveQueryParallelism(plan);
        int granted = desired;
        boolean admitted = false;
        if (connectorProvider != null && !localScan) {
            granted = budget.tryAdmit(queryId, desired);
            admitted = true;
            log.info("并行度准入: queryId={}, 应需={}, 授予={}, 可用={}",
                    queryId, desired, granted, budget.available());
        }
        // 扫描源总量探查（Progress 分母）：与并行度探查同源，供 QueryManager 挂到 Producer 任务
        Map<String, Long> sourceRows = probeSourceRows(plan);
        try {
            Planner effective = planner.withParallelism(localScan ? 1 : granted);
            if (localScan) {
                effective = effective.withLocalScanOnly(true);
                log.info("Master 本地执行命中: queryId={}, sql={}", queryId, sql);
            }
            if (scanFetchRows > 0L) {
                effective = effective.withScanFetchRows(scanFetchRows);
                log.info("LIMIT 截断下推命中: queryId={}, 扫描行数上限={}", queryId, scanFetchRows);
            }
            PhysicalPlan physical = effective.plan(plan);
            // 每查询一个 QueryManager 守护：注册后由执行路径实时采集算子指标
            QueryManager qm = new QueryManager(queryId, sql, physical, 0, sourceRows);
            QueryManagerRegistry.get().register(qm);
            if (plan instanceof LogicalPlan.Insert
                    || plan instanceof LogicalPlan.CreateTableAsSelect) {
                // 写语句：迭代算子链写入数据，返回写入行数摘要
                emit(chunkSink, respondWriteResult(queryId, physical, qm, startNanos));
                return;
            }
            respondRows(queryId, physical, qm, startNanos, chunkSink);
        } finally {
            if (admitted) {
                budget.release(queryId);
            }
        }
    }

    /**
     * 是否走「Master 本地直读」优化 —— 判定范围刻意收得很窄，只吃确定划算的形态：
     *
     * <ul>
     *   <li>单表 {@code SELECT}（无 join / 无派生表 / 无标量子查询）；</li>
     *   <li>无 GROUP BY / HAVING / 聚合 / 窗口函数（这些走分布式更快）；</li>
     *   <li><b>无 ORDER BY</b> —— 本地排序要先把整表拉进 Master 单个 JVM，2048 万行的表会炸；</li>
     *   <li>带 {@code LIMIT} 且 {@code 0 < limit <= }{@value #LOCAL_SCAN_MAX_LIMIT}。</li>
     * </ul>
     *
     * <p>命中的都是「只想看前几百行」的查询：派发 Worker + 跨进程搬数据的开销远大于在 Master
     * 直读（JDBC 拉满 limit 行即停），且 WHERE 仍然正常下推数据源。</p>
     *
     * <p>前四条 = {@link #isSimpleSingleTableSelect}（行数直通不变量），此处只多一个 LIMIT 上界；
     * 调用方还须叠加 {@link #scanRowsAreOneToOne} —— 谓词不由数据源执行时（如 Hive 的
     * {@code where content like … limit 500}），Master 直读等于把整表拉回单机过滤，必须走分布式
     * 让各 Worker 就地过滤。</p>
     */
    private static boolean isMasterLocalEligible(LogicalPlan plan) {
        if (!isSimpleSingleTableSelect(plan)) return false;
        LogicalPlan.Select s = (LogicalPlan.Select) plan;
        return s.limit().filter(n -> n > 0 && n <= LOCAL_SCAN_MAX_LIMIT).isPresent();
    }

    /**
     * 「简单单表直通查询」判定 —— 扫描出的行会<b>原样</b>流到顶层 {@code LimitOperator}：单表、
     * 无 join / 派生表 / 标量子查询、无 GROUP BY / HAVING / 聚合 / 窗口、<b>无 ORDER BY</b>。
     *
     * <p>行数不变是关键不变量：只有在这条链上，「少读一些源行」才等于「结果不变」，因而才可以
     * ① Master 本地直读（{@link #isMasterLocalEligible}）② 把顶层 LIMIT 下推给数据源
     * （{@link #computeScanFetchRows}）。ORDER BY 会引入全局顺序（本地排序要先把整表拉进单个 JVM），
     * 聚合 / DISTINCT / join 都会改变行数，一律排除。</p>
     *
     * <p><b>本判定只是必要条件的一半</b>：它保证「不再有算子改变行数」，但 WHERE 本身就会减行 ——
     * 谓词由谁执行决定了「读到的行」是否等于「输出的行」，那一半见 {@link #scanRowsAreOneToOne}。</p>
     */
    private static boolean isSimpleSingleTableSelect(LogicalPlan plan) {
        if (!(plan instanceof LogicalPlan.Select s)) return false;
        if (!s.joins().isEmpty() || s.fromRelations().size() != 1) return false;
        if (s.fromRelations().get(0).subquery().isPresent()) return false;
        if (!s.groupBy().isEmpty() || s.having().isPresent()) return false;
        if (!s.scalarSubqueries().isEmpty()) return false;
        if (!s.orderBy().isEmpty()) return false;
        for (LogicalPlan.SelectItem item : s.selectItems()) {
            if (item.expression() instanceof Expression.WindowFunction) return false;
            // findAggregateCalls：`round(sum(x),2)` 这类「聚合套在表达式里」同样改变行数，
            // 不能算简单单表直通（否则 LIMIT 截断 / Master 直读会漏掉聚合）。
            if (!SqlTextUtil.findAggregateCalls(item.expression().text()).isEmpty()) return false;
        }
        return true;
    }

    /**
     * 扫描读到的行是否<b>原样</b>成为最终输出（行数、行序都不变）—— 截断下推与 Master 直读的共同前提：
     * 只有「第 N 行结果 = 第 N 行读取」时，「少读一些源行」才等于「结果不变」。谓词会<b>减行</b>，
     * 因此判定落在「这条 WHERE 由谁执行」上：
     *
     * <ul>
     *   <li>无谓词 —— 成立，读到的行就是输出的行；</li>
     *   <li>有谓词，且连接器兑现下推契约（{@link DojaoConnector#supportsPredicatePushdown()}，如
     *       MySQL/PG 拼进 WHERE 执行）—— 成立，数据源吐出的已经只剩命中的行，
     *       {@code LIMIT n} 截断的正是「结果的前 n 行」；</li>
     *   <li>有谓词但连接器不兑现（如 Hive：只按切片读文件，谓词由引擎在扫描之上求值）—— <b>不成立</b>，
     *       截断 N 行源数据 = 拿「前 N 行里命中的那几行」冒充「命中的前 N 行」：后面文件里的命中行
     *       永远读不到（实测 {@code select * … where content like … limit 500} 只回 1 行，而全表命中
     *       225 行）。此时必须<b>放弃截断</b>，退回「扫全量 + 顶层 LimitOperator 流式收尾」：
     *       满 N 行即停拉上游 → {@code doClose()} 取消剩余任务，多读的代价换来正确性。</li>
     * </ul>
     *
     * <p>谓词两处都要认：{@code SELECT} 的 WHERE（单表时 Planner 下推到扫描）与 relation 上已下推的
     * filter（{@code Optimizer} 落下的）。探查不到连接器（未注册 / 无提供者）一律按不成立处理 —— 宁可
     * 不优化，不可少读。</p>
     */
    private boolean scanRowsAreOneToOne(LogicalPlan plan) {
        if (connectorProvider == null) return false;
        if (!(plan instanceof LogicalPlan.Select s)) return false;
        boolean hasPredicate = s.where().isPresent()
                || s.fromRelations().stream().anyMatch(r -> r.filter().isPresent());
        if (!hasPredicate) return true;      // 无谓词：读到的行即输出的行
        List<ScanTableExtractor.ScannedTable> scans = ScanTableExtractor.extractScans(plan);
        if (scans.isEmpty()) return false;
        for (ScanTableExtractor.ScannedTable scan : scans) {
            DojaoConnector conn;
            try {
                conn = connectorProvider.get(scan.table().catalog());
            } catch (Exception e) {
                conn = null;
            }
            if (conn == null || !conn.supportsPredicatePushdown()) return false;
        }
        return true;
    }

    /**
     * 顶层 LIMIT 可下推给数据源的「够用行数」= {@code offset + limit}（不能下推时 0）。
     *
     * <p>取 offset+limit 而非 limit：切片/数据源从第 0 行开始读，跳过 offset 的工作仍由顶层
     * {@link com.dojao.core.operator.LimitOperator} 完成，扫描侧只需保证结果前缀完整 ——
     * {@code [0, offset+limit)} 正是该前缀的超集。只认显式 {@code LIMIT}：仅有 {@code OFFSET}
     * （无上界）时无从截断。</p>
     *
     * <p><b>调用方必须先确认「扫描行 ↔ 输出行 1:1」</b>（{@link #scanRowsAreOneToOne}）：本方法只算
     * 数字，不算「这个数字是否够用」。谓词由引擎执行时前缀截断会少返回行。</p>
     */
    private static long computeScanFetchRows(LogicalPlan plan) {
        if (!isSimpleSingleTableSelect(plan)) return 0L;
        LogicalPlan.Select s = (LogicalPlan.Select) plan;
        if (s.limit().isEmpty() || s.limit().get() <= 0L) return 0L;
        long offset = s.offset().orElse(0L);
        long limit = s.limit().get();
        if (offset <= 0L) return limit;
        long sum = offset + limit;
        return sum < 0L ? Long.MAX_VALUE : sum;   // 溢出兜底
    }

    /**
     * 元数据库 —— 供 UI 目录树（catalog → schema → table）与元数据语句读已注册 catalog /
     * 连接器。与语义校验看到的是同一个 {@link SqlValidator} 里的 {@link MetaStore}。
     */
    public MetaStore metaStore() {
        return sqlValidator.metaStore();
    }

    /**
     * 兼容小结果调用方（测试 / 遗留代码）：执行并收集全部分块。
     * 注意：大结果 SELECT 请用流式 {@link #executeQuery(QueryRequest, java.util.function.Consumer)}，
     * 这里会把全量结果物化在内存里。
     */
    public List<QueryResponse> executeQuery(QueryRequest request) throws Exception {
        List<QueryResponse> out = new ArrayList<>();
        executeQuery(request, out::add);
        return out;
    }

    /** 把若干块按序投递到 sink（EXPLAIN / DDL / 写语句都是单块）。 */
    private static void emit(java.util.function.Consumer<QueryResponse> sink, List<QueryResponse> responses) {
        for (QueryResponse r : responses) {
            sink.accept(r);
        }
    }

    // ==================== 动态并行度决策 ====================

    /**
     * 初始并行度决策：提取扫描表 → 经连接器探查元数据（框架只调 {@link DojaoConnector#getTableStats} 接口，
     * 不关心各连接器实现）→ {@link ParallelismCalculator} 计算。探查失败/无提供者 → 回退默认。
     */
    private int resolveQueryParallelism(LogicalPlan plan) {
        // 每 query 硬上限 = budget.perQueryMax()：配置了 system.query.max.parallelism 用用户值，
        // 否则默认公式 = 集群容量 / worker 数（卡死，准入侧 tryAdmit 同上限双保险）
        int cap = budget.perQueryMax();
        if (connectorProvider == null) {
            return Math.min(ResourceConfig.masterParallelism(), cap);   // 无探查能力 → 沿用默认（仍受上限约束）
        }
        List<TableRef> tables = ScanTableExtractor.extract(plan);
        if (tables.isEmpty()) {
            return Math.min(ResourceConfig.masterParallelism(), cap);
        }
        int capacity = budget.clusterCapacity();
        int p = ParallelismCalculator.decide(tables, this::probeTableStats, capacity, cap);
        log.info("初始并行度决策: queryId=未定, 扫描表={}, 集群容量={}, 每query上限={}, 并行度={}",
                tables.stream().map(TableRef::qualifiedName).toList(), capacity, cap, p);
        return p;
    }

    /** 按表探查统计：经连接器提供者取该 catalog 的连接器，调 {@code getTableStats} 接口（失败 → null 未知）。 */
    private TableStats probeTableStats(TableRef table) {
        try {
            DojaoConnector conn = connectorProvider.get(table.catalog());
            return conn.getTableStats(table.catalog(), table.schema(), table.table());
        } catch (Exception e) {
            log.warn("表元数据探查失败（回退默认并行度）: {} - {}", table.qualifiedName(), e.getMessage());
            return null;
        }
    }

    /**
     * 探查每条扫描源表的行数估计 → {@code {schema.table : rowCount}}（{@code rowCount>0} 才收录）。
     * 执行期物理计划正是按这份源数据量决定分区并行度；这里把同一份探查复用到监控：
     * QueryManager 把它挂到 Producer 扫描任务上作为 {@code expectedRows}（Scan 进度条分母）。
     * 探查失败 / 连接器未启用 → 空表（Scan 节点退化为按完成任务数算进度）。
     *
     * <p><b>过滤感知</b>：取 relation 上已下推的谓词（{@code Optimizer} 已把 WHERE 落到基表），
     * 调连接器 {@link DojaoConnector#estimateRows} 估过滤后行数 —— 否则
     * {@code WHERE id between 3 and 8}（整表 46 万 → 实际 6 行）会把 Scan 进度条永远钉在 0。
     * 连接器不覆写估算 → 默认退化为整表 {@code TABLE_ROWS}（与旧行为一致）。</p>
     */
    private Map<String, Long> probeSourceRows(LogicalPlan plan) {
        if (connectorProvider == null) {
            return Map.of();
        }
        List<ScanTableExtractor.ScannedTable> scans = ScanTableExtractor.extractScans(plan);
        if (scans.isEmpty()) {
            return Map.of();
        }
        java.util.Map<String, Long> out = new java.util.HashMap<>();
        for (ScanTableExtractor.ScannedTable scan : scans) {
            TableRef t = scan.table();
            try {
                DojaoConnector conn = connectorProvider.get(t.catalog());
                long rows = conn.estimateRows(t.catalog(), t.schema(), t.table(), scan.filter());
                if (rows > 0) {
                    String key = (t.schema() == null ? "" : t.schema())
                            + "." + (t.table() == null ? "" : t.table());
                    out.put(key, rows);
                }
            } catch (Exception e) {
                log.debug("探查源表行数失败（跳过进度分母）: {} - {}",
                        t.qualifiedName(), e.getMessage());
            }
        }
        return out;
    }

    // ==================== EXPLAIN 响应 ====================

    private List<QueryResponse> respondExplain(String queryId, String explainText, long startNanos) {
        String[] lines = explainText.split("\n");
        QueryResponse.Builder builder = QueryResponse.newBuilder()
                .setQueryId(queryId)
                .addColumns(Column.newBuilder().setName("Plan").setType("VARCHAR").build());
        for (String line : lines) {
            builder.addRows(com.dojao.core.rpc.Row.newBuilder().addValues(line).build());
        }
        log.info("EXPLAIN 完成: queryId={}, rows={}, 耗时={}", queryId, lines.length,
                formatDuration((System.nanoTime() - startNanos) / 1_000_000));
        return List.of(builder.build());
    }

    // ==================== 元数据语句响应 ====================

    /**
     * 元数据语句（SHOW / DESCRIBE / USE）响应：单块结果集，列头 + 数据行与普通 SELECT 同构，
     * 客户端（CLI / WebUI）无须区分渲染。当前 catalog/schema 在执行时才读 —— {@code USE} 会改它。
     */
    private List<QueryResponse> respondMetadata(String queryId, LogicalPlan plan, long startNanos, String owner) {
        MetadataExecutor executor = new MetadataExecutor(sqlValidator.metaStore(),
                sqlValidator.currentCatalog(), sqlValidator.currentSchema(), owner);
        QueryResponse response = executor.execute(queryId, plan);
        log.info("元数据语句完成: queryId={}, 语句={}, columns={}, rows={}, 耗时={}", queryId,
                plan.getClass().getSimpleName(), response.getColumnsCount(), response.getRowsCount(),
                formatDuration((System.nanoTime() - startNanos) / 1_000_000));
        return List.of(response);
    }

    // ==================== DDL 响应 ====================

    /** CREATE FUNCTION 计划 → FunctionDefinition（type / class_name / code_path 取 WITH 属性）。 */
    private static FunctionDefinition toFunctionDefinition(LogicalPlan.CreateFunction cf, String owner) {
        String name = String.join(".", cf.qualifiedName());
        Map<String, String> props = cf.properties();
        // 属性值在语义层已剥引号（SemanticAnalyzer.extractProperties）；这里再 unquote 一次是
        // 防御性的（旧版曾保留 expression 原文，'python' 这类字面量带着引号）。
        String type = unquote(props.getOrDefault("type", ""));
        if (type.isEmpty()) {
            throw new IllegalArgumentException("CREATE FUNCTION 缺少 WITH (type='java'|'python'|...)");
        }
        String className = unquote(props.getOrDefault("class_name", ""));
        if (className.isEmpty()) {
            throw new IllegalArgumentException(
                    "CREATE FUNCTION 缺少 class_name（java 全限定类名 / 其他语言 module.ClassName）");
        }
        String codePath = unquote(props.getOrDefault("code_path", props.getOrDefault("jar_path", "")));
        String now = java.time.LocalDateTime.now().toString().replace("T", " ").substring(0, 19);
        return new FunctionDefinition(name, type, codePath, className, props,
                cf.comment().orElse(""), now, now, owner);
    }

    /** 去掉 WITH 属性值的外层引号（单引号 / 双引号）。 */
    private static String unquote(String v) {
        String s = v == null ? "" : v.trim();
        if (s.length() >= 2
                && ((s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'')
                    || (s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"'))) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    /** DDL 语句（CREATE FUNCTION 等）的统一 OK 响应。 */
    private List<QueryResponse> respondDdl(String queryId, String message, long startNanos) {
        log.info("DDL 完成: queryId={}, {}, 耗时={}", queryId, message,
                formatDuration((System.nanoTime() - startNanos) / 1_000_000));
        return List.of(QueryResponse.newBuilder()
                .setQueryId(queryId)
                .addColumns(Column.newBuilder().setName("status").setType("VARCHAR").build())
                .addRows(com.dojao.core.rpc.Row.newBuilder().addValues("OK").build())
                .build());
    }

    // ==================== 查询结果响应 ====================

    /**
     * 用户是否已请求取消：状态位已置 CANCELED，<b>或</b>执行线程已被中断
     * （{@link QueryManager#cancel()} 会 interrupt 执行线程，把阻塞在队列/线程池上的等待当场弹醒）。
     *
     * <p>两个条件都要看：状态位负责"说清这是取消不是故障"，中断标记负责兜住那些被中断后
     * 没走抛异常路径的情形（异常被中间层吞掉、或阻塞点自己清了标记）。</p>
     */
    private static boolean cancelRequested(QueryManager qm) {
        return qm.state() == QueryState.CANCELED || Thread.currentThread().isInterrupted();
    }

    /**
     * 查询结果流式回传：逐页拉取 → 转 rows → 攒满 {@link #RESULT_CHUNK_ROWS} 行封一块<b>立即</b>
     * 回调 {@code chunkSink}（Flight 端逐块 onNext）。master 内存有界 = 有界结果队列 + 当前一块
     * （不再把全量结果物化成一个巨型 protobuf / List，消除全量 join / 聚合结果回传时的 OOM）。
     * 每块带完整列头，客户端只读首块列名。
     *
     * <p><b>SELECT * 的列头还原</b>：规划期 {@code outputColumns()} 对纯 {@code *} 只留字面量
     * {@code "*"}（运行时由 {@code Page.select} 展开），若原样发给客户端，CLI/JDBC/UI 都只能看到
     * 一列 {@code "*"}。这里在拉到首个数据页时用其 Arrow schema 把真实列名补齐。</p>
     */
    private void respondRows(String queryId, PhysicalPlan physical,
                             QueryManager qm, long startNanos,
                             java.util.function.Consumer<QueryResponse> chunkSink) throws Exception {
        List<String> outCols = new ArrayList<>(physical.outputColumns());
        QueryResponse.Builder builder = newResultChunk(queryId, outCols);
        long chunkRows = 0;
        int chunks = 0;

        Operator op = physical.operator();
        long totalRows = 0;
        try {
            qm.markRunning();
            op.open();
            Page page;
            while (!(page = op.next()).isEOF()) {
                if (cancelRequested(qm)) {
                    page.close();
                    break;   // 用户 cancel：停止拉取，close() 释放后台线程
                }
                VectorSchemaRoot root = page.getRoot();
                if (root != null && page.getRowCount() > 0) {
                    int rows = page.getRowCount();
                    List<FieldVector> vectors = root.getFieldVectors();
                    // SELECT *：规划期列名是字面量 "*"，用首个数据页的真实 schema 还原列名（尚未发过块才重建）
                    if (chunks == 0 && chunkRows == 0 && isStarOnlyCols(outCols)
                            && vectors.size() != outCols.size()) {
                        outCols.clear();
                        for (FieldVector v : vectors) {
                            outCols.add(v.getName());
                        }
                        builder = newResultChunk(queryId, outCols);
                    }
                    for (int r = 0; r < rows; r++) {
                        com.dojao.core.rpc.Row.Builder row = com.dojao.core.rpc.Row.newBuilder();
                        for (FieldVector vec : vectors) {
                            // 嵌套值（LIST/MAP/STRUCT）的 getObject().toString() 是 Java 的
                            // [1, 2] / {a=1}，这里统一渲染成与 Hive 文本一致的 JSON 风格。
                            String value = NestedTypeSupport.isNested(vec)
                                    ? NestedTypeSupport.formatValue(vec, r)
                                    : (vec.isNull(r) ? "NULL" : String.valueOf(vec.getObject(r)));
                            row.addValues(value);
                        }
                        builder.addRows(row.build());
                    }
                    totalRows += rows;
                    chunkRows += rows;
                    // 首块不攒：第一页到手立即发（列头 + 已到行先给客户端）。否则要攒满 10 万行才发
                    // 首块，小结果集（如 limit 500 / GROUP BY 限量）整条查询只有收尾那一块 —— UI 在
                    // 执行全程拿不到列头，只能一直显示「计算中」。后续块仍按 RESULT_CHUNK_ROWS 攒。
                    if (chunks == 0 || chunkRows >= RESULT_CHUNK_ROWS) {
                        chunkSink.accept(builder.build());   // 攒满一块 → 立即发（客户端继续拉下一块）
                        chunks++;
                        builder = newResultChunk(queryId, outCols);
                        chunkRows = 0;
                    }
                }
                page.close();
            }
            // 最后一块（空结果也发一块：仅列头）
            if (chunkRows > 0 || chunks == 0) {
                chunkSink.accept(builder.build());
                chunks++;
            }
            qm.markFinished();
        } catch (Exception e) {
            if (cancelRequested(qm)) {
                // 取消 ≠ 失败：不打 error 日志、不标 FAILED、不向上抛（上层按 state==CANCELED 处理）。
                // 清掉中断标记 —— 执行线程来自共享线程池，标记留着会污染下一个查询。
                Thread.interrupted();
            } else {
                qm.markFailed(e);
                throw e;
            }
        } finally {
            op.close();
            QueryManagerRegistry.get().complete(qm);
        }

        log.info("查询执行完成: queryId={}, columns={}, rows={}, chunks={}, 耗时={}", queryId, outCols.size(),
                totalRows, chunks, formatDuration((System.nanoTime() - startNanos) / 1_000_000));
    }

    /** 新建一块结果响应：列头与首块一致（客户端只读首块列名，后续块列头冗余可忽略）。 */
    private static QueryResponse.Builder newResultChunk(String queryId, List<String> outCols) {
        QueryResponse.Builder b = QueryResponse.newBuilder().setQueryId(queryId);
        for (String col : outCols) {
            b.addColumns(Column.newBuilder().setName(col).setType("VARCHAR").build());
        }
        return b;
    }

    /** 列名是否只是规划期未展开的 {@code *} 占位（单列且值为 "*"）。 */
    private static boolean isStarOnlyCols(List<String> outCols) {
        return outCols.size() == 1 && "*".equals(outCols.get(0));
    }

    // ==================== 写语句响应 ====================

    /**
     * 执行写计划（INSERT INTO ... SELECT / CREATE TABLE AS SELECT）：
     * 迭代算子链逐页写入连接器，返回单行摘要（写入行数），不流式回传全量数据。
     */
    private List<QueryResponse> respondWriteResult(String queryId, PhysicalPlan physical,
                                                   QueryManager qm, long startNanos) throws Exception {
        Operator op = physical.operator();
        long totalRows = 0;
        // 分布式写（Planner 已给分布式算子挂 SinkStep）：数据页不回 Master，只等 N 个分区
        // FragmentFinished 判 EOF，行数 = totalOutputRows()（各分区回传 rowCount 之和）。
        boolean distributedWrite = op instanceof AbstractDistributedOperator;
        try {
            qm.markRunning();
            op.open();
            Page page;
            while (!(page = op.next()).isEOF()) {
                if (cancelRequested(qm)) {
                    page.close();
                    break;   // 用户 cancel：停止拉取，close() 释放后台线程
                }
                totalRows += page.getRowCount();
                page.close();
            }
            if (distributedWrite) {
                totalRows = ((AbstractDistributedOperator) op).totalOutputRows();
            }
            qm.markFinished();
        } catch (Exception e) {
            if (cancelRequested(qm)) {
                // 取消 ≠ 失败：不打 error 日志、不标 FAILED、不向上抛（上层按 state==CANCELED 处理）。
                // 清掉中断标记 —— 执行线程来自共享线程池，标记留着会污染下一个查询。
                Thread.interrupted();
            } else {
                qm.markFailed(e);
                throw e;
            }
        } finally {
            op.close();
            QueryManagerRegistry.get().complete(qm);
        }

        log.info("写入完成: queryId={}, rows={}, 耗时={}", queryId, totalRows,
                formatDuration((System.nanoTime() - startNanos) / 1_000_000));
        return List.of(QueryResponse.newBuilder()
                .setQueryId(queryId)
                .addColumns(Column.newBuilder().setName("rows_inserted").setType("BIGINT").build())
                .addRows(com.dojao.core.rpc.Row.newBuilder()
                        .addValues(String.valueOf(totalRows)).build())
                .build());
    }
}
