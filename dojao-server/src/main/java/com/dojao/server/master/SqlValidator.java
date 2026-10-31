package com.dojao.server.master;

import com.dojao.grammar.analyzer.SemanticAnalyzer;
import com.dojao.grammar.analyzer.error.SemanticError;
import com.dojao.grammar.analyzer.exception.SemanticException;
import com.dojao.grammar.analyzer.plan.LogicalPlan;
import com.dojao.grammar.optimize.Optimizer;
import com.dojao.metastore.MetaStore;
import com.dojao.spi.service.DojaoConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * SQL 语法 + 语义验证器 —— Master 启动时创建，作为独立组件注入到 QueryService。
 *
 * <p>验证内容包括：
 * <ol>
 *   <li>ANTLR 语法解析（词法 / 语法错误）</li>
 *   <li>catalog.schema.table 三级结构校验</li>
 *   <li>列引用校验（表.列 或 别名.列）</li>
 *   <li>GROUP BY 校验 —— 非聚合列必须出现在 GROUP BY 中</li>
 * </ol>
 */
public class SqlValidator {

    private static final Logger log = LoggerFactory.getLogger(SqlValidator.class);

    private final SemanticAnalyzer analyzer;
    private final Optimizer optimizer;
    private final MetaStore metaStore;

    public SqlValidator(MetaStore metaStore) {
        this.metaStore = metaStore;
        this.analyzer = new SemanticAnalyzer(metaStore);
        this.optimizer = new Optimizer();
        log.info("SQL 验证器已初始化");
    }

    /**
     * 元数据库 —— 供执行层（{@link MetadataExecutor}）查已注册 catalog / 取连接器。
     * 元数据语句（SHOW / DESCRIBE / USE）不需要 Planner，但要和语义校验看到同一个 MetaStore。
     */
    public MetaStore metaStore() {
        return metaStore;
    }

    /** 当前 catalog（{@code USE} 设置，可能为 null）：解析 {@code SHOW ...} 里省略的 catalog 段。 */
    public String currentCatalog() {
        return analyzer.currentCatalog();
    }

    /** 当前 schema（{@code USE} 设置，可能为 null）。 */
    public String currentSchema() {
        return analyzer.currentSchema();
    }

    /**
     * 验证 SQL 并返回优化后的逻辑计划。
     *
     * @param sql 原始 SQL 语句
     * @return 优化后的 LogicalPlan
     * @throws SemanticException 语法或语义错误时抛出
     */
    public LogicalPlan validate(String sql) {
        LogicalPlan plan = analyzer.analyze(sql);
        return optimizer.optimize(plan);
    }

    /**
     * 写语句目标表校验（目前仅 CTAS）—— Master 端在<b>任何计算开始之前</b>做的一次语义校验：
     * 目标表已存在且用户未写 {@code IF NOT EXISTS} → 立刻抛错；不存在 → 放行，交由后续建表与计算。
     *
     * <p><b>为什么必须前置</b>：CTAS 的建表 DDL 在两条路径上的时机都不理想 —— 分布式路径挂在
     * 派发前的 prepare（够早），而本地兜底路径（顶层是 Master 侧聚合 / 窗口等非分布式算子）
     * 挂在「首个数据页到达时」。后者要等整条链路算完才建表：像 2048 万行三表 join + 窗口 + 聚合
     * 这类查询会白跑四十秒，最后只丢出一句无信息量的
     * {@code Operator TableSink[...] next failed}（真因 "Table 'tmp1' already exists" 埋在
     * cause 链最底部）。这里先问一次数据源 —— 一次 {@code information_schema} 探查，毫秒级 ——
     * 把"表已存在"挡在全部计算之外。</p>
     *
     * <p>与 {@code INSERT OVERWRITE} 的「目标表必须存在」守卫方向相反：CTAS 要求表<b>不</b>存在
     * （标准 SQL 语义，不静默覆盖既有表）。要用 SELECT 结果覆盖既有表，应改用 INSERT OVERWRITE，
     * 或先 DROP TABLE。</p>
     *
     * <p>只判存在性、不建表 —— 建表需要运行期的输出页 Arrow schema，仍需等算子链产出。探查不可用
     * （catalog 未注册 / 连接器抛错）时<b>不</b>在此处拦截，留给后续建表路径报出真实原因，
     * 避免把「连不上数据源」误报成「表已存在」。</p>
     *
     * @param plan 已通过 {@link #validate} 的逻辑计划
     * @throws SemanticException 目标表已存在且未写 {@code IF NOT EXISTS}
     */
    public void validateWriteTarget(LogicalPlan plan) {
        if (!(plan instanceof LogicalPlan.CreateTableAsSelect ctas)) return;
        if (ctas.ifNotExists()) return;   // IF NOT EXISTS：表已存在 → 引擎跳过建表与写入，不算错误
        List<String> qname = ctas.qualifiedName();
        if (qname.size() < 3) return;     // 非全限定名 → 交给 Planner 报「须为全限定名 catalog.schema.table」
        String catalog = qname.get(0), schema = qname.get(1), table = qname.get(2);
        DojaoConnector connector = metaStore.getConnector(catalog).orElse(null);
        if (connector == null) return;    // catalog 未注册 → 交给 Planner 报
        boolean exists;
        try {
            exists = connector.tableExists(catalog, schema, table);
        } catch (RuntimeException e) {
            log.warn("CTAS 目标表存在性探查失败，放行交由建表时报出真因: {}.{}.{} cause={}",
                    catalog, schema, table, e.toString());
            return;
        }
        if (exists) {
            throw new SemanticException(List.of(new SemanticError(
                    "表已存在: " + catalog + "." + schema + "." + table
                            + " —— CREATE TABLE AS SELECT 不覆盖既有表（标准 SQL 语义）。"
                            + "如需用 SELECT 结果覆盖它，请改用：「INSERT OVERWRITE "
                            + catalog + "." + schema + "." + table + " SELECT ...」"
                            + "（注意 INSERT OVERWRITE 后直接跟表名，不要写 TABLE 关键字）；"
                            + "或先 DROP TABLE 再执行本语句。")));
        }
    }
}
