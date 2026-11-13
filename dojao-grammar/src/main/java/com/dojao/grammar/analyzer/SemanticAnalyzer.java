package com.dojao.grammar.analyzer;

import com.dojao.grammar.analyzer.error.SemanticError;
import com.dojao.grammar.analyzer.exception.SemanticException;
import com.dojao.grammar.analyzer.expression.Expression;
import com.dojao.grammar.analyzer.plan.LogicalPlan;
import com.dojao.grammar.analyzer.resolver.TableResolver;
import com.dojao.grammar.analyzer.scope.Scope;
import com.dojao.metastore.MetaStore;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.DojaoType;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.function.AggregateFunctionRegistry;
import com.dojao.spi.function.CollectAggregateRegistry;
import com.dojao.grammar.sql.SqlBaseBaseVisitor;
import com.dojao.grammar.sql.SqlBaseLexer;
import com.dojao.grammar.sql.SqlBaseParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Dojao 语义分析器：负责校验 SQL 语句的语义正确性。
 *
 * <p>校验内容包括：
 * <ol>
 *   <li>访问数据源（三级结构：catalog → schema → table）</li>
 *   <li>验证列引用（表.列 或 别名.列）</li>
 *   <li>简单列名在当前作用域中查找（含多表同名提示）</li>
 * </ol>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27 22:27
 */
public class SemanticAnalyzer extends SqlBaseBaseVisitor<LogicalPlan> {

    private final MetaStore metaStore;
    private final List<SemanticError> errors = new ArrayList<>();
    private final Deque<Scope> scopeStack = new ArrayDeque<>();
    private final Map<String, List<String>> cteDefinitions = new LinkedHashMap<>();
    private String currentCatalog = null;
    private String currentSchema = null;

    /**
     * 解析 HAVING 时临时放行的 SELECT 别名（小写）。HAVING 允许引用 SELECT 列表定义的列别名
     * （{@code SELECT COUNT(*) AS cnt ... HAVING cnt > 3}），别名不是任何表/派生关系的列，
     * 直接按列引用校验会报「not found in any table in scope」。仅在解析当前查询的 HAVING 期间生效。
     */
    private Set<String> havingAliases = Set.of();

    /**
     * 当前查询的 SELECT 别名 → 定义表达式原文（键为小写别名）。窗口定义里的
     * {@code ORDER BY pv}（{@code pv} = 同层聚合别名）需要靠它判定该别名在分组语义下
     * 是否合法（聚合输出 / 已分组表达式），并放行「别名不在任何表中」的列校验。
     */
    private Map<String, String> selectAliasExprs = Map.of();

    /**
     * 允许在窗口定义里引用的 SELECT 别名（小写）—— 仅当该别名的定义表达式是聚合函数或
     * GROUP BY 表达式。窗口 PARTITION BY / ORDER BY 在聚合之后求值，引用聚合输出列
     * （{@code ORDER BY pv}）是合法写法；未分组列的别名仍拒绝。
     */
    private Set<String> windowAliases = Set.of();

    /** {@code >0} 表示正在遍历窗口函数的 OVER 子树（此时才放行别名列引用）。 */
    private int windowSpecDepth = 0;

    private static final Set<String> AGGREGATE_FUNCTIONS = Set.of(
        "COUNT", "SUM", "AVG", "MAX", "MIN",
        // 条件聚合：count_if(cond) / sum_if(v, cond) / max_if(v, cond) / min_if(v, cond)
        "COUNT_IF", "SUM_IF", "MAX_IF", "MIN_IF",
        "COUNT_DISTINCT", "APPROX_COUNT_DISTINCT",
        "STDDEV", "STDDEV_POP", "STDDEV_SAMP",
        "VAR_POP", "VAR_SAMP", "VARIANCE",
        "COLLECT_LIST", "COLLECT_SET", "ARRAY_AGG",
        "STRING_AGG", "LISTAGG", "GROUP_CONCAT",
        "FIRST_VALUE", "LAST_VALUE",
        "CORR", "COVAR_POP", "COVAR_SAMP"
    );

    /**
     * 只能作为窗口函数使用的名字（{@code rank() / lag() …}）；{@code Planner.toWindowSpec}
     * 认识的内置窗口函数全集。聚合函数另由 {@link #AGGREGATE_FUNCTIONS} / 自定义聚合注册表覆盖。
     */
    private static final Set<String> WINDOW_ONLY_FUNCTIONS = Set.of(
        "ROW_NUMBER", "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST",
        "NTILE", "LAG", "LEAD", "NTH_VALUE"
    );

    private static final Pattern NUMERIC_PATTERN = Pattern.compile("^\\d+(\\.\\d+)?$");

    /** 函数调用名提取（isAggregateExpression 扫描自定义聚合用）：标识符后紧跟左括号。 */
    private static final Pattern FUNCTION_CALL = Pattern.compile("\\b([A-Za-z_][A-Za-z0-9_]*)\\s*\\(");

    /** 窗口 ORDER BY 排序项尾部方向后缀（{@code ASC / DESC [NULLS FIRST|LAST]}），校验列名时剥掉。 */
    private static final Pattern SORT_DIRECTION =
        Pattern.compile("(?i)\\s+(ASC|DESC)(\\s+NULLS\\s+(FIRST|LAST))?\\s*$");

    /**
     * 表达式解析过程中捕获到的窗口调用（仅针对当前 SELECT 项，按出现顺序）。
     *
     * <p>以前只保存<b>最后一个</b>窗口函数并让 SELECT 项直接等于它 —— 只要表达式里出现
     * {@code … OVER (…)}，外层的 {@code + - * /} 与其它窗口调用都会被静默丢弃
     * （{@code gmv / sum(gmv) OVER ()} 退化成 {@code sum(gmv) OVER ()}）。现在改为全部收集：
     * 整项就是一个窗口调用时仍产出 {@link Expression.WindowFunction}；否则保留完整表达式原文 +
     * 窗口调用清单，由 Planner 把每个窗口物化成合成列再回写。</p>
     */
    private final List<PendingWindowCall> pendingWindowCalls = new ArrayList<>();

    /** 已解析完成的窗口调用：{@code ctx} 用于判定它是否就是整个 SELECT 项。 */
    private record PendingWindowCall(SqlBaseParser.FunctionCallContext ctx,
                                     Expression.WindowFunction window,
                                     String text) {
        /** 常规形态：窗口调用原文 = {@code ctx} 的原文。 */
        PendingWindowCall(SqlBaseParser.FunctionCallContext ctx, Expression.WindowFunction window) {
            this(ctx, window, null);
        }
    }

    /**
     * 「包装形态」规范化后需要回写原文的片段。
     *
     * <p>{@code round(sum(amount), 2) OVER (PARTITION BY …)} 这类写法把返回标量的普通函数包在
     * 窗口外面，语义等价于把 {@code OVER} 贴到参数里的聚合调用上：
     * {@code round(sum(amount) OVER (PARTITION BY …), 2)}。分析阶段只做这层改写（窗口本体按
     * 「嵌在表达式里的窗口」收集），执行由 Planner 既有的 {@code OpaqueExpr + windowCalls} 路径完成。</p>
     */
    private record WindowExprRewrite(ParserRuleContext ctx, String replacement) {}

    private final List<WindowExprRewrite> pendingWindowExprRewrites = new ArrayList<>();

    /**
     * 是否正处于「允许出现窗口函数」的表达式里（只有 SELECT 项表达式允许）。
     *
     * <p>窗口调用只由 {@code processSelectItem} 收集并物化成合成列；若出现在 WHERE / HAVING /
     * GROUP BY / JOIN ON 里，则既不会被收集、也不会被求值 —— 下游 FilterOperator 对无法编译的
     * 谓词按「未知」逐行放行，等于整条窗口谓词被静默忽略（{@code WHERE row_number() OVER () <= 3}
     * 返回全表）。这里在语义分析期直接报错，杜绝静默。</p>
     */
    private boolean windowAllowedInExpression = false;

    /**
     * 语义分析期持久化的权威列绑定累积区：{@code visitColumnReference} 把
     * {@link Scope#resolveColumn} 已解析出的 {@link Expression.ColumnRef} 追加至此，
     * {@code captureExpression}/{@code processSelectItem} 用 {@code drainFrom(mark)} 把本次表达式
     * 产生的绑定随 {@link Expression.OpaqueExpr}/{@link Expression.WindowFunction} 一起带出，
     * 供列裁剪/谓词下推直接消费（替换 regex 重推）。
     */
    private final List<Expression.ColumnRef> pendingResolvedColumns = new ArrayList<>();

    /**
     * 标量子查询收集栈：每层查询（visitQuerySpecification）压入一个列表，`visitSubqueryExpression`
     * 把遇到的 {@link Expression.ScalarSubquery} 追加到栈顶；查询结束把栈顶列表随
     * {@link LogicalPlan.Select#scalarSubqueries()} 带出，供 Planner 去相关。
     */
    private final Deque<List<Expression.ScalarSubquery>> subqueryStack = new ArrayDeque<>();

    /** 标量子查询分析深度：>0 时对内层查询的列解析错误宽容（外层相关列暂不在内层作用域）。 */
    private int scalarSubqueryDepth = 0;

    public SemanticAnalyzer(MetaStore metaStore) {
        this.metaStore = Objects.requireNonNull(metaStore, "metaStore must not be null");
    }

    /**
     * 当前 catalog（{@code USE} 设置）；尚未 {@code USE} 过返回 null。
     * 供执行层解析 {@code SHOW ...} 里省略的 catalog 段（如 {@code SHOW TABLES}）。
     */
    public String currentCatalog() {
        return currentCatalog;
    }

    /**
     * 当前 schema（{@code USE} 设置）；尚未设置返回 null。
     */
    public String currentSchema() {
        return currentSchema;
    }

    // ==================== 入口（两阶段分析） ====================

    /**
     * 两阶段 SQL 分析：
     * <ol>
     *   <li><b>语法阶段</b>：ANTLR 词法/语法解析 + 语法错误收集。
     *       若存在语法错误，立即抛出 SemanticException 并标注行号。</li>
     *   <li><b>语义阶段</b>：AST 遍历 → 校验 catalog.schema.table 三级结构
     *       + 列引用 → 生成 LogicalPlan。</li>
     * </ol>
     */
    public LogicalPlan analyze(String sql) {
        // ★先剥掉 SQL 注释再词法分析：ANTLR 的注释走 HIDDEN 通道，但 ParserRuleContext.getText()
        //   会把区间内<b>隐藏通道 token 一并拼进来</b>，于是 `data_state = '0' --正常数据` 这类
        //   带注释的谓词原文会把「正常数据」当成标识符带进表达式文本 —— 派生表下推按列名解析
        //   时就多出一个不存在的列（规划期拒掉下沉），谓词被拼进连接器 SQL 时 `--` 还会把
        //   同一行后面的条件全部注释掉（静默少过滤）。字符串/引号内文本原样保留。
        sql = stripSqlComments(sql);
        SyntaxErrorCollector syntaxCollector = new SyntaxErrorCollector();
        SqlBaseLexer lexer = new SqlBaseLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners();
        lexer.addErrorListener(syntaxCollector);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SqlBaseParser parser = new SqlBaseParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(syntaxCollector);

        SqlBaseParser.SingleStatementContext tree = parser.singleStatement();

        if (syntaxCollector.hasErrors()) {
            throw new SemanticException(syntaxCollector.getErrors());
        }

        return analyze(tree);
    }

    /**
     * 仅执行语义分析（语法解析由调用方完成）。
     */
    public LogicalPlan analyze(ParseTree tree) {
        errors.clear();
        LogicalPlan plan = visit(tree);
        if (!errors.isEmpty()) {
            throw new SemanticException(new ArrayList<>(errors));
        }
        return plan;
    }

    // ==================== Helper ====================

    /**
     * 剥掉 SQL 注释（{@code -- 行注释} 与 <code>/* 块注释 *&#47;</code>），替换为一个空格以保持
     * token 分隔；<b>字符串字面量 / 引号标识符内的文本原样保留</b>（如 {@code 'a--b'} 不变）。
     *
     * <p>为什么必须在词法分析前做：ANTLR 把注释放在 HIDDEN 通道，但 {@code ctx.getText()} 会
     * 连带隐藏 token 一起拼接 ⇒ 表达式原文里残留注释文本，进而被规划器当作标识符解析、
     * 或被原样拼进下推给连接器的 WHERE（{@code --} 会注释掉同行后续条件，静默少过滤）。</p>
     */
    static String stripSqlComments(String sql) {
        if (sql == null || sql.isEmpty()) return sql;
        StringBuilder out = new StringBuilder(sql.length());
        int n = sql.length();
        int i = 0;
        while (i < n) {
            char c = sql.charAt(i);
            // 引号区域（字符串 / 双引号 / 反引号标识符）：整段拷贝，'' 视为转义
            if (c == '\'' || c == '"' || c == '`') {
                char q = c;
                out.append(c);
                i++;
                while (i < n) {
                    char d = sql.charAt(i);
                    out.append(d);
                    i++;
                    if (d == q) {
                        if (i < n && sql.charAt(i) == q) {   // '' / "" 转义
                            out.append(q);
                            i++;
                            continue;
                        }
                        break;
                    }
                }
                continue;
            }
            // -- 行注释：吃到行尾（保留换行符本身）
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                i += 2;
                while (i < n && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') i++;
                out.append(' ');
                continue;
            }
            // /* 块注释 */
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                i += 2;
                while (i + 1 < n && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) i++;
                i = Math.min(n, i + 2);
                out.append(' ');
                continue;
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    private Scope scope() {
        return scopeStack.peek();
    }

    private void error(ParserRuleContext ctx, String message) {
        Token start = ctx.getStart();
        errors.add(new SemanticError(message,
            start.getLine(), start.getCharPositionInLine()));
    }

    private List<String> extractQualifiedName(SqlBaseParser.QualifiedNameContext ctx) {
        if (ctx == null) return List.of();
        return ctx.identifier().stream().map(ParseTree::getText).toList();
    }

    private List<String> extractTableName(SqlBaseParser.TableNameContext ctx) {
        if (ctx == null) return List.of();
        return ctx.identifier().stream().map(ParseTree::getText).toList();
    }

    private Map<String, String> extractProperties(SqlBaseParser.TablePropertiesContext ctx) {
        if (ctx == null) return Map.of();
        Map<String, String> props = new LinkedHashMap<>();
        for (SqlBaseParser.PropertyContext prop : ctx.property()) {
            // 两种写法都要收：identifier EQ expression（裸键）与 string EQ expression（带引号的键）。
            // 带点号/特殊字符的键（如 'hive.metastore.uris'）只能写成后者 —— 只认 identifier 会 NPE。
            String key = prop.identifier() != null
                    ? prop.identifier().getText()
                    : unquoteString(prop.string().getText());
            String value = propertyValue(prop.expression());
            props.put(key, value);
        }
        return props;
    }

    /**
     * 属性值：整体是一个字符串字面量就剥掉外层引号（与 CREATE FUNCTION 的 {@code WITH} 属性一致），
     * 否则按原文 —— 不剥的话 {@code 'thrift://host:9083'} 连引号一起存进去，连接器拿到的是带引号的 URL。
     *
     * <p>内部成对的引号（SQL 的 {@code ''} 转义）会还原成一个，因此含单引号的密码
     * （{@code 'p@ss''word'}）不会再连外层引号一起落库。</p>
     */
    private String propertyValue(SqlBaseParser.ExpressionContext expr) {
        if (expr == null) return "";
        String raw = expr.getText();
        if (isSingleQuotedLiteral(raw)) {
            return unescapeQuoted(raw);
        }
        return raw;
    }

    /**
     * 判断 {@code raw} 是否恰好是一个字符串字面量：起止为同种引号，且内部的同种引号必须
     * 成对出现（{@code ''}）。遇到落单的引号说明是 {@code a || b} 这类拼接表达式 —— 按原文返回。
     */
    private static boolean isSingleQuotedLiteral(String raw) {
        if (raw.length() < 2) return false;
        char quote = raw.charAt(0);
        if ((quote != '\'' && quote != '"') || raw.charAt(raw.length() - 1) != quote) return false;
        int innerEnd = raw.length() - 1;
        for (int i = 1; i < innerEnd; i++) {
            if (raw.charAt(i) != quote) continue;
            if (i + 1 < innerEnd && raw.charAt(i + 1) == quote) {
                i++;                       // 成对转义
                continue;
            }
            return false;                  // 落单引号 = 不是单个字面量
        }
        return true;
    }

    /** 去掉起止引号并把成对引号还原成一个（调用方已用 {@link #isSingleQuotedLiteral} 校验）。 */
    private static String unescapeQuoted(String raw) {
        char quote = raw.charAt(0);
        String doubled = new String(new char[]{quote, quote});
        return raw.substring(1, raw.length() - 1).replace(doubled, String.valueOf(quote));
    }

    private LogicalPlan.ColumnDef buildColumnDef(SqlBaseParser.ColumnDefinitionContext ctx) {
        String name = ctx.identifier().getText();
        String dataType = ctx.dataType().getText();
        Optional<String> comment = ctx.string() != null
            ? Optional.of(unquoteString(ctx.string().getText()))
            : Optional.empty();
        Optional<String> defaultValue = ctx.DEFAULT() != null && ctx.expression() != null
            ? Optional.of(ctx.expression().getText())
            : Optional.empty();
        boolean nullable = ctx.NOT() == null;
        return new LogicalPlan.ColumnDef(name, dataType, comment, defaultValue, nullable);
    }

    private String unquoteString(String s) {
        return isSingleQuotedLiteral(s) ? unescapeQuoted(s) : s;
    }

    private String originalText(ParserRuleContext ctx) {
        int start = ctx.getStart().getStartIndex();
        int stop = ctx.getStop().getStopIndex();
        return ctx.getStart().getInputStream().getText(Interval.of(start, stop));
    }

    private void resolveExpressionColumns(ParserRuleContext ctx) {
        if (ctx != null) visit(ctx);
    }

    private Expression captureExpression(ParserRuleContext ctx) {
        if (ctx == null) return null;
        int mark = pendingResolvedColumns.size();
        resolveExpressionColumns(ctx);
        return new Expression.OpaqueExpr(originalText(ctx), drainFrom(mark));
    }

    /**
     * 取出并清空自 {@code mark} 起新累积的权威列绑定（本次表达式子树产生的）。
     * 空则返回 {@code List.of()}（零分配）。
     */
    private List<Expression.ColumnRef> drainFrom(int mark) {
        int size = pendingResolvedColumns.size();
        if (size == mark) return List.of();
        List<Expression.ColumnRef> out =
            List.copyOf(pendingResolvedColumns.subList(mark, size));
        pendingResolvedColumns.subList(mark, size).clear();
        return out;
    }

    private Optional<Expression> captureOptionalExpression(ParserRuleContext ctx) {
        return ctx != null ? Optional.of(captureExpression(ctx)) : Optional.empty();
    }

    /**
     * 单个 GROUP BY 元素 → 表达式：{@code CUBE / ROLLUP / GROUPING SETS} 展开为结构化
     * {@link Expression.GroupingSets}（供 Planner 做 UNION ALL 改写），其余普通键照旧 opaque。
     */
    private Expression captureGroupByElement(SqlBaseParser.GroupByContext gbCtx) {
        if (gbCtx instanceof SqlBaseParser.CubeContext cube) {
            return buildCubeSets(cube.expression(), cube, originalText(cube));
        }
        if (gbCtx instanceof SqlBaseParser.RollupContext rollup) {
            return buildRollupSets(rollup.expression(), originalText(rollup));
        }
        if (gbCtx instanceof SqlBaseParser.GroupingSetsContext gs) {
            return buildGroupingSetSets(gs.groupingSet(), originalText(gs));
        }
        return captureExpression(gbCtx);
    }

    /**
     * 裸多维分组子句（语法层面去掉 GROUP BY 的 CUBE/ROLLUP/GROUPING SETS）→ 同样产出
     * {@link Expression.GroupingSets} 塞进 Select.groupBy()，Planner 与 GROUP BY 形式走同一改写。
     */
    private Expression captureMultidimGroup(SqlBaseParser.MultidimGroupContext ctx) {
        if (ctx instanceof SqlBaseParser.BareCubeContext cube) {
            return buildCubeSets(cube.expression(), cube, originalText(cube));
        }
        if (ctx instanceof SqlBaseParser.BareRollupContext rollup) {
            return buildRollupSets(rollup.expression(), originalText(rollup));
        }
        if (ctx instanceof SqlBaseParser.BareGroupingSetsContext gs) {
            return buildGroupingSetSets(gs.groupingSet(), originalText(gs));
        }
        return null;
    }

    /** CUBE → 全 2^n 子集，掩码降序 = 列数从多到少（含 () 全局总计）；>8 维报错。 */
    private Expression buildCubeSets(List<SqlBaseParser.ExpressionContext> exprs,
                                     ParserRuleContext errCtx, String text) {
        List<Expression> cols = captureColumns(exprs);
        if (cols.size() > 8) {
            error(errCtx, "CUBE 维度过多（>8 将生成 >256 条 UNION ALL 腿）: " + text);
            return null;
        }
        long total = 1L << cols.size();
        List<List<Expression>> sets = new ArrayList<>((int) total);
        for (long mask = total - 1; mask >= 0; mask--) {
            sets.add(List.copyOf(subsetOf(cols, mask)));
        }
        return new Expression.GroupingSets(sets, text);
    }

    /** ROLLUP → 前缀子集（全列 → 空，含 () 全局总计）。 */
    private Expression buildRollupSets(List<SqlBaseParser.ExpressionContext> exprs, String text) {
        List<Expression> cols = captureColumns(exprs);
        List<List<Expression>> sets = new ArrayList<>(cols.size() + 1);
        for (int n = cols.size(); n >= 0; n--) {
            sets.add(List.copyOf(cols.subList(0, n)));
        }
        return new Expression.GroupingSets(sets, text);
    }

    /** GROUPING SETS(...) → 显式子集（groupingSet 为 () 空括号时 = 全局总计）。 */
    private Expression buildGroupingSetSets(List<SqlBaseParser.GroupingSetContext> setCtxs, String text) {
        List<List<Expression>> sets = new ArrayList<>(setCtxs.size());
        for (SqlBaseParser.GroupingSetContext setCtx : setCtxs) {
            sets.add(List.copyOf(captureColumns(setCtx.expression())));
        }
        return new Expression.GroupingSets(sets, text);
    }

    /** 捕获列表达式列表（各自 resolve 列引用，捕获失败抛空）。 */
    private List<Expression> captureColumns(List<SqlBaseParser.ExpressionContext> exprs) {
        List<Expression> cols = new ArrayList<>(exprs.size());
        for (SqlBaseParser.ExpressionContext e : exprs) {
            Expression expr = captureExpression(e);
            if (expr != null) cols.add(expr);
        }
        return cols;
    }

    /** 取 cols 中 bit 位为 1 的子集（保持原顺序）。 */
    private static List<Expression> subsetOf(List<Expression> cols, long mask) {
        List<Expression> subset = new ArrayList<>(Long.bitCount(mask));
        for (int i = 0; i < cols.size(); i++) {
            if ((mask & (1L << i)) != 0) subset.add(cols.get(i));
        }
        return subset;
    }

    // ==================== singleStatement ====================

    @Override
    public LogicalPlan visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        return visit(ctx.statement());
    }

    // ==================== Statement 分派 ====================

    @Override
    public LogicalPlan visitStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {
        return visit(ctx.query());
    }

    @Override
    public LogicalPlan visitStatementInsert(SqlBaseParser.StatementInsertContext ctx) {
        return visit(ctx.insertStatement());
    }

    @Override
    public LogicalPlan visitStatementCreateTable(SqlBaseParser.StatementCreateTableContext ctx) {
        return visit(ctx.createTableStatement());
    }

    @Override
    public LogicalPlan visitStatementCreateTableAsSelect(SqlBaseParser.StatementCreateTableAsSelectContext ctx) {
        return visit(ctx.createTableAsSelectStatement());
    }

    @Override
    public LogicalPlan visitStatementCreateCatalog(SqlBaseParser.StatementCreateCatalogContext ctx) {
        return visit(ctx.createCatalogStatement());
    }

    @Override
    public LogicalPlan visitStatementCreateSchema(SqlBaseParser.StatementCreateSchemaContext ctx) {
        return visit(ctx.createSchemaStatement());
    }

    @Override
    public LogicalPlan visitStatementCreateView(SqlBaseParser.StatementCreateViewContext ctx) {
        return visit(ctx.createViewStatement());
    }

    @Override
    public LogicalPlan visitStatementCreateFunction(SqlBaseParser.StatementCreateFunctionContext ctx) {
        return visit(ctx.createFunctionStatement());
    }

    @Override
    public LogicalPlan visitStatementDropCatalog(SqlBaseParser.StatementDropCatalogContext ctx) {
        return visit(ctx.dropCatalogStatement());
    }

    @Override
    public LogicalPlan visitStatementDropSchema(SqlBaseParser.StatementDropSchemaContext ctx) {
        return visit(ctx.dropSchemaStatement());
    }

    @Override
    public LogicalPlan visitStatementDropTable(SqlBaseParser.StatementDropTableContext ctx) {
        return visit(ctx.dropTableStatement());
    }

    @Override
    public LogicalPlan visitStatementTruncateTable(SqlBaseParser.StatementTruncateTableContext ctx) {
        return visit(ctx.truncateTableStatement());
    }

    @Override
    public LogicalPlan visitStatementDropView(SqlBaseParser.StatementDropViewContext ctx) {
        return visit(ctx.dropViewStatement());
    }

    @Override
    public LogicalPlan visitStatementDropFunction(SqlBaseParser.StatementDropFunctionContext ctx) {
        return visit(ctx.dropFunctionStatement());
    }

    @Override
    public LogicalPlan visitStatementAlterTable(SqlBaseParser.StatementAlterTableContext ctx) {
        return visit(ctx.alterTableStatement());
    }

    @Override
    public LogicalPlan visitUse(SqlBaseParser.UseContext ctx) {
        return visitUseStatement(ctx);
    }

    @Override
    public LogicalPlan visitShowCatalogs(SqlBaseParser.ShowCatalogsContext ctx) {
        return new LogicalPlan.ShowCatalogs();
    }

    @Override
    public LogicalPlan visitShowSchemas(SqlBaseParser.ShowSchemasContext ctx) {
        Optional<String> catalog = ctx.identifier() != null
            ? Optional.of(ctx.identifier().getText()) : Optional.empty();
        return new LogicalPlan.ShowSchemas(catalog);
    }

    @Override
    public LogicalPlan visitShowTables(SqlBaseParser.ShowTablesContext ctx) {
        Optional<List<String>> qname = ctx.qualifiedName() != null
            ? Optional.of(extractQualifiedName(ctx.qualifiedName())) : Optional.empty();
        Optional<String> pattern = ctx.string() != null
            ? Optional.of(unquoteString(ctx.string().getText())) : Optional.empty();
        return new LogicalPlan.ShowTables(qname, pattern);
    }

    @Override
    public LogicalPlan visitShowColumns(SqlBaseParser.ShowColumnsContext ctx) {
        return new LogicalPlan.ShowColumns(extractQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public LogicalPlan visitShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx) {
        return new LogicalPlan.ShowCreateTable(extractQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public LogicalPlan visitDescribe(SqlBaseParser.DescribeContext ctx) {
        return new LogicalPlan.Describe(extractQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public LogicalPlan visitExplain(SqlBaseParser.ExplainContext ctx) {
        boolean analyze = ctx.ANALYZE() != null;
        boolean verbose = ctx.VERBOSE() != null;
        return new LogicalPlan.Explain(visit(ctx.statement()), analyze, verbose);
    }

    // ==================== USE ====================

    public LogicalPlan visitUseStatement(SqlBaseParser.UseContext ctx) {
        List<SqlBaseParser.IdentifierContext> identifiers = ctx.identifier();
        String catalog = identifiers.get(0).getText();

        if (!metaStore.isCatalogRegistered(catalog)) {
            error(ctx, "Catalog '" + catalog + "' does not exist");
        }

        Optional<String> schema = Optional.empty();
        if (identifiers.size() > 1) {
            String schemaName = identifiers.get(1).getText();
            schema = Optional.of(schemaName);
            if (!metaStore.schemaExists(catalog, schemaName)) {
                error(ctx, "Schema '" + catalog + "." + schemaName + "' does not exist");
            }
        }

        this.currentCatalog = catalog;
        this.currentSchema = schema.orElse(null);
        return new LogicalPlan.Use(catalog, schema);
    }

    // ==================== Catalog DDL ====================

    @Override
    public LogicalPlan visitCreateCatalogStatement(SqlBaseParser.CreateCatalogStatementContext ctx) {
        String name = ctx.identifier(0).getText();
        String connectorType = ctx.identifier(1).getText();
        Map<String, String> properties = ctx.tableProperties() != null
            ? extractProperties(ctx.tableProperties()) : Map.of();
        boolean ifNotExists = ctx.IF() != null;
        return new LogicalPlan.CreateCatalog(name, connectorType, properties, ifNotExists);
    }

    @Override
    public LogicalPlan visitDropCatalogStatement(SqlBaseParser.DropCatalogStatementContext ctx) {
        String name = ctx.identifier().getText();
        boolean ifExists = ctx.IF() != null;
        if (!ifExists && !metaStore.isCatalogRegistered(name)) {
            error(ctx, "Catalog '" + name + "' does not exist");
        }
        return new LogicalPlan.DropCatalog(name, ifExists);
    }

    // ==================== Schema DDL ====================

    @Override
    public LogicalPlan visitCreateSchemaStatement(SqlBaseParser.CreateSchemaStatementContext ctx) {
        List<String> qname = extractQualifiedName(ctx.qualifiedName());
        Map<String, String> properties = ctx.tableProperties() != null
            ? extractProperties(ctx.tableProperties()) : Map.of();
        boolean ifNotExists = ctx.IF() != null;
        if (qname.size() >= 2 && !metaStore.isCatalogRegistered(qname.get(0))) {
            error(ctx, "Catalog '" + qname.get(0) + "' does not exist when creating schema");
        }
        return new LogicalPlan.CreateSchema(qname, properties, ifNotExists);
    }

    @Override
    public LogicalPlan visitDropSchemaStatement(SqlBaseParser.DropSchemaStatementContext ctx) {
        List<String> qname = extractQualifiedName(ctx.qualifiedName());
        boolean ifExists = ctx.IF() != null;
        boolean cascade = ctx.CASCADE() != null;
        if (!ifExists && qname.size() >= 2
            && !metaStore.schemaExists(qname.get(0), qname.get(1))) {
            error(ctx, "Schema '" + qname.get(0) + "." + qname.get(1) + "' does not exist");
        }
        return new LogicalPlan.DropSchema(qname, ifExists, cascade);
    }

    // ==================== Table DDL ====================

    @Override
    public LogicalPlan visitCreateTableStatement(SqlBaseParser.CreateTableStatementContext ctx) {
        List<String> qname = extractQualifiedName(ctx.qualifiedName());
        boolean ifNotExists = ctx.IF() != null;
        Map<String, String> properties = ctx.tableProperties() != null
            ? extractProperties(ctx.tableProperties()) : Map.of();
        Optional<String> comment = ctx.string() != null
            ? Optional.of(unquoteString(ctx.string().getText())) : Optional.empty();

        List<LogicalPlan.ColumnDef> columns = new ArrayList<>();
        for (SqlBaseParser.TableElementContext elem : ctx.tableElement()) {
            columns.add(buildColumnDef(elem.columnDefinition()));
        }

        validateCatalogSchemaForTable(ctx, qname);
        return new LogicalPlan.CreateTable(qname, columns, properties, comment, ifNotExists);
    }

    @Override
    public LogicalPlan visitCreateTableAsSelectStatement(SqlBaseParser.CreateTableAsSelectStatementContext ctx) {
        List<String> qname = extractQualifiedName(ctx.qualifiedName());
        Map<String, String> properties = ctx.tableProperties() != null
            ? extractProperties(ctx.tableProperties()) : Map.of();
        Optional<String> comment = ctx.string() != null
            ? Optional.of(unquoteString(ctx.string().getText())) : Optional.empty();
        Optional<List<String>> colAliases = ctx.columnAliases() != null
            ? Optional.of(ctx.columnAliases().identifier().stream()
                .map(ParseTree::getText).toList()) : Optional.empty();

        validateCatalogSchemaForTable(ctx, qname);
        boolean ifNotExists = ctx.IF() != null;
        LogicalPlan query = visit(ctx.query());
        return new LogicalPlan.CreateTableAsSelect(
                qname, colAliases, properties, comment, ifNotExists, query);
    }

    @Override
    public LogicalPlan visitDropTableStatement(SqlBaseParser.DropTableStatementContext ctx) {
        List<String> qname = extractQualifiedName(ctx.qualifiedName());
        boolean ifExists = ctx.IF() != null;
        if (!ifExists) validateTableExists(ctx, qname);
        return new LogicalPlan.DropTable(qname, ifExists);
    }

    @Override
    public LogicalPlan visitTruncateTableStatement(SqlBaseParser.TruncateTableStatementContext ctx) {
        List<String> qname = extractQualifiedName(ctx.qualifiedName());
        validateTableExists(ctx, qname);
        return new LogicalPlan.TruncateTable(qname);
    }

    @Override
    public LogicalPlan visitAlterTableStatement(SqlBaseParser.AlterTableStatementContext ctx) {
        List<String> qname = extractQualifiedName(ctx.qualifiedName());
        validateTableExists(ctx, qname);

        LogicalPlan.AlterAction action;
        if (ctx.RENAME() != null && ctx.TO() != null && ctx.identifier().size() >= 1) {
            List<SqlBaseParser.IdentifierContext> ids = ctx.identifier();
            action = new LogicalPlan.AlterAction.RenameTo(ids.get(ids.size() - 1).getText());
        } else if (ctx.ADD() != null && ctx.columnDefinition() != null) {
            action = new LogicalPlan.AlterAction.AddColumn(buildColumnDef(ctx.columnDefinition()));
        } else if (ctx.DROP() != null && ctx.COLUMN() != null) {
            action = new LogicalPlan.AlterAction.DropColumn(
                ctx.identifier().get(ctx.identifier().size() - 1).getText());
        } else if (ctx.RENAME() != null && ctx.COLUMN() != null) {
            List<SqlBaseParser.IdentifierContext> ids = ctx.identifier();
            action = new LogicalPlan.AlterAction.RenameColumn(
                ids.get(ids.size() - 2).getText(), ids.get(ids.size() - 1).getText());
        } else {
            action = new LogicalPlan.AlterAction.RenameTo("unknown");
        }
        return new LogicalPlan.AlterTable(qname, action);
    }

    // ==================== View DDL ====================

    @Override
    public LogicalPlan visitCreateViewStatement(SqlBaseParser.CreateViewStatementContext ctx) {
        List<String> qname = extractQualifiedName(ctx.qualifiedName());
        boolean orReplace = ctx.REPLACE() != null;
        return new LogicalPlan.CreateView(qname, orReplace, visit(ctx.query()));
    }

    @Override
    public LogicalPlan visitDropViewStatement(SqlBaseParser.DropViewStatementContext ctx) {
        return new LogicalPlan.DropView(extractQualifiedName(ctx.qualifiedName()), ctx.IF() != null);
    }

    // ==================== Function DDL ====================

    @Override
    public LogicalPlan visitCreateFunctionStatement(SqlBaseParser.CreateFunctionStatementContext ctx) {
        List<String> qname = extractQualifiedName(ctx.qualifiedName());
        boolean temporary = ctx.TEMPORARY() != null || ctx.SESSION() != null;
        boolean ifNotExists = ctx.IF() != null;
        Map<String, String> properties = ctx.tableProperties() != null
            ? extractProperties(ctx.tableProperties()) : Map.of();
        Optional<String> comment = ctx.string() != null
            ? Optional.of(unquoteString(ctx.string().getText())) : Optional.empty();
        return new LogicalPlan.CreateFunction(qname, temporary, properties, comment, ifNotExists);
    }

    @Override
    public LogicalPlan visitDropFunctionStatement(SqlBaseParser.DropFunctionStatementContext ctx) {
        List<String> qname = extractQualifiedName(ctx.qualifiedName());
        boolean temporary = ctx.TEMPORARY() != null || ctx.SESSION() != null;
        boolean ifExists = ctx.IF() != null;
        return new LogicalPlan.DropFunction(qname, temporary, ifExists);
    }

    // ==================== INSERT ====================

    @Override
    public LogicalPlan visitInsertStatement(SqlBaseParser.InsertStatementContext ctx) {
        List<String> qname = extractQualifiedName(ctx.qualifiedName());
        Optional<List<String>> colAliases = ctx.columnAliases() != null
            ? Optional.of(ctx.columnAliases().identifier().stream()
                .map(ParseTree::getText).toList()) : Optional.empty();

        TableResolver resolver = new TableResolver(metaStore, currentCatalog, currentSchema);
        TableResolver.ResolveResult result = resolver.resolve(qname);
        errors.addAll(result.errors());

        String catalog = qname.size() >= 3 ? qname.get(0)
            : currentCatalog;
        String schema = qname.size() >= 3 ? qname.get(1)
            : qname.size() == 2 ? qname.get(0) : currentSchema;
        String table = qname.get(qname.size() - 1);

        LogicalPlan query;
        if (ctx.query() != null) {
            query = visit(ctx.query());
        } else if (ctx.VALUES() != null) {
            List<List<Expression>> rows = new ArrayList<>();
            for (SqlBaseParser.ValuesContext vCtx : ctx.values()) {
                rows.add(vCtx.expression().stream()
                    .map(e -> (Expression) new Expression.OpaqueExpr(originalText(e))).toList());
            }
            query = new LogicalPlan.Values(rows);
        } else {
            query = new LogicalPlan.Unsupported("INSERT");
        }

        // 语义校验：INSERT INTO t [cols] SELECT ... 列数与同位置类型必须一致（提前报错，避免执行期失败）
        if (query instanceof LogicalPlan.Select select) {
            validateInsertSelectTypes(ctx, catalog, schema, table, colAliases, select);
        }

        boolean overwrite = ctx.OVERWRITE() != null;   // INSERT OVERWRITE（覆盖写），Feature B
        return new LogicalPlan.Insert(
            catalog != null ? catalog : "unknown",
            schema != null ? schema : "unknown",
            table, colAliases, overwrite, query);
    }

    /**
     * 校验 INSERT INTO ... SELECT 的列数一致 + 同位置类型一致。
     *
     * <p>类型比较统一走 DojaoType 归一化：目标表列经 {@link DojaoConnector#getColumnTypes}
     * 映射到标准枚举，来源 SELECT 输出列从源表列经同一映射得到，两侧同方言/跨方言都按枚举严格相等。
     * 这样屏蔽了 {@code int(11)} vs {@code INT}、{@code varchar(255)} vs {@code VARCHAR}
     * 这类同方言内部的写法差异。</p>
     *
     * <p>来源侧类型推导见 {@link #inferExprType}：纯列引用（含 {@code *} / {@code a.*} 展开）、
     * 内置聚合（count / sum / avg / min / max，按引擎的结果类型口径）、{@code CAST(...)} 目标类型、
     * 字面量（数值 / 字符串 / 布尔）都可推导；其余复杂表达式（算术、函数、CASE 等）语义阶段
     * 类型未知，对应位置跳过类型检查（不误报），仅列数仍严格校验。</p>
     *
     * <p>★W1：早期实现只认「列引用」，导致 {@code INSERT INTO t (string_col) SELECT sum(x)} 这类
     * 跨类型族写入被静默放行。现在聚合与字面量也参与校验。</p>
     */
    private void validateInsertSelectTypes(ParserRuleContext ctx,
                                           String catalog, String schema, String table,
                                           Optional<List<String>> colAliases,
                                           LogicalPlan.Select select) {
        if (catalog == null || schema == null) return;
        Optional<DojaoConnector> targetConn = metaStore.getConnector(catalog);
        if (targetConn.isEmpty()) return;

        List<Column> allTargetCols = metaStore.getColumns(catalog, schema, table);
        if (allTargetCols.isEmpty()) return;
        Map<String, DojaoType> targetTypeMap = targetConn.get().getColumnTypes(schema, table);

        // 目标列清单：显式列别名子句（INSERT INTO t(a,b)）优先，否则取目标表全部列
        List<Column> targetCols;
        if (colAliases.isPresent()) {
            List<Column> picked = new ArrayList<>();
            for (String alias : colAliases.get()) {
                Column found = null;
                for (Column c : allTargetCols) {
                    if (c.name().equalsIgnoreCase(alias)) { found = c; break; }
                }
                picked.add(found); // null 占位：别名在目标表中不存在（个数校验仍按别名个数）
            }
            targetCols = picked;
        } else {
            targetCols = allTargetCols;
        }

        // 来源 SELECT 输出列类型（可解析位置给 DojaoType，未知为 null）
        List<DojaoType> sourceTypes = deriveSelectOutputTypes(select);

        // 1) 列数校验
        if (targetCols.size() != sourceTypes.size()) {
            error(ctx, "INSERT 列数不匹配：目标表 " + table + " 提供 " + targetCols.size()
                    + " 列，SELECT 输出 " + sourceTypes.size() + " 列");
            return;
        }
        // 2) 类型校验（位置对应；source 未知 / 目标类型缺失 → 跳过不误报）
        for (int i = 0; i < targetCols.size(); i++) {
            Column tc = targetCols.get(i);
            if (tc == null) {
                error(ctx, "INSERT 列别名不存在于目标表："
                        + (colAliases.isPresent() ? colAliases.get().get(i) : "?"));
                continue;
            }
            DojaoType s = sourceTypes.get(i);
            if (s == null) continue; // SELECT 输出为表达式/字面量，语义阶段类型未知
            DojaoType t = targetTypeMap.get(tc.name());
            if (t == null) continue;
            if (s != t) {
                error(ctx, "INSERT 列类型不匹配：目标列 " + tc.name() + " (" + t
                        + ")，SELECT 第 " + (i + 1) + " 列 (" + s + ") 不一致");
            }
        }
    }

    /**
     * 推导 SELECT 输出列的类型（DojaoType）。纯列引用 / {@code *} / {@code a.*}、内置聚合、
     * {@code CAST}、字面量可解析；其余表达式 → null（语义阶段类型未知，跳过严格类型检查）。
     */
    private List<DojaoType> deriveSelectOutputTypes(LogicalPlan.Select select) {
        // qualifier（别名或表名）→ 列顺序 + 列名→DojaoType
        Map<String, RelTypeInfo> rels = new LinkedHashMap<>();
        for (LogicalPlan.ResolvedRelation rel : select.fromRelations()) {
            RelTypeInfo info = new RelTypeInfo(rel.columns());
            if (rel.subquery().isEmpty()) { // 派生表/CTE 类型未知，保持空映射
                metaStore.getConnector(rel.catalog()).ifPresent(conn ->
                        info.types = new HashMap<>(conn.getColumnTypes(rel.schema(), rel.table())));
            }
            rel.alias().ifPresent(a -> rels.putIfAbsent(a.toLowerCase(Locale.ROOT), info));
            rels.putIfAbsent(rel.table().toLowerCase(Locale.ROOT), info);
        }

        List<DojaoType> out = new ArrayList<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            if (!(item.expression() instanceof Expression.OpaqueExpr opaque)) {
                out.add(null); // WindowFunction 等非 OpaqueExpr
                continue;
            }
            String text = opaque.originalText().trim();
            String upper = text.toUpperCase(Locale.ROOT);
            if (text.equals("*")) { // SELECT *：按 fromRelations 顺序展开全部列
                for (LogicalPlan.ResolvedRelation rel : select.fromRelations()) {
                    for (String c : rel.columns()) out.add(typeOf(rels, null, c));
                }
                continue;
            }
            if (upper.endsWith(".*")) { // a.*
                String qual = text.substring(0, text.length() - 2).trim();
                RelTypeInfo info = rels.get(qual.toLowerCase(Locale.ROOT));
                if (info == null) {
                    out.add(null);
                } else {
                    for (String c : info.columnOrder) out.add(info.typeOf(c));
                }
                continue;
            }
            // 表达式类型（聚合 / CAST / 字面量 / 限定列引用）
            DojaoType exprType = inferExprType(text, rels);
            if (exprType != null) {
                out.add(exprType);
                continue;
            }
            int dot = text.indexOf('.');
            if (dot > 0) { // a.id
                String qual = text.substring(0, dot).trim();
                String col = text.substring(dot + 1).trim();
                RelTypeInfo info = rels.get(qual.toLowerCase(Locale.ROOT));
                out.add(info == null ? null : info.typeOf(col));
            } else { // 无 qualifier：跨 relation 查同名列
                out.add(typeOf(rels, null, text));
            }
        }
        return out;
    }

    /**
     * 表达式输出类型推断（INSERT 类型校验用，纯文本、不依赖物理层）。
     *
     * <p>只覆盖能<b>确定</b>推导的形态；无法确定一律返回 {@code null}（调用方跳过检查，宁放过不误报）：</p>
     * <ul>
     *   <li>列引用：{@code col} / {@code a.col}（查 FROM 关系元数据）；</li>
     *   <li>内置聚合：{@code count(...)} → BIGINT；{@code sum} 整型 → BIGINT、decimal → DECIMAL、
     *       浮点 → DOUBLE；{@code avg} decimal → DECIMAL、其余数值 → DOUBLE；{@code min/max}
     *       数值 → DOUBLE（引擎历史口径）、decimal → DECIMAL、其余同参数类型；</li>
     *   <li>{@code CAST(expr AS 类型)} → 目标类型；</li>
     *   <li>字面量：数值 → 整数 BIGINT / 带小数点或指数 DOUBLE；{@code '...'} → STRING；
     *       TRUE / FALSE → BOOLEAN；NULL → 未知。</li>
     * </ul>
     */
    private DojaoType inferExprType(String text, Map<String, RelTypeInfo> rels) {
        String t = stripOuterParens(text == null ? "" : text.trim());
        if (t.isEmpty()) return null;

        // 字面量
        DojaoType lit = literalType(t);
        if (lit != null) return lit;

        // 列引用（含限定）
        DojaoType column = columnRefType(t, rels);
        if (column != null) return column;

        // 顶层函数调用 name(...)（CAST 与聚合共用括号解析）
        String[] call = splitCall(t);
        if (call == null) return null;
        String fn = call[0].toLowerCase(Locale.ROOT);
        String arg = call[1].trim();

        if ("cast".equals(fn)) {
            int as = indexOfKeyword(arg, "AS");
            return as < 0 ? null : dojaoTypeName(arg.substring(as + 2).trim());
        }
        return aggregateOutputType(fn, arg, rels);
    }

    /** 内置聚合的结果类型（与 {@code AggregationHashTable.resultTypeFor} 口径一致）。 */
    private DojaoType aggregateOutputType(String fn, String arg, Map<String, RelTypeInfo> rels) {
        String a = arg.trim();
        if (a.toUpperCase(Locale.ROOT).startsWith("DISTINCT ")) {
            a = a.substring("DISTINCT ".length()).trim();
        }
        if ("count".equals(fn)) {
            return DojaoType.BIGINT;
        }
        if (!"sum".equals(fn) && !"avg".equals(fn) && !"min".equals(fn) && !"max".equals(fn)) {
            return null;
        }
        if ("*".equals(a)) return null;  // sum(*) 等非法/未知形态不猜
        DojaoType in = inferExprType(a, rels);
        if (in == null) return null;
        boolean integral = in == DojaoType.TINYINT || in == DojaoType.SMALLINT
                || in == DojaoType.INT || in == DojaoType.BIGINT;
        boolean floating = in == DojaoType.FLOAT || in == DojaoType.DOUBLE;
        if ("sum".equals(fn)) {
            if (integral) return DojaoType.BIGINT;
            if (floating) return DojaoType.DOUBLE;
            return in == DojaoType.DECIMAL ? DojaoType.DECIMAL : null;
        }
        if ("avg".equals(fn)) {
            if (in == DojaoType.DECIMAL) return DojaoType.DECIMAL;
            return (integral || floating) ? DojaoType.DOUBLE : null;
        }
        // min / max：数值（含 decimal）→ DOUBLE（decimal 保 DECIMAL），其余同参数类型
        if (in == DojaoType.DECIMAL) return DojaoType.DECIMAL;
        if (integral || floating) return DojaoType.DOUBLE;
        return switch (in) {
            case STRING, DATE, DATETIME, TIMESTAMP, TIMESTAMP_TZ, BOOLEAN -> in;
            default -> null;
        };
    }

    /** 字面量类型；非字面量（含 NULL，未知即跳过）→ null。 */
    private static DojaoType literalType(String t) {
        if (t.isEmpty()) return null;
        if (t.equalsIgnoreCase("TRUE") || t.equalsIgnoreCase("FALSE")) return DojaoType.BOOLEAN;
        if (t.length() >= 2 && t.charAt(0) == '\'' && t.charAt(t.length() - 1) == '\'') {
            return DojaoType.STRING;
        }
        String body = t;
        if (body.startsWith("+") || body.startsWith("-")) body = body.substring(1);
        if (body.isEmpty()) return null;
        boolean digit = true, dot = false, exp = false;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c >= '0' && c <= '9') continue;
            if (c == '.' && !dot && !exp) { dot = true; continue; }
            if ((c == 'e' || c == 'E') && !exp) { exp = true; continue; }
            digit = false;
            break;
        }
        if (!digit) return null;
        if (dot || exp) return DojaoType.DOUBLE;
        // 纯整数：语义阶段不区分宽度，按 BIGINT 记（INSERT 目标为 INT 时会正确报出「不可无损写入」）
        return DojaoType.BIGINT;
    }

    /** 列引用类型：{@code col} / {@code a.col}；不是列引用 → null。 */
    private static DojaoType columnRefType(String t, Map<String, RelTypeInfo> rels) {
        if (t.matches("[\\p{L}_][\\p{L}\\p{N}_$]*")) {
            return typeOf(rels, null, t);
        }
        int dot = t.indexOf('.');
        if (dot <= 0 || t.indexOf('.', dot + 1) >= 0) return null;
        String qual = t.substring(0, dot).trim();
        String col = t.substring(dot + 1).trim();
        if (!qual.matches("[\\p{L}_][\\p{L}\\p{N}_$]*")
                || !col.matches("[\\p{L}_][\\p{L}\\p{N}_$]*")) {
            return null;
        }
        return typeOf(rels, qual, col);
    }

    /** 去配对的外层括号；只有整段被一对括号包住时才剥。 */
    private static String stripOuterParens(String t) {
        while (t.length() >= 2 && t.charAt(0) == '(' && t.charAt(t.length() - 1) == ')') {
            int depth = 0;
            boolean wraps = true;
            for (int i = 0; i < t.length(); i++) {
                char c = t.charAt(i);
                if (c == '\'') { // 跳过字符串字面量
                    i++;
                    while (i < t.length() && t.charAt(i) != '\'') i++;
                    continue;
                }
                if (c == '(') depth++;
                else if (c == ')') {
                    depth--;
                    if (depth == 0 && i < t.length() - 1) { wraps = false; break; }
                }
            }
            if (!wraps) break;
            t = t.substring(1, t.length() - 1).trim();
        }
        return t;
    }

    /** 顶层 {@code name(arg)} → {"name","arg"}；不是单层函数调用 → null。 */
    private static String[] splitCall(String t) {
        if (t.isEmpty() || t.charAt(t.length() - 1) != ')') return null;
        int open = t.indexOf('(');
        if (open <= 0) return null;
        String name = t.substring(0, open).trim();
        if (!name.matches("[\\p{L}_][\\p{L}\\p{N}_$]*")) return null;
        // 括号必须配对到字符串末尾（且字符串内不出现）
        int depth = 0;
        for (int i = open; i < t.length(); i++) {
            char c = t.charAt(i);
            if (c == '\'') {
                i++;
                while (i < t.length() && t.charAt(i) != '\'') i++;
                continue;
            }
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0 && i != t.length() - 1) return null;
            }
        }
        if (depth != 0) return null;
        return new String[]{name, t.substring(open + 1, t.length() - 1)};
    }

    /** 顶层关键字定位（忽略括号与字符串内的文本）；未找到 → -1。 */
    private static int indexOfKeyword(String t, String keyword) {
        int depth = 0;
        for (int i = 0; i + keyword.length() <= t.length(); i++) {
            char c = t.charAt(i);
            if (c == '\'') {
                i++;
                while (i < t.length() && t.charAt(i) != '\'') i++;
                continue;
            }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && t.regionMatches(true, i, keyword, 0, keyword.length())) {
                boolean before = i == 0 || !Character.isLetterOrDigit(t.charAt(i - 1));
                int end = i + keyword.length();
                boolean after = end >= t.length() || !Character.isLetterOrDigit(t.charAt(end));
                if (before && after) return i;
            }
        }
        return -1;
    }

    /** 类型名（CAST 目标）→ DojaoType；未知 → null。 */
    private static DojaoType dojaoTypeName(String type) {
        String n = type.trim().toUpperCase(Locale.ROOT);
        int p = n.indexOf('(');   // DECIMAL(12,2) / VARCHAR(32)
        if (p > 0) n = n.substring(0, p).trim();
        return switch (n) {
            case "BOOLEAN", "BOOL" -> DojaoType.BOOLEAN;
            case "TINYINT" -> DojaoType.TINYINT;
            case "SMALLINT" -> DojaoType.SMALLINT;
            case "INT", "INTEGER" -> DojaoType.INT;
            case "BIGINT" -> DojaoType.BIGINT;
            case "FLOAT", "REAL" -> DojaoType.FLOAT;
            case "DOUBLE" -> DojaoType.DOUBLE;
            case "DECIMAL", "NUMERIC" -> DojaoType.DECIMAL;
            case "STRING", "VARCHAR", "CHAR", "TEXT" -> DojaoType.STRING;
            case "DATE" -> DojaoType.DATE;
            case "DATETIME" -> DojaoType.DATETIME;
            case "TIMESTAMP" -> DojaoType.TIMESTAMP;
            default -> null;
        };
    }

    /** 解析列类型：优先 qualifier 限定，否则无 qualifier 跨 relation 查同名列。 */
    private static DojaoType typeOf(Map<String, RelTypeInfo> rels, String qual, String col) {
        if (qual != null) {
            RelTypeInfo info = rels.get(qual.toLowerCase(Locale.ROOT));
            return info == null ? null : info.typeOf(col);
        }
        DojaoType found = null;
        for (RelTypeInfo info : rels.values()) {
            DojaoType t = info.typeOf(col);
            if (t != null) {
                if (found != null && found != t) return null; // 多表同名且类型不同 → 未知
                found = t;
            }
        }
        return found;
    }

    /** 关系类型信息：列顺序 + 列名→DojaoType（列名大小写不敏感查找）。 */
    private static final class RelTypeInfo {
        final List<String> columnOrder;
        Map<String, DojaoType> types = Map.of();
        RelTypeInfo(List<String> columnOrder) { this.columnOrder = columnOrder; }
        DojaoType typeOf(String col) {
            for (Map.Entry<String, DojaoType> e : types.entrySet()) {
                if (e.getKey().equalsIgnoreCase(col)) return e.getValue();
            }
            return null;
        }
    }

    // ==================== Query ====================

    @Override public LogicalPlan visitQuery(SqlBaseParser.QueryContext ctx) {
        if (ctx.WITH() != null) {
            // 本层 WITH 是新的 CTE 作用域：外层 CTE 仍可见，本层同名定义遮蔽外层；离开本层恢复外层。
            // 旧实现离开时直接 cteDefinitions.clear()，嵌套 WITH（CTE 体内 / 子查询内）会把外层
            // 已注册的 CTE 一并抹掉 —— 之后主查询再引用兄弟 CTE 就变成「表不存在」。
            Map<String, List<String>> savedScope = new LinkedHashMap<>(cteDefinitions);
            try {
                // 解析 CTE，自上而下注册，嵌套 CTE 可引用已注册的 CTE
                List<LogicalPlan.CteDef> cteDefs = new ArrayList<>();
                for (SqlBaseParser.CommonTableExpressionContext cte : ctx.commonTableExpression()) {
                    String cteName = cte.identifier().getText();
                    LogicalPlan innerPlan = visit(cte.query());
                    List<String> columns;
                    if (cte.columnAliases() != null) {
                        columns = cte.columnAliases().identifier().stream()
                            .map(ParseTree::getText).toList();
                    } else {
                        // 与内联派生表（deriveDerivedTableColumns）同一推导：有别名→别名；select *→按
                        // FROM 展开；裸列→剥表限定符（r.user_id → user_id）。原来直接用表达式文本会
                        // 把 "r.user_id" 整段当列标签 → CTE 内 join 后外层永远解析不到 user_id（bug）。
                        columns = deriveDerivedTableColumns(innerPlan);
                    }
                    cteDefinitions.put(cteName, columns);
                    cteDefs.add(new LogicalPlan.CteDef(cteName, columns, innerPlan));
                }
                LogicalPlan mainQuery = visit(ctx.queryNoWith());
                return new LogicalPlan.With(cteDefs, mainQuery);
            } finally {
                cteDefinitions.clear();
                cteDefinitions.putAll(savedScope);
            }
        }
        return visit(ctx.queryNoWith());
    }

    @Override public LogicalPlan visitQueryNoWith(SqlBaseParser.QueryNoWithContext ctx) {
        LogicalPlan plan = visit(ctx.queryTerm());
        // 顶层 ORDER BY / LIMIT / OFFSET（语法挂在 queryNoWith，见 SqlBase.g4:148-153）——
        // 以前整段被丢弃（不排序不截断静默算错）。这里读进来随 Select/SetOperation 带出，
        // Planner 在 plan() 里包全局 Sort/Limit。ORDER BY 项不 resolveExpressionColumns：
        // 别名 / 位置序号不是 FROM 列，逐列校验会误报"未知列"。
        List<LogicalPlan.SortItem> orderBy = List.of();
        if (ctx.sortItem() != null && !ctx.sortItem().isEmpty()) {
            orderBy = ctx.sortItem().stream().map(this::captureSortItem).toList();
        }
        Optional<Long> limit = parseLimitOffset(ctx.limit);
        Optional<Long> offset = parseLimitOffset(ctx.offset);
        if (orderBy.isEmpty() && limit.isEmpty() && offset.isEmpty()) {
            return plan;
        }
        if (plan instanceof LogicalPlan.Select s) {
            return new LogicalPlan.Select(s.selectItems(), s.fromRelations(), s.joins(), s.where(),
                    s.groupBy(), s.having(), s.scalarSubqueries(), orderBy, limit, offset, s.distinct());
        }
        if (plan instanceof LogicalPlan.SetOperation so) {
            return new LogicalPlan.SetOperation(so.opType(), so.distinct(), so.left(), so.right(),
                    orderBy, limit, offset);
        }
        // 其余形态（理论不出现：ORDER BY 只挂在 queryNoWith 而非 query）原样返回
        return plan;
    }

    /** 单个 ORDER BY 排序项 → SortItem。只取原文文本，不做列校验（可能引用别名/序号）。 */
    private LogicalPlan.SortItem captureSortItem(SqlBaseParser.SortItemContext si) {
        boolean ascending = si.ordering == null
                || si.ordering.getText().equalsIgnoreCase("ASC");   // 缺省 ASC
        Optional<Boolean> nullsFirst = si.nullOrdering == null
                ? Optional.empty()
                : Optional.of(si.nullOrdering.getText().equalsIgnoreCase("FIRST"));
        return new LogicalPlan.SortItem(
                new Expression.OpaqueExpr(originalText(si.expression())), ascending, nullsFirst);
    }

    /** LIMIT / OFFSET 整数字面量 → 行数；缺失 / LIMIT ALL → empty。 */
    private static Optional<Long> parseLimitOffset(Token t) {
        if (t == null) return Optional.empty();
        String text = t.getText();
        if (text.equalsIgnoreCase("ALL")) return Optional.empty();
        try {
            return Optional.of(Long.parseLong(text));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    @Override public LogicalPlan visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx) {
        return visit(ctx.queryPrimary());
    }

    @Override
    public LogicalPlan visitSetOperation(SqlBaseParser.SetOperationContext ctx) {
        LogicalPlan left = visit(ctx.left);
        LogicalPlan right = visit(ctx.right);
        LogicalPlan.SetOpType opType = ctx.operator.getText().equalsIgnoreCase("UNION")
            ? LogicalPlan.SetOpType.UNION : LogicalPlan.SetOpType.EXCEPT;
        boolean distinct = ctx.setQuantifier() == null
            || !ctx.setQuantifier().getText().equalsIgnoreCase("ALL");
        return new LogicalPlan.SetOperation(opType, distinct, left, right);
    }

    @Override
    public LogicalPlan visitIntersect(SqlBaseParser.IntersectContext ctx) {
        LogicalPlan left = visit(ctx.left);
        LogicalPlan right = visit(ctx.right);
        boolean distinct = ctx.setQuantifier() == null
            || !ctx.setQuantifier().getText().equalsIgnoreCase("ALL");
        return new LogicalPlan.SetOperation(LogicalPlan.SetOpType.INTERSECT, distinct, left, right);
    }

    @Override public LogicalPlan visitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx) {
        return visit(ctx.querySpecification());
    }

    @Override
    public LogicalPlan visitSubquery(SqlBaseParser.SubqueryContext ctx) {
        scopeStack.push(new Scope());
        try {
            return visit(ctx.query());
        } finally {
            scopeStack.pop();
        }
    }

    // ==================== QuerySpecification（核心 SELECT 处理）====================

    @Override
    public LogicalPlan visitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx) {
        Scope currentScope = new Scope();
        scopeStack.push(currentScope);
        subqueryStack.push(new ArrayList<>());

        // ★SELECT 别名预扫描：窗口定义（OVER 的 PARTITION BY / ORDER BY）在聚合之后求值，
        //   允许引用同层<b>聚合/分组别名</b>（`count(*) AS pv … OVER (… ORDER BY pv DESC)`）。
        //   别名不是任何表的列，列校验必须先放行；未分组列的别名不放行（仍是非法引用）。
        //   必须在使用前登记（SELECT 项解析期间窗口列引用就会被校验），解析完恢复外层状态。
        Map<String, String> aliasExprs = new LinkedHashMap<>();
        for (SqlBaseParser.SelectItemContext itemCtx : ctx.selectItem()) {
            if (itemCtx instanceof SqlBaseParser.SelectSingleContext single && single.identifier() != null) {
                aliasExprs.put(single.identifier().getText().toLowerCase(Locale.ROOT),
                        originalText(single.expression()));
            }
        }
        Set<String> legalWindowAliases = new LinkedHashSet<>();
        for (Map.Entry<String, String> e : aliasExprs.entrySet()) {
            if (isAggregateExpression(e.getValue())) {
                legalWindowAliases.add(e.getKey());
                continue;
            }
            for (SqlBaseParser.GroupByContext gbCtx : ctx.groupBy()) {
                if (stripQualifierLower(originalText(gbCtx))
                        .equals(stripQualifierLower(e.getValue()))) {
                    legalWindowAliases.add(e.getKey());
                    break;
                }
            }
        }
        Map<String, String> savedAliasExprs = selectAliasExprs;
        Set<String> savedWindowAliases = windowAliases;
        selectAliasExprs = aliasExprs;
        windowAliases = legalWindowAliases;

        try {
            List<LogicalPlan.ResolvedRelation> fromRelations = new ArrayList<>();
            List<LogicalPlan.JoinInfo> joins = new ArrayList<>();
            if (ctx.FROM() != null) {
                for (SqlBaseParser.RelationContext relCtx : ctx.relation()) {
                    visitRelationIntoScope(relCtx, fromRelations, joins);
                }
            }

            Optional<Expression> whereExpr = Optional.empty();
            if (ctx.where != null) {
                whereExpr = captureOptionalExpression(ctx.where);
            }

            List<LogicalPlan.SelectItem> selectItems = new ArrayList<>();
            for (SqlBaseParser.SelectItemContext itemCtx : ctx.selectItem()) {
                selectItems.add(processSelectItem(itemCtx));
            }

            List<Expression> groupByExprs = new ArrayList<>();
            if (ctx.groupBy() != null) {
                for (SqlBaseParser.GroupByContext gbCtx : ctx.groupBy()) {
                    Expression expr = captureGroupByElement(gbCtx);
                    if (expr != null) groupByExprs.add(expr);
                }
            }
            // 裸多维分组子句（CUBE(...) / ROLLUP(...) / GROUPING SETS(...)，语法层面无 GROUP BY）
            if (ctx.multidimGroup() != null) {
                if (!groupByExprs.isEmpty()) {
                    error(ctx.multidimGroup(),
                        "GROUP BY 与裸 CUBE/ROLLUP/GROUPING SETS 不能同时使用: "
                            + originalText(ctx.multidimGroup()));
                } else {
                    Expression gs = captureMultidimGroup(ctx.multidimGroup());
                    if (gs != null) groupByExprs.add(gs);
                }
            }

            // GROUP BY 校验：非聚合列必须出现在 GROUP BY（GroupingSets 视全部成员列为匹配集）
            if (!groupByExprs.isEmpty()) {
                validateGroupBy(ctx, selectItems, groupByExprs);
            }

            Optional<Expression> havingExpr = Optional.empty();
            if (ctx.having != null) {
                // HAVING 允许引用 SELECT 别名（如 `HAVING cnt > 3`）：别名不是表列，需临时放行。
                Set<String> aliases = new HashSet<>();
                for (SqlBaseParser.SelectItemContext itemCtx : ctx.selectItem()) {
                    if (itemCtx instanceof SqlBaseParser.SelectSingleContext single
                            && single.identifier() != null) {
                        aliases.add(single.identifier().getText().toLowerCase(Locale.ROOT));
                    }
                }
                Set<String> saved = havingAliases;
                havingAliases = aliases;
                try {
                    havingExpr = captureOptionalExpression(ctx.having);
                } finally {
                    havingAliases = saved;
                }
            }

            // 本查询内收集到的标量子查询（SELECT 项 / WHERE / HAVING 中），随 Select 带出供 Planner 去相关
            List<Expression.ScalarSubquery> subqueries = List.copyOf(subqueryStack.peek());
            // SELECT DISTINCT：这个 setQuantifier 以前从不读 → DISTINCT 被整条静默忽略（不去重、不报错）。
            // 这里只把它带进逻辑计划，真正的去重由 Planner 在「投影之后」做（键 = 输出列）。
            boolean distinct = ctx.setQuantifier() != null
                    && "DISTINCT".equalsIgnoreCase(ctx.setQuantifier().getText());
            return new LogicalPlan.Select(selectItems, fromRelations, joins,
                whereExpr, groupByExprs, havingExpr, subqueries, List.of(), Optional.empty(),
                Optional.empty(), distinct);
        } finally {
            selectAliasExprs = savedAliasExprs;
            windowAliases = savedWindowAliases;
            subqueryStack.pop();
            scopeStack.pop();
        }
    }

    private LogicalPlan.SelectItem processSelectItem(SqlBaseParser.SelectItemContext ctx) {
        if (ctx instanceof SqlBaseParser.SelectSingleContext single) {
            List<PendingWindowCall> saved = List.copyOf(pendingWindowCalls);
            List<WindowExprRewrite> savedRewrites = List.copyOf(pendingWindowExprRewrites);
            boolean savedWindowAllowed = windowAllowedInExpression;
            pendingWindowCalls.clear();
            pendingWindowExprRewrites.clear();
            int mark = pendingResolvedColumns.size();
            try {
                windowAllowedInExpression = true;
                resolveExpressionColumns(single.expression());
                List<Expression.ColumnRef> resolved = drainFrom(mark);
                List<PendingWindowCall> calls = List.copyOf(pendingWindowCalls);
                List<WindowExprRewrite> rewrites = List.copyOf(pendingWindowExprRewrites);
                Expression expr;
                if (rewrites.isEmpty() && calls.size() == 1
                        && isWindowCallRoot(single.expression(), calls.get(0).ctx())) {
                    // 整项就是一个窗口调用：保持既有形态（Planner 的根窗口路径）。
                    expr = calls.get(0).window().withResolvedColumns(resolved);
                } else {
                    // 窗口嵌在表达式里：保留完整原文，窗口调用按顺序附在 SelectItem 上，
                    // 否则 `+ - * /` 与外层运算会被静默丢弃。
                    // 包装形态（`round(sum(x),2) OVER (…)`）在这里先回写成规范文本。
                    String text = applyWindowExprRewrites(single.expression(), rewrites);
                    expr = new Expression.OpaqueExpr(text, resolved);
                    Optional<String> alias = single.identifier() != null
                        ? Optional.of(single.identifier().getText()) : Optional.empty();
                    List<LogicalPlan.WindowCall> windowCalls = new ArrayList<>(calls.size());
                    for (PendingWindowCall c : calls) {
                        windowCalls.add(new LogicalPlan.WindowCall(
                            c.text() != null ? c.text() : originalText(c.ctx()), c.window()));
                    }
                    return new LogicalPlan.SelectItem(expr, alias, windowCalls);
                }
                Optional<String> alias = single.identifier() != null
                    ? Optional.of(single.identifier().getText()) : Optional.empty();
                return new LogicalPlan.SelectItem(expr, alias);
            } finally {
                pendingWindowCalls.clear();
                pendingWindowCalls.addAll(saved);
                pendingWindowExprRewrites.clear();
                pendingWindowExprRewrites.addAll(savedRewrites);
                windowAllowedInExpression = savedWindowAllowed;
            }
        } else if (ctx instanceof SqlBaseParser.SelectAllContext all) {
            return new LogicalPlan.SelectItem(
                new Expression.OpaqueExpr(originalText(all)), Optional.empty());
        }
        return new LogicalPlan.SelectItem(
            new Expression.OpaqueExpr(originalText(ctx)), Optional.empty());
    }

    /**
     * 把 SELECT 项原文里的包装形态片段替换成规范化文本。
     *
     * <p>按下标从右往左替换，避免前面的替换让后面的区间位移；区间取 {@code ctx} 在原始输入里的
     * 绝对字符下标，再减去 SELECT 项表达式的起点换算成相对位置。</p>
     */
    private String applyWindowExprRewrites(SqlBaseParser.ExpressionContext expr,
                                           List<WindowExprRewrite> rewrites) {
        String text = originalText(expr);
        if (rewrites.isEmpty()) return text;
        int base = expr.getStart().getStartIndex();
        List<WindowExprRewrite> sorted = new ArrayList<>(rewrites);
        sorted.sort((a, b) -> Integer.compare(
            b.ctx().getStart().getStartIndex(), a.ctx().getStart().getStartIndex()));
        StringBuilder sb = new StringBuilder(text);
        for (WindowExprRewrite rw : sorted) {
            int s = rw.ctx().getStart().getStartIndex() - base;
            int e = rw.ctx().getStop().getStopIndex() - base + 1;
            if (s < 0 || e > sb.length() || s >= e) continue;
            sb.replace(s, e, rw.replacement());
        }
        return sb.toString();
    }

    /**
     * 判定 {@code target} 是否就是整棵表达式树（沿「只有一个子节点」的包装节点向下走到它）。
     *
     * <p>{@code sum(x) OVER ()} 的语法树是一条单链（expression → … → functionCall），而
     * {@code gmv / sum(x) OVER ()} 在分叉处即停下，{@code (sum(x) OVER ())} 也因括号节点有
     * 三个孩子而停下 —— 后者走「嵌套窗口」路径同样能得到正确结果，只是多一次合成列。</p>
     */
    private static boolean isWindowCallRoot(ParserRuleContext root, ParserRuleContext target) {
        ParserRuleContext node = root;
        while (true) {
            if (node == target) return true;
            if (node.getChildCount() != 1) return false;
            ParseTree child = node.getChild(0);
            if (!(child instanceof ParserRuleContext next)) return false;
            node = next;
        }
    }

    // ==================== Relation（FROM / JOIN）====================

    /** 返回最右（最底层）关系别名，用于连接信息收集。 */
    private String visitRelationIntoScope(SqlBaseParser.RelationContext ctx,
                                           List<LogicalPlan.ResolvedRelation> fromRelations,
                                           List<LogicalPlan.JoinInfo> joins) {
        if (ctx instanceof SqlBaseParser.JoinRelationContext join) {
            if (containsUnnestRelation(join.left)) {
                error(join.left, "UNNEST 暂不支持作为 JOIN 左表；请改写成 FROM 左表, UNNEST(表达式): "
                    + originalText(join.left));
                return null;
            }
            boolean rightUnnest = isUnnestRelation(join.right);
            LogicalPlan.JoinType jt = resolveJoinType(join);
            boolean lateralCross = rightUnnest
                && (jt == LogicalPlan.JoinType.CROSS
                    || (jt == LogicalPlan.JoinType.INNER && join.booleanExpression() == null));
            if (rightUnnest && !lateralCross) {
                error(join.right, "UNNEST 只能与左侧关系 CROSS JOIN（或写成逗号分隔）: "
                    + originalText(join));
                return null;
            }
            String leftAlias = visitRelationIntoScope(join.left, fromRelations, joins);
            String rightAlias = visitSampledRelation(join.right, fromRelations);
            // 横向展开无连接条件：不登记 JoinInfo，按「逗号连接」顺序交给 Planner 逐个应用
            if (lateralCross) {
                return rightAlias;
            }
            Expression cond = join.booleanExpression() != null
                ? captureExpression(join.booleanExpression())
                : new Expression.OpaqueExpr("");
            joins.add(new LogicalPlan.JoinInfo(jt,
                leftAlias != null ? leftAlias : "",
                rightAlias != null ? rightAlias : "",
                cond));
            return rightAlias;
        } else if (ctx instanceof SqlBaseParser.RelationDefaultContext def) {
            return visitSampledRelation(def.sampledRelation(), fromRelations);
        } else if (ctx instanceof SqlBaseParser.SubqueryRelationContext sub) {
            return visitSubqueryRelationIntoScope(sub, fromRelations);
        }
        return null;
    }

    /** 该 FROM 项的直接形态是否是 {@code UNNEST(...)}。 */
    private static boolean isUnnestRelation(SqlBaseParser.SampledRelationContext sampled) {
        return sampled != null
            && sampled.aliasedRelation() != null
            && sampled.aliasedRelation().relationPrimary() instanceof SqlBaseParser.UnnestContext;
    }

    /** 关系子树里是否含 UNNEST（用于拒绝「UNNEST 作为 JOIN 左表」）。 */
    private static boolean containsUnnestRelation(SqlBaseParser.RelationContext rel) {
        if (rel instanceof SqlBaseParser.JoinRelationContext join) {
            return containsUnnestRelation(join.left) || isUnnestRelation(join.right);
        }
        if (rel instanceof SqlBaseParser.RelationDefaultContext def) {
            return isUnnestRelation(def.sampledRelation());
        }
        return false;
    }

    private LogicalPlan.JoinType resolveJoinType(SqlBaseParser.JoinRelationContext join) {
        // 语法层接受 SEMI JOIN，但执行层没有半连接语义：按 INNER/LEFT 执行会放大行数、
        // SELECT * 还会多出右表列（静默错结果）。本版本显式拒绝，引导改写为 EXISTS / IN。
        if (join.SEMI() != null) {
            error(join, "当前版本不支持 SEMI JOIN（LEFT/RIGHT SEMI JOIN）；"
                + "请改用 EXISTS 或 IN 子查询代替，例如："
                + "WHERE EXISTS (SELECT 1 FROM 右表 WHERE 连接条件)，"
                + "或 WHERE 左键 IN (SELECT 右键 FROM 右表)。");
        }
        if (join.CROSS() != null) return LogicalPlan.JoinType.CROSS;
        if (join.LEFT()  != null) return LogicalPlan.JoinType.LEFT;
        if (join.RIGHT() != null) return LogicalPlan.JoinType.RIGHT;
        if (join.FULL()  != null) return LogicalPlan.JoinType.FULL;
        return LogicalPlan.JoinType.INNER; // 默认：INNER JOIN
    }

    private String visitSampledRelation(SqlBaseParser.SampledRelationContext ctx,
                                         List<LogicalPlan.ResolvedRelation> fromRelations) {
        return visitAliasedRelation(ctx.aliasedRelation(), fromRelations);
    }

    @Override
    public LogicalPlan visitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * 防歧义兜底：{@code FROM t CUBE(a, b)}（无 WHERE）可能被解析成"表别名 CUBE + 列别名 (a,b)"，
     * 而不是裸多维分组子句——这类静默错读极难排查。凡是带列别名且别名恰为 CUBE/ROLLUP/GROUPING/SETS
     * 关键字的一律报错，引导用户改写成 {@code WHERE ... CUBE(...)} 或 {@code GROUP BY CUBE(...)}。
     */
    private void guardRelationAliasIsGroupingKeyword(ParserRuleContext ctx,
                                                     String alias,
                                                     boolean hasColumnAliases) {
        if (hasColumnAliases && alias != null
                && ("cube".equals(alias.toLowerCase())
                    || "rollup".equals(alias.toLowerCase())
                    || "grouping".equals(alias.toLowerCase())
                    || "sets".equals(alias.toLowerCase()))) {
            error(ctx, "关系别名不能是 CUBE/ROLLUP/GROUPING SETS 关键字（紧跟 FROM 的裸 CUBE(...) 写法歧义；"
                + "请写成 WHERE ... CUBE(...) 或 GROUP BY CUBE(...)）: " + originalText(ctx));
        }
    }

    private String visitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx,
                                         List<LogicalPlan.ResolvedRelation> fromRelations) {
        guardRelationAliasIsGroupingKeyword(ctx,
            ctx.identifier() != null ? ctx.identifier().getText() : null,
            ctx.columnAliases() != null);
        // 表函数（FROM TABLE(fn(常量))）直接在此构造 DTO：alias / columnAliases 在这里可得
        if (ctx.relationPrimary() instanceof SqlBaseParser.TableFunctionContext tf) {
            return visitTableFunctionRelation(tf, ctx, fromRelations);
        }
        // UNNEST（FROM t, UNNEST(json_array) [WITH ORDINALITY] AS u(item[, ord])）
        if (ctx.relationPrimary() instanceof SqlBaseParser.UnnestContext un) {
            return visitUnnestRelation(un, ctx, fromRelations);
        }
        Scope.RelationInfo info = resolveRelationPrimary(ctx.relationPrimary());
        if (info == null) return null;

        String effectiveAlias = ctx.identifier() != null
            ? ctx.identifier().getText() : info.tableName();

        Scope.RelationInfo aliasedInfo = new Scope.RelationInfo(
            info.catalog(), info.schema(), info.tableName(),
            effectiveAlias, info.columns(), info.subquery());

        scope().addRelation(aliasedInfo);
        fromRelations.add(new LogicalPlan.ResolvedRelation(
            info.catalog(), info.schema(), info.tableName(),
            Optional.of(effectiveAlias), info.columns(),
            Optional.empty(), info.subquery()));
        return effectiveAlias;
    }

    /**
     * 表函数关系：{@code FROM TABLE(fn('a', 1)) [AS t(x, y)]}。
     * <p>表生成器语义（独立切片，无 LATERAL / 列引用）：构造 {@link LogicalPlan.TableFunction} DTO
     * 承载在 {@code ResolvedRelation.subquery} 槽位，Planner buildScan 识别后构建叶子算子。</p>
     */
    private String visitTableFunctionRelation(SqlBaseParser.TableFunctionContext tf,
                                               SqlBaseParser.AliasedRelationContext ctx,
                                               List<LogicalPlan.ResolvedRelation> fromRelations) {
        List<String> name = extractQualifiedName(tf.qualifiedName());
        List<String> args = tf.functionArgument().stream()
            .map(a -> a.expression().getText())
            .toList();
        String lastName = name.get(name.size() - 1);
        String alias = ctx.identifier() != null ? ctx.identifier().getText() : lastName;
        List<String> columnAliases = ctx.columnAliases() != null
            ? ctx.columnAliases().identifier().stream().map(i -> i.getText()).toList()
            : List.of();
        LogicalPlan.TableFunction tfPlan =
            new LogicalPlan.TableFunction(name, args, Optional.of(alias), columnAliases);

        scope().addRelation(new Scope.RelationInfo("", "", lastName, alias, columnAliases, Optional.of(tfPlan)));
        fromRelations.add(new LogicalPlan.ResolvedRelation(
            "", "", lastName, Optional.of(alias), columnAliases,
            Optional.empty(), Optional.of(tfPlan)));
        return alias;
    }

    /**
     * UNNEST 关系：{@code FROM t, UNNEST(json_array_expr) [WITH ORDINALITY] [AS u(item[, ord])]}。
     *
     * <p>横向语义（隐式 LATERAL）：表达式可以引用<b>前面已出现</b>的关系列（FROM 关系按顺序登记进
     * 作用域，故此处解析列引用天然可用）。只支持一个表达式；多个表达式在旧版 Hive/Trino 里语义是
     * 多列并排展开，本引擎尚未实现，明确报错而不是静默忽略。</p>
     *
     * <p>输出列：元素列（缺省 {@code unnest}，可由第一个列别名改名）；{@code WITH ORDINALITY}
     * 追加 BIGINT 序号列（1 起，缺省名 {@code ordinality}，可由第二个列别名改名）。</p>
     */
    private String visitUnnestRelation(SqlBaseParser.UnnestContext un,
                                       SqlBaseParser.AliasedRelationContext ctx,
                                       List<LogicalPlan.ResolvedRelation> fromRelations) {
        if (un.expression().size() != 1) {
            error(un, "UNNEST 目前只支持一个表达式: " + originalText(un));
            return null;
        }
        boolean withOrdinality = un.WITH() != null;
        String alias = ctx.identifier() != null ? ctx.identifier().getText() : "unnest";
        List<String> columnAliases = ctx.columnAliases() != null
            ? ctx.columnAliases().identifier().stream().map(i -> i.getText()).toList()
            : List.of();
        int maxColumns = withOrdinality ? 2 : 1;
        if (columnAliases.size() > maxColumns) {
            error(ctx, "UNNEST" + (withOrdinality ? " ... WITH ORDINALITY" : "")
                + " 最多 " + maxColumns + " 个列别名（元素列"
                + (withOrdinality ? "、序号列" : "") + "）: " + originalText(ctx));
            return null;
        }
        String elementColumn = columnAliases.size() >= 1 ? columnAliases.get(0) : "unnest";
        String ordinalityColumn = withOrdinality
            ? (columnAliases.size() >= 2 ? columnAliases.get(1) : "ordinality")
            : null;

        Expression expression = captureExpression(un.expression(0));
        List<String> columns = new ArrayList<>(maxColumns);
        columns.add(elementColumn);
        if (withOrdinality) columns.add(ordinalityColumn);

        LogicalPlan.Unnest spec = new LogicalPlan.Unnest(
            expression, Optional.of(alias), elementColumn,
            Optional.ofNullable(ordinalityColumn));

        scope().addRelation(new Scope.RelationInfo("", "", "unnest", alias, columns,
            Optional.of(spec)));
        fromRelations.add(new LogicalPlan.ResolvedRelation(
            "", "", "unnest", Optional.of(alias), columns,
            Optional.empty(), Optional.of(spec)));
        return alias;
    }

    private String visitSubqueryRelationIntoScope(SqlBaseParser.SubqueryRelationContext ctx,
                                                   List<LogicalPlan.ResolvedRelation> fromRelations) {
        guardRelationAliasIsGroupingKeyword(ctx,
            ctx.identifier() != null ? ctx.identifier().getText() : null,
            ctx.columnAliases() != null);
        LogicalPlan innerPlan;
        scopeStack.push(new Scope());
        try {
            innerPlan = visit(ctx.query());
        } finally {
            scopeStack.pop();
        }

        String alias = ctx.identifier() != null
            ? ctx.identifier().getText() : "subquery";
        // 修复：原来硬编码 List.of()，派生表的任何列都无法被外层解析（如 where rr<=10）。
        // 现在从子查询的输出列推导真实列名，与 CTE 的列推导保持一致。
        List<String> columns = deriveDerivedTableColumns(innerPlan);
        Scope.RelationInfo info = new Scope.RelationInfo(
            "", "", alias, alias, columns, Optional.of(innerPlan));
        scope().addRelation(info);
        fromRelations.add(new LogicalPlan.ResolvedRelation(
            "", "", alias, Optional.of(alias), columns,
            Optional.empty(), Optional.of(innerPlan)));
        return alias;
    }

    /**
     * 推导派生表（FROM 子查询）的输出列名。
     * <ul>
     *   <li>有别名 → 别名</li>
     *   <li>select *（含限定星号 a.*）→ 按 FROM 关系展开真实列名</li>
     *   <li>其余 → 表达式原始文本</li>
     * </ul>
     */
    private List<String> deriveDerivedTableColumns(LogicalPlan innerPlan) {
        if (innerPlan instanceof LogicalPlan.Select select) {
            List<String> cols = new ArrayList<>();
            for (LogicalPlan.SelectItem item : select.selectItems()) {
                if (item.alias().isPresent()) {
                    cols.add(item.alias().get());
                } else {
                    String text = item.expression().text();
                    if (text != null && text.trim().endsWith("*")) {
                        String qualifier = text.contains(".")
                            ? text.substring(0, text.lastIndexOf('.')).trim()
                            : null;
                        for (LogicalPlan.ResolvedRelation rel : select.fromRelations()) {
                            String relAlias = rel.alias().orElse(rel.table());
                            if (qualifier == null
                                || relAlias.equalsIgnoreCase(qualifier)
                                || rel.table().equalsIgnoreCase(qualifier)) {
                                cols.addAll(rel.columns());
                            }
                        }
                    } else {
                        // 去掉表别名限定符: "c.product_name" → "product_name"
                        String colName = text.contains(".")
                                ? text.substring(text.lastIndexOf('.') + 1)
                                : text;
                        cols.add(colName);
                    }
                }
            }
            return cols;
        }
        // 集合运算（UNION / UNION ALL）：结果列名取首个操作数（标准 SQL：结果集列名来自第一个查询）。
        // 此前返回空列表 ⇒ 派生表 / CTE 一个列都没有，外层引用列报
        // 「Column 'a' is not found in any table in scope」。
        if (innerPlan instanceof LogicalPlan.SetOperation so) {
            return deriveDerivedTableColumns(so.left());
        }
        // CTE 体内自带 WITH（嵌套 CTE）：列名取自最内层查询体，而不是 WITH 包装层。
        // 旧实现落到 return List.of() ⇒ 外层引用该 CTE 任何列都报「not found in table (columns: [])」。
        if (innerPlan instanceof LogicalPlan.With w) {
            return deriveDerivedTableColumns(w.query());
        }
        return List.of();
    }

    @Override
    public LogicalPlan visitTableNameRef(SqlBaseParser.TableNameRefContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public LogicalPlan visitQualifiedNameRef(SqlBaseParser.QualifiedNameRefContext ctx) {
        return visitChildren(ctx);
    }

    private Scope.RelationInfo resolveRelationPrimary(SqlBaseParser.RelationPrimaryContext ctx) {
        List<String> parts;

        switch (ctx) {
            case SqlBaseParser.TableNameRefContext tableRef -> {
                parts = extractTableName(tableRef.tableName());
            }
            case SqlBaseParser.QualifiedNameRefContext qnameRef -> {
                parts = extractQualifiedName(qnameRef.qualifiedName());
            }
            case SqlBaseParser.SubqueryRelationPrimaryContext subquery -> {
                LogicalPlan innerPlan;
                scopeStack.push(new Scope());
                try { innerPlan = visit(subquery.query()); } finally { scopeStack.pop(); }
                return new Scope.RelationInfo("", "", "subquery", "subquery",
                    deriveDerivedTableColumns(innerPlan), Optional.of(innerPlan));
            }
            case SqlBaseParser.UnnestContext ignored -> {
                return new Scope.RelationInfo("", "", "unnest", "unnest", List.of());
            }
            case SqlBaseParser.TableFunctionContext ignored -> {
                return new Scope.RelationInfo("", "", "table_function", "table_function", List.of());
            }
            case null, default -> { return null; }
        }

        // 1) CTE 优先 —— 单段表名先查 CTE 定义
        if (parts.size() == 1 && cteDefinitions.containsKey(parts.get(0))) {
            String cteName = parts.get(0);
            List<String> columns = cteDefinitions.get(cteName);
            return new Scope.RelationInfo("", "", cteName, cteName, columns);
        }

        // 2) 实体表 —— 通过 MetaStore 逐级校验
        TableResolver resolver = new TableResolver(metaStore, currentCatalog, currentSchema);
        TableResolver.ResolveResult result = resolver.resolve(parts);
        errors.addAll(result.errors());

        if (result.resolved().isPresent()) {
            TableResolver.ResolvedName resolved = result.resolved().get();
            return new Scope.RelationInfo(
                resolved.catalog(), resolved.schema(), resolved.table(),
                resolved.table(), resolved.columns());
        }

        return null;
    }

    // ==================== 列引用 ====================

    @Override
    public LogicalPlan visitColumnReference(SqlBaseParser.ColumnReferenceContext ctx) {
        // 不在查询作用域中（例如 DDL 语句中），跳过
        if (scope() == null) return null;
        // 表解析失败，作用域为空 → 静默跳过（表级错误已累积）
        if (scope().isEmpty()) return null;

        List<String> parts = ctx.identifier().stream()
            .map(ParseTree::getText).toList();

        Scope.ResolveResult result = scope().resolveColumn(parts);
        // 标量子查询内层：外层相关列（如 WHERE t2.id = t1.id 的 t1.id）在内层作用域中解析不到，
        // 属正常相关引用 —— 宽容不报错，由 Planner 去相关时做相关性检测（相关列名/外层列名校验）。
        if (scalarSubqueryDepth == 0) {
            // HAVING 引用 SELECT 别名：别名不在任何表/派生关系里，解析必然失败 —— 只要名字命中
            // 当前查询的 SELECT 别名就放行（Planner 侧把别名替换为其定义表达式后求值）。
            if (parts.size() == 1
                    && havingAliases.contains(parts.get(0).toLowerCase(Locale.ROOT))) {
                return null;
            }
            // 窗口定义里引用同层聚合/分组别名（ORDER BY pv）：别名不在表作用域内，
            // 但它是聚合后的输出列，物理层能按名解析 —— 校验期放行。
            if (parts.size() == 1 && windowSpecDepth > 0
                    && windowAliases.contains(parts.get(0).toLowerCase(Locale.ROOT))) {
                return null;
            }
            errors.addAll(result.errors());
            // 持久化权威绑定：解析成功（非歧义/列存在）→ 累积给所属表达式。
            // 歧义/不存在时 columnRef() 为空 ⇒ 不落绑定 ⇒ 列裁剪/下推回退保守兜底。
            result.columnRef().ifPresent(pendingResolvedColumns::add);
        }
        return null;
    }

    // ==================== 标量子查询（SELECT 列表 / WHERE 中的 (SELECT ...)） ====================

    /**
     * 表达式位置子查询：{@code (SELECT ...)} 出现在 SELECT 项 / WHERE / HAVING 中时进入此规则
     * （FROM 派生表走 {@code subqueryRelation}，不经过这里）。分析内层 {@code ctx.query()} 得
     * {@link LogicalPlan}，包成 {@link Expression.ScalarSubquery} 追加到当前查询的收集栈。
     *
     * <p>相关列不在此处解析（Dojao 谓词是 opaque 文本）：分析深度 {@link #scalarSubqueryDepth}
     * 递增期间，内层未解析的列引用（外层相关列）不累积错误，交给 Planner 去相关时做相关性检测。</p>
     */
    @Override
    public LogicalPlan visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx) {
        LogicalPlan inner;
        scalarSubqueryDepth++;
        scopeStack.push(new Scope());
        try {
            inner = visit(ctx.query());
        } finally {
            scopeStack.pop();
            scalarSubqueryDepth--;
        }
        Expression.ScalarSubquery sub = new Expression.ScalarSubquery(inner, originalText(ctx));
        List<Expression.ScalarSubquery> top = subqueryStack.peek();
        if (top != null) top.add(sub);
        return null;   // 不继续 visitChildren（内层 query 已单独分析）
    }

    // ==================== EXISTS / NOT EXISTS 子查询 ====================

    /**
     * {@code [NOT] EXISTS (query)}：与标量子查询同样分析内层（递增 {@link #scalarSubqueryDepth}
     * 容忍外层相关列），但把内层改写成<b>存在性计数</b> {@code SELECT count(*)}：
     * 存在性 ⇔ count > 0，Planner 去相关时对派生列做 {@code > 0} / {@code = 0} 判定。
     *
     * <p>此前没有本规则的 visitor，默认访问器直接把内层 query 当独立查询分析（深度 0、新作用域）
     * ⇒ 相关形式 {@code WHERE r.k = k.k} 报「Table or alias 'k' is not found in scope」。</p>
     *
     * <p>内层限定：须为单条 SELECT；不支持 HAVING（会过滤掉分组，存在性不再等价于「有输入行」）、
     * LIMIT/OFFSET（LIMIT 0 语义会变）。GROUP BY / DISTINCT / ORDER BY 对存在性无影响，直接丢弃。
     * JOIN / 派生 FROM / 嵌套子查询等由其后的 Planner 去相关逻辑给出明确的不支持错误。</p>
     */
    @Override
    public LogicalPlan visitExists(SqlBaseParser.ExistsContext ctx) {
        // NOT EXISTS 在语法上是布尔层的 NOT(booleanExpression) 包住 EXISTS 谓词
        // （logicalNot 先于 predicate 匹配，故 ExistsContext.NOT() 实际恒空）。
        //
        // ★必须沿父链穿过「语法透明」的包装节点去找 NOT —— `NOT (EXISTS (...))`（报告 BUG-009）
        // 在 Exists 与 LogicalNot 之间夹着 BooleanPredicate / expression / parenthesizedExpression /
        // valueExpression / BooleanDefault 一整串；旧实现只比对「直接父 = BooleanPredicate 且
        // 祖父 = LogicalNot」，括号形式一律判成「非否定」⇒ NOT 被静默丢弃、结果等于 EXISTS 本身。
        // 遇到 AND/OR（LogicalBinary）立即停止：`NOT (EXISTS(...) AND p)` 的 NOT 属于整个合取式，
        // 不能算到 EXISTS 头上（否则语义反转，见报告 BUG-010）。双层 NOT 通过沿途取反抵消。
        boolean negated = negatedBySyntax(ctx, ctx.NOT() != null);
        LogicalPlan inner;
        // 内层解析到的列绑定是内层表的，不属于外层表达式 —— 分析完丢弃，避免污染外层
        // OpaqueExpr.resolvedColumns（列裁剪/谓词下推会把它们当成外层引用）。
        int mark = pendingResolvedColumns.size();
        scalarSubqueryDepth++;
        scopeStack.push(new Scope());
        try {
            inner = visit(ctx.query());
        } finally {
            scopeStack.pop();
            scalarSubqueryDepth--;
            pendingResolvedColumns.subList(mark, pendingResolvedColumns.size()).clear();
        }
        if (!(inner instanceof LogicalPlan.Select s)) {
            error(ctx, "EXISTS 内层暂不支持该查询形态（须为单条 SELECT）: " + originalText(ctx));
            return null;
        }
        if (s.having().isPresent() || s.limit().isPresent() || s.offset().isPresent()) {
            // ★给出可操作的替代写法（缺陷报告 BUG-012c）：`EXISTS (... GROUP BY ... HAVING ...)`
            // 是「按维表聚合条件过滤事实表」的标准写法，只说「不支持」等于让用户放弃需求。
            // 单层 GROUP BY 本身是支持的（存在性与分组无关，下面直接丢弃），文案里不再提。
            error(ctx, "EXISTS 内层暂不支持 HAVING / LIMIT / OFFSET，可改写为："
                    + "JOIN (SELECT <键> FROM <表> GROUP BY <键> [HAVING ...]) v"
                    + " ON <外层键> = v.<键>: " + originalText(ctx));
            return null;
        }
        // SELECT count(*) 覆盖内层输入行数；保留 from/joins/where 与已收集的标量子查询
        // （后者会让 Planner 的 buildDerivedJoin 明确拒绝，而不是静默丢语义）。
        LogicalPlan counting = new LogicalPlan.Select(
            List.of(new LogicalPlan.SelectItem(
                new Expression.OpaqueExpr("count(*)"), Optional.empty())),
            s.fromRelations(), s.joins(), s.where(), List.of(), Optional.empty(),
            s.scalarSubqueries());
        Expression.ScalarSubquery sub =
            new Expression.ScalarSubquery(counting, originalText(ctx), true, negated);
        List<Expression.ScalarSubquery> top = subqueryStack.peek();
        if (top != null) top.add(sub);
        return null;   // 不继续 visitChildren（内层 query 已单独分析）
    }

    /**
     * 谓词是否被语法层 {@code NOT} 否定 —— 沿父链穿过括号/布尔包装节点找 {@link SqlBaseParser.LogicalNotContext}，
     * 遇到 {@code AND/OR} 立即停止（{@code NOT (p AND q)} 的 NOT 属于整个合取式，不能算到单个谓词头上）。
     * 双层 NOT 沿途取反抵消。
     */
    private static boolean negatedBySyntax(ParseTree node, boolean ownNot) {
        boolean negated = ownNot;
        for (ParseTree p = node.getParent(); p != null; p = p.getParent()) {
            if (p instanceof SqlBaseParser.LogicalNotContext) {
                negated = !negated;
                continue;
            }
            if (p instanceof SqlBaseParser.BooleanPredicateContext
                    || p instanceof SqlBaseParser.BooleanDefaultContext
                    || p instanceof SqlBaseParser.ValueExpressionDefaultContext
                    || p instanceof SqlBaseParser.ExpressionContext
                    || p instanceof SqlBaseParser.ParenthesizedExpressionContext) {
                continue;   // 语法包装，不改变 NOT 的作用域
            }
            break;
        }
        return negated;
    }

    /**
     * {@code x [NOT] IN (query)}：与 EXISTS 同路 —— 内层分析后改写为「{@code 内层列 = 外层表达式}
     * 的相关计数」{@code SELECT count(*)}，再交给 Planner 去相关成 LEFT JOIN 派生表。
     *
     * <p>此前没有本 visitor，IN 子查询原样留成 OpaqueExpr 文本 ⇒ {@code FilterOperator} 既无法
     * 求值也不能下推，最终落 {@code Form.UNKNOWN} <b>逐行放行</b>：跨源 {@code IN (SELECT ...)}
     * 会静默返回全量（缺陷报告 BUG-007 / BUG-013 的「返回分区全量」特征）。</p>
     *
     * <p>{@code NOT IN (子查询)} 显式拒绝：SQL 三值逻辑下「子查询含 NULL ⇒ 恒为 unknown」，
     * 改写成反连接（NOT EXISTS）在子查询有 NULL 时会静默算成「保留该行」。给出可操作的
     * LEFT JOIN 改写模板，绝不静默。{@code IN} 正向形态（仅保留 true 的行）与
     * {@code EXISTS(... WHERE 内层列 = 外层表达式)} 等价，NULL 语义一致。</p>
     */
    @Override
    public LogicalPlan visitInSubquery(SqlBaseParser.InSubqueryContext ctx) {
        ParserRuleContext owner = ctx.getParent();
        if (!(owner instanceof SqlBaseParser.BooleanDefaultContext bd)) {
            error(ctx, "IN 子查询缺少左侧表达式: " + originalText(ctx));
            return null;
        }
        String leftText = originalText(bd.valueExpression());
        String predicateText = originalText(bd);   // 含左侧表达式：`x IN (SELECT ...)`
        boolean negated = negatedBySyntax(ctx, ctx.NOT() != null);
        if (negated) {
            error(ctx, "NOT IN (子查询) 暂不支持（子查询含 NULL 时为 unknown，改写成反连接会静默算错）。"
                    + "可改写为：LEFT JOIN (SELECT <列> FROM <表> [WHERE ...] GROUP BY <列>) v"
                    + " ON v.<列> = <外层表达式>"
                    + " WHERE v.<列> IS NULL AND <外层表达式> IS NOT NULL: " + predicateText);
            return null;
        }

        LogicalPlan inner;
        int mark = pendingResolvedColumns.size();
        scalarSubqueryDepth++;
        scopeStack.push(new Scope());
        try {
            inner = visit(ctx.query());
        } finally {
            scopeStack.pop();
            scalarSubqueryDepth--;
            pendingResolvedColumns.subList(mark, pendingResolvedColumns.size()).clear();
        }
        if (!(inner instanceof LogicalPlan.Select s)) {
            error(ctx, "IN 子查询内层暂不支持该查询形态（须为单条 SELECT）: " + predicateText);
            return null;
        }
        if (s.having().isPresent() || s.limit().isPresent() || s.offset().isPresent()) {
            error(ctx, "IN 子查询内层暂不支持 HAVING / LIMIT / OFFSET，可改写为："
                    + "JOIN (SELECT <列> FROM <表> GROUP BY <列> [HAVING ...]) v"
                    + " ON <外层表达式> = v.<列>: " + predicateText);
            return null;
        }
        if (s.selectItems().size() != 1) {
            error(ctx, "IN 子查询须返回单列（当前 " + s.selectItems().size() + " 列）: " + predicateText);
            return null;
        }
        // 相关性谓词：内层输出列 = 外层表达式。与既有 WHERE 用 AND 相加（交给 buildDerivedJoin
        // 逐合取项分类：引用外层列的等值 → JOIN 键，其余 → 下推进内层派生表）。
        String innerItem = s.selectItems().get(0).expression().text().trim();
        String correlation = innerItem + " = " + leftText;
        Optional<Expression> where = s.where().isPresent()
                ? Optional.of((Expression) new Expression.OpaqueExpr(
                        s.where().get().text() + " AND " + correlation))
                : Optional.of((Expression) new Expression.OpaqueExpr(correlation));
        LogicalPlan counting = new LogicalPlan.Select(
            List.of(new LogicalPlan.SelectItem(
                new Expression.OpaqueExpr("count(*)"), Optional.empty())),
            s.fromRelations(), s.joins(), where, List.of(), Optional.empty(),
            s.scalarSubqueries());
        Expression.ScalarSubquery sub =
            new Expression.ScalarSubquery(counting, predicateText, true, false);
        List<Expression.ScalarSubquery> top = subqueryStack.peek();
        if (top != null) top.add(sub);
        return null;   // 内层 query 已单独分析，不再 visitChildren
    }

    // ==================== 窗口函数（FunctionCall with OVER） ====================

    @Override
    public LogicalPlan visitFunctionCall(SqlBaseParser.FunctionCallContext ctx) {
        // 窗口函数：检测 OVER 子句
        if (ctx.OVER() != null && ctx.windowSpecification() != null) {
            if (!windowAllowedInExpression) {
                // 只有 SELECT 项表达式里的窗口会被物化求值；WHERE / HAVING / GROUP BY / JOIN ON
                // 里的窗口既收集不到也求不了值，FilterOperator 会按「未知谓词」逐行放行
                // ⇒ 整条窗口条件被静默忽略。必须在语义分析期拒绝（ORDER BY 里的窗口由
                // Planner 显式报不支持，不在此列）。
                error(ctx, "窗口函数不能出现在 WHERE / HAVING / GROUP BY / JOIN ON 中，"
                    + "请先用子查询把窗口结果物化成列: " + originalText(ctx));
            }
            String funcName = ctx.functionName.getText();

            List<SqlBaseParser.ExpressionContext> argCtxs = ctx.expression();
            List<String> args = new ArrayList<>();
            for (SqlBaseParser.ExpressionContext e : argCtxs) {
                args.add(originalText(e));
            }

            List<String> partitionBy = new ArrayList<>();
            List<String> orderBy = new ArrayList<>();
            Optional<String> windowFrame = Optional.empty();

            SqlBaseParser.WindowSpecificationContext ws = ctx.windowSpecification();
            if (ws instanceof SqlBaseParser.WindowDefinitionInlineContext inline) {
                SqlBaseParser.WindowDefinitionContext wd = inline.windowDefinition();
                if (wd != null) {
                    for (SqlBaseParser.ExpressionContext pe : wd.expression()) {
                        partitionBy.add(originalText(pe));
                    }
                    for (SqlBaseParser.SortItemContext si : wd.sortItem()) {
                        orderBy.add(originalText(si));
                    }
                    if (wd.windowFrame() != null) {
                        windowFrame = Optional.of(originalText(wd.windowFrame()));
                    }
                }
            } else if (ws instanceof SqlBaseParser.WindowReferenceContext ref) {
                windowFrame = Optional.of("WINDOW_REF:" + ref.windowName().getText());
            }

            // 窗口定义（PARTITION BY / ORDER BY）不允许聚合函数调用（Postgres 同样禁止）：
            // 物理层 parseOrderKey 只能取单列，遇到 `order by sum(x)` 会以
            // UnsupportedOperationException 抛出 —— 这里在语义分析期直接拒绝（无 GROUP BY 也拒绝）。
            rejectAggregateInWindow(ctx, funcName, partitionBy, false, "PARTITION BY");
            rejectAggregateInWindow(ctx, funcName, orderBy, true, "ORDER BY");

            // 返回标量的普通函数包在窗口外面：`round(sum(amount), 2) OVER (PARTITION BY …)`。
            // 语义等价于把 OVER 贴到参数里的聚合调用上：`round(sum(amount) OVER (PARTITION BY …), 2)`。
            // 引擎本来就支持「窗口嵌在表达式里」（OpaqueExpr + windowCalls），这里只做规范化：
            // 窗口本体按内层聚合调用登记，外层函数留给 Planner 的标量投影求值。
            // 只处理「参数整体就是一个聚合调用」的形态；`round(sum(a)+1,2) OVER (…)` 之类仍走下面的拒绝。
            if (!isWindowCapableFunction(funcName)) {
                String specText = originalText(ws);
                List<String> rewrittenArgs = new ArrayList<>(args.size());
                List<PendingWindowCall> wrapped = new ArrayList<>();
                boolean leftoverAggregate = false;   // 聚合嵌在参数表达式里（`sum(a)+1`）→ 不做规范化
                for (int i = 0; i < args.size(); i++) {
                    SqlBaseParser.FunctionCallContext inner = soleAggregateCall(argCtxs.get(i));
                    if (inner == null) {
                        if (isAggregateExpression(args.get(i))) leftoverAggregate = true;
                        rewrittenArgs.add(args.get(i));
                        continue;
                    }
                    List<String> innerArgs = new ArrayList<>();
                    for (SqlBaseParser.ExpressionContext ie : inner.expression()) {
                        innerArgs.add(originalText(ie));
                    }
                    String windowText = originalText(inner) + " OVER " + specText;
                    wrapped.add(new PendingWindowCall(inner, new Expression.WindowFunction(
                        inner.functionName.getText(), innerArgs, partitionBy, orderBy, windowFrame),
                        windowText));
                    rewrittenArgs.add(windowText);
                }
                if (!wrapped.isEmpty() && !leftoverAggregate) {
                    pendingWindowCalls.addAll(wrapped);
                    pendingWindowExprRewrites.add(new WindowExprRewrite(ctx,
                        funcName + "(" + String.join(", ", rewrittenArgs) + ")"));
                    windowSpecDepth++;
                    try {
                        return visitChildren(ctx);
                    } finally {
                        windowSpecDepth--;
                    }
                }
            }

            // 窗口参数（值参数）不允许聚合函数调用：物理层 stripWindowArg 只能取单列。
            // 包装形态已在上面规范化并提前返回；走到这里说明没有可规范化的聚合参数。
            rejectAggregateInWindow(ctx, funcName, args, false, "参数");

            pendingWindowCalls.add(new PendingWindowCall(ctx, new Expression.WindowFunction(
                funcName, args, partitionBy, orderBy, windowFrame)));
            // OVER 子树内的列引用放行「聚合/分组别名」（见 windowAliases）
            windowSpecDepth++;
            try {
                return visitChildren(ctx);
            } finally {
                windowSpecDepth--;
            }
        }

        // 继续遍历子树以解析列引用
        return visitChildren(ctx);
    }

    // ==================== VALUES ====================

    @Override
    public LogicalPlan visitValues(SqlBaseParser.ValuesContext ctx) {
        return new LogicalPlan.Values(new ArrayList<>());
    }

    // ==================== 私有校验 ====================

    private void validateCatalogSchemaForTable(ParserRuleContext ctx, List<String> qname) {
        if (qname.size() >= 2) {
            String catalog = qname.get(0);
            String schema = qname.get(1);
            if (!metaStore.isCatalogRegistered(catalog)) {
                error(ctx, "Catalog '" + catalog + "' does not exist");
            } else if (!metaStore.schemaExists(catalog, schema)) {
                error(ctx, "Schema '" + catalog + "." + schema + "' does not exist");
            }
        } else if (qname.size() == 1) {
            if (currentCatalog == null || currentSchema == null) {
                error(ctx, "Cannot resolve table '" + qname.get(0)
                    + "' -- no current catalog/schema. Use USE catalog.schema first.");
            }
        }
    }

    private void validateTableExists(ParserRuleContext ctx, List<String> qname) {
        TableResolver resolver = new TableResolver(metaStore, currentCatalog, currentSchema);
        TableResolver.ResolveResult result = resolver.resolve(qname);
        errors.addAll(result.errors());
    }

    // ==================== GROUP BY 校验 ====================

    private void validateGroupBy(ParserRuleContext ctx,
                                  List<LogicalPlan.SelectItem> selectItems,
                                  List<Expression> groupByExprs) {
        for (LogicalPlan.SelectItem item : selectItems) {
            Expression expr = item.expression();
            String exprText = expr.text().trim();

            // SELECT * 跳过
            if ("*".equals(exprText)) continue;

            // 窗口嵌在表达式里（`gmv / sum(gmv) OVER ()`）：逐个校验窗口定义/参数的列，
            // 表达式其余部分（外层算术）交给物理层 requireColumn —— 整段原文既不是聚合
            // 也不能匹配 GROUP BY 表达式，不能落到下面的通用检查。
            if (!item.windowCalls().isEmpty()) {
                for (LogicalPlan.WindowCall wc : item.windowCalls()) {
                    validateWindowGroupByRefs(ctx, wc.window(), groupByExprs);
                }
                continue;
            }

            // 窗口函数：OVER 的 PARTITION BY / ORDER BY / 参数引用的列同样必须已分组、或被聚合
            //   包裹、或为常量（否则物理层 requireColumn 才报「找不到 PARTITION BY/ORDER BY 列」，
            //   属语义错误却以 UnsupportedOperationException 形式抛出）。这里提前给出语义错误。
            if (expr instanceof Expression.WindowFunction wf) {
                validateWindowGroupByRefs(ctx, wf, groupByExprs);
                continue;
            }

            // 函数调用（COUNT / SUM / AVG / MAX / MIN 等）跳过
            if (isAggregateExpression(exprText)) continue;

            // 出现在 GROUP BY 中的表达式跳过
            if (matchesGroupBy(exprText, groupByExprs)) continue;

            // 常量 / 字面量跳过
            if (isConstant(exprText)) continue;

            // 余下的认为是应出现在 GROUP BY 中的列引用
            String display = item.alias().isPresent()
                ? exprText + " AS " + item.alias().get()
                : exprText;
            error(ctx, "Column '" + display
                + "' must appear in GROUP BY clause or be used in an aggregate function");
        }
    }

    /**
     * 窗口函数在 GROUP BY 场景下的列校验：{@code OVER (PARTITION BY …)}、{@code OVER (ORDER BY …)}
     * 以及窗口参数引用的每个列，都必须出现在 GROUP BY 中或为常量；聚合表达式一律不接受。
     *
     * <p>与物理层 {@code Planner.buildWindows} 的 {@code requireColumn} 口径一致（那边只校验
     * PARTITION BY / ORDER BY 列）；此处额外校验参数，把「{@code SUM(amount) OVER (…) } 而 amount
     * 未分组」这类同样非法的写法也提前挡下（否则参数由窗口算子在运行期解析后才报错）。</p>
     */
    private void validateWindowGroupByRefs(ParserRuleContext ctx, Expression.WindowFunction wf,
                                           List<Expression> groupByExprs) {
        for (String a : wf.args()) {
            checkWindowGroupByRef(ctx, wf, a, groupByExprs, "参数");
        }
        for (String p : wf.partitionBy()) {
            checkWindowGroupByRef(ctx, wf, p, groupByExprs, "PARTITION BY");
        }
        for (String o : wf.orderBy()) {
            checkWindowGroupByRef(ctx, wf, stripSortDirection(o), groupByExprs, "ORDER BY");
        }
    }

    /** 单个窗口引用：已分组 / 常量 → 放行；其它未分组列 → 记语义错误（聚合已由 {@code visitFunctionCall} 拒绝）。 */
    private void checkWindowGroupByRef(ParserRuleContext ctx, Expression.WindowFunction wf, String refText,
                                       List<Expression> groupByExprs, String where) {
        String ref = refText.trim();
        if (ref.isEmpty() || "*".equals(ref)) return;              // COUNT(*) 的 *
        if (isConstant(ref)) return;                              // LAG(x, 1, 0) 的 1 / 0
        if (isAggregateExpression(ref)) return;                   // 聚合表达式已由 visitFunctionCall 报错，避免重复
        // 同层 SELECT 别名：定义表达式是聚合或已分组表达式 → 合法（窗口在聚合之后求值）。
        // 未分组列的别名不放行 —— 那仍然是非法引用。
        String aliasDef = selectAliasExprs.get(ref.toLowerCase(Locale.ROOT));
        if (aliasDef != null) {
            if (isAggregateExpression(aliasDef)) return;
            if (matchesGroupBy(aliasDef, groupByExprs)) return;
            if (matchesGroupByBare(aliasDef, groupByExprs)) return;
        }
        if (matchesGroupBy(ref, groupByExprs)) return;
        if (matchesGroupByBare(ref, groupByExprs)) return;        // OVER (PARTITION BY user_id) ↔ GROUP BY b.user_id
        error(ctx, "Column '" + ref + "' must appear in GROUP BY clause or be used in an aggregate function"
            + "（窗口函数 " + wf.functionName() + " 的 " + where + " 引用）");
    }

    /**
     * 拒绝窗口定义里的聚合函数调用：{@code OVER (PARTITION BY … / ORDER BY …)} 与窗口参数都只能
     * 引用/求值为单列，写聚合（如 {@code order by sum(age)}）在物理层无法规划。语义分析期直接报错。
     *
     * @param orderBy 该列表是否为 ORDER BY 项（需剥掉尾部 {@code ASC/DESC} 再判）
     */
    private void rejectAggregateInWindow(ParserRuleContext ctx, String funcName, List<String> refs,
                                         boolean orderBy, String where) {
        for (String raw : refs) {
            String ref = orderBy ? stripSortDirection(raw) : raw.trim();
            if (ref.isEmpty() || "*".equals(ref)) continue;       // COUNT(*) 的 *
            if (isConstant(ref)) continue;
            if (isAggregateExpression(ref)) {
                error(ctx, "窗口函数 " + funcName + " 的 " + where + " 不支持聚合表达式: '" + ref
                    + "'，请先用子查询把聚合结果物化为列");
            }
        }
    }

    /** 该函数名能否直接作为窗口函数（内置聚合 / 自定义聚合 / 纯窗口函数）。 */
    private static boolean isWindowCapableFunction(String name) {
        String upper = name.toUpperCase(Locale.ROOT);
        return AGGREGATE_FUNCTIONS.contains(upper)
            || WINDOW_ONLY_FUNCTIONS.contains(upper)
            || AggregateFunctionRegistry.hasGlobal(upper)
            || CollectAggregateRegistry.hasGlobal(upper);
    }

    /**
     * 参数表达式是否「整体就是一个聚合函数调用」（{@code sum(x)}，不含 {@code OVER}）。
     * 是则返回内层调用上下文，否则 null（算术包裹 / 括号包裹 / 非聚合都返回 null）。
     */
    private SqlBaseParser.FunctionCallContext soleAggregateCall(SqlBaseParser.ExpressionContext arg) {
        SqlBaseParser.FunctionCallContext fc = soleFunctionCall(arg);
        if (fc == null || fc.OVER() != null) return null;
        if (!isWindowCallRoot(arg, fc)) return null;   // 表达式里还有其它运算（算术 / 逗号…）
        return isAggregateExpression(originalText(fc)) ? fc : null;
    }

    /** 返回表达式树里唯一的函数调用上下文；出现多个（或没有）则 null。 */
    private static SqlBaseParser.FunctionCallContext soleFunctionCall(ParserRuleContext root) {
        SqlBaseParser.FunctionCallContext found = null;
        for (int i = 0; i < root.getChildCount(); i++) {
            if (!(root.getChild(i) instanceof ParserRuleContext child)) continue;
            SqlBaseParser.FunctionCallContext sub = (child instanceof SqlBaseParser.FunctionCallContext f)
                ? f : soleFunctionCall(child);
            if (sub != null) {
                if (found != null) return null;
                found = sub;
            }
        }
        return found;
    }

    /** 剥掉窗口 ORDER BY 排序项尾部的 {@code ASC / DESC [NULLS FIRST|LAST]}，只留列表达式。 */
    private static String stripSortDirection(String text) {
        return SORT_DIRECTION.matcher(text.trim()).replaceFirst("").trim();
    }

    /** 去限定符（{@code a.b} → {@code b}）+ 小写；非限定名整体小写。 */
    private static String stripQualifierLower(String text) {
        String t = text.trim();
        int dot = t.lastIndexOf('.');
        if (dot >= 0 && dot + 1 < t.length()) t = t.substring(dot + 1).trim();
        return t.toLowerCase(Locale.ROOT);
    }

    /** 去限定符后比较：{@code OVER (PARTITION BY user_id)} 与 {@code GROUP BY b.user_id} 视为同列。 */
    private static boolean matchesGroupByBare(String ref, List<Expression> groupByExprs) {
        String bare = stripQualifierLower(ref);
        for (Expression gb : groupByExprs) {
            if (gb instanceof Expression.GroupingSets gs) {
                for (List<Expression> set : gs.sets()) {
                    for (Expression member : set) {
                        if (stripQualifierLower(member.text()).equals(bare)) return true;
                    }
                }
                continue;
            }
            if (stripQualifierLower(gb.text()).equals(bare)) return true;
        }
        return false;
    }

    /** 检测表达式文本是否包含聚合函数调用（含嵌套场景，如 CONCAT_WS(',', COLLECT_LIST(...))）。 */
    static boolean isAggregateExpression(String exprText) {
        String upper = exprText.toUpperCase(Locale.ROOT).trim();
        for (String func : AGGREGATE_FUNCTIONS) {
            if (upper.contains(func + "(")) return true;
        }
        // 用户自定义聚合（CREATE FUNCTION 后由 FunctionManager.ensureLoaded 灌进
        // AggregateFunctionRegistry / CollectAggregateRegistry，查询期在 plan 前已加载）：
        // 扫描表达式中的函数调用名，命中已注册聚合即视为聚合表达式
        // （大小写不敏感注册表，这里统一大写比对；collect-evaluate 型同样放行）。
        var m = FUNCTION_CALL.matcher(upper);
        while (m.find()) {
            if (AggregateFunctionRegistry.hasGlobal(m.group(1))
                    || CollectAggregateRegistry.hasGlobal(m.group(1))) {
                return true;
            }
        }
        return false;
    }

    /** 检测表达式文本是否与 GROUP BY 中的某个表达式匹配（GroupingSets 视其全部成员列为匹配集）。 */
    private static boolean matchesGroupBy(String exprText, List<Expression> groupByExprs) {
        String trimmed = exprText.trim();
        for (Expression gb : groupByExprs) {
            if (gb instanceof Expression.GroupingSets gs) {
                for (List<Expression> set : gs.sets()) {
                    for (Expression member : set) {
                        if (member.text().trim().equalsIgnoreCase(trimmed)) return true;
                    }
                }
                continue;
            }
            if (gb.text().trim().equalsIgnoreCase(trimmed)) return true;
        }
        return false;
    }

    /** 检测是否是常量 / 字面量（数字、字符串）。 */
    private static boolean isConstant(String exprText) {
        String trimmed = exprText.trim();
        if (trimmed.isEmpty()) return true;
        if (NUMERIC_PATTERN.matcher(trimmed).matches()) return true;
        // 引号包裹的字符串字面量
        char first = trimmed.charAt(0);
        if ((first == '\'' || first == '"') && trimmed.length() >= 2
            && trimmed.charAt(trimmed.length() - 1) == first) {
            return true;
        }
        // NULL / TRUE / FALSE
        String upper = trimmed.toUpperCase(Locale.ROOT);
        return "NULL".equals(upper) || "TRUE".equals(upper) || "FALSE".equals(upper);
    }
}
