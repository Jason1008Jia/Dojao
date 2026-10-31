package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.core.function.scalar.ScalarEvaluator;
import com.dojao.core.function.scalar.ScalarExpr;
import com.dojao.core.operator.expr.ExprEvaluator;
import com.dojao.core.operator.expr.ExprNode;
import com.dojao.core.operator.expr.ExprParser;
import com.dojao.core.planner.SqlTextUtil;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.dojao.core.function.scalar.ScalarExpr.unquote;

/**
 * Filter operator - filters rows from upstream Page based on condition.
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/12
 */
public class FilterOperator extends AbstractMeasurableOperator {

    private Operator upstream;
    private final String filterExpression;

    /** 首张非空页已校验过「谓词列都在上游 schema 里」——只做一次，热路径零开销。 */
    private boolean columnsChecked;

    /** 编译后的谓词形态。 */
    private enum Form {
        COLUMN_IS_NOT_NULL, COLUMN_IS_NULL, COLUMN_COMPARE, COLUMN_COLUMN_COMPARE,
        FUNC_IS_NOT_NULL, FUNC_IS_NULL, FUNC_COMPARE,
        COLUMN_LIKE, COLUMN_NOT_LIKE,
        COLUMN_IN, COLUMN_NOT_IN,
        EXPR,     // 复合布尔表达式（AND/OR/NOT 组合、函数比较）→ 通用表达式求值器
        UNKNOWN   // 无法解析 → 保守放行（保持旧行为）
    }

    private Form form;
    private String column;              // 列形态：列名
    private String op;                  // 比较形态：操作符（函数在右时已翻转）
    private String literal;             // 比较形态：右侧字面量（已去引号）
    private String rightColumn;         // 列间比较形态：右侧列名
    private ScalarEvaluator funcEvaluator;  // 函数形态：标量函数批求值器
    private ExprEvaluator exprEvaluator;    // EXPR 形态：复合布尔表达式批求值器
    private java.util.regex.Pattern likePattern;  // LIKE 形态：由 SQL 模式编译的匹配器
    private List<String> inValues;      // IN 形态：右侧字面量列表（已去引号）

    public FilterOperator(Operator upstream, String filterExpression) {
        super("Filter", "Filter", 1);
        this.upstream = upstream;
        this.filterExpression = filterExpression;
        compile();
    }

    /** 构造期一次性编译谓词（列 IS NULL / 列比较 / 标量函数谓词），运行时零重解析。 */
    private void compile() {
        // 先剥外层括号：谓词常由上游拼装而来（如连接器下推契约的 `(f) AND (p)` 合取、优化器的
        // `(a = 1)`），若不剥则左侧变成 `(col`、标识符判定失败 → 落 UNKNOWN → 静默放行全部行。
        String expr = SqlTextUtil.normalizeFilter(
                SqlTextUtil.stripOuterParens(filterExpression == null ? "" : filterExpression.trim()));
        String upper = expr.toUpperCase(Locale.ROOT);

        // 1. IS [NOT] NULL —— 函数或<b>裸列</b>
        //    ★后缀匹配天然贪婪：`a = 1 and b is not null` 这类<b>没拆开的合取</b>也会命中，
        //    若不加约束就会把左侧整段当成列名（实测 `dt ='2026-08-25' and spu_id` 被当成列名
        //    → 伪列并进扫描列集 → Hive「表中不存在列」；不并则恒假 → 静默 0 行）。
        //    故只认「目标是标量函数调用」或「目标是列引用（ident / alias.ident）」；两者都不是 →
        //    不认这个形态，继续往下判定（最终落 UNKNOWN 保守放行），绝不臆造列名。
        if (upper.endsWith(" IS NOT NULL")) {
            String target = expr.substring(0, expr.length() - " IS NOT NULL".length()).trim();
            Optional<ScalarExpr> fn = ScalarExpr.tryParse(target);
            if (fn.isPresent()) {
                form = Form.FUNC_IS_NOT_NULL;
                funcEvaluator = new ScalarEvaluator(fn.get());
                return;
            }
            if (SqlTextUtil.isColumnRef(target)) {
                form = Form.COLUMN_IS_NOT_NULL;
                column = target;
                return;
            }
        } else if (upper.endsWith(" IS NULL")) {
            String target = expr.substring(0, expr.length() - " IS NULL".length()).trim();
            Optional<ScalarExpr> fn = ScalarExpr.tryParse(target);
            if (fn.isPresent()) {
                form = Form.FUNC_IS_NULL;
                funcEvaluator = new ScalarEvaluator(fn.get());
                return;
            }
            if (SqlTextUtil.isColumnRef(target)) {
                form = Form.COLUMN_IS_NULL;
                column = target;
                return;
            }
        }

        // 2. LIKE / NOT LIKE —— 只认「裸列 LIKE '字面量模式'」这一形态。形状判定统一收在
        //    SqlTextUtil.parseLike（CaseProjectOperator 的 CASE 条件、Planner 的 CASE 条件校验与列收集
        //    共用同一份），避免多处漂移导致「校验通过但求值落空」。
        //    引擎侧唯一的模式匹配原语：数据源不兑现谓词下推时（如 Hive 连接器不求值 filterExpression），
        //    谓词就落在扫描节点的本算子上执行，缺了这一形态它们会退化成 Form.UNKNOWN → 静默放行全部行。
        //    通配符按 SQL 语义翻译：% → 任意长度（含空），_ → 任意单字符；其余字符按字面量转义。
        //    大小写敏感 —— 对齐 Hive / PostgreSQL 的 LIKE；MySQL 大小写不敏感，但它自己下推执行，不过这里。
        //    注：多个 LIKE 用 AND 串起来时须由调用方逐项包裹（见 SqlTextUtil.splitConjuncts）——
        //    本算子只编译单个简单谓词。
        SqlTextUtil.LikeSpec like = SqlTextUtil.parseLike(expr);
        if (like != null) {
            form = like.negated() ? Form.COLUMN_NOT_LIKE : Form.COLUMN_LIKE;
            column = like.column();
            likePattern = like.pattern();
            return;
        }

        // 2.5 IN / NOT IN —— 只认「裸列 [NOT] IN (字面量列表)」这一形态。理由同 LIKE：数据源不兑现
        //     谓词下推时（如 Hive 只按切片读文件、不求值 filterExpression），谓词落在本算子上执行；
        //     缺这一形态会退化成 UNKNOWN → 静默放行全部行（实测 CTAS 的 `event_type in ('purchase')`
        //     被丢弃，写出的表含全部事件类型）。形状判定收在 SqlTextUtil.parseInList。
        SqlTextUtil.InSpec in = SqlTextUtil.parseInList(expr);
        if (in != null) {
            form = in.negated() ? Form.COLUMN_NOT_IN : Form.COLUMN_IN;
            column = in.column();
            inValues = in.values();
            return;
        }

        // 3. 比较谓词：左 op 右（左/右任一可为标量函数调用）
        Optional<ScalarExpr.Comparison> cmp = ScalarExpr.splitComparison(expr);
        if (cmp.isPresent()) {
            ScalarExpr.Comparison c = cmp.get();
            Optional<ScalarExpr> leftFn = ScalarExpr.tryParse(c.left());
            if (leftFn.isPresent()) {
                form = Form.FUNC_COMPARE;
                funcEvaluator = new ScalarEvaluator(leftFn.get());
                op = c.op();
                literal = unquote(c.right());
                return;
            }
            Optional<ScalarExpr> rightFn = ScalarExpr.tryParse(c.right());
            if (rightFn.isPresent()) {
                // 字面量 op func(args) → 翻转操作符，统一为 func(args) reverseOp 字面量
                form = Form.FUNC_COMPARE;
                funcEvaluator = new ScalarEvaluator(rightFn.get());
                op = reverseOp(c.op());
                literal = unquote(c.left());
                return;
            }
            // 列 vs 列 / 字面量 判定：两侧均非字面量 → 列间比较（如 rating > __subq_val）；
            // 右侧字面量 → 列比较；左侧字面量右侧列 → 翻转；两侧均字面量 → 无列可读 ⇒ UNKNOWN
            String leftRaw = c.left().trim();
            String rightRaw = c.right().trim();
            boolean leftLit = isLiteralToken(leftRaw);
            boolean rightLit = isLiteralToken(rightRaw);
            String rawLiteral;
            if (!leftLit && rightLit) {
                form = Form.COLUMN_COMPARE;
                column = leftRaw;
                op = c.op();
                rawLiteral = rightRaw;
            } else if (!leftLit) {
                form = Form.COLUMN_COLUMN_COMPARE;
                column = leftRaw;
                op = c.op();
                rightColumn = SqlTextUtil.stripQualifier(rightRaw);
                rawLiteral = null;
            } else if (!rightLit) {
                form = Form.COLUMN_COMPARE;
                column = rightRaw;
                op = reverseOp(c.op());
                rawLiteral = leftRaw;
            } else {
                // 两侧都是字面量 → 压根没有列可读。旧行为是拿左侧字面量当「列名」去取向量
                // （取不到 → 恒假 → 静默 0 行），且会把 "1" 这类伪列登记进扫描列集。改判 UNKNOWN。
                form = Form.UNKNOWN;
                return;
            }
            // ★校验列名与字面量的<b>形态完整性</b>：列必须是真列名（ident / alias.ident），
            //   字面量必须整串就是字面量。不满足 ⇒ 这压根不是「单个简单谓词」——典型是没拆开的
            //   合取（`dt ='2026-08-25' and spu_id is not null`，右段被 isLiteralToken 因首位引号
            //   误判为字面量）⇒ 落 UNKNOWN 保守放行，<b>绝不臆造列名</b>：伪列一旦被登记进扫描列集，
            //   轻则连接器报「表中不存在列」，重则过滤恒假 ⇒ 静默丢光所有行。
            if (form == Form.COLUMN_COLUMN_COMPARE) {
                if (!looksLikeColumnName(column) || !looksLikeColumnName(rightRaw)) {
                    form = Form.UNKNOWN;
                }
            } else {
                if (!looksLikeColumnName(column) || !isCompleteLiteralToken(rawLiteral)) {
                    form = Form.UNKNOWN;
                    return;
                }
                literal = unquote(rawLiteral);
            }
            return;
        }

        // 3.5 复合布尔表达式（`NOT (...)` / `(a AND b)` / `(f(x) > 0) AND ...`）—— 上面的
        //     单谓词形态都装不下，旧实现直接落 UNKNOWN ⇒ <b>逐行放行、静默丢弃整个谓词</b>。
        //     报告 BUG-009/010（`NOT (EXISTS(...))`、`NOT (EXISTS(...) AND p)`）正踩在这里：
        //     去相关改写后剩下的就是一条复合布尔式，NOT 因此被静默丢弃。
        //     交给通用表达式求值器逐行求值（ExprParser 能解析 AND/OR/NOT/函数/比较/CAST…）。
        try {
            ExprNode node = ExprParser.parse(expr);
            exprEvaluator = new ExprEvaluator(node);
            form = Form.EXPR;
            return;
        } catch (RuntimeException ignored) {
            // 解析不了（不是合法表达式）→ 维持旧的保守放行语义
        }

        // 3.6 含子查询的谓词：FilterOperator 没有执行子查询的能力 —— 子查询必须先在
        //     SemanticAnalyzer 登记、由 Planner.decorrelate 改写成派生列。走到这里说明去相关
        //     没覆盖到，落 UNKNOWN 会是「逐行放行」⇒ 过滤条件被静默丢弃、返回全量（缺陷报告
        //     BUG-007/013 的签名：跨源 EXISTS 返回恰好等于分区全量）。宁可在规划期快速失败。
        if (containsSubqueryPredicate(expr)) {
            throw new UnsupportedOperationException(
                    "过滤谓词包含未能去相关的子查询，拒绝静默丢弃过滤条件（会返回全量）: "
                            + filterExpression);
        }

        // 3. 无法解析 → 保守放行（保持旧 evaluateRow 对未知格式返回 true 的行为）
        form = Form.UNKNOWN;
    }

    /** 获取过滤表达式原文（供 Planner 下推窗口过滤条件）。 */
    public String filterExpression() {
        return filterExpression;
    }

    /**
     * 谓词<span>真正引用</span>的上游列（供 Planner 列裁剪登记「上游必须保留哪些列」）。
     *
     * <p>直接取自构造期编译结果，<b>不再对谓词原文做「取第一个空格前的 token」式猜测</b>：
     * 那种做法对 {@code cnt>0} 这类无空格谓词会把整串 {@code "cnt>0"} 当成列名，真列
     * {@code cnt} 反而不被登记 → 列裁剪把 {@code cnt} 裁掉 → 运行时 Filter 找不到列、
     * 逐行恒假 → <b>静默 0 行</b>（HAVING / 派生表外层 WHERE 都走这条路）。</p>
     */
    public List<String> referencedColumns() {
        List<String> cols = new ArrayList<>(2);
        switch (form) {
            case COLUMN_IS_NOT_NULL, COLUMN_IS_NULL, COLUMN_COMPARE,
                 COLUMN_LIKE, COLUMN_NOT_LIKE,
                 COLUMN_IN, COLUMN_NOT_IN -> addColumn(cols, column);
            case COLUMN_COLUMN_COMPARE -> {
                addColumn(cols, column);
                addColumn(cols, rightColumn);
            }
            // 函数谓词（含比较参数里的条件列）→ 复用求值器的列清单
            case FUNC_IS_NOT_NULL, FUNC_IS_NULL, FUNC_COMPARE -> {
                if (funcEvaluator != null) {
                    for (String c : funcEvaluator.columnArgs()) addColumn(cols, c);
                }
            }
            // 复合布尔表达式 → 求值器解析出的全部上游列
            case EXPR -> {
                if (exprEvaluator != null) {
                    for (String c : exprEvaluator.columnArgs()) addColumn(cols, c);
                }
            }
            // 无法解析 → 运行时保守放行全部行，故无列需求
            case UNKNOWN -> { }
        }
        return cols;
    }

    private static void addColumn(List<String> cols, String col) {
        if (col == null || col.isEmpty()) return;
        // ★只登记真列名 —— 谓词片段（如 `dt ='2026-08-25' and spu_id`）绝不允许流进扫描列集：
        //   下游拿它当列名请求投影 ⇒ Hive 报「表中不存在列」；或取不到向量 ⇒ 过滤恒假 ⇒ 静默 0 行。
        if (!looksLikeColumnName(col)) return;
        String c = SqlTextUtil.stripQualifier(col);
        if (!c.isEmpty() && !cols.contains(c)) cols.add(c);
    }

    /** 形如 {@code ident(.ident)*} 的列名（允许 1~N 段限定符）—— 拒绝含空白 / 运算符 / 括号的谓词片段。 */
    private static boolean looksLikeColumnName(String s) {
        String t = s == null ? "" : s.trim();
        if (t.isEmpty()) return false;
        for (String seg : t.split("\\.", -1)) {
            if (!SqlTextUtil.isSingleIdentifier(seg.trim())) return false;
        }
        return true;
    }

    /**
     * 是否为<b>完整</b>字面量 token：整串被引号包裹 / 纯数字 / {@code true|false|null}。
     *
     * <p>与 {@link #isLiteralToken} 的分工：后者是「能不能当字面量<b>类</b>处理」的宽松归类
     * （谓词片段也算，用于选分支）；本方法是「这个 token 是不是<b>就只是一个</b>字面量」的严格判定，
     * 用于挡住把整段合取当常量（{@code '2026-08-25' and spu_id is not null} 首位是引号但显然不是）。</p>
     */
    private static boolean isCompleteLiteralToken(String s) {
        // 口径与 SqlTextUtil.parseInList 的列表项校验同一份（勿各自维护两份正则，会漂移）
        return SqlTextUtil.isCompleteLiteral(s);
    }

    /** 更新上游算子引用（列裁剪优化使用，须在 {@link #doOpen()} 前调用）。 */
    public void setUpstream(Operator upstream) { this.upstream = upstream; }

    @Override
    protected void doOpen() throws Exception {
        upstream.open();
    }

    @Override
    protected Page doNext() throws Exception {
        while (true) {
            Page input = upstream.next();
            recordInput(input);
            if (input.isEOF()) return input;

            int rowCount = input.getRowCount();
            if (rowCount == 0) {
                input.close();
                continue;   // 空页跳过，继续拉下一页
            }
            checkColumnsPresent(input);
            // 函数谓词：对整个 batch 求值一次得到结果向量，再逐行比较（不逐行调函数）
            boolean funcForm = form == Form.FUNC_COMPARE
                    || form == Form.FUNC_IS_NULL || form == Form.FUNC_IS_NOT_NULL
                    || form == Form.EXPR;
            FieldVector funcResult = null;
            if (funcForm) {
                BufferAllocator allocator = input.getRoot().getVector(0).getAllocator();
                funcResult = form == Form.EXPR
                        ? exprEvaluator.evaluate(input.getRoot(), allocator, "__expr_filter")
                        : funcEvaluator.evaluate(input.getRoot(), allocator, "__func_filter");
            }

            int[] selected = new int[rowCount];
            int outCount = 0;
            for (int i = 0; i < rowCount; i++) {
                boolean keep = funcForm
                        ? evaluateRowFunc(input.getRoot(), i, funcResult)
                        : evaluateRow(input.getRoot(), i);
                if (keep) {
                    selected[outCount++] = i;
                }
            }

            if (funcResult != null) funcResult.close();

            if (outCount == 0) {
                input.close();
                continue;   // 本页无匹配行 → 继续拉下一页（此前错误返回 EOF 导致整个查询提前终止）
            }

            VectorSchemaRoot inRoot = input.getRoot();
            List<Field> fields = new ArrayList<>();
            List<FieldVector> vectors = new ArrayList<>();

            for (int col = 0; col < inRoot.getFieldVectors().size(); col++) {
                Field field = inRoot.getSchema().getFields().get(col);
                FieldVector src = inRoot.getVector(col);
                FieldVector dst = field.createVector(src.getAllocator());
                dst.setInitialCapacity(outCount);
                dst.allocateNew();
                TransferPair transfer = src.makeTransferPair(dst);
                for (int j = 0; j < outCount; j++) {
                    transfer.copyValueSafe(selected[j], j);
                }
                dst.setValueCount(outCount);
                fields.add(field);
                vectors.add(dst);
            }

            VectorSchemaRoot outRoot = new VectorSchemaRoot(fields, vectors, outCount);
            input.close();
            return Page.fromRoot(outRoot);
        }
    }

    @Override
    protected void doClose() throws Exception {
        upstream.close();
    }

    /**
     * <b>谓词列必须在上游 schema 里</b> —— 缺列时响亮报错，绝不静默丢光所有行。
     *
     * <p>缺列的后果不是「过滤失效」而是「<b>逐行恒假</b>」：{@code evaluateRow} 取不到向量即返回
     * {@code false}（IS [NOT] NULL 形态更会直接 NPE）⇒ 结果集<b>空</b>且<b>无任何报错</b>。
     * 实测过的事故：Hive（不兑现谓词下推 → 引擎侧 Filter 承接 {@code dt >= '2026-08-25'}）
     * 的扫描列被列裁剪成只剩 {@code user_id} ⇒ 读 2200 万行、分发 0 行、join 结果集空。
     * 规划期已按 {@code FilterOperator.referencedColumns()} 补齐扫描列（见
     * {@code Planner.scanColumns}）；这里作为<b>最后一道防线</b>，把同类漏列从「静默空结果」
     * 变成一条能定位到谓词与实到列的错误。</p>
     */
    private void checkColumnsPresent(Page input) {
        if (columnsChecked) {
            return;
        }
        columnsChecked = true;
        VectorSchemaRoot root = input.getRoot();
        for (String c : referencedColumns()) {
            if (root.getVector(c) == null) {
                List<String> have = new ArrayList<>();
                for (Field f : root.getSchema().getFields()) {
                    have.add(f.getName());
                }
                input.close();
                throw new IllegalStateException("过滤谓词引用的列 \"" + c + "\" 不在上游输出中"
                        + "（谓词：" + filterExpression + "；实到列：" + have + "）"
                        + "—— 缺列会让谓词逐行恒假、结果静默为空，请检查扫描列集是否漏了谓词列。");
            }
        }
    }

    /** 列形态谓词求值（编译后的字段，运行时零重解析）。 */
    private boolean evaluateRow(VectorSchemaRoot root, int rowIndex) {
        switch (form) {
            case COLUMN_IS_NOT_NULL:
                return !root.getVector(column).isNull(rowIndex);
            case COLUMN_IS_NULL:
                return root.getVector(column).isNull(rowIndex);
            case COLUMN_COMPARE: {
                FieldVector vector = root.getVector(column);
                if (vector == null || vector.isNull(rowIndex)) return false;
                Object rowVal = vector.getObject(rowIndex);
                if (rowVal == null) return false;
                return ScalarExpr.compare(rowVal.toString(), op, literal);
            }
            case COLUMN_COLUMN_COMPARE: {
                // 列间比较：两侧同为列引用（如 rating > __subq_val），按标量字符串比较
                FieldVector lv = root.getVector(column);
                FieldVector rv = root.getVector(rightColumn);
                if (lv == null || rv == null) return false;
                if (lv.isNull(rowIndex) || rv.isNull(rowIndex)) return false;
                Object l = lv.getObject(rowIndex);
                Object r = rv.getObject(rowIndex);
                if (l == null || r == null) return false;
                return ScalarExpr.compare(l.toString(), op, r.toString());
            }
            case COLUMN_IN, COLUMN_NOT_IN: {
                // NULL ∈ 任意集合都是未知 → 不保留（同 LIKE 分支的三值逻辑口径）
                FieldVector vector = root.getVector(column);
                if (vector == null || vector.isNull(rowIndex)) return false;
                Object v = vector.getObject(rowIndex);
                if (v == null) return false;
                boolean hit = inValues.contains(v.toString());
                return form == Form.COLUMN_IN ? hit : !hit;
            }
            case COLUMN_LIKE, COLUMN_NOT_LIKE: {
                // NULL 与任何模式匹配结果都是未知 → 不保留（SQL 三值逻辑：WHERE 只留 TRUE）
                FieldVector vector = root.getVector(column);
                if (vector == null || vector.isNull(rowIndex)) return false;
                Object v = vector.getObject(rowIndex);
                if (v == null) return false;
                boolean hit = likePattern.matcher(v.toString()).matches();
                return form == Form.COLUMN_LIKE ? hit : !hit;
            }
            default: // UNKNOWN：保守放行
                return true;
        }
    }

    /** 函数形态谓词求值：结果向量由 doNext 对整批求值一次产生。 */
    private boolean evaluateRowFunc(VectorSchemaRoot root, int rowIndex, FieldVector funcResult) {
        if (funcResult == null) {
            return form == Form.FUNC_IS_NULL; // 空批退化：IS NULL 恒真，其余恒假
        }
        switch (form) {
            case FUNC_IS_NOT_NULL:
                return !funcResult.isNull(rowIndex);
            case FUNC_IS_NULL:
                return funcResult.isNull(rowIndex);
            case FUNC_COMPARE: {
                if (funcResult.isNull(rowIndex)) return false;
                Object v = funcResult.getObject(rowIndex);
                if (v == null) return false;
                return ScalarExpr.compare(v.toString(), op, literal);
            }
            case EXPR: {
                // 三值逻辑：NULL（未知）不保留，只留 TRUE —— 与 LIKE / IN 分支同口径
                if (funcResult.isNull(rowIndex)) return false;
                return ExprEvaluator.truthy(funcResult.getObject(rowIndex));
            }
            default:
                return true;
        }
    }

    /**
     * 谓词（单个简单形态）真正引用的列 —— 不建算子也能问的静态入口。
     *
     * <p>与 {@link #referencedColumns()} 同源（同样取自构造期编译结果），故不做「取第一个空格前的
     * token」式猜测：那种做法对 {@code cnt>0} 这类<b>无空格</b>谓词会把整串 {@code "cnt>0"} 当列名、
     * 真列 {@code cnt} 反而不被登记 → 列裁剪把 {@code cnt} 裁掉 → 下游 Filter 找不到列
     * （恒假 → 静默 0 行，或扫描端 Filter 直接报错）。</p>
     *
     * <p>实现上就是个无上游的临时实例：{@link #compile()} 只读谓词原文，不触碰 upstream。</p>
     */
    public static List<String> referencedColumnsOf(String filterExpression) {
        try {
            return new FilterOperator(null, filterExpression).referencedColumns();
        } catch (UnsupportedOperationException e) {
            // 谓词里残留子查询（{@code compile()} 的快速失败）—— 这里只是「能不能下推/裁剪」的
            // 探测，返回空集 = 不可编译，调用方自会拒绝下推/抛错；真正构造算子时仍会快速失败。
            return List.of();
        }
    }

    /** 判定文本是否为字面量（字符串 / 数字 / 关键字字面量），而非列引用。 */
    private static boolean isLiteralToken(String s) {
        String t = s.trim();
        if (t.isEmpty()) return true;
        char c0 = t.charAt(0);
        if (c0 == '\'' || c0 == '"' || c0 == '`') return true;              // 字符串字面量
        if (NUMERIC_LITERAL.matcher(t).matches()) return true;              // 数字字面量
        if (t.equalsIgnoreCase("true") || t.equalsIgnoreCase("false")
                || t.equalsIgnoreCase("null")) return true;                 // SQL 关键字字面量
        // 含空白 / 运算符 / 括号等 → 非裸标识符 → 按字面量保守处理
        return !SqlTextUtil.isSingleIdentifier(t);
    }

    private static final java.util.regex.Pattern NUMERIC_LITERAL =
            java.util.regex.Pattern.compile("[+-]?\\d+(\\.\\d+)?([eE][+-]?\\d+)?");

    /** 谓词里残留的子查询（{@code x IN (SELECT …)} / {@code EXISTS (SELECT …)} / {@code ANY|ALL|SOME}）。 */
    private static final java.util.regex.Pattern SUBQUERY_PREDICATE =
            java.util.regex.Pattern.compile(
                    "(?is)\\b(?:IN|EXISTS|ANY|ALL|SOME)\\s*\\(\\s*SELECT\\b");

    /**
     * 谓词原文里是否还包含子查询（先屏蔽字符串字面量与注释，避免 {@code note = 'IN (SELECT'} 这类误判）。
     */
    private static boolean containsSubqueryPredicate(String expr) {
        String masked = maskLiteralsAndComments(expr);
        return SUBQUERY_PREDICATE.matcher(masked).find();
    }

    /** 把字符串字面量 / 行注释 / 块注释的内容替换成空格（保留长度与位置无关，仅供模式匹配）。 */
    private static String maskLiteralsAndComments(String s) {
        if (s == null || s.isEmpty()) return "";
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                out.append(c);
                int j = i + 1;
                while (j < s.length()) {
                    char d = s.charAt(j);
                    if (d == '\\' && j + 1 < s.length()) {   // 转义（MySQL 风格）
                        j += 2;
                        continue;
                    }
                    if (d == c) {
                        if (j + 1 < s.length() && s.charAt(j + 1) == c) {   // '' 转义
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j++;
                }
                int end = Math.min(j, s.length() - 1);
                for (int k = i + 1; k < end; k++) out.append(' ');
                if (end > i) out.append(c);
                i = end;
            } else if (c == '-' && i + 1 < s.length() && s.charAt(i + 1) == '-') {
                int j = i;
                while (j < s.length() && s.charAt(j) != '\n') { out.append(' '); j++; }
                i = j - 1;
            } else if (c == '/' && i + 1 < s.length() && s.charAt(i + 1) == '*') {
                int j = i;
                out.append("  ");
                j += 2;
                while (j + 1 < s.length() && !(s.charAt(j) == '*' && s.charAt(j + 1) == '/')) {
                    out.append(' ');
                    j++;
                }
                if (j + 1 < s.length()) { out.append("  "); j += 2; }
                i = j - 1;
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    /** 函数在比较符右侧时翻转操作符：{@code 0.5 < func(...)} → {@code func(...) > 0.5}。 */
    private static String reverseOp(String o) {
        return switch (o) {
            case ">" -> "<";
            case ">=" -> "<=";
            case "<" -> ">";
            case "<=" -> ">=";
            case "=", "==", "!=", "<>" -> o;
            default -> o;
        };
    }

}
