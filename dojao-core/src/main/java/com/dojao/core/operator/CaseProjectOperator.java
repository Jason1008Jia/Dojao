package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.core.function.scalar.ScalarExpr;
import com.dojao.core.operator.expr.ExprEvaluator;
import com.dojao.core.operator.expr.ExprNode;
import com.dojao.core.operator.expr.ExprParser;
import com.dojao.core.planner.SqlTextUtil;
import com.dojao.core.operator.expr.ExprTypeInference;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * CASE 投影算子 —— 在上游页面上逐行计算 {@code CASE WHEN cond THEN x ELSE y END}，把结果作为
 * 派生列<b>追加</b>到页面。Arrow 15 的 {@link VectorSchemaRoot} 不可就地加列（addVector 返回新
 * root 且只能在已有槽位插入），故重建 root：原列零拷贝 transfer + 派生列（零整页数据拷贝）。
 *
 * <p>供聚合参数改写使用：{@code SUM(CASE WHEN rating >= 5 THEN 1 ELSE 0 END)} →
 * CaseProject 先逐行算出派生列 {@code __case_0} → 聚合改写为 {@code SUM(__case_0)}。</p>
 *
 * <p>条件优先 {@code col op literal} / {@code col IN (lit,...)} 快路径（复用 {@link FilterOperator}
 * 的比较逻辑），形状不符则回退<b>通用表达式</b>（{@code rating-1 > 3}、{@code a > b}、{@code AND/OR/NOT}、
 * {@code BETWEEN}、{@code NOT IN}、{@code IS NULL}、函数调用 ...，由 {@link ExprParser} 解析、
 * {@link ExprEvaluator} 逐行求值）；THEN / ELSE 支持数值字面量、NULL、列引用或算术/函数表达式
 * （如 {@code rating-1}）。全为整数字面量（无列引用）→ BigInt 派生列；含列引用/表达式时按
 * <b>实际求值结果</b>定类型：出现字符串（{@code concat}/{@code if(...,'x','y')} 等）→ Utf8
 * 派生列，否则 → double 派生列（旧口径）。逐行求值、与分区无关 → 分布式路径可在 Worker
 * 本地计算（consumer 侧），co-partitioning 不变量不受影响，结果与单机一致。</p>
 *
 * @author jiaxin
 * @date 2026/8/21
 */
public final class CaseProjectOperator extends AbstractMeasurableOperator {

    private Operator upstream;
    private final List<CompiledCase> compiled;

    public CaseProjectOperator(Operator upstream, List<CaseProjection> projections) {
        super("CaseProject", "CaseProject", 1);
        this.upstream = upstream;
        this.compiled = new ArrayList<>();
        for (CaseProjection p : projections) {
            compiled.add(CompiledCase.compile(p));
        }
    }

    /** 更新上游算子引用（列裁剪优化使用，须在 {@link #doOpen()} 前调用）。 */
    public void setUpstream(Operator upstream) { this.upstream = upstream; }

    /** 派生列名集合（自身产生，子节点不包含，供列裁剪）。 */
    public Set<String> computedColumns() {
        Set<String> cols = new LinkedHashSet<>();
        for (CompiledCase c : compiled) cols.add(c.derivedColumn);
        return cols;
    }

    /** 条件列 + 结果值列引用的上游列（供列裁剪，必须保留）。 */
    public Set<String> conditionColumns() {
        Set<String> cols = new LinkedHashSet<>();
        for (CompiledCase c : compiled) cols.addAll(c.referencedColumns());
        return cols;
    }

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
            if (input.getRoot() == null) return input;   // 分区结束标记：透传
            int rowCount = input.getRowCount();
            if (rowCount == 0) { input.close(); continue; }
            VectorSchemaRoot inRoot = input.getRoot();
            BufferAllocator allocator = inRoot.getVector(0).getAllocator();

            // 1) 先求值派生列：读原始向量（此时尚未被掏空）
            List<FieldVector> derived = new ArrayList<>(compiled.size());
            for (CompiledCase c : compiled) {
                derived.add(c.evaluate(inRoot, allocator, rowCount));
            }

            // 2) 零拷贝 transfer 原始列到新向量（掏空原向量，与 Page.select 同款）。
            //    重建 root：原列 + 派生列（Arrow 15 无就地 addVector）。
            List<Field> fields = new ArrayList<>(inRoot.getSchema().getFields());
            List<FieldVector> vectors = new ArrayList<>(fields.size() + derived.size());
            for (int i = 0; i < inRoot.getFieldVectors().size(); i++) {
                FieldVector src = inRoot.getVector(i);
                FieldVector dst = fields.get(i).createVector(allocator);
                src.makeTransferPair(dst).transfer();
                // 直接 transfer 不推进 lastSet：置位使 setValueCount 的 fillHoles 变 no-op，
                // 避免 var offset 被清零（与 Page.select 零拷贝分支同款）
                if (dst instanceof BaseVariableWidthVector dv) dv.setLastSet(rowCount - 1);
                dst.setValueCount(rowCount);
                vectors.add(dst);
            }
            for (FieldVector d : derived) {
                fields.add(d.getField());
                vectors.add(d);
            }

            // 3) 新 root + 新 Page（保留 partitionId / batchIndex）；旧页已掏空，close 安全
            VectorSchemaRoot newRoot = new VectorSchemaRoot(fields, vectors, rowCount);
            input.close();
            return input.getPartitionId()
                    .map(pid -> (Page) Page.partitioned(newRoot, pid))
                    .orElseGet(() -> Page.fromRoot(newRoot, input.getBatchIndex()));
        }
    }

    @Override
    protected void doClose() throws Exception {
        upstream.close();
    }

    /** 编译后的单个 CASE：有序条件分支 + ELSE 兜底，条件/结果均支持字面量与列引用。 */
    private static final class CompiledCase {
        final CaseProjection projection;       // 原始定义（静态类型推断用）
        final String derivedColumn;
        final List<CompiledBranch> branches;   // 顺序短路：首个命中生效
        final ValueRef elseVal;                // 全不命中 → 此值（null = NULL）
        final boolean intMode;                 // true → BigIntVector；false → Float8Vector
        final boolean condExpr;                // 条件含通用表达式（需列名→下标映射逐行求值）

        CompiledCase(CaseProjection projection, List<CompiledBranch> branches, ValueRef elseVal,
                     boolean intMode, boolean condExpr) {
            this.projection = projection;
            this.derivedColumn = projection.derivedColumn();
            this.branches = branches;
            this.elseVal = elseVal;
            this.intMode = intMode;
            this.condExpr = condExpr;
        }

        static CompiledCase compile(CaseProjection p) {
            List<CompiledBranch> bs = new ArrayList<>(p.branches().size());
            boolean allIntLiterals = true;
            boolean anyColumnRef = false;
            boolean condExpr = false;
            for (CaseProjection.CaseBranch br : p.branches()) {
                CompiledCond cond = CompiledCond.compile(br.condition());
                if (cond.expr != null) condExpr = true;
                ValueRef v = compileValue(br.value());
                if (v.referencesColumn()) anyColumnRef = true;
                else if (v.doubleVal != null && v.longVal == null) allIntLiterals = false;   // 小数
                bs.add(new CompiledBranch(cond, v));
            }
            ValueRef elseV = compileValue(p.elseValue());
            if (elseV.referencesColumn()) anyColumnRef = true;
            else if (elseV.doubleVal != null && elseV.longVal == null) allIntLiterals = false;
            // 整数模式：全为整数字面量（含 NULL），无列引用/表达式 → BigInt 派生列（与 v1 语义一致）；
            // 出现小数 / 列引用 / 表达式 → double 派生列（列引用与表达式结果均转 double）。
            // 条件只决定命中哪一支，不参与派生列类型推断 —— 表达式条件不影响 intMode。
            boolean intMode = !anyColumnRef && allIntLiterals;
            return new CompiledCase(p, bs, elseV, intMode, condExpr);
        }

        FieldVector evaluate(VectorSchemaRoot root, BufferAllocator allocator, int rowCount) {
            // ★DECIMAL 派生列必须按<b>静态类型</b>建 DecimalVector（而不是按本页值猜成 Float8）：
            //   ① CAST(x AS DECIMAL(p,s)) / decimal 列参与算术（SUM(a-b)）的精度靠它保持；
            //   ② 类型不能随页变化（整页全 NULL 时若退回 Float8，列型会在页间漂移）。
            ArrowType declared = declaredType(root);
            if (declared instanceof ArrowType.Decimal d) {
                DecimalVector v = new DecimalVector(derivedColumn, allocator, d.getPrecision(), d.getScale());
                v.allocateNew(rowCount);
                Map<String, Integer> idx = ExprEvaluator.indexOf(root);
                for (int r = 0; r < rowCount; r++) {
                    Object x = evalValue(root, r, idx);
                    if (x == null) v.setNull(r);
                    else AbstractSpillableOperator.setVectorValue(v, r, x);
                }
                v.setValueCount(rowCount);
                return v;
            }
            // ★整数派生列（CAST(x AS BIGINT) / 整数算术 / 整数 CASE）同样按静态类型建 BIGINT 向量，
            //   不能回退成 Float8 —— 否则 sum(CAST(x AS BIGINT)) 会以 double 累加/展示
            //   （8.076270371E9），且大整数超过 2^53 后直接丢精度。
            if (declared instanceof ArrowType.Int) {
                BigIntVector v = new BigIntVector(derivedColumn, allocator);
                v.allocateNew(rowCount);
                Map<String, Integer> idx = ExprEvaluator.indexOf(root);
                for (int r = 0; r < rowCount; r++) {
                    Object x = evalValue(root, r, idx);
                    if (x == null) v.setNull(r);
                    else AbstractSpillableOperator.setVectorValue(v, r, x);
                }
                v.setValueCount(rowCount);
                return v;
            }
            // 列名→下标映射仅在有表达式（条件或结果值）时建；纯快路径零开销。
            Map<String, Integer> indexOf = (intMode && !condExpr) ? null : ExprEvaluator.indexOf(root);
            if (intMode) {
                BigIntVector v = new BigIntVector(derivedColumn, allocator);
                v.allocateNew(rowCount);
                for (int r = 0; r < rowCount; r++) {
                    ValueRef val = pick(root, r, indexOf);
                    if (val == null || val.isNull() || (val.isColumn() && root.getVector(val.column).isNull(r))) {
                        v.setNull(r);
                    } else {
                        v.setSafe(r, val.longVal != null ? val.longVal : readLong(root, val, r));
                    }
                }
                v.setValueCount(rowCount);
                return v;
            }
            // 非整数模式：先逐行求出原始值，再按实际结果类选向量类型。
            // ★分支结果可以是字符串（concat(province, city) / if(cond,'y','n')）—— 若无条件按
            //   Float8 写，会把字符串送进 asDouble 直接抛「非数值无法转 double」（B4）。
            Object[] values = new Object[rowCount];
            boolean numericOnly = true;
            for (int r = 0; r < rowCount; r++) {
                Object x = evalValue(root, r, indexOf);
                values[r] = x;
                if (x != null && !(x instanceof Number) && !(x instanceof Boolean)) numericOnly = false;
            }
            if (!numericOnly) {
                VarCharVector v = new VarCharVector(derivedColumn, allocator);
                v.allocateNew(rowCount);
                for (int r = 0; r < rowCount; r++) {
                    Object x = values[r];
                    if (x == null) v.setNull(r);
                    else if (x instanceof byte[] b) v.setSafe(r, b);
                    else v.setSafe(r, ExprEvaluator.string(x).getBytes(StandardCharsets.UTF_8));
                }
                v.setValueCount(rowCount);
                return v;
            }
            Float8Vector v = new Float8Vector(derivedColumn, allocator);
            v.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                Object x = values[r];
                if (x == null) v.setNull(r);
                else if (x instanceof Number n) v.setSafe(r, n.doubleValue());
                else v.setSafe(r, ExprEvaluator.asDouble(x));
            }
            v.setValueCount(rowCount);
            return v;
        }

        /** 派生列的静态类型（按本页上游列元数据推断；推断不出 → null 走运行期旧逻辑）。 */
        private ArrowType declaredType(VectorSchemaRoot root) {
            Map<String, Field> fields = new HashMap<>();
            for (int i = 0; i < root.getFieldVectors().size(); i++) {
                Field f = root.getVector(i).getField();
                fields.putIfAbsent(f.getName(), f);
            }
            return ExprTypeInference.inferProjection(projection, fields);
        }

        /** 单行原始值：表达式逐行求值 / 列引用取值 / 字面量；NULL 与缺失列 → null。 */
        private Object evalValue(VectorSchemaRoot root, int row, Map<String, Integer> indexOf) {
            ValueRef val = pick(root, row, indexOf);
            if (val == null || val.isNull()) return null;
            if (val.expr() != null) return ExprEvaluator.evalRow(val.expr(), root, row, indexOf);
            if (val.isColumn()) {
                FieldVector vec = root.getVector(val.column);
                return vec == null || vec.isNull(row) ? null : vec.getObject(row);
            }
            return val.longVal() != null ? val.longVal() : val.doubleVal();
        }


        /** 求值依赖的上游列：所有分支条件列 + 结果值/ELSE 的列引用与表达式源列（供列裁剪保留）。 */
        Set<String> referencedColumns() {
            Set<String> cols = new LinkedHashSet<>();
            for (CompiledBranch b : branches) {
                if (b.cond.expr != null) cols.addAll(new ExprEvaluator(b.cond.expr).columnArgs());
                else cols.add(b.cond.column);
                if (b.value.isColumn()) cols.add(b.value.column());
                if (b.value.expr() != null) cols.addAll(new ExprEvaluator(b.value.expr()).columnArgs());
            }
            if (elseVal.isColumn()) cols.add(elseVal.column());
            if (elseVal.expr() != null) cols.addAll(new ExprEvaluator(elseVal.expr()).columnArgs());
            return cols;
        }

        /** 按顺序取首个命中的分支结果值；全不命中 → ELSE。 */
        private ValueRef pick(VectorSchemaRoot root, int row, Map<String, Integer> indexOf) {
            for (CompiledBranch b : branches) {
                if (b.cond.matches(root, row, indexOf)) return b.value;
            }
            return elseVal;
        }

        private long readLong(VectorSchemaRoot root, ValueRef v, int row) {
            Object o = root.getVector(v.column).getObject(row);
            if (o instanceof Number n) return n.longValue();
            return Long.parseLong(o.toString().trim());
        }
    }

    /** 单个 WHEN 分支：条件 + 结果值。 */
    private static final class CompiledBranch {
        final CompiledCond cond;
        final ValueRef value;

        CompiledBranch(CompiledCond cond, ValueRef value) {
            this.cond = cond;
            this.value = value;
        }
    }

    /** 结果值：数值字面量常量、列引用（逐行取列值）或表达式（逐行求值，如 {@code rating-1}）。 */
    private record ValueRef(String text, Long longVal, Double doubleVal, String column, ExprNode expr) {
        boolean isColumn() { return column != null; }
        /** 是否引用上游列（裸列引用或表达式内含列引用）——决定 double 派生列 + 列裁剪保留。 */
        boolean referencesColumn() { return column != null || expr != null; }
        boolean isNull() { return text == null; }
    }

    /**
     * 条件：优先 {@code col op literal} / {@code col IN (lit,...)} / {@code col [NOT] LIKE '模式'}
     * 快路径（字符串比较与 LIKE 语义，与 {@link FilterOperator} 一致，形状判定收在
     * {@link SqlTextUtil#parseLike}）；形状不符（列对列、算术、AND/OR、BETWEEN、NOT IN、IS NULL、
     * 函数调用 ...）→ 回退通用表达式逐行求值（{@link ExprParser}/{@link ExprEvaluator}）。
     */
    private static final class CompiledCond {
        final String column;        // 快路径条件列（已去限定符）
        final String op;            // 比较操作符；null = IN / LIKE / 表达式模式
        final String literal;       // 比较字面量（已去引号）；null = IN / LIKE / 表达式模式
        final Set<String> inValues; // IN 模式：命中集合
        final ExprNode expr;        // 表达式模式：整条条件表达式（null = 快路径）
        final SqlTextUtil.LikeSpec like;  // LIKE 模式：非 null 时 column 为左列（NULL 恒不命中）

        private CompiledCond(String column, String op, String literal, Set<String> inValues,
                             ExprNode expr, SqlTextUtil.LikeSpec like) {
            this.column = column;
            this.op = op;
            this.literal = literal;
            this.inValues = inValues;
            this.expr = expr;
            this.like = like;
        }

        static CompiledCond compile(String cond) {
            // 快路径 1：col op literal —— 仅当左侧是裸列、右侧是简单字面量才成立。形状不符
            // （a = b、rating-1 > 3 ...）必须回退表达式，否则 "a = b" 会被当成「a 与字符串 'b'
            // 比较」静默算错。
            ScalarExpr.Comparison c = ScalarExpr.splitComparison(cond).orElse(null);
            if (c != null && SqlTextUtil.isColumnRef(c.left()) && ScalarExpr.isLiteral(c.right())) {
                return new CompiledCond(SqlTextUtil.stripQualifier(c.left()), c.op(),
                        ScalarExpr.unquote(c.right()), null, null, null);
            }
            // 快路径 2：col IN (lit, ...) —— 左侧须是裸列（排除 "x NOT IN ..."、"(a+b) IN ..."）
            if (c == null) {
                int inIdx = cond.toUpperCase(Locale.ROOT).indexOf(" IN (");
                if (inIdx > 0 && SqlTextUtil.isColumnRef(cond.substring(0, inIdx).trim())) {
                    return inCond(cond, inIdx);
                }
            }
            // 快路径 3：col [NOT] LIKE '字面量模式' —— 形状判定收在 SqlTextUtil.parseLike，
            // 与 FilterOperator / Planner 的 CASE 条件校验与列收集是同一份实现（三处不得漂移）。
            SqlTextUtil.LikeSpec like = SqlTextUtil.parseLike(cond);
            if (like != null) {
                return new CompiledCond(like.column(), null, null, null, null, like);
            }
            // 通用表达式：rating-1 > 3、a > b、AND/OR/NOT、BETWEEN、NOT IN、IS NULL、函数调用 ...
            ExprNode expr = parseExprOrNull(cond);
            if (expr != null) return new CompiledCond(null, null, null, null, expr, null);
            throw new IllegalStateException("CASE 条件无法解析: " + cond);
        }

        /** IN 快路径：解析左列 + 命中集合。 */
        private static CompiledCond inCond(String cond, int inIdx) {
            String column = SqlTextUtil.stripQualifier(cond.substring(0, inIdx).trim());
            int open = cond.indexOf('(', inIdx);
            int close = SqlTextUtil.findMatchingParen(cond, open);
            String list = cond.substring(open + 1, close).trim();
            Set<String> vals = new LinkedHashSet<>();
            for (String it : ScalarExpr.splitTopLevelComma(list)) {
                String v = ScalarExpr.unquote(it.trim());
                if (!v.equalsIgnoreCase("NULL")) vals.add(v);
            }
            return new CompiledCond(column, null, null, vals, null, null);
        }

        /** 条件求值：快路径比较 或 通用表达式逐行求值（列缺失 / 空值 / 不可判定 → false）。 */
        boolean matches(VectorSchemaRoot root, int row, Map<String, Integer> indexOf) {
            if (expr != null) {
                return ExprEvaluator.truthy(ExprEvaluator.evalRow(expr, root, row, indexOf));
            }
            FieldVector condVec = root.getVector(column);
            if (condVec == null || condVec.isNull(row)) return false;
            Object val = condVec.getObject(row);
            if (val == null) return false;
            if (like != null) return like.matches(val);
            String s = val.toString();
            if (inValues != null) return inValues.contains(s);
            return ScalarExpr.compare(s, op, literal);
        }
    }

    /** 编译结果值：数值字面量常量、列引用或算术/函数表达式。 */
    private static ValueRef compileValue(String v) {
        if (v == null) return new ValueRef(null, null, null, null, null);   // NULL
        String t = v.trim();
        // 空串 / "NULL" 文本一律按 NULL：传输层可空字符串字段被 proto3 空串化后，若读侧还原点漏一处，
        // 本意 NULL 的值会以 "" 到达 —— 空串不是数字、不是 Java null，会被下面误编译成"幽灵列引用"
        // （column=""）→ intMode 翻转 → 非命中行 root.getVector("") 返回 null → NPE。此处兜底杜绝。
        if (t.isEmpty() || t.equalsIgnoreCase("NULL")) {
            return new ValueRef(null, null, null, null, null);
        }
        Long l = parseLong(t);
        if (l != null) return new ValueRef(t, l, l.doubleValue(), null, null);
        Double d = parseDouble(t);
        if (d != null) return new ValueRef(t, null, d, null, null);
        // 算术 / 函数表达式（rating-1、rating*2、a+b ...）→ 逐行求值。
        // 裸列引用（ExprNode.Column）不在此分支，仍走下面的 column 路径，语义/类型与 v1 完全一致。
        ExprNode expr = parseExprOrNull(t);
        if (expr != null && !(expr instanceof ExprNode.Column)) {
            return new ValueRef(t, null, null, null, expr);
        }
        return new ValueRef(t, null, null, SqlTextUtil.stripQualifier(t), null);   // 列引用
    }

    /** 尝试把文本解析为标量表达式；不可解析返回 null（回退到列引用路径）。 */
    private static ExprNode parseExprOrNull(String text) {
        try {
            return ExprParser.parse(text);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static Long parseLong(String s) {
        if (s == null) return null;
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double parseDouble(String s) {
        if (s == null) return null;
        try {
            return Double.parseDouble(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
