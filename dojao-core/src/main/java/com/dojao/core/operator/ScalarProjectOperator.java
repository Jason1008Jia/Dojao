package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.core.function.scalar.ScalarEvaluator;
import com.dojao.core.function.scalar.ScalarExpr;
import com.dojao.core.operator.expr.ExprEvaluator;
import com.dojao.core.operator.expr.ExprNode;
import com.dojao.core.operator.expr.ExprParser;
import com.dojao.core.operator.expr.ExprTypeInference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 标量投影算子 —— 在上游 batch 上逐行求值表达式，并与透传列按输出顺序组装结果页。
 *
 * <p>与 {@link ProjectOperator} 的关系：Project 只做列投影，ScalarProject 在投影之外
 * 还求值表达式列。两类表达式统一由本算子承担：</p>
 * <ul>
 *   <li><b>标量函数调用</b>（{@code cosine_similarity(embedding, '[0.1,...]')}）→
 *       {@link ScalarExpr}/{@link ScalarEvaluator}（含 {@code IF(cond,a,b)} 等注册函数）；</li>
 *   <li><b>通用表达式</b>（{@code CASE WHEN ... END}、算术、{@code AND/OR} 等）→
 *       {@link ExprParser}/{@link ExprEvaluator}（与 GROUP BY 表达式同一子系统）。</li>
 * </ul>
 *
 * <p><b>行外层求值（宽表 JSON 关键优化）</b>：类型可静态确定（{@link ExprTypeInference}）的
 * 通用表达式按「行外层」求值 —— 外层循环行、内层循环投影，配合
 * {@link ExprEvaluator} 的行级 JSON 缓存，同一行的同一段 JSON 在整页只解析一次。
 * 旧实现是「投影外层、逐行内层」，143 个 {@code get_json_object} 投影会把同一行解析 143 次；
 * 行外层后页内解析次数从 行数 × 同列投影数 降为 行数。运行期值类别与静态类型不符时
 * （如 {@code coalesce(int_col, 'x')} 本页全是整数）该投影回退列式整批求值，保证输出类型
 * 与旧口径完全一致。</p>
 *
 * <p>输出列 = 透传列（整页拷贝）+ 计算列（批求值向量），严格按构造传入的
 * {@code outputColumns} 顺序组装，保证 {@code SELECT cosine(...) AS score, id}
 * 这类"计算列在前"的查询结果顺序正确。</p>
 *
 * <p>列裁剪：{@link #expressionColumns()} 返回表达式引用的上游列，
 * {@link #computedColumns()} 返回自身产生的计算列名，供 Planner 精确推算子节点需求。</p>
 *
 * @author jiaxin
 * @date 2026/8/9
 */
public final class ScalarProjectOperator extends AbstractMeasurableOperator {

    /** 标量投影项：SQL 表达式文本 + 输出列名。 */
    public record Projection(String exprText, String outputName) {}

    private static final ArrowType INT64 = new ArrowType.Int(64, true);
    private static final ArrowType FLOAT8 =
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

    /** 投影项求值器：屏蔽"标量函数 / 通用表达式"两种实现的差异。 */
    private interface Evaluator {
        FieldVector evaluate(VectorSchemaRoot root, BufferAllocator allocator, String outputName);
        Collection<String> columnArgs();
    }

    /** 编译后的投影项：求值器 + 通用表达式 AST（{@code node != null} 时可参与行外层求值）。 */
    private static final class Compiled {
        final Projection projection;
        final Evaluator evaluator;
        final ExprNode node;

        Compiled(Projection projection, Evaluator evaluator, ExprNode node) {
            this.projection = projection;
            this.evaluator = evaluator;
            this.node = node;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ScalarProjectOperator.class);

    private Operator upstream;
    private final List<String> outputColumns;   // 结果列（透传列 + 计算列），按 SELECT 顺序
    private final Set<String> computedColumns;  // 计算列名集合
    private final List<Projection> projections; // 表达式列表
    private final List<Compiled> compiled;
    /** 顶层 JSON 函数投影数（含 0 说明本算子无 JSON 投影，收尾不打统计日志）。 */
    private final int jsonProjections;
    /** 行外层诊断统计（算子实例单线程，页内累加，收尾打一行 INFO）。 */
    private long statRows, statRowOuterMax, statFallback, statParses, statEvalMillis;

    public ScalarProjectOperator(Operator upstream,
                                 List<String> outputColumns,
                                 Set<String> computedColumns,
                                 List<Projection> projections) {
        super("ScalarProject", "ScalarProject", 1);
        this.upstream = upstream;
        this.outputColumns = outputColumns;
        this.computedColumns = computedColumns;
        this.projections = projections;
        this.compiled = new ArrayList<>(projections.size());
        for (Projection p : projections) {
            this.compiled.add(compile(p));
        }
        int json = 0;
        for (Compiled c : this.compiled) {
            if (c.node instanceof ExprNode.FuncCall f && ExprEvaluator.isJsonCachedFunction(f.name())) json++;
        }
        this.jsonProjections = json;
    }

    /**
     * 编译单个投影项。默认口径不变：已注册标量函数且参数形态简单 → 标量求值器；否则 →
     * 通用表达式求值器（CASE / IF / 算术 / 函数嵌套），通用表达式解析失败时仍回退标量求值器。
     *
     * <p>★JSON 函数族（{@code get_json_object} / {@code json_extract} / {@code from_str_to_array} /
     * {@code json_array_explode[_with_separator]}）即使命中了注册表快路径也强制走通用表达式：
     * 行级 JSON 缓存只存在于 {@link ExprEvaluator#evalFuncCall}，注册表实现每次调用都会重新
     * {@code readTree} —— 否则行外层求值对顶层 {@code get_json_object} 投影没有任何收益。</p>
     */
    private static Compiled compile(Projection p) {
        ExprNode parsed = null;
        IllegalArgumentException parseError = null;
        try {
            parsed = ExprParser.parse(p.exprText());
        } catch (IllegalArgumentException e) {
            parseError = e;   // 落到下面的标量快路径
        }
        if (parsed instanceof ExprNode.FuncCall f && ExprEvaluator.isJsonCachedFunction(f.name())) {
            return new Compiled(p, genericEvaluator(parsed), parsed);
        }
        Optional<ScalarExpr> fn = ScalarExpr.tryParse(p.exprText());
        if (fn.isPresent() && ScalarExpr.isSimpleArgs(fn.get())) {
            return new Compiled(p, scalarEvaluator(fn.get()), null);
        }
        if (parsed != null) {
            return new Compiled(p, genericEvaluator(parsed), parsed);
        }
        if (fn.isPresent()) return new Compiled(p, scalarEvaluator(fn.get()), null);
        throw new IllegalStateException("投影表达式无法解析: " + p.exprText(), parseError);
    }

    private static Evaluator scalarEvaluator(ScalarExpr expr) {
        ScalarEvaluator scalar = new ScalarEvaluator(expr);
        return new Evaluator() {
            @Override public FieldVector evaluate(VectorSchemaRoot r, BufferAllocator a, String n) {
                return scalar.evaluate(r, a, n);
            }
            @Override public Collection<String> columnArgs() { return scalar.columnArgs(); }
        };
    }

    private static Evaluator genericEvaluator(ExprNode node) {
        ExprEvaluator generic = new ExprEvaluator(node);
        return new Evaluator() {
            @Override public FieldVector evaluate(VectorSchemaRoot r, BufferAllocator a, String n) {
                return generic.evaluate(r, a, n);
            }
            @Override public Collection<String> columnArgs() { return generic.columnArgs(); }
        };
    }

    /** 更新上游算子引用（列裁剪优化使用，须在 {@link #doOpen()} 前调用）。 */
    public void setUpstream(Operator upstream) { this.upstream = upstream; }

    /** 上游算子（scan-sink 形态探测用）。 */
    public Operator upstream() { return upstream; }

    /** 表达式引用的上游列（供列裁剪）。 */
    public Set<String> expressionColumns() {
        Set<String> cols = new LinkedHashSet<>();
        for (Compiled c : compiled) cols.addAll(c.evaluator.columnArgs());
        return cols;
    }

    /** 计算列名集合（自身产生，子节点不包含）。 */
    public Set<String> computedColumns() { return computedColumns; }

    /** 结果列清单（透传列 + 计算列，按顺序）。 */
    public List<String> outputColumns() { return outputColumns; }

    /** 标量投影项（表达式文本 → 输出列名）。 */
    public List<Projection> projections() { return projections; }

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
            if (input.getRowCount() == 0) { input.close(); continue; }

            VectorSchemaRoot inRoot = input.getRoot();
            int rowCount = inRoot.getRowCount();
            BufferAllocator allocator = inRoot.getVector(0).getAllocator();

            // 1. 求值所有表达式 → 计算列向量（先全部求完，再统一组装，避免重复读上游）
            Map<String, FieldVector> computed = new HashMap<>();
            long parsesBefore = ExprEvaluator.jsonParseCount();
            long evalStart = System.nanoTime();
            try {
                computeColumns(inRoot, rowCount, allocator, computed);
            } catch (RuntimeException | Error e) {
                // 求值失败（参数个数错 / 非法输入等）：已算出的计算列与上游页必须就地释放。
                // 否则查询报错后收尾时的 "Memory was leaked by query" 会盖住真正的异常原因。
                for (FieldVector v : computed.values()) {
                    try {
                        v.close();
                    } catch (RuntimeException ignored) {
                        // 释放失败不能盖住原始异常
                    }
                }
                input.close();
                throw e;
            } finally {
                statRows += rowCount;
                statParses += Math.max(0, ExprEvaluator.jsonParseCount() - parsesBefore);
                statEvalMillis += (System.nanoTime() - evalStart) / 1_000_000L;
                // 行级 JSON 缓存持有本页 root / 解析树，页结束即释放（工作线程空闲期不背大 buffer）
                ExprEvaluator.clearJsonCache();
            }

            // 2. 按 outputColumns 顺序组装：透传列整页拷贝，计算列取求值结果
            List<Field> fields = new ArrayList<>();
            List<FieldVector> vectors = new ArrayList<>();
            Map<String, Integer> occurrence = new HashMap<>();
            for (String name : outputColumns) {
                if (computedColumns.contains(name)) {
                    FieldVector v = computed.get(name);
                    fields.add(v.getField());
                    vectors.add(v);
                    continue;
                }
                // 透传列：按出现次序解析（同名列第 k 次 → schema 第 k 个同名向量）
                int k = occurrence.merge(name, 1, Integer::sum);
                FieldVector src = findColumn(inRoot, name, k);
                if (src == null) {
                    throw new IllegalStateException("标量投影透传列不存在: " + name);
                }
                FieldVector dst = src.getField().createVector(allocator);
                dst.setInitialCapacity(rowCount);
                dst.allocateNew();
                org.apache.arrow.vector.util.TransferPair tp = src.makeTransferPair(dst);
                for (int r = 0; r < rowCount; r++) tp.copyValueSafe(r, r);
                dst.setValueCount(rowCount);
                fields.add(src.getField());
                vectors.add(dst);
            }

            VectorSchemaRoot outRoot = new VectorSchemaRoot(fields, vectors, rowCount);
            input.close();
            return Page.fromRoot(outRoot);
        }
    }

    /**
     * 计算全部表达式列：可静态定型且与运行期类型阶梯一致的投影走<b>行外层</b>求值
     * （同一行的 JSON 解析结果跨投影共享），不一致的投影回退列式整批求值。
     */
    private void computeColumns(VectorSchemaRoot inRoot, int rowCount, BufferAllocator allocator,
                                Map<String, FieldVector> computed) {
        Map<String, Integer> indexOf = ExprEvaluator.indexOf(inRoot);
        Map<String, Field> inFields = new HashMap<>();
        for (Field f : inRoot.getSchema().getFields()) inFields.putIfAbsent(f.getName(), f);

        List<RowOuter> rowOuter = new ArrayList<>();
        List<Compiled> columnWise = new ArrayList<>();
        for (Compiled c : compiled) {
            String name = c.projection.outputName();
            ArrowType declared = c.node == null ? null
                    : rowOuterType(ExprTypeInference.infer(c.node, inFields));
            if (declared == null) {
                columnWise.add(c);
                continue;
            }
            FieldVector out = ExprTypeInference.toField(name, declared, true).createVector(allocator);
            out.setInitialCapacity(rowCount);
            out.allocateNew();
            if (out instanceof ListVector lv) {
                // 元素总数未知（行外层没有第一遍统计）→ 少预分配、按 setSafe 自动扩容
                FieldVector child = lv.getDataVector();
                child.setInitialCapacity(Math.max(1, rowCount));
                child.allocateNew();
            }
            computed.put(name, out);
            rowOuter.add(new RowOuter(c, out, declared));
        }

        statRowOuterMax = Math.max(statRowOuterMax, rowOuter.size());
        if (!rowOuter.isEmpty()) {
            // 行外层：外层行、内层投影 —— 同一 (root,row) 的 JSON 只解析一次，
            // 页内解析次数 行数 × 同列投影数 → 行数（143 个投影即 143 倍）
            for (int r = 0; r < rowCount; r++) {
                for (RowOuter ro : rowOuter) {
                    if (ro.fallback) continue;
                    ro.write(r, ExprEvaluator.evalRow(ro.compiled.node, inRoot, r, indexOf));
                }
            }
            // 类型守卫：本页实际值口径与静态声明不符 → 放弃行外层结果，回退列式（保持旧输出类型）
            for (RowOuter ro : rowOuter) {
                if (ro.finish(rowCount, inRoot, indexOf)) continue;
                statFallback++;
                computed.remove(ro.compiled.projection.outputName());
                ro.out.close();
                columnWise.add(ro.compiled);
            }
        }

        for (Compiled c : columnWise) {
            computed.put(c.projection.outputName(),
                    c.evaluator.evaluate(inRoot, allocator, c.projection.outputName()));
        }
    }

    /**
     * 行外层可安全预建向量的静态类型 —— 只接受与 {@link ExprEvaluator} 运行期类型阶梯
     * 一一对应的形态（Int64 / Float8 / Utf8 / Bool / List）。DECIMAL 的精度与小数位依赖
     * 数据（列式求值按实际值定标），其余类型无法可靠写入 → 返回 null，投影留在列式求值。
     */
    private static ArrowType rowOuterType(ArrowType t) {
        if (t == null) return null;
        if (t instanceof ArrowType.Int i && i.getBitWidth() == 64 && i.getIsSigned()) return INT64;
        if (t instanceof ArrowType.FloatingPoint fp
                && fp.getPrecision() == FloatingPointPrecision.DOUBLE) return FLOAT8;
        if (t instanceof ArrowType.Utf8) return ArrowType.Utf8.INSTANCE;
        if (t instanceof ArrowType.Bool) return ArrowType.Bool.INSTANCE;
        if (t instanceof ArrowType.List) return ArrowType.List.INSTANCE;
        return null;
    }

    /**
     * 行外层求值状态：预建结果向量 + 逐行写入 + 运行期类型校验。
     *
     * <p>静态类型只决定「预建什么向量」；本页实际非空值的类别若与声明不符（多态函数被
     * 数据选中了别的分支等），置 {@link #fallback}，整列交给列式求值重算 —— 输出类型与
     * 旧口径逐字节一致。</p>
     */
    private static final class RowOuter {
        final Compiled compiled;
        final FieldVector out;
        final ArrowType declared;
        private boolean anyList, anyStr, anyFloat, anyDecimal, anyInt, anyBool;
        private int childIndex;
        private boolean fallback;

        RowOuter(Compiled compiled, FieldVector out, ArrowType declared) {
            this.compiled = compiled;
            this.out = out;
            this.declared = declared;
        }

        /** 写一行；值类别与声明类型不兼容 → 置 fallback，后续行不再写（整列回退列式求值）。 */
        void write(int row, Object v) {
            if (fallback) return;
            if (v == null) {
                if (out instanceof ListVector lv) {
                    lv.startNewValue(row);
                    lv.endValue(row, 0);
                }
                out.setNull(row);
                return;
            }
            try {
                if (out instanceof ListVector lv) {
                    if (!(v instanceof List<?> values)) {
                        fallback = true;
                        return;
                    }
                    anyList = true;
                    lv.startNewValue(row);
                    VarCharVector child = (VarCharVector) lv.getDataVector();
                    for (Object o : values) {
                        if (o == null) child.setNull(childIndex);
                        else child.setSafe(childIndex,
                                ExprEvaluator.string(o).getBytes(StandardCharsets.UTF_8));
                        childIndex++;
                    }
                    lv.endValue(row, values.size());
                    return;
                }
                if (v instanceof List<?>) {   // 声明不是 LIST 却算出数组 → 列式口径可能落 LIST
                    fallback = true;
                    return;
                }
                markCategory(v);
                if (out instanceof VarCharVector vc) {
                    vc.setSafe(row, ExprEvaluator.string(v).getBytes(StandardCharsets.UTF_8));
                } else if (out instanceof Float8Vector f) {
                    f.setSafe(row, ExprEvaluator.asDouble(v));
                } else if (out instanceof BigIntVector b) {
                    b.setSafe(row, ExprEvaluator.asLong(v));
                } else if (out instanceof BitVector bt) {
                    bt.setSafe(row, ExprEvaluator.truthy(v) ? 1 : 0);
                } else {
                    fallback = true;
                }
            } catch (RuntimeException e) {
                fallback = true;   // 值与声明类型不兼容：交给列式求值抛原始错误
            }
        }

        /** 与 {@link ExprEvaluator#evaluateTo} 的逐值类别标记同一口径。 */
        private void markCategory(Object v) {
            if (v instanceof BigDecimal) anyDecimal = true;
            else if (v instanceof Double || v instanceof Float) anyFloat = true;
            else if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
                anyInt = true;
            } else if (v instanceof Boolean) anyBool = true;
            else anyStr = true;
        }

        /**
         * 收尾：本页值类别推出的结果类型与静态声明一致 → 设值数并保留；否则返回 false，
         * 由调用方 close 并回退列式求值。
         */
        boolean finish(int rowCount, VectorSchemaRoot inRoot, Map<String, Integer> indexOf) {
            if (fallback) return false;
            ArrowType actual;
            if (anyList) actual = ArrowType.List.INSTANCE;
            else if (anyStr) actual = ArrowType.Utf8.INSTANCE;
            else if (anyFloat) actual = FLOAT8;
            else if (anyDecimal) return false;   // DECIMAL 精度/小数位依赖数据 → 列式口径才是权威
            else if (anyInt) actual = INT64;
            else if (anyBool) actual = ArrowType.Bool.INSTANCE;
            else actual = ExprEvaluator.staticNullType(compiled.node, inRoot, indexOf);
            if (!declared.equals(actual)) return false;
            if (out instanceof ListVector lv) {
                lv.getDataVector().setValueCount(childIndex);
            }
            out.setValueCount(rowCount);
            return true;
        }
    }

    /**
     * 取 schema 中第 occurrence 个名为 name 的向量；不存在返回 null。
     *
     * <p><b>出现次数回退</b>：投影的输出列允许同一源列重复出现（{@code SELECT s1 AS users,
     * s1 AS base, s1 AS prev} 的 outCols = [s1, s1, s1]），而列裁剪后的上游往往只保留该列的
     * <b>一个</b>物理副本 ⇒ 第 2/3 次查找必须回退到最后一个同名向量，否则运行期直接抛
     * 「标量投影透传列不存在」。真正存在多份同名列时（join 的 {@code __jk_*} 已在上游消歧）
     * 仍按序号精确定位。</p>
     */
    private static FieldVector findColumn(VectorSchemaRoot root, String name, int occurrence) {
        List<Field> schemaFields = root.getSchema().getFields();
        int seen = 0;
        FieldVector last = null;
        for (int i = 0; i < schemaFields.size(); i++) {
            if (schemaFields.get(i).getName().equals(name)) {
                seen++;
                last = root.getVector(i);
                if (seen == occurrence) return last;
            }
        }
        return last;   // 出现次数超过物理列数 → 复用最后一个同名向量
    }

    @Override
    protected void doClose() throws Exception {
        if (jsonProjections > 0 && statRows > 0) {
            // JSON解析 ≈ 行数 ⇒ 行外层共享生效；≈ 行数 × 投影数 ⇒ 掉回了列式路径（需排查）
            log.info("[ScalarProject] JSON 投影统计: 行数={} JSON投影={}/{} 行外层={} 类型回退={}次 "
                            + "JSON解析={} 求值耗时={}ms",
                    statRows, jsonProjections, projections.size(), statRowOuterMax,
                    statFallback, statParses, statEvalMillis);
        }
        upstream.close();
    }
}
