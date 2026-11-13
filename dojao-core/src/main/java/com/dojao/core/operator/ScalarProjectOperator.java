package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.core.function.scalar.ScalarEvaluator;
import com.dojao.core.function.scalar.ScalarExpr;
import com.dojao.core.operator.expr.ExprEvaluator;
import com.dojao.core.operator.expr.ExprNode;
import com.dojao.core.operator.expr.ExprParser;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

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

    /** 投影项求值器：屏蔽"标量函数 / 通用表达式"两种实现的差异。 */
    private interface Evaluator {
        FieldVector evaluate(VectorSchemaRoot root, BufferAllocator allocator, String outputName);
        Collection<String> columnArgs();
    }

    private Operator upstream;
    private final List<String> outputColumns;   // 结果列（透传列 + 计算列），按 SELECT 顺序
    private final Set<String> computedColumns;  // 计算列名集合
    private final List<Projection> projections; // 表达式列表
    private final List<Evaluator> evaluators;

    public ScalarProjectOperator(Operator upstream,
                                 List<String> outputColumns,
                                 Set<String> computedColumns,
                                 List<Projection> projections) {
        super("ScalarProject", "ScalarProject", 1);
        this.upstream = upstream;
        this.outputColumns = outputColumns;
        this.computedColumns = computedColumns;
        this.projections = projections;
        this.evaluators = new ArrayList<>();
        for (Projection p : projections) {
            this.evaluators.add(compileEvaluator(p.exprText()));
        }
    }

    /**
     * 编译单个投影项：已注册标量函数且参数形态简单 → 标量求值器；否则 → 通用表达式求值器
     * （CASE / IF / 算术）。通用表达式解析失败时仍回退标量求值器，避免既有查询倒退。
     */
    private static Evaluator compileEvaluator(String exprText) {
        Optional<ScalarExpr> fn = ScalarExpr.tryParse(exprText);
        if (fn.isPresent() && ScalarExpr.isSimpleArgs(fn.get())) {
            return scalarEvaluator(fn.get());
        }
        try {
            return genericEvaluator(ExprParser.parse(exprText));
        } catch (IllegalArgumentException e) {
            if (fn.isPresent()) return scalarEvaluator(fn.get());
            throw new IllegalStateException("投影表达式无法解析: " + exprText, e);
        }
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
        for (Evaluator e : evaluators) cols.addAll(e.columnArgs());
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

            // 1. 求值所有标量表达式 → 计算列向量（先全部求完，再统一组装，避免重复读上游）
            Map<String, FieldVector> computed = new HashMap<>();
            for (int i = 0; i < projections.size(); i++) {
                Projection p = projections.get(i);
                computed.put(p.outputName(),
                        evaluators.get(i).evaluate(inRoot, allocator, p.outputName()));
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
        upstream.close();
    }
}
