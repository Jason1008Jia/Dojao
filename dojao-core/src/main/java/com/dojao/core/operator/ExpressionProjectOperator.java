package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.core.operator.expr.ExprEvaluator;
import com.dojao.core.operator.expr.ExprNode;
import com.dojao.core.operator.expr.ExprParser;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 分组键表达式投影算子 —— 在上游页面上逐行求值 {@code GROUP BY <表达式>}，把每个表达式算出的
 * 派生列<b>追加</b>到页面（列头 = {@link GroupKeyExpression#derivedColumn()}）。Arrow 15 的
 * {@link VectorSchemaRoot} 不可就地加列，故重建 root：原列零拷贝 transfer + 派生列（零整页数据拷贝，
 * 与 {@link CaseProjectOperator} 同款）。
 *
 * <p><b>分区前物化（本算子存在的根本原因）</b>：group key 表达式结果是分区键，必须在 Worker
 * <b>Producer 侧、{@code HashPartitionOperator} 之前</b>先算出派生列，否则同值行会按源列散到不同
 * 分区 → 每个分组跨分区不完整 → 聚合计数错误。故本算子随 Producer 片段序列化（group key 派生列
 * 只在扫描后、分区前追加）。{@code SUM(CASE WHEN ...)} 的 CASE 派生列在 Consumer 聚合侧算（分区无关），
 * 两者互不影响。</p>
 *
 * <p>逐行求值内核 = {@link ExprEvaluator}（算术 + 内置/已注册标量函数 + CASE WHEN，类型跨行稳定），
 * 表达式引用的列须在扫描页面中（规划期已 {@code requireColumn} 校验）。</p>
 *
 * @author jiaxin
 * @date 2026/9/6
 */
public final class ExpressionProjectOperator extends AbstractMeasurableOperator {

    private Operator upstream;
    private final List<CompiledExpression> compiled;

    public ExpressionProjectOperator(Operator upstream, List<GroupKeyExpression> expressions) {
        super("ExpressionProject", "ExpressionProject", 1);
        this.upstream = upstream;
        this.compiled = new ArrayList<>(expressions.size());
        for (GroupKeyExpression gk : expressions) {
            compiled.add(CompiledExpression.compile(gk));
        }
    }

    /** 更新上游算子引用（列裁剪优化使用，须在 {@link #doOpen()} 前调用）。 */
    public void setUpstream(Operator upstream) { this.upstream = upstream; }

    /** 派生列名集合（自身产生，子节点不包含，供列裁剪）。 */
    public Set<String> computedColumns() {
        Set<String> cols = new LinkedHashSet<>();
        for (CompiledExpression c : compiled) cols.add(c.derivedColumn);
        return cols;
    }

    /** 表达式引用的全部上游列（供列裁剪，必须保留）。 */
    public Set<String> sourceColumns() {
        Set<String> cols = new LinkedHashSet<>();
        for (CompiledExpression c : compiled) cols.addAll(c.columnArgs);
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
            for (CompiledExpression c : compiled) {
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

    /** 编译后的单个分组键表达式：派生列名 + 通用表达式求值器（AST 解析一次、逐页求值）。 */
    private static final class CompiledExpression {
        final String derivedColumn;
        final ExprNode node;
        final Set<String> columnArgs;   // 表达式引用的上游列（供列裁剪）

        CompiledExpression(String derivedColumn, ExprNode node) {
            this.derivedColumn = derivedColumn;
            this.node = node;
            this.columnArgs = new ExprEvaluator(node).columnArgs();
        }

        static CompiledExpression compile(GroupKeyExpression gk) {
            // 规划期已校验可解析 + 函数存在；此处仍 fail-fast
            return new CompiledExpression(gk.derivedColumn(), ExprParser.parse(gk.exprText()));
        }

        FieldVector evaluate(VectorSchemaRoot root, BufferAllocator allocator, int rowCount) {
            return new ExprEvaluator(node).evaluate(root, allocator, derivedColumn);
        }
    }
}
