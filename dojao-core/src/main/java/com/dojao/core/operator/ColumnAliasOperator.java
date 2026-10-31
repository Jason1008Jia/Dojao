package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.List;

/**
 * 同名列消歧算子 —— 为上游页中「第 {@code occurrence} 次出现的 {@code sourceName}」追加一个
 * <b>改名副本</b> {@code aliasName}，上游列原样保留。
 *
 * <p><b>为什么需要</b>：外连接（LEFT / RIGHT / FULL）必须保留右侧 join 键（NULL vs 非 NULL 是
 * 外连接语义的一部分），而左右键常常同名（{@code a.user_id = b.user_id}）⇒ join 输出出现两个
 * {@code user_id}。物理层表达式按<b>裸列名</b>解析、同名取首个（{@link com.dojao.core.operator.expr.ExprEvaluator}
 * 的 {@code indexOf} 用 {@code putIfAbsent}）⇒ {@code b.user_id IS NULL} / {@code count(b.user_id)}
 * 会静默绑到左列（外连接后恒非 NULL），谓词恒假、计数退化成 {@code count(*)}。
 *
 * <p>本算子把「第 k 个同名列」复制成唯一名，Planner 再把限定引用 {@code b.user_id} 改写为该副本名，
 * 使限定引用真正落到右侧数据上（见 {@code Planner#aliasOuterJoinKeyRefs}）。对外结果列名由
 * Planner 末尾的改名投影还原，用户看不到内部名。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/17
 */
public final class ColumnAliasOperator extends AbstractMeasurableOperator {

    /**
     * 一条别名规则：上游第 {@code occurrence}（从 1 起，按同名列出现次序）个 {@code sourceName}
     * 复制一份并命名为 {@code aliasName}。
     */
    public record Alias(String sourceName, int occurrence, String aliasName) {}

    private Operator upstream;
    private final List<Alias> aliases;
    private final List<String> outputColumns;

    public ColumnAliasOperator(Operator upstream, List<String> upstreamColumns, List<Alias> aliases) {
        super("ColumnAlias", "ColumnAlias", 1);
        this.upstream = upstream;
        this.aliases = List.copyOf(aliases);
        List<String> out = new ArrayList<>(upstreamColumns);
        for (Alias a : aliases) out.add(a.aliasName());
        this.outputColumns = List.copyOf(out);
    }

    /** 更新上游算子引用（列裁剪优化使用，须在 {@link #doOpen()} 前调用）。 */
    public void setUpstream(Operator upstream) { this.upstream = upstream; }

    public List<Alias> aliases() { return aliases; }

    /** 输出列 = 上游列（原样）+ 各别名副本列。 */
    public List<String> outputColumns() { return outputColumns; }

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

            List<Field> fields = new ArrayList<>();
            List<FieldVector> vectors = new ArrayList<>();
            // ★ 别名副本必须先拷：下面转移上游向量所有权后源向量即被掏空。
            List<Field> aliasFields = new ArrayList<>();
            List<FieldVector> aliasVectors = new ArrayList<>();
            for (Alias a : aliases) {
                FieldVector src = findColumn(inRoot, a.sourceName(), a.occurrence());
                if (src == null) {
                    throw new IllegalStateException("同名列副本源列不存在: " + a.sourceName()
                            + "（第 " + a.occurrence() + " 次）");
                }
                Field renamed = new Field(a.aliasName(), src.getField().getFieldType(),
                        src.getField().getChildren());
                FieldVector dst = renamed.createVector(allocator);
                dst.setInitialCapacity(rowCount);
                dst.allocateNew();
                TransferPair tp = src.makeTransferPair(dst);
                for (int r = 0; r < rowCount; r++) tp.copyValueSafe(r, r);
                dst.setValueCount(rowCount);
                aliasFields.add(renamed);
                aliasVectors.add(dst);
            }
            // 上游列：零拷贝转移所有权（源向量被掏空，input.close() 安全 —— 同 Page.select 的做法）
            for (int i = 0; i < inRoot.getFieldVectors().size(); i++) {
                FieldVector src = inRoot.getVector(i);
                FieldVector dst = src.getField().createVector(allocator);
                src.makeTransferPair(dst).transfer();
                dst.setValueCount(rowCount);
                fields.add(src.getField());
                vectors.add(dst);
            }
            fields.addAll(aliasFields);
            vectors.addAll(aliasVectors);

            VectorSchemaRoot outRoot = new VectorSchemaRoot(fields, vectors, rowCount);
            input.close();
            return Page.fromRoot(outRoot);
        }
    }

    /** 取 schema 中第 occurrence 个名为 name 的向量；不存在返回 null。 */
    private static FieldVector findColumn(VectorSchemaRoot root, String name, int occurrence) {
        List<Field> schemaFields = root.getSchema().getFields();
        int seen = 0;
        for (int i = 0; i < schemaFields.size(); i++) {
            if (schemaFields.get(i).getName().equals(name)) {
                seen++;
                if (seen == occurrence) return root.getVector(i);
            }
        }
        return null;
    }

    @Override
    protected void doClose() throws Exception {
        upstream.close();
    }
}
