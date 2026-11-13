package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * Project operator - selects specified columns from upstream Page.
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/12
 */
public class ProjectOperator extends AbstractMeasurableOperator {

    /**
     * 带改名的投影项：取上游第 {@code occurrence}（从 1 起）个 {@code sourceName}，输出列名为
     * {@code outputName}。用于把内部消歧列名（如 {@code __jk_user_id_2}）还原成用户可见列名。
     */
    public record ColumnRef(String sourceName, int occurrence, String outputName) {}

    private Operator upstream;
    private final List<String> columns;
    /** 非 null = 改名投影模式（{@link #aliased}）；columns 此时是各输出项的上游源列名。 */
    private List<ColumnRef> refs;

    public ProjectOperator(Operator upstream, List<String> columns) {
        super("Project", "Project", 1);
        this.upstream = upstream;
        this.columns = columns;
        this.refs = null;
    }

    /** 改名投影：源列 → 输出列（支持同名列按出现序号取）。 */
    public static ProjectOperator aliased(Operator upstream, List<ColumnRef> refs) {
        ProjectOperator op = new ProjectOperator(upstream, new ArrayList<>());
        op.refs = List.copyOf(refs);
        for (ColumnRef r : refs) op.columns.add(r.sourceName());
        return op;
    }

    /** 更新上游算子引用（列裁剪优化使用，须在 {@link #doOpen()} 前调用）。 */
    public void setUpstream(Operator upstream) { this.upstream = upstream; }

    /** 上游算子（scan-sink 形态探测用）。 */
    public Operator upstream() { return upstream; }

    /**
     * 收窄改名投影为指定的投影项（列裁剪优化使用）：父节点不需要的输出列必须同步从投影项中
     * 移除 —— 否则裁剪会把对应源列从子节点裁掉，运行期 {@link #rename} 取不到源列而抛
     * 「投影源列不存在」。
     */
    public void setRefs(List<ColumnRef> refs) {
        this.refs = List.copyOf(refs);
        this.columns.clear();
        for (ColumnRef r : refs) this.columns.add(r.sourceName());
    }

    /** 更新投影列清单（列裁剪优化使用，用于合并连续冗余 Project 节点）；改名模式随之失效。 */
    public void setColumns(List<String> columns) {
        this.columns.clear();
        this.columns.addAll(columns);
        this.refs = null;
    }

    /** 投影列清单（上游源列名；改名模式下同样返回源列名，供列裁剪使用）。 */
    public List<String> columns() { return columns; }

    /** 改名模式下的投影项；普通投影返回 null。 */
    public List<ColumnRef> refs() { return refs; }

    /** 是否改名投影。改名投影不能与普通 Project 合并（合并会丢输出列名）。 */
    public boolean isAliased() { return refs != null; }

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
            if (refs == null) {
                Page projected = input.select(columns);
                input.close();
                return projected;
            }
            if (input.getRowCount() == 0) { input.close(); continue; }
            Page renamed = rename(input);
            input.close();
            return renamed;
        }
    }

    /** 按 {@link ColumnRef} 逐列取源向量、改名后组装新页（源页随后由调用方关闭）。 */
    private Page rename(Page input) {
        VectorSchemaRoot inRoot = input.getRoot();
        int rowCount = inRoot.getRowCount();
        BufferAllocator allocator = inRoot.getVector(0).getAllocator();
        List<Field> fields = new ArrayList<>(refs.size());
        List<FieldVector> vectors = new ArrayList<>(refs.size());
        for (ColumnRef r : refs) {
            FieldVector src = findColumn(inRoot, r.sourceName(), r.occurrence());
            if (src == null) {
                throw new IllegalStateException("投影源列不存在: " + r.sourceName()
                        + "（第 " + r.occurrence() + " 次）");
            }
            Field out = new Field(r.outputName(), src.getField().getFieldType(),
                    src.getField().getChildren());
            FieldVector dst = out.createVector(allocator);
            dst.setInitialCapacity(rowCount);
            dst.allocateNew();
            org.apache.arrow.vector.util.TransferPair tp = src.makeTransferPair(dst);
            for (int i = 0; i < rowCount; i++) tp.copyValueSafe(i, i);
            dst.setValueCount(rowCount);
            fields.add(out);
            vectors.add(dst);
        }
        return Page.fromRoot(new VectorSchemaRoot(fields, vectors, rowCount));
    }

    /**
     * 取 schema 中第 occurrence 个名为 name 的向量；不存在返回 null。
     *
     * <p><b>出现次数回退</b>：与 {@link ScalarProjectOperator} 同口径 —— 输出列允许同一源列
     * 重复出现，而裁剪后的上游只有一个物理副本，第 k 次查找回退到最后一个同名向量。</p>
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
        return last;
    }

    @Override
    protected void doClose() throws Exception {
        upstream.close();
    }
}
