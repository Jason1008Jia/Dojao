package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * <b>多维聚合展开算子</b> —— 把每行按 grouping set 个数复制成多行，未参与的维度列置 NULL，
 * 并追加 {@code __grouping_id}（第 i 个 set 填 i）。
 *
 * <p>用途：{@code GROUP BY ROLLUP/CUBE/GROUPING SETS(a, b)} 原先被
 * {@code Planner} 展开成 N 条<b>独立</b>的 SELECT 腿（每条腿各自扫表 + join + 聚合）——
 * 2 维 ROLLUP 就是 3 次全表扫描。改成本算子在 join 输出之后插一层：</p>
 * <pre>
 *   scan(1 次) → join → Expand(set0, set1, set2) → 按 (全部维度…, __grouping_id) 一次聚合
 * </pre>
 * <p>行数放大 = set 个数，但换来扫描 / join 只做一次（真实 Hive 场景下省掉的是几倍的文件读取）。</p>
 *
 * <p><b>为什么必须有 {@code __grouping_id}</b>：真实数据里的 NULL 与「该维度在此维度组合里不参与
 * 聚合」的占位 NULL 必须区分 —— 否则 {@code (NULL, 北京)} 与 {@code (总, 北京)} 会落进同一个分组，
 * 静默算错。gid 是分组键的一部分，聚合后由上层投影裁掉（不进结果列）。</p>
 *
 * <p>逐行纯函数、与分区无关 ⇒ 分布式路径可在 Worker 本地执行（Producer 侧 ScanStep 之后 /
 * 最末层 join 之后、哈希分区之前），结果与单机逐字节一致。</p>
 *
 * @author jiaxin
 * @date 2026/10/29
 */
public final class ExpandOperator extends AbstractMeasurableOperator {

    /** 维度组合编号列名（分组键的一部分，聚合后由上层投影裁掉）。 */
    public static final String GROUPING_ID_COLUMN = "__grouping_id";

    /** {@link #GROUPING_ID_COLUMN} 的 Arrow 类型（BIGINT，可空=false 在此不重要）。 */
    public static final Field GROUPING_ID_FIELD = new Field(GROUPING_ID_COLUMN,
            FieldType.nullable(new ArrowType.Int(64, true)), null);

    private final List<String> dimensionColumns;
    /** 每个 grouping set 参与的维度列名（其余维度在那一份副本里置 NULL）。 */
    private final List<List<String>> groupingSets;
    /** 归一化后的维度名（大写）→ 是否维度列。 */
    private final Set<String> dimensions = new LinkedHashSet<>();
    /** 每个 set 的成员名（大写）。 */
    private final List<Set<String>> setMembers = new ArrayList<>();

    private Operator upstream;
    /** 打开时解析一次：维度列在上游 schema 里的下标（-1 = 不存在）。 */
    private int[] dimIndex;
    private int[] dimOfColumn;

    public ExpandOperator(Operator upstream, List<String> dimensionColumns, List<List<String>> groupingSets) {
        super("Expand", "Expand", 1);
        this.upstream = upstream;
        this.dimensionColumns = List.copyOf(dimensionColumns);
        this.groupingSets = copySets(groupingSets);
        for (String d : this.dimensionColumns) dimensions.add(norm(d));
        for (List<String> set : this.groupingSets) {
            Set<String> members = new HashSet<>();
            for (String m : set) members.add(norm(m));
            setMembers.add(members);
        }
    }

    private static List<List<String>> copySets(List<List<String>> sets) {
        List<List<String>> out = new ArrayList<>(sets.size());
        for (List<String> s : sets) out.add(List.copyOf(s));
        return out;
    }

    private static String norm(String s) {
        return s == null ? "" : s.trim().toUpperCase(Locale.ROOT);
    }

    /** 更新上游算子引用（列裁剪优化使用，须在 {@link #doOpen()} 前调用）。 */
    public void setUpstream(Operator upstream) {
        this.upstream = upstream;
    }

    public List<String> dimensionColumns() {
        return dimensionColumns;
    }

    public List<List<String>> groupingSets() {
        return groupingSets;
    }

    /** 输出列 = 上游全部列 + {@code __grouping_id}（列裁剪按名处理，不依赖本方法）。 */
    public static List<String> outputColumnsOf(List<String> upstreamColumns) {
        List<String> out = new ArrayList<>(upstreamColumns);
        out.add(GROUPING_ID_COLUMN);
        return out;
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
            resolveDimensions(inRoot);

            List<Field> inFields = inRoot.getSchema().getFields();
            int cols = inFields.size();
            int sets = groupingSets.size();
            int outRows = rowCount * sets;
            List<Field> outFields = new ArrayList<>(cols + 1);
            outFields.addAll(inFields);
            outFields.add(GROUPING_ID_FIELD);
            VectorSchemaRoot outRoot = VectorSchemaRoot.create(new Schema(outFields), allocator);
            outRoot.allocateNew();
            List<FieldVector> outVectors = outRoot.getFieldVectors();

            for (int s = 0; s < sets; s++) {
                Set<String> members = setMembers.get(s);
                int base = s * rowCount;
                for (int c = 0; c < cols; c++) {
                    FieldVector src = inRoot.getVector(c);
                    FieldVector dst = outVectors.get(c);
                    boolean masked = dimOfColumn[c] >= 0
                            && !members.contains(norm(dimensionColumns.get(dimOfColumn[c])));
                    if (masked) {
                        for (int r = 0; r < rowCount; r++) dst.setNull(base + r);
                    } else {
                        for (int r = 0; r < rowCount; r++) dst.copyFromSafe(r, base + r, src);
                    }
                    if (dst instanceof BaseVariableWidthVector v) v.setLastSet(base + rowCount - 1);
                }
                BigIntVector gid = (BigIntVector) outVectors.get(cols);
                for (int r = 0; r < rowCount; r++) gid.setSafe(base + r, s);
            }
            outRoot.setRowCount(outRows);
            int pid = input.getPartitionId().orElse(-1);
            long batchIndex = input.getBatchIndex();
            input.close();
            return pid >= 0 ? Page.partitioned(outRoot, pid) : Page.fromRoot(outRoot, batchIndex);
        }
    }

    @Override
    protected void doClose() throws Exception {
        upstream.close();
    }

    /** 解析维度列 → 上游列下标（首帧做一次；缺列出明确错误而非静默置 NULL）。 */
    private void resolveDimensions(VectorSchemaRoot root) {
        if (dimIndex != null) return;
        List<Field> fields = root.getSchema().getFields();
        List<String> names = new ArrayList<>(fields.size());
        for (Field f : fields) names.add(norm(f.getName()));
        int[] dims = new int[dimensionColumns.size()];
        int[] ofColumn = new int[fields.size()];
        for (int c = 0; c < ofColumn.length; c++) ofColumn[c] = -1;
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < dimensionColumns.size(); i++) {
            int idx = names.indexOf(norm(dimensionColumns.get(i)));
            dims[i] = idx;
            if (idx < 0) missing.add(dimensionColumns.get(i));
            else ofColumn[idx] = i;
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException("多维聚合展开缺少维度列 " + missing
                    + "（上游可用列: " + fields.stream().map(Field::getName).toList() + "）");
        }
        this.dimIndex = dims;
        this.dimOfColumn = ofColumn;
    }
}
