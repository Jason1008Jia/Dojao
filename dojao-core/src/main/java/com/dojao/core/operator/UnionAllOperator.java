package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.List;

/**
 * UNION ALL 算子 —— 顺序串联多个孩子的输出，并把每个孩子的页对齐到统一的规范列布局。
 *
 * <p>服务于两类场景：</p>
 * <ul>
 *   <li><b>普通 UNION / UNION ALL</b>：两个孩子输出列已同序同类型（{@code childSlots} 均为恒等映射），
 *       本算子退化为纯流式串联（页直接上抛、零拷贝），仅在首张非空页上做类型一致性校验。</li>
 *   <li><b>多维聚合（CUBE/ROLLUP/GROUPING SETS）改写后的 UNION ALL 腿</b>：每条腿只输出自己参与分组的
 *       维度列 + 聚合列，本算子按规范槽位把腿的页<b>重排 + 缺席维度列补 typed NULL</b>，使各腿输出布局
 *       一致。缺席槽位的类型在 {@link #doOpen()} 从各孩子<b>首张非空页</b>学得（能拿到真实 Arrow 类型，
 *       含 DECIMAL 精度等），学不到（该维度的所有提供腿在过滤/HAVING 后都为空）才回退到静态兜底类型
 *       {@code staticOutputFields}。</li>
 * </ul>
 *
 * <p>列裁剪需要替换孩子引用时用 {@link #replaceChild(int, Operator)}（须在 open 前调用）。</p>
 *
 * @author jiaxin
 * @date 2026/9/4
 */
public class UnionAllOperator extends AbstractMeasurableOperator {

    /** 规范输出列名（与 PhysicalNode.outputColumns 一致，供客户端读取列头）。 */
    private final List<String> outputColumns;
    /** 规范输出列的兜底 Field（可为 null 表示无静态类型；运行时能学到就学）。 */
    private final List<Field> staticOutputFields;
    /** children[i] 的第 j 列对应规范槽位 childSlots[i][j]（升序、互异）。 */
    private final int[][] childSlots;

    private Operator[] children;
    /** 孩子 i 是否恒等布局（输出列恰为规范列、逐槽位 1:1）→ 直接透传页。 */
    private final boolean[] identity;

    // ==================== 运行时状态 ====================
    /** 规范槽位 → 学到的 Field（首张非空页提供者即权威类型）。 */
    private Field[] learned;
    /**
     * 槽位学到的类型是否<b>弱</b>（首个提供页该列整列全 NULL ⇒ 类型只是运行时兜底，
     * 例如裸 {@code NULL} 落 Int64）。弱类型可被其它孩子的具体类型覆盖，也不参与一致性校验。
     */
    private boolean[] learnedWeak;
    /** open 时预取的孩子首张页（含 EOF；取出后置 null）。 */
    private Page[] firstPages;
    /** 当前正在排空的孩子下标。 */
    private int emitChild;
    /** 已预取但尚未返回的页（当前孩子的 carried 页）。 */
    private Page carried;

    public UnionAllOperator(List<Operator> children,
                            List<String> outputColumns,
                            List<Field> staticOutputFields,
                            List<int[]> childSlots) {
        super("UnionAll", "UnionAll", 1);
        this.children = children.toArray(new Operator[0]);
        this.outputColumns = outputColumns;
        this.staticOutputFields = staticOutputFields;
        this.childSlots = childSlots.toArray(new int[0][]);
        this.identity = new boolean[this.children.length];
        for (int i = 0; i < this.children.length; i++) {
            int[] slots = this.childSlots[i];
            boolean id = slots.length == outputColumns.size();
            for (int j = 0; id && j < slots.length; j++) id = slots[j] == j;
            identity[i] = id;
        }
    }

    /** 替换孩子引用（列裁剪优化使用，须在 {@link #doOpen()} 前调用）。 */
    public void replaceChild(int index, Operator operator) {
        children[index] = operator;
    }

    public List<String> outputColumns() { return outputColumns; }

    /** 孩子算子列表（供列裁剪递归）。 */
    public Operator[] childrenOperators() { return children; }

    @Override
    protected void doOpen() throws Exception {
        learned = new Field[outputColumns.size()];
        learnedWeak = new boolean[outputColumns.size()];
        firstPages = new Page[children.length];
        carried = null;
        emitChild = 0;
        for (int i = 0; i < children.length; i++) {
            children[i].open();
        }
        // 预取各孩子的首个<b>非空</b>页（EOF 或首个 rowCount>0 页）：把每个槽位的类型学到
        // （首个非空提供页 = 权威类型），之后逐页对齐时缺席槽位已有类型可用。跳过空页可让
        // "孩子先空后实"的场景也能学到类型，且后续 emit 时该孩子直接续拉。
        for (int i = 0; i < children.length; i++) {
            Page p;
            while (true) {
                p = children[i].next();
                if (p.isEOF()) break;
                if (p.getRowCount() > 0) break;
                p.close();   // 学习阶段的空页无信息，直接丢弃
            }
            firstPages[i] = p;
            learnSchema(i, p);
        }
    }

    /**
     * 从孩子首页学习各提供槽位的类型；同槽位跨孩子类型不一致时按 SQL 数值提升规则统一，
     * 非数值类型不一致 → fail-fast。
     *
     * <p><b>全 NULL 列是弱证据</b>：裸 {@code NULL} / 聚合在过滤后整列为空时，运行时类型只是
     * 「全 NULL 时的兜底」（常是 Int64），不能当成该槽位的权威类型 —— 否则会与另一分支的
     * 具体类型（如 DOUBLE）判「类型不一致」而拒绝整条 UNION（压测缺陷 4）。</p>
     *
     * <p><b>数值提升</b>：跨源 {@code UNION ALL}（MySQL DECIMAL × Hive DOUBLE）是核心场景，
     * 只要两侧都是数值类型就按 {@link #promoteNumeric} 统一（DECIMAL + DOUBLE → DOUBLE，
     * INT + DECIMAL → DECIMAL，INT 宽度/浮点宽度取宽者），{@link #align} 负责逐值转换。</p>
     */
    private void learnSchema(int child, Page page) {
        if (page.isEOF() || page.getRowCount() == 0) return;
        List<Field> fields = page.getSchema().getFields();
        int[] slots = childSlots[child];
        for (int j = 0; j < slots.length; j++) {
            Field f = fields.get(j);
            int slot = slots[j];
            boolean weak = isAllNullColumn(page.getRoot(), j);
            Field known = learned[slot];
            if (known == null) {
                learned[slot] = f;
                learnedWeak[slot] = weak;
            } else if (learnedWeak[slot]) {
                if (!weak) {
                    // 已知类型是弱证据，具体类型覆盖之
                    learned[slot] = f;
                    learnedWeak[slot] = false;
                }
            } else if (weak) {
                // 具体类型已确定，弱证据不参与校验
                continue;
            } else if (!known.getType().equals(f.getType())) {
                ArrowType promoted = promoteNumeric(known.getType(), f.getType());
                if (promoted == null) {
                    throw new IllegalStateException(
                            "UNION ALL 第 " + (slot + 1) + " 列两侧类型不一致: "
                                    + known.getType() + " vs " + f.getType());
                }
                FieldType kft = known.getFieldType();
                learned[slot] = new Field(known.getName(),
                        new FieldType(kft.isNullable(), promoted, kft.getDictionary(), kft.getMetadata()),
                        known.getChildren());
            }
        }
    }

    /**
     * SQL 数值类型提升：两侧都是数值时返回统一类型，否则 {@code null}（保持 fail-fast）。
     *
     * <ul>
     *   <li>DECIMAL × DECIMAL → precision/scale 取并集（整数位取大、小数位取大）；</li>
     *   <li>DECIMAL × 整数 → DECIMAL（按整数位宽扩精度，保留 scale）；</li>
     *   <li>DECIMAL × 浮点 → FLOAT64；</li>
     *   <li>整数 × 整数 → 更宽位宽；浮点 × 浮点 → FLOAT64；整数 × 浮点 → FLOAT64。</li>
     * </ul>
     */
    static ArrowType promoteNumeric(ArrowType a, ArrowType b) {
        if (a.equals(b)) return a;
        if (a instanceof ArrowType.Decimal da && b instanceof ArrowType.Decimal db) {
            return decimalOf(Math.max(da.getScale(), db.getScale()),
                    Math.max(da.getPrecision() - da.getScale(), db.getPrecision() - db.getScale()));
        }
        if (a instanceof ArrowType.Decimal da && b instanceof ArrowType.Int bi) {
            return decimalOf(da.getScale(), Math.max(da.getPrecision() - da.getScale(), intDigits(bi)));
        }
        if (b instanceof ArrowType.Decimal db && a instanceof ArrowType.Int ai) {
            return decimalOf(db.getScale(), Math.max(db.getPrecision() - db.getScale(), intDigits(ai)));
        }
        if (a instanceof ArrowType.Decimal || b instanceof ArrowType.Decimal) {
            // DECIMAL × 浮点（其类型的组合已在前两条排除）
            return a instanceof ArrowType.FloatingPoint || b instanceof ArrowType.FloatingPoint
                    ? new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)
                    : null;
        }
        if (a instanceof ArrowType.Int ai && b instanceof ArrowType.Int bi) {
            return new ArrowType.Int(Math.max(ai.getBitWidth(), bi.getBitWidth()),
                    ai.getIsSigned() || bi.getIsSigned());
        }
        if (a instanceof ArrowType.FloatingPoint && b instanceof ArrowType.FloatingPoint) {
            return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        }
        if ((a instanceof ArrowType.Int && b instanceof ArrowType.FloatingPoint)
                || (a instanceof ArrowType.FloatingPoint && b instanceof ArrowType.Int)) {
            return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        }
        return null;
    }

    /** 整数类型能容纳的十进制位数（含符号位留量）。 */
    private static int intDigits(ArrowType.Int i) {
        return switch (i.getBitWidth()) {
            case 8 -> 3;
            case 16 -> 5;
            case 32 -> 10;
            default -> 19;
        };
    }

    /** DECIMAL(intDigits, scale)：精度封顶 38，且至少给 scale 留 1 位整数。 */
    private static ArrowType.Decimal decimalOf(int scale, int intDigits) {
        int s = Math.max(0, Math.min(scale, 38));
        int precision = Math.max(s + 1, Math.min(38, intDigits + s));
        return new ArrowType.Decimal(precision, s, 128);
    }

    /** 页的第 col 列是否整列全 NULL（= 类型证据弱）。 */
    private static boolean isAllNullColumn(VectorSchemaRoot root, int col) {
        if (root == null || col >= root.getFieldVectors().size()) return false;
        FieldVector v = root.getVector(col);
        return v.getValueCount() > 0 && v.getNullCount() == v.getValueCount();
    }

    @Override
    protected Page doNext() throws Exception {
        while (true) {
            if (emitChild >= children.length) {
                return Page.eof();
            }
            Page page = carried;
            carried = null;
            if (page == null) {
                page = firstPages[emitChild];
                if (page == null) {
                    page = children[emitChild].next();
                } else {
                    firstPages[emitChild] = null;
                }
            }
            recordInput(page);
            if (page.isEOF()) {
                emitChild++;
                continue;
            }
            if (page.getRowCount() == 0) {
                releaseInput(page);
                page.close();
                continue;
            }
            if (identity[emitChild] && schemaNamesMatch(page, outputColumns)
                    && pageTypesMatchLearned(page)) {
                // 恒等布局 + 页字段名已与规范列名一致（两侧同名/别名一致）+ 类型与学到的权威
                // 类型一致 → 页直接上抛（零拷贝）。字段名不一致（如 SELECT a ... UNION ALL
                // SELECT b ...）或该页是「全 NULL 的弱类型列」时不能透传：下游依赖统一的
                // schema，须走 align 按权威类型重建。
                return normalizePartition(page);
            }
            Page aligned = align(page, emitChild);
            releaseInput(page);
            page.close();
            return normalizePartition(aligned);
        }
    }

    /** 把孩子的页重排到规范布局：provided 槽位拷自源向量，缺席槽位补常量 NULL。 */
    private Page align(Page input, int child) {
        VectorSchemaRoot inRoot = input.getRoot();
        int rowCount = input.getRowCount();
        BufferAllocator allocator = inRoot.getVector(0).getAllocator();
        int[] slots = childSlots[child];

        List<Field> outFields = new ArrayList<>(outputColumns.size());
        List<FieldVector> outVectors = new ArrayList<>(outputColumns.size());
        int col = 0;   // 孩子当前列下标（slots 升序）
        for (int slot = 0; slot < outputColumns.size(); slot++) {
            if (col < slots.length && slots[col] == slot) {
                // provided 槽位：类型以源向量为准（权威，含 DECIMAL 精度等），仅把列名规范化为 outputColumns。
                // 例外：源列整列全 NULL（类型只是运行时兜底）且该槽位已学到具体类型 → 按具体类型
                // 输出整列 NULL；或源列类型与学到/提升后的权威类型不同（数值提升）→ 逐值转换，
                // 避免同一 UNION 的不同腿吐出不同 schema。
                FieldVector src = inRoot.getVector(col);
                boolean allNull = isAllNullColumn(inRoot, col);
                Field authoritative = learned[slot] != null && (!learnedWeak[slot] || allNull)
                        ? fieldForSlot(slot) : null;
                boolean convert = authoritative != null
                        && !src.getField().getType().equals(authoritative.getType());
                Field cf;
                FieldVector dst;
                if (authoritative != null && (allNull || convert)) {
                    cf = authoritative;
                    dst = cf.createVector(allocator);
                    dst.setInitialCapacity(rowCount);
                    dst.allocateNew();
                    if (!allNull) {
                        for (int r = 0; r < rowCount; r++) {
                            if (src.isNull(r)) dst.setNull(r);
                            else AbstractSpillableOperator.setVectorValue(dst, r, src.getObject(r));
                        }
                    }
                    dst.setValueCount(rowCount);   // allNull: allocateNew 后 validity 全 0 = 全 NULL
                } else {
                    cf = renamedField(src.getField(), outputColumns.get(slot));
                    dst = cf.createVector(allocator);
                    dst.setInitialCapacity(rowCount);
                    dst.allocateNew();
                    TransferPair tp = src.makeTransferPair(dst);
                    for (int r = 0; r < rowCount; r++) tp.copyValueSafe(r, r);
                    dst.setValueCount(rowCount);
                }
                outFields.add(cf);
                outVectors.add(dst);
                col++;
            } else {
                // 缺席槽位：typed NULL（allocateNew 后 validity 全 0 = 全 NULL）
                Field cf = fieldForSlot(slot);
                FieldVector dst = cf.createVector(allocator);
                dst.setInitialCapacity(rowCount);
                dst.allocateNew();
                dst.setValueCount(rowCount);
                outFields.add(cf);
                outVectors.add(dst);
            }
        }
        VectorSchemaRoot outRoot = new VectorSchemaRoot(outFields, outVectors, rowCount);
        return Page.fromRoot(outRoot);
    }

    /**
     * 归一分区号：UNION 是顺序单流，输出须为单一逻辑分区（pid=0）。
     * 否则各腿页带底层分布式扫描的真实分区号 → 上游若再包聚合（如 UNION distinct 的
     * 全列去重 HashAggregation，每个"槽"被当作 co-partitioned 互斥键域）会把同一键分到
     * 多个槽 → 重复输出。下游所有按分区独立聚合的算子都依赖此不变量。
     */
    private static Page normalizePartition(Page page) {
        return page.getPartitionId().map(pid -> pid == 0 ? page : page.withPartitionId(0))
                .orElse(page);
    }

    /**
     * 恒等布局的页，各列类型是否与学到的权威类型一致。
     *
     * <p>学到的是弱类型（全 NULL 提供者）时，页里出现具体类型也视为可接受 —— 此时类型以页为准；
     * 但「学到具体类型、页却是全 NULL 的弱类型」不能透传，交给 {@link #align} 按权威类型重建。</p>
     */
    private boolean pageTypesMatchLearned(Page page) {
        List<Field> fs = page.getSchema().getFields();
        if (fs.size() != learned.length) return false;
        for (int i = 0; i < fs.size(); i++) {
            Field known = learned[i];
            if (known == null) continue;
            if (learnedWeak[i]) continue;
            if (!known.getType().equals(fs.get(i).getType())) return false;
        }
        return true;
    }

    /** 页 schema 字段名是否与规范列名逐位一致（忽略大小写）—— 一致才可零拷贝透传。 */
    private static boolean schemaNamesMatch(Page page, List<String> names) {
        List<Field> fs = page.getSchema().getFields();
        if (fs.size() != names.size()) return false;
        for (int i = 0; i < fs.size(); i++) {
            if (!fs.get(i).getName().equalsIgnoreCase(names.get(i))) return false;
        }
        return true;
    }

    /** 以源 Field 为类型骨架、只换列名为 name 的 Field 副本（列名规范化用）。 */
    private static Field renamedField(Field src, String name) {
        FieldType ft = src.getFieldType();
        return new Field(name,
                new FieldType(ft.isNullable(), ft.getType(), ft.getDictionary(), ft.getMetadata()),
                src.getChildren());
    }

    /** 槽位类型：运行学到 > 静态兜底；都没有 → 明确报错。 */
    private Field fieldForSlot(int slot) {
        Field f = learned[slot];
        if (f != null) {
            // 规范列名统一为 outputColumns（孩子的同槽位列名可能与规范别名不同）
            FieldType ft = f.getFieldType();
            return new Field(outputColumns.get(slot),
                    new FieldType(ft.isNullable(), ft.getType(), ft.getDictionary(), ft.getMetadata()),
                    f.getChildren());
        }
        Field sf = staticOutputFields.get(slot);
        if (sf != null) return sf;
        throw new IllegalStateException(
                "无法推断 UNION ALL 第 " + (slot + 1) + " 列（" + outputColumns.get(slot)
                        + "）的类型：所有提供该列的输入都为空，且无静态类型兜底");
    }

    @Override
    protected void doClose() throws Exception {
        // open 阶段预取的首张页（含 open 失败 / 提前取消时尚未发射的页）必须先关：
        // 否则孩子算子先 close 会释放自己的分配器，而这些页仍持有其 buffer → Arrow 报
        // "Memory was leaked by query"（UNION 两侧类型不一致等 open 失败路径必现）。
        if (carried != null) {
            carried.close();
            carried = null;
        }
        if (firstPages != null) {
            for (int i = 0; i < firstPages.length; i++) {
                if (firstPages[i] != null) {
                    firstPages[i].close();
                    firstPages[i] = null;
                }
            }
        }
        for (Operator child : children) {
            try {
                child.close();
            } catch (Exception ignored) {
                // 单个孩子 close 失败不阻断其余
            }
        }
    }
}
