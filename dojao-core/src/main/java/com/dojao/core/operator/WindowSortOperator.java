package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.core.config.ResourceConfig;
import com.dojao.spi.util.NestedTypeSupport;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 窗口排序算子 —— 继承 {@link AbstractSpillableOperator}，开窗型键控（append 模式）。
 *
 * <h3>设计</h3>
 * <p>三槽溢写管线由基类 {@link AbstractSpillableOperator} 提供。本类只需：
 * <ol>
 *   <li>创建 {@link RowAccumulator} 槽位（append 原始行）</li>
 *   <li>提供 ORDER BY 排序逻辑（组内排序 + Phase 2 归并排序）</li>
 * </ol>
 *
 * @author jiaxin
 * @version 6.0
 * @date 2026/7/23
 */
public class WindowSortOperator extends AbstractSpillableOperator<RowAccumulator> implements Explainable {

    /** EXPLAIN 明细：排序键与方向。 */
    @Override
    public List<String> explainLines() {
        List<String> dirs = new ArrayList<>(orderAscending.size());
        for (Boolean asc : orderAscending) dirs.add(asc ? "ASC" : "DESC");
        return List.of("sort keys=" + orderKeyColumns + " directions=" + dirs);
    }

    /** 输出页行数（query.window.perBatchKeys 可配）：排序结果每页多少行。 */
    private static final int OUTPUT_PAGE_ROWS = ResourceConfig.windowPerBatchKeys();

    /**
     * 溢写文件 / 归并输出批次的<b>最大行数</b>（query.window.spill.perBatchRows 可配，默认 10000）。
     *
     * <p>为什么必须限制单个文件的行数：Phase 2 之前会把一个 key 批涉及的<b>整份</b>溢写文件
     * 一次性读进内存（{@code ArrowFileReader} 以「record batch」为最小单位，无法只读文件的一部分）。
     * 一个槽位溢写时写的是<b>整槽快照</b>（可达 {@code slot.memory} 量级，实测数百 MB），于是
     * 「读几个文件」= 把整个分区搬进堆外内存 —— 溢写次数越多、Phase 2 越容易把 Worker 撑死
     * （这正是「一开窗触发溢写 worker 就挂」的直接原因）。把文件切成有界大小后，Phase 1.5 归并
     * 与 Phase 2 流式读取的峰值内存都只与<b>单批大小</b>相关，与已溢写总量脱钩。</p>
     *
     * <p><b>只作用于「写」侧</b>（Phase 1 落盘切分 + 归并产物 record batch 行数 + 归并输出向量分配粒度）。
     * 「读」侧的成页批量见 {@link #FETCH_PER_KEYS}，两者已拆开、互不干扰。</p>
     */
    private static final int SPILL_BATCH_ROWS = ResourceConfig.windowSpillPerBatchRows();

    /**
     * Phase 2 <b>拉取</b>侧每页容纳的 key 组数（query.window.spill.fetchPerKeys 可配，默认 10000）。
     *
     * <p>与 {@link #SPILL_BATCH_ROWS}（写侧）刻意拆开：写侧决定<b>文件切多细</b>，本值决定<b>一次回传
     * 多少 key</b>。旧实现是「一个 key 组一页」—— 高基数 PARTITION BY 下分组只有个位数行，
     * 于是每 9 行就付一次「新建 12 个 Arrow 向量 + 下游建页 + Hive 建 ORC 批」的固定开销，
     * 实测把 277 万个组变成 277 万个页、吞吐掉到 ~3K 行/s。改为<b>跨组累积</b>后页数降到千级。</p>
     *
     * <p>页行数另受 {@link #OUTPUT_PAGE_ROWS}（query.window.perBatchKeys）兜底：单组极大时也不会
     * 生成超大页。所以「一次拉取」= 至多 {@code FETCH_PER_KEYS} 个 key 组、至多 {@code OUTPUT_PAGE_ROWS} 行。</p>
     */
    private static final int FETCH_PER_KEYS = ResourceConfig.windowSpillFetchPerKeys();

    /**
     * Phase 1.5 K 路归并的路数上限：峰值内存 ≈ MAX_MERGE_K × 单批大小，恒有界。
     *
     * <p>只是上限 —— 实际路数由「本次归并新增的分配器占用 &lt; 输入预算」动态决定（见
     * {@link #mergeOneRound}）。取 32 是因为「一个槽位一个文件」后文件数通常只有几十个，
     * 32 路一轮即可归并完成，避免多轮全量重写（写放大）。</p>
     */
    private static final int MAX_MERGE_K = 32;

    // ==================== 组合键 ====================

    record CompositeKey(Object[] values) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CompositeKey ck)) return false;
            return Arrays.equals(values, ck.values);
        }

        @Override
        public int hashCode() { return Arrays.hashCode(values); }

        @Override
        public String toString() { return Arrays.toString(values); }
    }

    // ==================== 字段 ====================

    private final List<String> orderKeyColumns;
    private final List<Boolean> orderAscending;
    private int[] groupKeyIndices;
    private int[] orderKeyIndices;
    private boolean isCompositeKey;

    // ==================== Phase 2 分段计时（诊断用） ====================

    private final AtomicLong sortNanos = new AtomicLong();      // 组内排序 + 展平
    private final AtomicLong allocNanos = new AtomicLong();     // 输出向量分配
    private final AtomicLong copyFixedNanos = new AtomicLong(); // 定长列 memcpy
    private final AtomicLong copyVarNanos = new AtomicLong();   // 变长列 copyValueSafe

    /** Phase 2 过滤配置：null / isActive=false 时走原有全量输出路径；激活时走 Filter→Sort→Output。 */
    private WindowFilterConfig filterConfig;

    // ==================== 构造函数 ====================

    public WindowSortOperator(Operator upstream,
                              List<String> groupKeyColumns,
                              List<String> orderKeyColumns,
                              List<Boolean> orderAscending,
                              long memoryLimit) {
        this(upstream, groupKeyColumns, orderKeyColumns, orderAscending, memoryLimit, DEFAULT_PARTITIONS);
    }

    /**
     * 指定分区数的构造（分布式 Worker 侧使用）：分区槽数组按查询布局的 numPartitions 分配，
     * 只接收 own 桶（pid &lt; numPartitions），否则越界。
     */
    public WindowSortOperator(Operator upstream,
                              List<String> groupKeyColumns,
                              List<String> orderKeyColumns,
                              List<Boolean> orderAscending,
                              long memoryLimit,
                              int numPartitions) {
        super("WindowSort", "WindowSort", 1, upstream, memoryLimit, Math.max(1, numPartitions));
        this.groupKeyColumns = groupKeyColumns;
        this.orderKeyColumns = orderKeyColumns;
        this.orderAscending = orderAscending;
        // 下游 WindowFunctionOperator 按「相邻行 PARTITION BY 键」识别分区边界：同一工人持有多个桶时，
        // 多分区并行吐页会把不同分区交错到一起而被误判为分区切换 ⇒ 吐页阶段必须按分区串行。
        this.orderedEmit = true;
    }

    public WindowSortOperator(Operator upstream,
                              String groupKeyColumn,
                              List<String> orderKeyColumns,
                              List<Boolean> orderAscending,
                              long memoryLimit) {
        this(upstream, List.of(groupKeyColumn), orderKeyColumns, orderAscending, memoryLimit);
    }

    /** 配置 Phase 2 过滤（null 或 isActive=false 表示无过滤，走全量输出）。 */
    public void setFilterConfig(WindowFilterConfig filterConfig) {
        this.filterConfig = filterConfig;
    }

    /** ORDER BY 列列表（供 Planner 判断是否可下推 MIN/MAX）。 */
    public List<String> getOrderKeyColumns() { return orderKeyColumns; }

    // ==================== 抽象方法实现 ====================

    @Override
    protected RowAccumulator createSlot(BufferAllocator allocator) {
        return new RowAccumulator();
    }

    @Override
    protected void addRow(RowAccumulator slot, Page input, int row) {
        slot.addRow(input, row);
    }

    @Override
    protected List<String> resolveGroupKeyColumns() {
        return groupKeyColumns;
    }

    public List<String> getGroupKeyColumns() { return groupKeyColumns; }

    @Override
    protected List<Field> resolveGroupKeyFields(Schema schema) {
        List<Field> fields = new ArrayList<>();
        for (String col : groupKeyColumns) fields.add(schema.findField(col));
        return fields;
    }

    @Override
    protected void doClose() throws Exception {
        long sortMs = sortNanos.get() / 1_000_000;
        long allocMs = allocNanos.get() / 1_000_000;
        long copyFixedMs = copyFixedNanos.get() / 1_000_000;
        long copyVarMs = copyVarNanos.get() / 1_000_000;
        System.err.printf(
                "[WindowSort] Phase 2 breakdown: sort=%.0fms alloc=%.0fms copyFixed=%.0fms copyVar=%.0fms (sum=%.0fms)%n",
                sortNanos.get() / 1e6, allocNanos.get() / 1e6,
                copyFixedNanos.get() / 1e6, copyVarNanos.get() / 1e6,
                (double) (sortMs + allocMs + copyFixedMs + copyVarMs));
        super.doClose();
    }

    @Override
    protected void prepareSlot(RowAccumulator slot, Page input) {
        VectorSchemaRoot root = input.getRoot();
        if (orderKeyIndices == null) resolveIndices();
        slot.initIfNeeded(inputSchema, allocator, OUTPUT_PAGE_ROWS);
        slot.ensureTransfers(root, groupKeyIndices, isCompositeKey);
        // SUM per-key 符号：累积期间同步追踪
        if (filterConfig != null && filterConfig.isSumWithOrderBy()) {
            slot.setSumColumnIndex(filterConfig.aggregateColIndex());
        }
    }

    @Override
    protected void outputMemory(PartitionState<RowAccumulator> pa, int pid) {
        beginOutput(pid);
        for (int s = 0; s < SLOT_COUNT; s++) {
            checkCancelled();
            RowAccumulator acc = pa.slots.get(s);
            if (acc == null || acc.isEmpty()) continue;
            outputPagesFromAccumulator(acc, pid);
            acc.clear();
            pa.slots.set(s, null);
        }
    }

    @Override
    protected void mergeAndOutput(PartitionState<RowAccumulator> pa, int pid) {
        if (pa.batchFiles.isEmpty()) return;
        // 排障计时：把「Phase 1.5 归并」与「Phase 2 流式读取输出」分开报 —— 二者瓶颈性质完全不同
        // （前者是磁盘写放大，后者是顺序读 + 逐行 gather 的 CPU / 或下游背压）。
        long t0 = System.nanoTime();
        // Phase 1.5：把「每个溢写批各按 key 排好序」的多个文件 K 路归并成 key 连续文件。
        // 之所以必须有这一步（而不是像以前那样按 key 随机拉取）：随机拉取要按 key 反查批次，
        // 一个 key 批会命中几乎所有文件 ⇒ 把整份分区读进内存；归并后的文件是顺序读的，天然有界。
        compactPartition(pa, pid);
        // 归并（磁盘重写）已可并行完成；从这里开始吐页 → 按活跃分区串行，避免下游按相邻行识别分区边界时被交错页打断。
        beginOutput(pid);
        long tMid = System.nanoTime();
        // Phase 2：逐文件流式读取。
        //   · 无过滤：归并产物是全局 (分组键, ORDER BY) 有序，<b>整文件顺序成页</b>即可（不按组物化，
        //     巨组不占内存）。分组可以跨页 —— 下游 WindowFunctionOperator 按「相邻行键比较」延续分区，
        //     （见其 detectPartitionBoundary）。
        //   · 有过滤：窗口过滤要「整组一起算」，走逐组路径（组不跨批，由 Phase 1.5 保证）。
        boolean flatStream = filterConfig == null || !filterConfig.isActive();
        for (int bi = 0; bi < pa.batchFiles.size(); bi++) {
            checkCancelled();
            if (flatStream) {
                streamBatchFileFlat(pa, bi, pid);
            } else {
                streamBatchFile(pa, bi, pid);
            }
        }
        long tEnd = System.nanoTime();
        System.err.printf("[%s] Phase 2 分区 %d：归并 %dms + 流式读取输出 %dms%n",
                getClass().getSimpleName(), pid,
                (tMid - t0) / 1_000_000, (tEnd - tMid) / 1_000_000);
    }

    // ==================== 溢写：覆盖 spillSlotSync 以提供排序逻辑 ====================

    /**
     * 溢写一个槽位：把「按 (PARTITION BY, ORDER BY) 排好序」的全量行列数据切成
     * {@link #SPILL_BATCH_ROWS} 行一批写盘。
     *
     * <p><b>不再物化整槽快照</b>：以前先 {@code buildGroupSortedOutput} 造一份与槽等大的 Arrow 副本
     * 再整块写盘（峰值 = 2 × 槽 + 文件缓冲）。现在只算一个 {@code int[]} 行号视图，然后直接从
     * 累加器列向量按批 gather —— 峰值 = 槽 + 单批。</p>
     *
     * <p><b>不建内存索引</b>：索引只服务于「按 key 随机拉取」，而那正是被本轮改造移除的路径。
     * 归并/输出全部按文件顺序进行，不需要 key→行号 的反查表（高基数窗口下该表本身就与数据同量级）。</p>
     */
    @Override
    protected void spillSlotSync(PartitionState<RowAccumulator> pa, int slotIdx,
                                  RowAccumulator acc, int pid) {
        if (acc.isEmpty()) return;
        try {
            // ★ 只累加「本次新写入的字节」：直接累加分区累计 dataFileSize 会让统计量随溢写次数
            //   平方级虚增（实测 165MB 被报成 6.7GB）。
            int[] order = flattenGroupSortedIndices(acc);
            accountSpilledBytes(writeSortedBatches(pa, acc, order, pid));
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Arrow batch for partition " + pid, e);
        } finally {
            // 无论成败都释放：写失败时槽位若不 close 会泄漏 Arrow 缓冲（曾见 40MB 泄漏）
            acc.clear();
            pa.slots.set(slotIdx, null);
        }
    }

    /**
     * 按 {@link #SPILL_BATCH_ROWS} 行一批，把 {@code order} 指定的行从累加器 gather 到
     * <b>一个多批 Arrow 文件</b>；返回写入总字节。
     *
     * <p><b>为什么一个槽位只写一个文件、而不是每 10k 行一个文件</b>：Phase 1.5 的归并轮数约为
     * {@code log_K(文件数)}，而每轮都会把全部数据重写一遍（真实 I/O，计入溢写总量）。旧实现把
     * 一个槽位切成几十个文件后，91M 行级的窗口会产生上千个文件，K 路归并因此要多轮重写 ——
     * 线上出现过「输入 6~7GB、监控溢写 217GB」的写放大。Arrow 文件天然支持「一个文件多个
     * record batch」，Phase 2 的 {@link #streamBatchFile}/{@link #streamBatchFileFlat} 本来就按
     * block 逐个读取，所以文件数从「行数/1万」降到「槽位溢写次数」，归并通常一轮即可完成，
     * 而单批内存上限（{@link #SPILL_BATCH_ROWS}）保持不变。</p>
     */
    private long writeSortedBatches(PartitionState<RowAccumulator> pa, RowAccumulator acc,
                                    int[] order, int pid) throws IOException {
        // ★ 用 order.length 而不是 acc.rowCount：{@link #pruneKeepRows()} 命中时 flattenGroupSortedIndices
        //   已按组裁到前 N 行，order 短于 rowCount；用 rowCount 会越界读 order 并写出垃圾行。
        int total = order.length;
        if (total == 0) return 0;
        List<Field> fields = inputSchema.getFields();

        int batchIndex = pa.batchCount.getAndIncrement();
        // 每次写溢写文件前查一次所在盘容量（超上限换盘，全超上限抛 SpillSpaceException）
        Path file = spillDirForWrite().resolve("p" + pid + "_b" + batchIndex + ".arrow");
        pa.batchFiles.add(file);
        if (pa.dataFilePath == null) pa.dataFilePath = file;

        List<FieldVector> outVectors = new ArrayList<>(fields.size());
        for (Field field : fields) {
            FieldVector dst = field.createVector(allocator);
            dst.setInitialCapacity(Math.min(SPILL_BATCH_ROWS, total));
            dst.allocateNew();
            outVectors.add(dst);
        }
        VectorSchemaRoot root = new VectorSchemaRoot(fields, outVectors, 0);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
             ArrowFileWriter writer = new ArrowFileWriter(root, null, ch)) {
            int off = 0;
            while (off < total) {
                int rows = Math.min(SPILL_BATCH_ROWS, total - off);
                // 缓冲容量按实际 valueCapacity 判（Arrow 按字节取整，不同类型换算行数不同）
                while (rows > minValueCapacity(outVectors)) {
                    for (FieldVector v : outVectors) v.reAlloc();
                }
                copyPagePermuted(acc.vectors, outVectors, order, off, rows, null, null);
                for (FieldVector v : outVectors) v.setValueCount(rows);
                root.setRowCount(rows);
                writer.writeBatch();
                off += rows;
            }
        } finally {
            root.close();
        }
        long writtenBytes = Files.size(file);
        // 并发溢写线程共享该字段：同步累加（与 writeBatchToArrow 同口径）
        synchronized (pa) { pa.dataFileSize += writtenBytes; }
        return writtenBytes;
    }

    // ==================== 索引：覆盖以使用 CompositeKey ====================

    @Override
    protected Object buildKeyFromSpill(VectorSchemaRoot sorted, int row) {
        if (isCompositeKey) {
            Object[] vals = new Object[groupKeyIndices.length];
            for (int i = 0; i < groupKeyIndices.length; i++) {
                FieldVector vec = sorted.getVector(groupKeyIndices[i]);
                vals[i] = RowAccumulator.readKeyValue(vec, row);
            }
            return new CompositeKey(vals);
        }
        if (groupKeyIndices.length == 0) {
            // 全局窗口（无 PARTITION BY）→ 单一全局分组
            return RowAccumulator.NULL_KEY;
        }
        FieldVector vec = sorted.getVector(groupKeyIndices[0]);
        return RowAccumulator.readKeyValue(vec, row);
    }

    @Override
    protected void sortKeys(List<Object> keys) {
        keys.sort(WindowSortOperator::compareGroupKeys);
    }

    /**
     * 分组键比较器（PartitionKey 语义）：NULL 键排最前；复合键逐列比较。
     *
     * <p>Phase 1 溢写写盘、Phase 1.5 K 路归并都用它 —— 归并要求<b>每个源文件自身按该键有序</b>，
     * 因此 {@link #flattenGroupSortedIndices} 必须先按此键把分组排好，否则归并结果不正确。</p>
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compareGroupKeys(Object a, Object b) {
        if (a == RowAccumulator.NULL_KEY && b == RowAccumulator.NULL_KEY) return 0;
        if (a == RowAccumulator.NULL_KEY) return -1;
        if (b == RowAccumulator.NULL_KEY) return 1;
        if (a instanceof CompositeKey cka && b instanceof CompositeKey ckb)
            return compareCompositeKeys(cka, ckb);
        if (a instanceof List<?> la && b instanceof List<?> lb)
            return ArrowComparators.compareLists(la, lb);
        if (a instanceof Comparable ca && b instanceof Comparable) return ca.compareTo(b);
        return String.valueOf(a).compareTo(String.valueOf(b));
    }

    // ==================== 索引解析 ====================

    /**
     * 把键列名解析成 schema 列下标。
     *
     * <p>★旧实现找不到列时<b>静默留在下标 0</b>：排序键写成不存在的列名（列裁剪裁掉、别名对不上、
     * 大小写不一致）时既不报错也不按该列排，而是拿第一列当排序键 —— 结果<b>只是顺序错</b>，
     * 既不崩也没有任何提示，线上极难定位（TopN 取到的还是「看起来合理」的行）。这里改成
     * fail-fast，并把可用列名一并给出，便于对照是裁剪还是命名问题。</p>
     */
    private void resolveIndices() {
        groupKeyIndices = new int[groupKeyColumns.size()];
        for (int i = 0; i < groupKeyColumns.size(); i++) {
            groupKeyIndices[i] = indexOfColumn(groupKeyColumns.get(i), "分组键列");
        }
        isCompositeKey = groupKeyIndices.length > 1;

        orderKeyIndices = new int[orderKeyColumns.size()];
        for (int i = 0; i < orderKeyColumns.size(); i++) {
            orderKeyIndices[i] = indexOfColumn(orderKeyColumns.get(i), "ORDER BY 列");
        }
    }

    /** 按名找列下标；找不到直接抛（绝不回落 0 —— 那等于按错误的列排序）。 */
    private int indexOfColumn(String name, String what) {
        List<Field> fields = inputSchema.getFields();
        for (int j = 0; j < fields.size(); j++) {
            if (fields.get(j).getName().equals(name)) {
                NestedTypeSupport.requireSupported(fields.get(j), "WindowSort 的" + what);
                return j;
            }
        }
        List<String> available = new ArrayList<>(fields.size());
        for (Field f : fields) {
            available.add(f.getName());
        }
        throw new IllegalStateException("WindowSort 的" + what + " '" + name
                + "' 不在输入列中（可用列: " + available + "）—— 通常是列裁剪误删或别名不一致");
    }

    // ==================== Map 分组内存排序 ====================

    private int compareOrderByOnly(List<FieldVector> vecs, int a, List<FieldVector> vecsB, int b) {
        for (int k = 0; k < orderKeyIndices.length; k++) {
            int colIdx = orderKeyIndices[k];
            boolean asc = orderAscending.get(k);
            int cmp = ArrowComparators.compare(vecs.get(colIdx), a, vecsB.get(colIdx), b);
            if (cmp != 0) return asc ? cmp : -cmp;
        }
        return 0;
    }

    // ==================== 内存路径输出 ====================

    /**
     * 按 <b>(分组键有序, 组内按 ORDER BY)</b> 展平所有行号为 int[]。
     *
     * <p><b>分组键必须有序</b>（此前只按 map 插入顺序遍历，键是乱序的）：溢写文件的每一批都必须是
     * 「按分组键排序的顺串」，Phase 1.5 的 K 路归并才能把同一个 key 的分散行凑到一起。组内仍按
     * ORDER BY 排好，于是归并出来的文件天然就是最终需要的「组连续 + 组内有序」形态。</p>
     *
     * <p>兼容两种分组存储：Int32 分区键走 {@link RowAccumulator#intKeyMap} 快速路径
     * （{@code keyTw==4} 时行号进 primitive map 免装箱、不进 {@code keyToRowIndices}），
     * 其它类型走通用 {@code keyToRowIndices}。此前的输出路径只遍历 keyToRowIndices，
     * 导致 Int32 分区键（如 {@code PARTITION BY int_col}）时映射为空、静默输出 0 行。
     */
    private int[] flattenGroupSortedIndices(RowAccumulator acc) {
        int keep = pruneKeepRows();   // 每组只保留前 N 行（位置型等值/≤/<N 过滤时），否则不裁
        int[] sortedIndices = new int[acc.rowCount];
        int pos = 0;
        if (acc.hasIntKeys()) {
            for (int key : acc.sortedIntKeys()) {
                List<Integer> indices = acc.getIntRowIndices(key);
                if (indices == null || indices.isEmpty()) continue;
                if (indices.size() > 1) {
                    indices.sort((a, b) -> compareOrderByOnly(acc.vectors, a, acc.vectors, b));
                }
                int lim = Math.min(keep, indices.size());
                for (int i = 0; i < lim; i++) sortedIndices[pos++] = indices.get(i);
            }
        } else {
            List<Object> keys = new ArrayList<>(acc.keyToRowIndices.keySet());
            sortKeys(keys);
            for (Object key : keys) {
                List<Integer> indices = acc.keyToRowIndices.get(key);
                if (indices == null || indices.isEmpty()) continue;
                if (indices.size() > 1) {
                    indices.sort((a, b) -> compareOrderByOnly(acc.vectors, a, acc.vectors, b));
                }
                int lim = Math.min(keep, indices.size());
                for (int i = 0; i < lim; i++) sortedIndices[pos++] = indices.get(i);
            }
        }
        // 裁剪后行数可能少于 acc.rowCount ⇒ 必须截断（否则调用方按 length 会写出 0 值尾行）
        return pos == sortedIndices.length ? sortedIndices : Arrays.copyOf(sortedIndices, pos);
    }

    /**
     * Phase 1 溢写 / Phase 1.5 归并的「组内预裁剪行数」—— 位置型窗口（{@code ROW_NUMBER} / {@code COUNT}）
     * 的等值 / ≤N / &lt;N 过滤只可能命中每组的<b>最前面若干行</b>（组内已按 ORDER BY 排好），其后的行
     * 永远进不了结果。故在<b>溢写时就地丢弃</b>，避免把整组（实测可达 7G/Worker）写盘、再在 Phase 2
     * 全量归并后才过滤掉 —— 这正是 {@code row_number()=1 ... limit 500} 这类「高基数去重」查询卡死
     * 在 Phase 2 归并（写放大 ~100G 磁盘 I/O）的根因。
     *
     * <p><b>可分解性</b>：{@code top-N(并集) = top-N(⋃ 各部分的 top-N)} ⇒ 分槽 / 分批 / 分文件各自裁剪后
     * 再归并，结果与「全量归并后再裁」逐行一致。归并阶段另用「分组键 + ORDER BY」二级比较（见
     * {@link #mergeOneRound}），保证组内按 ORDER BY 有序 ⇒ 取前 N 行即全局前 N 行。</p>
     *
     * <p><b>仅位置型成立</b>：{@code RANK} / {@code DENSE_RANK} 的等值需保留<b>全部并列行</b>，聚合型
     * （MIN/MAX/SUM/AVG）非单调，均<b>不裁剪</b>；范围取「从第 N 行到末尾」（GE/GT）也不能只留前几行。
     * 无 ORDER BY（全量窗口）时位置语义本身是任意的，同样不裁（保守）。</p>
     *
     * @return 每组保留的最大行数；{@link Integer#MAX_VALUE} = 不裁剪
     */
    private int pruneKeepRows() {
        WindowFilterConfig f = filterConfig;
        if (f == null || !f.isActive() || !f.isPositional() || f.noOrderBy()) {
            return Integer.MAX_VALUE;
        }
        long n = f.targetValue();
        return switch (f.operator()) {
            case EQ, LE -> (int) Math.min(Integer.MAX_VALUE, Math.max(1L, n));
            case LT -> (int) Math.min(Integer.MAX_VALUE, Math.max(0L, n - 1));
            case GE, GT -> Integer.MAX_VALUE;
        };
    }

    private void outputPagesFromAccumulator(RowAccumulator acc, int pid) {
        // Phase 1 末尾：排序 + 过滤（修改 keyToRowIndices，剔除不合格数据）
        long t0 = System.nanoTime();
        preFilter(acc);
        sortNanos.addAndGet(System.nanoTime() - t0);

        // Phase 2：纯输出
        FlattenedIndices fl = flattenAfterFilter(acc);
        int[] sortedIndices = fl.indices();
        int[] groupEnds = fl.groupEnds();
        int total = sortedIndices.length;
        if (total == 0) return;

        List<Field> fields = inputSchema.getFields();
        // ★ 按「分组边界」成页（而不是按固定行数硬切）：下游 WindowFunctionOperator 在每页 row==0
        //   无条件 resetPartitionState（单键 / 全局分区路径），页首若不是组首就会把分组切断、结果错。
        int offset = 0;
        int gi = 0;
        while (offset < total) {
            checkCancelled();
            // 至少并入一个完整分组；随后继续并入，直到凑够一页（或没有更多分组）
            int end;
            do {
                end = groupEnds[gi++];
            } while (end < total && end - offset < OUTPUT_PAGE_ROWS && gi < groupEnds.length);

            int rows = end - offset;
            List<FieldVector> outVectors = new ArrayList<>();
            long tAlloc = System.nanoTime();
            for (Field field : fields) {
                FieldVector dst = field.createVector(allocator);
                dst.setInitialCapacity(rows);
                dst.allocateNew();
                outVectors.add(dst);
            }
            allocNanos.addAndGet(System.nanoTime() - tAlloc);
            copyPagePermuted(acc.vectors, outVectors, sortedIndices, offset, rows,
                    copyFixedNanos, copyVarNanos);
            for (FieldVector v : outVectors) v.setValueCount(rows);
            emitPage(Page.partitioned(
                    new VectorSchemaRoot(fields, outVectors, rows), pid));
            bumpMergeProgress();   // 内存路径每吐一页 → 归并推进
            offset = end;
        }
    }

    // ==================== Phase 1 末尾过滤：Sort → Filter ====================

    /**
     * Phase 1 末尾过滤：对已聚合完毕的 RowAccumulator，先排序再根据 {@link WindowFilterConfig}
     * 过滤每个 key 分组。不合格的 key 被移除，合格的 key 仅保留符合过滤条件的行索引。
     * 完成后 acc.rowCount 更新为合格总行数。
     *
     * <h3>按函数类型分三种路径</h3>
     * <ul>
     *   <li><b>位置型</b>（ROW_NUMBER / COUNT）：排序后按序号/区间裁剪行列表</li>
     *   <li><b>排名型</b>（RANK / DENSE_RANK）：排序后逐行计算排名，按操作符收集</li>
     *   <li><b>聚合型</b>（MIN / MAX 无 ORDER BY）：分区级单值，不符合→整组移除</li>
     * </ul>
     *
     * <h3>不支持下推的函数（不单调）</h3>
     * SUM / AVG 不走此方法，filterConfig 为 null。
     */
    private void preFilter(RowAccumulator acc) {
        if (filterConfig == null || !filterConfig.isActive()) return;

        long N = filterConfig.targetValue();
        WindowFunctionType fnType = filterConfig.functionType();
        boolean positional = filterConfig.isPositional();
        boolean rank = filterConfig.isRank();
        boolean aggregate = filterConfig.isAggregate();
        boolean valueMonotonic = filterConfig.isValueMonotonic();
        boolean sumWithOrderBy = filterConfig.isSumWithOrderBy();
        int aggCol = filterConfig.aggregateColIndex();
        WindowFilterConfig.FilterOp op = filterConfig.operator();

        // 分区级聚合和逐行单调值型都需要读取聚合列；位置型/排名型不需要排序聚合列
        boolean needSort = !aggregate;

        int newRowCount = 0;

        if (acc.hasIntKeys()) {
            int[] keys = acc.sortedIntKeys();
            for (int key : keys) {
                checkCancelled();
                List<Integer> indices = acc.getIntRowIndices(key);
                if (indices == null || indices.isEmpty()) continue;

                if (needSort && indices.size() > 1) {
                    indices.sort((a, b) -> compareOrderByOnly(acc.vectors, a, acc.vectors, b));
                }

                byte keySign = sumWithOrderBy ? getKeySign(acc, key) : 0;
                List<Integer> filtered = applyFilter(acc.vectors, indices, fnType, positional,
                        rank, aggregate, valueMonotonic, sumWithOrderBy, keySign, aggCol, N, op);
                if (filtered.isEmpty()) {
                    acc.intKeyMap.removeByKey(key);
                    continue;
                }
                indices.clear();
                indices.addAll(filtered);
                newRowCount += filtered.size();
            }
        } else {
            var it = acc.keyToRowIndices.entrySet().iterator();
            while (it.hasNext()) {
                checkCancelled();
                var entry = it.next();
                List<Integer> indices = entry.getValue();
                if (indices.isEmpty()) { it.remove(); continue; }

                if (needSort && indices.size() > 1) {
                    indices.sort((a, b) -> compareOrderByOnly(acc.vectors, a, acc.vectors, b));
                }

                byte keySign = sumWithOrderBy ? getKeySign(acc, entry.getKey()) : 0;
                List<Integer> filtered = applyFilter(acc.vectors, indices, fnType, positional,
                        rank, aggregate, valueMonotonic, sumWithOrderBy, keySign, aggCol, N, op);
                if (filtered.isEmpty()) {
                    it.remove();
                    continue;
                }
                indices.clear();
                indices.addAll(filtered);
                newRowCount += filtered.size();
            }
        }
        acc.rowCount = newRowCount;
    }

    /** per-key 符号辅助：从 acc 查符号。null→混合(0)，±1 pass through。 */
    private static byte getKeySign(RowAccumulator acc, Object key) {
        Byte s = acc.getKeySign(key);
        return s != null ? s : 0;
    }

    private static byte getKeySign(RowAccumulator acc, int key) {
        Byte s = acc.getIntKeySign(key);
        return s != null ? s : 0;
    }

    /** 统一过滤入口：按函数类型分发到五种路径。 */
    private List<Integer> applyFilter(List<FieldVector> vectors, List<Integer> sortedIndices,
                                       WindowFunctionType fnType,
                                       boolean positional, boolean rank,
                                       boolean aggregate, boolean valueMonotonic,
                                       boolean sumWithOrderBy,
                                       byte signMode,
                                       int aggCol, long N, WindowFilterConfig.FilterOp op) {
        if (aggregate) {
            return filterPartitionAggregate(vectors, sortedIndices, fnType, aggCol, N, op);
        }
        if (valueMonotonic) {
            return filterValueMonotonic(vectors, sortedIndices, fnType, aggCol, N, op);
        }
        if (sumWithOrderBy) {
            return filterSumBySign(vectors, sortedIndices, aggCol, signMode, N, op);
        }
        if (positional) {
            return filterPositional(sortedIndices, N, op);
        }
        return filterRank(vectors, sortedIndices, fnType, N, op);
    }

    // ---- 分区级聚合（全量窗口）：MIN / MAX / SUM / AVG ----

    private List<Integer> filterPartitionAggregate(List<FieldVector> vectors,
                                                    List<Integer> sortedIndices,
                                                    WindowFunctionType fnType,
                                                    int aggCol, long N,
                                                    WindowFilterConfig.FilterOp op) {
        double value = computeAggregate(vectors, sortedIndices, fnType, aggCol);
        return matchOp((long) value, N, op) ? new ArrayList<>(sortedIndices) : List.of();
    }

    private static double computeAggregate(List<FieldVector> vectors, List<Integer> indices,
                                           WindowFunctionType fnType, int aggCol) {
        FieldVector colVec = vectors.get(aggCol);
        return switch (fnType) {
            case MIN -> aggregateMin(colVec, indices);
            case MAX -> aggregateMax(colVec, indices);
            case SUM -> aggregateSum(colVec, indices);
            case AVG -> aggregateAvg(colVec, indices);
            default -> 0;
        };
    }

    private static double aggregateMin(FieldVector colVec, List<Integer> indices) {
        double value = Double.POSITIVE_INFINITY;
        for (int r : indices) {
            if (colVec.isNull(r)) continue;
            value = Math.min(value, toDouble(colVec, r));
        }
        return value;
    }

    private static double aggregateMax(FieldVector colVec, List<Integer> indices) {
        double value = Double.NEGATIVE_INFINITY;
        for (int r : indices) {
            if (colVec.isNull(r)) continue;
            value = Math.max(value, toDouble(colVec, r));
        }
        return value;
    }

    private static double aggregateSum(FieldVector colVec, List<Integer> indices) {
        double sum = 0;
        for (int r : indices) {
            if (!colVec.isNull(r)) sum += toDouble(colVec, r);
        }
        return sum;
    }

    private static double aggregateAvg(FieldVector colVec, List<Integer> indices) {
        double sum = 0;
        int count = 0;
        for (int r : indices) {
            if (colVec.isNull(r)) continue;
            sum += toDouble(colVec, r);
            count++;
        }
        return count > 0 ? sum / count : 0;
    }

    // ---- 逐行单调值型（有排序窗口）：MIN / MAX ----

    private List<Integer> filterValueMonotonic(List<FieldVector> vectors,
                                                List<Integer> sortedIndices,
                                                WindowFunctionType fnType,
                                                int aggCol, long N,
                                                WindowFilterConfig.FilterOp op) {
        boolean isMax = fnType == WindowFunctionType.MAX;
        FieldVector colVec = vectors.get(aggCol);
        double running = isMax ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        List<Integer> result = new ArrayList<>();

        for (int row : sortedIndices) {
            if (!colVec.isNull(row)) {
                double v = toDouble(colVec, row);
                running = isMax ? Math.max(running, v) : Math.min(running, v);
            }
            if (matchOp((long) running, N, op)) {
                result.add(row);
            }
            // 早停：MAX 单调不降，超过阈值后 LE/LT/EQ 可停
            if (isMax && (long) running > N
                    && (op == WindowFilterConfig.FilterOp.LE
                        || op == WindowFilterConfig.FilterOp.LT
                        || op == WindowFilterConfig.FilterOp.EQ)) {
                break;
            }
            // 早停：MIN 单调不增，低于阈值后 GE/GT/EQ 可停
            if (!isMax && (long) running < N
                    && (op == WindowFilterConfig.FilterOp.GE
                        || op == WindowFilterConfig.FilterOp.GT
                        || op == WindowFilterConfig.FilterOp.EQ)) {
                break;
            }
        }
        return result;
    }

    // ---- SUM + ORDER BY：per-key 符号检查（全同号→单调，混合→不过滤）----

    /**
     * 按 pre-computed 符号过滤 SUM 运行值。
     * signMode: 0=混合(不过滤全量), >0=全非负(不降), <0=全非正(不增)。
     * 符号已在 RowAccumulator.addPage 中随数据累积同步追踪。
     */
    private List<Integer> filterSumBySign(List<FieldVector> vectors,
                                           List<Integer> sortedIndices,
                                           int aggCol, byte signMode,
                                           long N, WindowFilterConfig.FilterOp op) {
        if (signMode == 0) return new ArrayList<>(sortedIndices); // 混合 → 全量

        FieldVector colVec = vectors.get(aggCol);
        boolean nonDecreasing = signMode > 0;
        double running = 0.0;
        List<Integer> result = new ArrayList<>();

        for (int row : sortedIndices) {
            double v = colVec.isNull(row) ? 0 : toDouble(colVec, row);
            running += v;   // SUM: accumulating from 0; direction only affects early-stop
            if (matchOp((long) running, N, op)) result.add(row);
            // 早停：不降方向 → running > N 且 LE/LT/EQ 时可停
            if (nonDecreasing && (long) running > N
                    && (op == WindowFilterConfig.FilterOp.LE
                        || op == WindowFilterConfig.FilterOp.LT
                        || op == WindowFilterConfig.FilterOp.EQ)) break;
            // 早停：不增方向 → running < N 且 GE/GT/EQ 时可停
            if (!nonDecreasing && (long) running < N
                    && (op == WindowFilterConfig.FilterOp.GE
                        || op == WindowFilterConfig.FilterOp.GT
                        || op == WindowFilterConfig.FilterOp.EQ)) break;
        }
        return result;
    }

    // ---- 位置型（ROW_NUMBER / COUNT）----

    private List<Integer> filterPositional(List<Integer> sortedIndices, long N,
                                            WindowFilterConfig.FilterOp op) {
        int size = sortedIndices.size();
        if (size < filterConfig.minGroupSize()) return List.of();
        if (op == WindowFilterConfig.FilterOp.EQ) {
            return List.of(sortedIndices.get((int) N - 1));
        }
        int start = 0, end = size;
        switch (op) {
            case LE -> end = Math.min((int) N, size);
            case LT -> end = Math.min((int) N - 1, size);
            case GE -> start = Math.max(0, (int) N - 1);
            case GT -> start = Math.min((int) N, size);
        }
        List<Integer> result = new ArrayList<>();
        for (int i = start; i < end; i++) result.add(sortedIndices.get(i));
        return result;
    }

    // ---- 排名型（RANK / DENSE_RANK）----

    private List<Integer> filterRank(List<FieldVector> vectors, List<Integer> sortedIndices,
                                      WindowFunctionType fnType, long N,
                                      WindowFilterConfig.FilterOp op) {
        List<Integer> result = new ArrayList<>();
        long rankVal = 1, sameCount = 1, denseRank = 1;
        for (int i = 0; i < sortedIndices.size(); i++) {
            if (i > 0) {
                int cmp = compareOrderByOnly(vectors, sortedIndices.get(i),
                        vectors, sortedIndices.get(i - 1));
                if (cmp != 0) { rankVal += sameCount; sameCount = 1; denseRank++; }
                else { sameCount++; }
            }
            long cur = fnType == WindowFunctionType.RANK ? rankVal : denseRank;
            if (matchOp(cur, N, op)) {
                result.add(sortedIndices.get(i));
            } else if (cur > N && (op == WindowFilterConfig.FilterOp.LE
                    || op == WindowFilterConfig.FilterOp.LT)) {
                break;
            }
        }
        return result;
    }

    private static boolean matchOp(long left, long right, WindowFilterConfig.FilterOp op) {
        return switch (op) {
            case EQ -> left == right;
            case LE -> left <= right;
            case LT -> left < right;
            case GE -> left >= right;
            case GT -> left > right;
        };
    }

    private static double toDouble(FieldVector vec, int row) {
        Object obj = vec.getObject(row);
        if (obj instanceof Number n) return n.doubleValue();
        return 0;
    }

    /**
     * 展平结果：{@code indices} 为按 (分组有序, 组内 ORDER BY) 排好的行号；
     * {@code groupEnds[i]} 是第 i 个分组在 {@code indices} 中的结束下标（不含），用于「按组边界成页」。
     */
    private record FlattenedIndices(int[] indices, int[] groupEnds) { }

    /**
     * 预过滤后展平分组索引：各 key 已在内排好序，直接拼接；并记录每个分组的结束下标。
     *
     * <p>为什么要记录组边界：内存路径的成页<b>必须落在分组边界</b>。下游
     * {@code WindowFunctionOperator} 在每页 {@code row == 0} 无条件 {@code resetPartitionState()}
     * （单键 / 全局分区路径），若一个分组被固定行数硬切到两页，第 2 页首行会被误判为新分区 ⇒
     * ROW_NUMBER 重数、RANK 重置（结果错）。</p>
     */
    private FlattenedIndices flattenAfterFilter(RowAccumulator acc) {
        int[] result = new int[acc.rowCount];
        int[] ends = new int[Math.max(1, Math.min(acc.rowCount, 256))];
        int pos = 0;
        int gc = 0;
        if (acc.hasIntKeys()) {
            for (int key : acc.sortedIntKeys()) {
                List<Integer> indices = acc.getIntRowIndices(key);
                if (indices == null || indices.isEmpty()) continue;
                if (indices.size() > 1) {
                    indices.sort((a, b) -> compareOrderByOnly(acc.vectors, a, acc.vectors, b));
                }
                for (int idx : indices) result[pos++] = idx;
                if (gc == ends.length) ends = Arrays.copyOf(ends, ends.length * 2);
                ends[gc++] = pos;
            }
        } else {
            for (List<Integer> indices : acc.keyToRowIndices.values()) {
                if (indices.isEmpty()) continue;
                if (indices.size() > 1) {
                    indices.sort((a, b) -> compareOrderByOnly(acc.vectors, a, acc.vectors, b));
                }
                for (int idx : indices) result[pos++] = idx;
                if (gc == ends.length) ends = Arrays.copyOf(ends, ends.length * 2);
                ends[gc++] = pos;
            }
        }
        return new FlattenedIndices(result, Arrays.copyOf(ends, gc));
    }

    // ==================== Phase 1.5: K 路归并 → key 连续文件 ====================

    /**
     * 把分区内全部溢写批文件归并成 <b>key 连续</b>文件（同一分组的所有行落在同一个文件内），
     * 文件数 &gt; {@link #MAX_MERGE_K} 时多轮归并。
     *
     * <p>前提：每个源文件已是「按分组键有序的顺串」（见 {@link #flattenGroupSortedIndices}）。
     * 归并按分组键比较，因此输出也是分组键有序的 —— 同一 key 的行自然相邻；再配合
     * 「key 变化才允许切分输出文件」，保证一个分组不被切到两个文件里，Phase 2 便能逐文件顺序处理。</p>
     *
     * <p>峰值内存 ≈ {@code 每轮归并路数 × 单批大小}（每个源只驻留一个 record batch），与已溢写总量无关。
     * 归并路数<b>不写死也不靠估算</b>：边开源边读<b>分配器实时占用</b>，一旦接近本分区可用的输入预算就停手，
     * 因此对 Arrow「分配按 2 的幂向上取整」等内部行为天然免疫 —— 分配器上限越小，路段数越少，绝不撑爆堆外内存。</p>
     */
    private void compactPartition(PartitionState<RowAccumulator> pa, int pid) {
        long tStartNanos = System.nanoTime();
        List<Path> sources = new ArrayList<>(pa.batchFiles);
        if (sources.size() <= 1) {
            pa.compacted = true;
            return;
        }
        int initialFiles = sources.size();
        long inputCap = mergeInputCap();
        int round = 0;
        while (sources.size() > 1) {
            checkCancelled();
            List<Path> next = new ArrayList<>();
            int i = 0;
            while (i < sources.size()) {
                checkCancelled();
                int[] consumedOut = new int[1];
                next.addAll(mergeOneRound(sources, i, pid, round, inputCap, consumedOut));
                i += Math.max(1, consumedOut[0]);
            }
            for (Path old : sources) {
                if (!next.contains(old)) {
                    try { Files.deleteIfExists(old); } catch (IOException ignored) {}
                }
            }
            sources = next;
            round++;
            bumpMergeProgress();   // 每完成一轮 K 路归并（文件数减半）→ 归并推进
        }
        pa.batchFiles.clear();
        pa.batchFiles.addAll(sources);
        pa.compacted = true;
        System.err.printf("[%s] 溢写归并: 分区 %d, %d 批 -> %d 文件 / %d 轮，用时 %dms%n",
                getClass().getSimpleName(), pid, initialFiles, sources.size(), round,
                (System.nanoTime() - tStartNanos) / 1_000_000);
    }

    /**
     * 单次归并可供输入侧使用的字节上限 = 算子分配器上限 / 并发归并的分区数 / 2（另一半留给输出缓冲）。
     *
     * <p>Phase 2 每分区一个线程并行跑归并，共享同一个分配器，因此必须按并发分区数摊分，否则
     * {@code 分区数 × 路段数 × 单批大小} 会随并发一起放大。</p>
     */
    private long mergeInputCap() {
        int concurrent = 0;
        for (PartitionState<RowAccumulator> p : partitions) {
            if (p != null) concurrent++;
        }
        long perMerge = allocator.getLimit() / Math.max(1, concurrent);
        return Math.max(1, perMerge / 2);
    }

    /**
     * 单轮 K 路归并：从 {@code start} 起<b>边开源边读</b>，路数取到「再加一路就要超输入上限」为止
     * （至少 2 路，保证每轮文件数至少减半；最多 {@link #MAX_MERGE_K} 路）。
     *
     * <p>输出<b>每组恰好一个 Arrow 文件</b>，但文件内按 {@link #SPILL_BATCH_ROWS} 行一批<b>追加多个
     * record batch</b>（ArrowFileWriter 支持多批）。这样两件事同时成立：
     * <ul>
     *   <li><b>收敛</b>：K 个源 → 1 个文件，每轮文件数至少减半，不会出现「2 个源切出 2 个文件」
     *       导致轮数无穷的死循环；</li>
     *   <li><b>内存有界</b>：任何时刻只驻留 K 个源批 + 一个输出缓冲，与分组总量无关。</li>
     * </ul>
     * 切批：缓冲满<b>且</b>（分组键已变 <b>或</b> 当前无窗口过滤下推）时发生。无过滤时允许「组内切批」，
     * 于是一个超过 {@link #SPILL_BATCH_ROWS} 的巨组也被切成多个 record batch（输出缓冲恒有界）；
     * 归并产物是全局 (分组键, ORDER BY) 有序，故 Phase 2 的<b>流式</b>路径顺序成页即可。有过滤
     * （组要整组算）时仍只在分组边界切批，组不跨批，Phase 2 走逐组路径。</p>
     *
     * @param consumedOut 出参：本轮实际消费掉的源文件个数（包括读空的），供调用方推进游标
     * @return 归并产物；若只消费了 1 个源，则原样返回该源（无需归并）
     */
    @SuppressWarnings("unchecked")
    private List<Path> mergeOneRound(List<Path> sources, int start, int pid, int round,
                                     long inputCap, int[] consumedOut) {
        List<Path> outputFiles = new ArrayList<>();
        int n = sources.size();
        ArrowFileReader[] readers = new ArrowFileReader[n];
        FileChannel[] channels = new FileChannel[n];
        List<ArrowBlock>[] blockLists = new List[n];
        VectorSchemaRoot[] roots = new VectorSchemaRoot[n];
        int[] blockIdx = new int[n];
        int[] positions = new int[n];
        int[] rowCounts = new int[n];
        Path[] loadedPaths = new Path[n];
        int loaded = 0;      // 已装载的源（数组按 0..loaded-1 密排）
        int cursor = start;  // 已消费到的源下标（含读空跳过的）
        Path outFile = null;
        FileChannel outCh = null;
        ArrowFileWriter outWriter = null;
        VectorSchemaRoot outRoot = null;
        boolean wroteAny = false;
        try {
            // ★ 基线：只看「本次归并自己新装载的源」占了多少，而不是分配器的全局占用。
            //   多个分区并发归并共享同一个算子分配器，若用全局占用跟 inputCap（已按并发分区数
            //   摊薄）比较，别处分区占用的内存会把本次归并的路数直接压到 2 路 —— 文件数上千时
            //   就要多轮重写（每轮都重写全量数据），线上表现为「溢写量 200+GB」。
            long baseAlloc = allocator.getAllocatedMemory();
            while (cursor < n && loaded < MAX_MERGE_K) {
                checkCancelled();
                // 用分配器「实时占用（增量）」判路数，而不是预估单批大小：Arrow 分配按 2 的幂
                // 向上取整，预估会低估近一倍。至少放 2 路进来，保证多轮归并必然收敛。
                if (loaded >= 2 && allocator.getAllocatedMemory() - baseAlloc >= inputCap) break;
                Path srcPath = sources.get(cursor++);
                FileChannel ch = FileChannel.open(srcPath, StandardOpenOption.READ);
                ArrowFileReader r = new ArrowFileReader(ch, allocator);
                List<ArrowBlock> blocks = r.getRecordBlocks();
                if (blocks.isEmpty()) {
                    r.close();
                    ch.close();
                    // 空溢写文件没有任何行，直接删掉回收磁盘（归并期间不再占用）
                    try { Files.deleteIfExists(srcPath); } catch (IOException ignored) {}
                    continue;
                }
                // ★ 每个源可能是「一个文件多个 record batch」（上一轮的归并产物），首个批只是开头，
                //   后续批在消费过程中用 advanceBlock 逐个续上 —— 只读首批会静默丢掉后面所有行。
                blockLists[loaded] = blocks;
                r.loadRecordBatch(blocks.get(0));
                readers[loaded] = r;
                channels[loaded] = ch;
                roots[loaded] = r.getVectorSchemaRoot();
                loadedPaths[loaded] = srcPath;
                rowCounts[loaded] = roots[loaded].getRowCount();
                blockIdx[loaded] = 1;
                loaded++;
            }
            consumedOut[0] = cursor - start;
            if (loaded <= 1) {   // 只剩一个源：原样带过，无需归并（注意带过的是「真正非空」的那个）
                return loaded == 1 ? new ArrayList<>(List.of(loadedPaths[0])) : outputFiles;
            }

            final int sCount = loaded;
            // 二级比较：分组键相等时按 ORDER BY 比 —— 归并产物因此是<b>全局 (分组键, ORDER BY) 有序</b>。
            //   · 裁剪（pruneKeepRows）时：组内有序 ⇒ 前 N 行即全局前 N 行，可在归并阶段就丢弃尾部行。
            //   · 不裁剪时（keep=MAX）：仍要二级排序 —— 无过滤的 Phase 2 走<b>流式</b>输出，直接按文件
            //     顺序成页，依赖「组内已按 ORDER BY 有序」跳过排序（避免对超大组做 Integer[] 装箱排序）。
            final int keep = pruneKeepRows();
            // ★ 无过滤时可「组内切批」：Phase 2 走流式输出（不按组物化），跨批的同一个组照样正确。
            //   这样输出缓冲恒 ≤ SPILL_BATCH_ROWS 行 —— 单个巨组不再需要整组驻留内存（曾把 VarChar
            //   缓冲按 2 的幂翻倍撑爆分配器：Unable to allocate buffer of size 134217728）。
            //   有过滤时仍只在分组边界切批：组的 window 过滤要「整组一起算」，组不能跨批。
            final boolean canSplitMidGroup = filterConfig == null || !filterConfig.isActive();
            // ★ 每个源缓存「当前游标行的分组键」：堆比较里一行动辄触发 log2(k) 次比较，若每次都
            //   keyAt() 现场构造（变长键要 new String/装箱，复合键还要 Object[]+CompositeKey），
            //   同一条键会被重复构造十几次 —— 实测 199 文件 / 320MB 的归并要 60s+。键只随源推进
            //   而变，故「推进时算一次、比较时直接读缓存」即可，语义完全相同。
            final Object[] curKeys = new Object[sCount];
            final TransferPair[][] rowTps = new TransferPair[sCount][];
            final VectorSchemaRoot[] rowTpsOwner = new VectorSchemaRoot[sCount];
            PriorityQueue<Integer> heap = new PriorityQueue<>((a, b) -> {
                int c = compareGroupKeys(curKeys[a], curKeys[b]);
                if (c != 0) return c;
                return compareOrderByOnly(roots[a].getFieldVectors(), positions[a],
                        roots[b].getFieldVectors(), positions[b]);
            });
            for (int i = 0; i < sCount; i++) {
                if (positions[i] < rowCounts[i]
                        || advanceBlock(i, readers, blockLists, blockIdx, roots, positions, rowCounts)) {
                    curKeys[i] = keyAt(roots[i], positions[i]);
                    heap.offer(i);
                } else {
                    // 空源：立即关句柄并删文件
                    releaseSource(i, readers, channels, roots, loadedPaths);
                }
            }

            Schema schema = inputSchema;
            List<FieldVector> outVectors = null;
            int outRowCount = 0;
            int outCapacity = 0;
            Object lastKey = null;
            int groupOut = 0;   // 当前分组已写出的行数（用于每组只留前 keep 行）

            while (!heap.isEmpty()) {
                checkCancelled();
                int si = heap.poll();
                VectorSchemaRoot src = roots[si];
                int row = positions[si];

                if (outVectors == null) {
                    outVectors = createOutputVectors(schema, SPILL_BATCH_ROWS);
                    // ★ 闸门取所有列的最小容量：Arrow 按字节取整后不同类型换算回的行数不同
                    //   （TINYINT 一次 reAlloc 只有 116504 行，INT 有 127100 行），用首列会在窄列上越界。
                    outCapacity = minValueCapacity(outVectors);
                    outRoot = new VectorSchemaRoot(new ArrayList<>(outVectors));
                    // ★ 文件名带「本组起始源下标 start」：同一轮会做多组归并，只按 round 命名会互相覆盖。
                    outFile = spillDirForWrite().resolve(
                            "p" + pid + "_c" + round + "_s" + start + ".arrow");
                    outCh = FileChannel.open(outFile, StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                    outWriter = new ArrowFileWriter(outRoot, null, outCh);
                }
                Object curKey = curKeys[si];
                // newGroup：curKey 与上一个 key 不同（含首行）。
                boolean newGroup = lastKey == null || !keysEqual(lastKey, curKey);
                // 缓冲满即切批：无过滤时允许组内切（组可跨批，Phase 2 流式处理）；有过滤时只在组边界切。
                if (outRowCount >= SPILL_BATCH_ROWS && (newGroup || canSplitMidGroup)) {
                    outRoot.setRowCount(outRowCount);
                    outWriter.writeBatch();
                    wroteAny = true;
                    bumpMergeProgress();   // 每写完一个输出批 → 归并推进
                    for (FieldVector v : outVectors) v.setValueCount(0);   // 缓冲复位，继续追加
                    outRowCount = 0;
                }
                // 单个分组超过一批时扩容（组不拆批）。★ 容量必须用「实际已分配的 valueCapacity」判断，
                //   不能用 getValueCount()：allocateNew() 后它是 0 ⇒ 每行都判满 ⇒ 每行 reAlloc，
                //   变长列缓冲按行翻倍 ⇒ 直接撑爆分配器（实测 33MB 卡死）。
                if (outRowCount >= outCapacity) {
                    for (FieldVector v : outVectors) v.reAlloc();
                    outCapacity = minValueCapacity(outVectors);
                }

                // ★ 每组只写前 keep 行：组内已按 ORDER BY 有序（二级比较保证），故前 keep 行即全局前 keep。
                //   keep=MAX 时恒真 = 旧行为；超出部分仍要从堆里消费（positions 照常推进）只是不拷贝。
                if (newGroup) groupOut = 0;
                if (groupOut < keep) {
                    // 变长列的 TransferPair 按「源 root 身份」缓存复用（逐行 makeTransferPair 会为每行
                    // 每个变长列新建一个对象；大归并下是数百万次无谓分配）。
                    if (rowTps[si] == null || rowTpsOwner[si] != src) {
                        rowTps[si] = new TransferPair[src.getFieldVectors().size()];
                        rowTpsOwner[si] = src;
                    }
                    copyRow(src, row, outVectors, outRowCount, rowTps[si]);
                    outRowCount++;
                }
                groupOut++;
                lastKey = curKey;

                positions[si]++;
                if (positions[si] >= rowCounts[si]) {
                    // 当前批读完 → 续上该源的下一个 record batch（可能没有）
                    if (advanceBlock(si, readers, blockLists, blockIdx, roots, positions, rowCounts)) {
                        curKeys[si] = keyAt(roots[si], positions[si]);
                        heap.offer(si);
                    } else {
                        // ★ 该源已读尽：立刻关句柄并删除溢写文件。
                        //   归并产物与源同量，若等整轮结束再删，峰值 ≈ 2× 溢写量 ——
                        //   盘上只剩 8GB 而溢写 18.8GB 时必然触发 SpillSpaceException。
                        //   边读边删把峰值压回 ≈ 1×（源减少多少、产物增加多少）。
                        releaseSource(si, readers, channels, roots, loadedPaths);
                    }
                } else {
                    curKeys[si] = keyAt(roots[si], positions[si]);
                    heap.offer(si);
                }
            }
            if (outWriter != null && outRowCount > 0) {
                outRoot.setRowCount(outRowCount);
                outWriter.writeBatch();
                wroteAny = true;
                bumpMergeProgress();   // 收尾批 → 归并推进
            }
            if (wroteAny) outputFiles.add(outFile);
            return outputFiles;
        } catch (IOException e) {
            throw new RuntimeException("窗口溢写归并失败: partition " + pid + " round " + round, e);
        } finally {
            if (outWriter != null) { try { outWriter.close(); } catch (Exception ignored) {} }
            if (outCh != null) { try { outCh.close(); } catch (Exception ignored) {} }
            if (outRoot != null) { try { outRoot.close(); } catch (Exception ignored) {} }
            if (wroteAny && outFile != null) {
                // 归并重写也是真实溢写 I/O：文件已关闭，按实际大小计入（否则监控明显偏低）。
                accountCompactionFile(partitions[pid], outFile);
            } else if (outFile != null) {
                try { Files.deleteIfExists(outFile); } catch (IOException ignored) {}
            }
            for (int i = 0; i < loaded; i++) {
                if (roots[i] != null) { try { roots[i].close(); } catch (Exception ignored) {} }
                if (readers[i] != null) { try { readers[i].close(); } catch (Exception ignored) {} }
                if (channels[i] != null) { try { channels[i].close(); } catch (Exception ignored) {} }
            }
        }
    }

    /**
     * 释放一个已读尽的归并源：关 root / reader / channel 后删除溢写文件。
     *
     * <p>删文件必须发生在关句柄之后（Windows 下打开中的文件不能删）。把「源读尽即删」放进
     * 归并循环，是为了把 Phase 1.5 的磁盘峰值从 <b>≈2× 溢写量</b>（旧文件全留到整轮结束）
     * 压到 <b>≈1×</b> —— 否则磁盘余量小于溢写量一半时，归并必然撞
     * {@link SpillDirs.SpillSpaceException}。数组槽位随即置 null，外层 finally 不会重复关闭。</p>
     */
    private static void releaseSource(int i, ArrowFileReader[] readers, FileChannel[] channels,
                                      VectorSchemaRoot[] roots, Path[] loadedPaths) {
        if (roots[i] != null) {
            try { roots[i].close(); } catch (Exception ignored) { }
            roots[i] = null;
        }
        if (readers[i] != null) {
            try { readers[i].close(); } catch (Exception ignored) { }
            readers[i] = null;
        }
        if (channels[i] != null) {
            try { channels[i].close(); } catch (Exception ignored) { }
            channels[i] = null;
        }
        Path p = loadedPaths[i];
        if (p != null) {
            try { Files.deleteIfExists(p); } catch (IOException ignored) { }
            loadedPaths[i] = null;
        }
    }

    /** 把源 {@code si} 推进到它文件里的下一个非空 record batch；成功（有非空批）返回 true。 */
    private boolean advanceBlock(int si, ArrowFileReader[] readers, List<ArrowBlock>[] blockLists,
                                 int[] blockIdx, VectorSchemaRoot[] roots,
                                 int[] positions, int[] rowCounts) {
        List<ArrowBlock> blocks = blockLists[si];
        while (blockIdx[si] < blocks.size()) {
            int rc;
            try {
                readers[si].loadRecordBatch(blocks.get(blockIdx[si]++));
                roots[si] = readers[si].getVectorSchemaRoot();
                rc = roots[si].getRowCount();
            } catch (IOException e) {
                throw new RuntimeException("窗口溢写归并读取下一批失败", e);
            }
            positions[si] = 0;
            rowCounts[si] = rc;
            if (rc > 0) return true;
        }
        return false;
    }

    private List<FieldVector> createOutputVectors(Schema schema, int capacity) {
        List<FieldVector> vecs = new ArrayList<>(schema.getFields().size());
        for (Field f : schema.getFields()) {
            FieldVector v = f.createVector(allocator);
            v.setInitialCapacity(capacity);
            v.allocateNew();
            vecs.add(v);
        }
        return vecs;
    }

    /**
     * 逐行拷贝一行的所有列。
     *
     * @param tps 变长列的 TransferPair 缓存（按列下标；定长列为 null）——由调用方按「源 root 身份」
     *            复用，避免每行 {@code makeTransferPair} 的分配
     */
    private void copyRow(VectorSchemaRoot src, int srcRow, List<FieldVector> dst, int dstRow,
                         TransferPair[] tps) {
        List<FieldVector> srcVecs = src.getFieldVectors();
        for (int c = 0; c < srcVecs.size(); c++) {
            FieldVector sv = srcVecs.get(c);
            FieldVector dv = dst.get(c);
            int tw = RowAccumulator.typeWidthOf(sv.getField().getType());
            if (tw > 0) {
                dv.getDataBuffer().setBytes((long) dstRow * tw,
                        sv.getDataBuffer(), (long) srcRow * tw, tw);
                // ★ 定长列的 memcpy 快路径只管数据、不管 validity：必须显式把非空行的 validity 置位
                //   （新建缓冲全 0 = 全 NULL；缓冲复用后还可能残留上一批的 0）。变长列交给
                //   copyValueSafe（它自带 validity 处理）。漏了这步 ⇒ 溢写归并产物整列变 NULL。
                if (sv.isNull(srcRow)) {
                    dv.setNull(dstRow);
                } else {
                    ArrowBuf val = dv.getValidityBuffer();
                    if (val != null) BitVectorHelper.setBit(val, dstRow);
                }
            } else {
                TransferPair tp = tps[c];
                if (tp == null) {
                    tp = sv.makeTransferPair(dv);
                    tps[c] = tp;
                }
                tp.copyValueSafe(srcRow, dstRow);
            }
        }
    }

    private Object keyAt(VectorSchemaRoot root, int row) {
        return buildKeyFromSpill(root, row);
    }

    // ==================== Phase 2: 顺序流式读取（key 连续） ====================

    /**
     * 读一个「key 连续」文件，按分组边界切出连续区间，<b>跨组累积成页</b>后回传下游。
     *
     * <p><b>仅用于「有窗口过滤下推」的路径</b>（无过滤走 {@link #streamBatchFileFlat}）。此时切批只在分组
     * 边界上，<b>同一分组不会跨批</b>，所以每个批各自扫一遍即可（组的 window 过滤要整组一起算）。
     * 归并产物是<b>一个文件多个 record batch</b>（见 {@link #mergeOneRound}），故要遍历文件内全部批。
     * 整个 Phase 2 任意时刻只驻留<b>一个 record batch</b>（外加累积中的输出页）。</p>
     *
     * <p><b>成页批量</b>：旧实现「一个 key 组一页」，高基数下每组只有个位数行、每页都付一次固定开销
     * （建 12 个向量 / 下游建页 / Hive 建 ORC 批），实测把 277 万个组变成 277 万个页 ⇒ 吞吐掉到 ~3K 行/s。
     * 现在用 {@link Phase2Out} 跨组累积，攒够 {@link #FETCH_PER_KEYS} 个 key 组（或 {@link #OUTPUT_PAGE_ROWS}
     * 行）才吐一页，页数降到千级。</p>
     */
    private void streamBatchFile(PartitionState<RowAccumulator> pa, int batchIndex, int pid) {
        Path file = pa.batchFiles.get(batchIndex);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
             ArrowFileReader reader = new ArrowFileReader(ch, allocator)) {
            Phase2Out out = new Phase2Out(pid);
            try {
                for (ArrowBlock block : reader.getRecordBlocks()) {
                    checkCancelled();
                    reader.loadRecordBatch(block);
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    int n = root.getRowCount();
                    int from = 0;
                    while (from < n) {
                        checkCancelled();
                        Object key = buildKeyFromSpill(root, from);
                        int to = from + 1;
                        while (to < n && keysEqual(key, buildKeyFromSpill(root, to))) to++;
                        out.appendGroup(root, from, to);
                        from = to;
                    }
                }
                out.flush();   // 文件尾残留的不足一批也发出去
            } finally {
                out.closeQuietly();
            }
        } catch (IOException e) {
            throw new RuntimeException("读取窗口溢写文件失败: " + file, e);
        }
    }

    /**
     * Phase 2 <b>流式</b>路径（无窗口过滤下推时）：归并产物是全局 (分组键, ORDER BY) 有序的，直接
     * <b>顺序</b>把文件内每个 record batch 的行追加进输出页，攒够 {@link #OUTPUT_PAGE_ROWS} 行就吐一页。
     *
     * <p>与原逐组路径的关键差别：<b>不按组物化、不做 Integer[] 装箱排序</b>。于是一个超过
     * {@link #SPILL_BATCH_ROWS} 行的大分组可以跨多个 record batch、跨多页输出，Phase 2 任意时刻只驻留
     * 「一个 record batch + 一个输出页」（≈ {@link #OUTPUT_PAGE_ROWS} 行），与分组大小、已溢写总量都无关。
     * 分组跨页是<b>正确</b>的：下游按相邻行分区键比较延续分区状态（见 {@code WindowFunctionOperator}）。</p>
     */
    private void streamBatchFileFlat(PartitionState<RowAccumulator> pa, int batchIndex, int pid) {
        Path file = pa.batchFiles.get(batchIndex);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
             ArrowFileReader reader = new ArrowFileReader(ch, allocator)) {
            FlatOut out = new FlatOut(pid);
            try {
                for (ArrowBlock block : reader.getRecordBlocks()) {
                    checkCancelled();
                    reader.loadRecordBatch(block);
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    int n = root.getRowCount();
                    if (n > 0) out.appendAll(root, n);
                }
                out.flush();   // 文件尾残留的不足一批也发出去
            } finally {
                out.closeQuietly();
            }
        } catch (IOException e) {
            throw new RuntimeException("读取窗口溢写文件失败: " + file, e);
        }
    }

    /**
     * Phase 2 流式路径的输出页累积器：把行按文件顺序拷进累积向量，攒够 {@link #OUTPUT_PAGE_ROWS} 行成页。
     *
     * <p>与 {@link Phase2Out} 的区别：不管分组边界、不排序、不套过滤 —— 归并产物已全局有序，顺序成页即可。
     * 累积向量按需 {@code reAlloc}（上限≈ {@link #OUTPUT_PAGE_ROWS} 行），TransferPair 缓存复用。</p>
     */
    private final class FlatOut {
        private final int pid;
        private List<Field> fields;
        private List<FieldVector> vectors;   // 累积中的输出列（懒建）
        private List<TransferPair> transfers;
        private VectorSchemaRoot srcRoot;    // transfers 当前绑定的源 root
        private int rows;

        FlatOut(int pid) { this.pid = pid; }

        /** 追加源 root 的 [0,n) 行（顺序，不排序）。够一页即吐。 */
        void appendAll(VectorSchemaRoot root, int n) {
            ensureVectors(root);
            int need = rows + n;
            boolean grown = false;
            for (FieldVector v : vectors) {
                while (v.getValueCapacity() < need) { v.reAlloc(); grown = true; }
            }
            if (grown) transfers = null;   // reAlloc 换缓冲后重建映射（保险）
            if (transfers == null || srcRoot != root) {
                List<TransferPair> tp = new ArrayList<>(vectors.size());
                for (int col = 0; col < vectors.size(); col++) {
                    tp.add(root.getVector(col).makeTransferPair(vectors.get(col)));
                }
                transfers = tp;
                srcRoot = root;
            }
            for (int i = 0; i < n; i++) {
                for (TransferPair tp : transfers) tp.copyValueSafe(i, rows + i);
            }
            rows = need;
            if (rows >= OUTPUT_PAGE_ROWS) flush();
        }

        /** 把累积的行成页 emit（不足一批也发）；空则 no-op。向量所有权随之移交，本对象可继续累积。 */
        void flush() {
            if (rows == 0) return;
            for (FieldVector v : vectors) v.setValueCount(rows);
            emitPage(Page.partitioned(new VectorSchemaRoot(fields, vectors, rows), pid));
            bumpMergeProgress();   // 每吐一页 → 归并推进
            vectors = null;
            transfers = null;
            srcRoot = null;
            rows = 0;
        }

        /** 异常路径释放尚未成页的累积向量（已移交的已置 null，不受影响）。 */
        void closeQuietly() {
            if (vectors == null) return;
            for (FieldVector v : vectors) {
                try { v.close(); } catch (Exception ignored) { }
            }
            vectors = null;
            transfers = null;
            srcRoot = null;
            rows = 0;
        }

        private void ensureVectors(VectorSchemaRoot root) {
            if (vectors != null) return;
            this.fields = root.getSchema().getFields();
            this.vectors = new ArrayList<>(fields.size());
            for (Field f : fields) {
                FieldVector v = f.createVector(allocator);
                v.setInitialCapacity(Math.min(OUTPUT_PAGE_ROWS, 1024));
                v.allocateNew();
                vectors.add(v);
            }
            this.transfers = null;
            this.srcRoot = null;
        }
    }

    /**
     * 按 ORDER BY 排好一个 key 组（归并只保证<b>组间</b>有序，组内跨批次的相对顺序不保证），再套用
     * 窗口过滤下推 —— 与内存路径 {@code preFilter} 的「排序 → 按时序逐组过滤」语义一致，返回该组的
     * 绝对行号（已过滤；空列表 = 该组被过滤光）。
     *
     * <p><b>不复制分组数据</b>：直接对 {@code root} 里的绝对行号排序/过滤，实际拷贝在成页时一次完成。</p>
     */
    private List<Integer> orderAndFilterGroup(VectorSchemaRoot root, int from, int to) {
        int n = to - from;
        Integer[] sorted = new Integer[n];
        for (int i = 0; i < n; i++) sorted[i] = from + i;
        if (n > 1) {
            Arrays.sort(sorted, (a, b) -> {
                for (int k = 0; k < orderKeyIndices.length; k++) {
                    int colIdx = orderKeyIndices[k];
                    boolean asc = orderAscending.get(k);
                    int cmp = ArrowComparators.compare(
                            root.getVector(colIdx), a, root.getVector(colIdx), b);
                    if (cmp != 0) return asc ? cmp : -cmp;
                }
                return 0;
            });
        }

        List<Integer> ordered = new ArrayList<>(Arrays.asList(sorted));

        // ★ 溢写归并路径同样要应用过滤下推：preFilter 只在内存路径（outputPagesFromAccumulator）
        //   调用，而过滤下推会「消除」外层 Filter 节点 —— 一旦分区触发溢写，被下推的过滤就无人执行
        //   ⇒ 结果多行（潜在错误）。此处每个 key 的分组已跨批次完整归并，过滤语义与内存路径一致。
        if (filterConfig != null && filterConfig.isActive()) {
            ordered = runGroupFilter(root, ordered);
        }
        return ordered;
    }

    /**
     * Phase 2 <b>输出累积器</b>（每个分区归并任务一个实例、线程本地）：把一个个 key 组的行拷进累积向量，
     * 攒够 {@link #FETCH_PER_KEYS} 个组或 {@link #OUTPUT_PAGE_ROWS} 行才成页 emit。
     *
     * <p>为什么必须<b>拷贝</b>而不能只记住行号：{@link ArrowFileReader#loadRecordBatch} 复用同一个
     * {@code VectorSchemaRoot}，下一批会覆盖其缓冲 ⇒ 要跨 record batch 累积就只能把行拷出来。</p>
     *
     * <p>累积向量按需 {@code reAlloc} 增长（上限 {@link #OUTPUT_PAGE_ROWS} 行），TransferPair 缓存复用，
     * 不在每个组上重造。</p>
     */
    private final class Phase2Out {
        private final int pid;
        private List<Field> fields;
        private List<FieldVector> vectors;   // 累积中的输出列（懒建）
        private List<TransferPair> transfers;
        private VectorSchemaRoot srcRoot;    // transfers 当前绑定的源 root
        private int rows;
        private int keys;

        Phase2Out(int pid) { this.pid = pid; }

        /** 追加一个 key 组（文件内 {@code [from,to)}）：组内排序 + 过滤后拷入累积页，够批即吐。 */
        void appendGroup(VectorSchemaRoot root, int from, int to) {
            List<Integer> ordered = orderAndFilterGroup(root, from, to);
            if (ordered.isEmpty()) return;
            ensureVectors(root);
            int need = rows + ordered.size();
            boolean grown = false;
            for (FieldVector v : vectors) {
                while (v.getValueCapacity() < need) { v.reAlloc(); grown = true; }
            }
            if (grown) transfers = null;   // reAlloc 换缓冲后重建映射（保险）
            if (transfers == null || srcRoot != root) {
                List<TransferPair> tp = new ArrayList<>(vectors.size());
                for (int col = 0; col < vectors.size(); col++) {
                    tp.add(root.getVector(col).makeTransferPair(vectors.get(col)));
                }
                transfers = tp;
                srcRoot = root;
            }
            for (int i = 0; i < ordered.size(); i++) {
                int src = ordered.get(i);
                for (TransferPair tp : transfers) tp.copyValueSafe(src, rows + i);
            }
            rows = need;
            keys++;
            if (keys >= FETCH_PER_KEYS || rows >= OUTPUT_PAGE_ROWS) flush();
        }

        /** 把累积的行成页 emit（不足一批也发）；空则 no-op。向量所有权随之移交，本对象可继续累积。 */
        void flush() {
            if (rows == 0) return;
            for (FieldVector v : vectors) v.setValueCount(rows);
            emitPage(Page.partitioned(new VectorSchemaRoot(fields, vectors, rows), pid));
            bumpMergeProgress();   // 溢写归并路径每吐一页 → 归并推进
            vectors = null;        // 所有权已移交页面，不得再 close
            transfers = null;
            srcRoot = null;
            rows = 0;
            keys = 0;
        }

        /** 异常路径释放尚未成页的累积向量（已移交的已置 null，不受影响）。 */
        void closeQuietly() {
            if (vectors == null) return;
            for (FieldVector v : vectors) {
                try { v.close(); } catch (Exception ignored) { }
            }
            vectors = null;
            transfers = null;
            srcRoot = null;
            rows = 0;
            keys = 0;
        }

        private void ensureVectors(VectorSchemaRoot root) {
            if (vectors != null) return;
            this.fields = root.getSchema().getFields();
            this.vectors = new ArrayList<>(fields.size());
            for (Field f : fields) {
                FieldVector v = f.createVector(allocator);
                v.setInitialCapacity(Math.min(OUTPUT_PAGE_ROWS, 1024));
                v.allocateNew();
                vectors.add(v);
            }
            this.transfers = null;
            this.srcRoot = null;
        }
    }

    /**
     * 对「单个已完整归并的分组」应用窗口过滤（与内存路径 {@code preFilter} 的按组过滤等价）。
     *
     * <p>调用前 {@code orderedIndices} 已按 ORDER BY 排好（分组内有序），与 preFilter 中
     * {@code indices.sort(compareOrderByOnly)} 后调用 {@code applyFilter} 的输入形态一致。</p>
     */
    private List<Integer> runGroupFilter(VectorSchemaRoot root, List<Integer> orderedIndices) {
        byte signMode = 0;
        if (filterConfig.isSumWithOrderBy()) {
            signMode = signOfGroup(root, filterConfig.aggregateColIndex(), orderedIndices);
        }
        return applyFilter(root.getFieldVectors(), orderedIndices,
                filterConfig.functionType(), filterConfig.isPositional(), filterConfig.isRank(),
                filterConfig.isAggregate(), filterConfig.isValueMonotonic(),
                filterConfig.isSumWithOrderBy(), signMode,
                filterConfig.aggregateColIndex(), filterConfig.targetValue(), filterConfig.operator());
    }

    /** SUM + ORDER BY 的分组符号：全非负 → 1，全非正 → -1，混合 → 0（不过滤）。只看该组的行。 */
    private static byte signOfGroup(VectorSchemaRoot root, int aggCol, List<Integer> groupRows) {
        if (aggCol < 0 || aggCol >= root.getFieldVectors().size()) return 0;
        FieldVector v = root.getVector(aggCol);
        boolean anyPos = false, anyNeg = false;
        for (int r : groupRows) {
            if (v.isNull(r)) continue;
            double d = toDouble(v, r);
            if (d >= 0) anyPos = true; else anyNeg = true;
            if (anyPos && anyNeg) return 0;
        }
        return anyNeg ? (byte) -1 : (byte) 1;
    }

    // ==================== 列批量拷贝 ====================

    /** 包级可见：供 FilterOperator / HashJoinOperator 复用固定宽列批量 memcpy 快路径。 */
    static void copyPagePermuted(List<FieldVector> srcVectors,
                                 List<FieldVector> dstVectors,
                                 int[] indices, int idxOff, int count,
                                 AtomicLong fixedTiming, AtomicLong varTiming) {
        for (int col = 0; col < srcVectors.size(); col++) {
            FieldVector src = srcVectors.get(col);
            FieldVector dst = dstVectors.get(col);
            // FixedSizeList（Vector 类型）：不能用 getDataBuffer()（抛 UnsupportedOperationException）。
            // 父级 validity 预置 0xFF + child 逐行 gather（child 定长元素一次 setBytes 拷整行）。
            if (src.getField().getType() instanceof ArrowType.FixedSizeList fl) {
                int listSize = fl.getListSize();
                FieldVector srcC = ((FixedSizeListVector) src).getDataVector();
                FieldVector dstC = ((FixedSizeListVector) dst).getDataVector();
                int elementTw = RowAccumulator.typeWidthOf(srcC.getField().getType());
                long t0 = (fixedTiming != null) ? System.nanoTime() : 0L;
                if (elementTw > 0) {
                    // 常见情况：child 为定长（Float32 等）
                    long childBytes = (long) listSize * elementTw;
                    org.apache.arrow.memory.ArrowBuf srcBuf = srcC.getDataBuffer();
                    org.apache.arrow.memory.ArrowBuf dstBuf = dstC.getDataBuffer();
                    while (dstBuf.capacity() < (long) count * childBytes) {
                        dst.reAlloc(); // 同时增长 parent validity 与 child
                        dstBuf = dstC.getDataBuffer();
                    }
                    org.apache.arrow.memory.ArrowBuf dstVal = dst.getValidityBuffer();
                    int valBytes = (count + 7) / 8;
                    for (int b = 0; b < valBytes; b++) dstVal.setByte(b, (byte) 0xFF);
                    // child validity：有效行的所有元素全 valid（getObject 逐元素读 child）
                    org.apache.arrow.memory.ArrowBuf dstCVal = dstC.getValidityBuffer();
                    if (dstCVal != null) {
                        int cValBytes = (int) (((long) count * listSize + 7) / 8);
                        for (int b = 0; b < cValBytes; b++) dstCVal.setByte(b, (byte) 0xFF);
                    }
                    for (int i = 0; i < count; i++) {
                        int si = indices[idxOff + i];
                        dstBuf.setBytes((long) i * childBytes, srcBuf, (long) si * childBytes, childBytes);
                        if (src.isNull(si)) dst.setNull(i);
                    }
                } else {
                    // child 为变长元素（罕见）：列表级 copyValueSafe（递归处理 child）
                    TransferPair tp = src.makeTransferPair(dst);
                    for (int i = 0; i < count; i++) tp.copyValueSafe(indices[idxOff + i], i);
                }
                if (fixedTiming != null) fixedTiming.addAndGet(System.nanoTime() - t0);
                continue;
            }
            int tw = RowAccumulator.typeWidthOf(src.getField().getType());
            boolean fixed = tw > 0;
            long t0 = (fixedTiming != null) ? System.nanoTime() : 0L;
            if (fixed) {
                org.apache.arrow.memory.ArrowBuf srcBuf = src.getDataBuffer();
                org.apache.arrow.memory.ArrowBuf dstBuf = dst.getDataBuffer();
                org.apache.arrow.memory.ArrowBuf dstVal = dst.getValidityBuffer();
                if (dstVal != null) {
                    int valBytes = (count + 7) / 8;
                    for (int b = 0; b < valBytes; b++) dstVal.setByte(b, (byte) 0xFF);
                }
                // ★ 不能按 src.getNullCount() 走「全 valid」快路径：本方法的源恒为
                //   {@link RowAccumulator#vectors}，其数据是 copyColumnBatch 直写缓冲、从不
                //   setValueCount ⇒ Arrow 的 nullCount 缓存恒为 0（实测），而 validity 位里可能
                //   确实有 NULL。若信 nullCount 就会把 NULL 行原样拷数据 + 置 valid ⇒ NULL 静默
                //   变 0 / 0.00 / 1970-01-01（LAG/LEAD 越界行正是这种行）。逐行读 validity 位，
                //   代价每行一次位测试，可忽略。
                for (int i = 0; i < count; i++) {
                    int si = indices[idxOff + i];
                    dstBuf.setBytes((long) i * tw, srcBuf, (long) si * tw, tw);
                    if (src.isNull(si)) dst.setNull(i);
                }
            } else {
                // 变长列（VarChar/VarBinary 等）：逐行 copyValueSafe。
                // copyValueSafe 内部推进 lastSet，下游再 setValueCount 时 fillHoles 变 no-op，var 数据安全。
                TransferPair tp = src.makeTransferPair(dst);
                for (int i = 0; i < count; i++) tp.copyValueSafe(indices[idxOff + i], i);
            }
            if (fixedTiming != null) {
                (fixed ? fixedTiming : varTiming).addAndGet(System.nanoTime() - t0);
            }
        }
    }

    private static int compareCompositeKeys(CompositeKey a, CompositeKey b) {
        int len = Math.min(a.values.length, b.values.length);
        for (int i = 0; i < len; i++) {
            Object va = a.values[i], vb = b.values[i];
            if (va == RowAccumulator.NULL_KEY && vb == RowAccumulator.NULL_KEY) continue;
            if (va == RowAccumulator.NULL_KEY) return 1;
            if (vb == RowAccumulator.NULL_KEY) return -1;
            if (va instanceof Comparable ca && vb instanceof Comparable) {
                int cmp = ca.compareTo(vb);
                if (cmp != 0) return cmp;
            } else if (va instanceof List<?> la && vb instanceof List<?> lb) {
                int cmp = ArrowComparators.compareLists(la, lb);
                if (cmp != 0) return cmp;
            }
        }
        return a.values.length - b.values.length;
    }

    // ==================== OperatorMemoryHandle ====================

    @Override
    public SpillResult spill(long targetBytes) {
        long totalFreed = 0;
        for (int pid = 0; pid < partitions.length; pid++) {
            PartitionState<RowAccumulator> pa = partitions[pid];
            if (pa == null) continue;
            RowAccumulator active = pa.activeSlot();
            if (active != null && !active.isEmpty()) {
                long before = active.estimateMemoryBytes();
                pa.submitSpill(spillExecutor, this, pid);
                pa.awaitIdleSlot();
                totalFreed += before;
            }
        }
        return totalFreed > 0 ? SpillResult.completed(totalFreed, 0) : SpillResult.noOp();
    }

    @Override
    public boolean isSpilling() {
        for (PartitionState<RowAccumulator> p : partitions) {
            if (p != null && p.pendingSpills.get() > 0) return true;
        }
        return false;
    }
}
