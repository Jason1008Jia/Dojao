package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.core.config.ResourceConfig;
import com.dojao.spi.function.AggregateFunction;
import com.dojao.spi.function.CollectAggregateFunction;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Hash 聚合算子 —— 继承 {@link AbstractSpillableOperator}，聚合型键控（accumulate 模式）。
 *
 * <h3>两种槽位形态</h3>
 * <ul>
 *   <li><b>增量型</b>：槽位 {@link AggregationHashTable}（聚合函数状态哈希表，四阶段累加）</li>
 *   <li><b>collect-evaluate 型</b>：槽位 {@link CollectHashTable}（按 key 攒原始值，Java 侧 Arrow 列），
 *       Phase 2 拼接成 N-key batch 调 {@link CollectAggregateFunction#evaluateBatch} 交付外部语言</li>
 * </ul>
 * 本轮单 GROUP BY 只支持其一（构造时校验），collect 型每查询一个。
 *
 * @author jiaxin
 * @version 4.0
 * @date 2026/7/23
 */
public class HashAggregationOperator extends AbstractSpillableOperator<Slot> implements Explainable {

    /** EXPLAIN 明细：分组键 + 聚合函数（{@code sum(x) AS total}）。 */
    @Override
    public List<String> explainLines() {
        return List.of("algorithm: HASH_AGGREGATION (spillable)"
                + " groupBy=" + (keyColumns.isEmpty() ? "<global>" : keyColumns)
                + " aggs=" + aggregations.stream().map(HashAggregationOperator::describeAgg).toList());
    }

    private static String describeAgg(AggregationSpec spec) {
        if (spec.isCollect()) {
            return "collect(" + spec.collectInputColumns() + ") AS " + spec.outputColumn();
        }
        String fn = spec.aggregate() == null ? "aggregate" : spec.aggregate().functionName();
        return fn + "(" + (spec.inputColumn() == null ? "*" : spec.inputColumn()) + ") AS "
                + spec.outputColumn();
    }

    private final List<String> keyColumns;
    private final List<AggregationSpec> aggregations;
    private AggregationHavingConfig havingConfig;

    /** collect-evaluate 型（单 collect 聚合）：Phase 1 用 CollectHashTable，Phase 2 拼接交付。 */
    private final boolean collectMode;

    /**
     * 增量型 + {@code count_distinct}：溢写拆成「标量流（每组一行）+ 逐值去重流（每值一行，
     * 按 {@code (key, value)} 有序）」两条，Phase 2 统一 K 路归并 + 流式去重计数。
     * 纯标量聚合（无 count_distinct）与 collect 型沿用原路径，逐字节不变。
     */
    private final boolean distinctMode;
    /** {@code count_distinct} 规格下标（distinct ordinal → specs 下标）。 */
    private final int[] distinctSpecs;
    /** {@code specs} 下标是否 count_distinct（避免每条 key 线性查找）。 */
    private final boolean[] distinctSpecFlag;

    /** 去重流归并的最大并行路数（与 Window 的 MAX_MERGE_K 同口径）。 */
    private static final int MAX_DISTINCT_MERGE_K = 16;

    /** 标量溢写流（键 + 标量状态）归并的最大并行路数：超过即先多轮归并压缩文件数。 */
    private static final int MAX_SCALAR_MERGE_K = 16;

    /** 溢写归并每批 key 数（query.agg.spill.perBatchKeys 可配）：归并读取 + 状态 merge 每批多少个 key。 */
    private static final int AGG_SPILL_PER_BATCH_KEYS = ResourceConfig.aggSpillPerBatchKeys();

    /**
     * collect 溢写交付每批的<b>值总数</b>预算（query.agg.collectDeliverValues 可配）。
     * collect 契约要一次性交付每个 key 的全部值 ⇒ 只按 key 数切批无法约束内存，必须按值总数切。
     */
    private static final int AGG_COLLECT_DELIVER_VALUES = ResourceConfig.aggCollectDeliverValues();
    private List<String> collectValueColumns;
    private List<Field> collectValueFields;

    public HashAggregationOperator(Operator upstream,
                                   List<String> keyColumns,
                                   List<AggregationSpec> aggregations,
                                   long memoryLimit) {
        this(upstream, keyColumns, aggregations, memoryLimit, DEFAULT_PARTITIONS);
    }

    /**
     * 指定分区数的构造（分布式 Worker 侧使用）：分区槽数组按查询布局的 numPartitions 分配，
     * 只接收 own 桶（pid &lt; numPartitions），否则越界。
     */
    public HashAggregationOperator(Operator upstream,
                                   List<String> keyColumns,
                                   List<AggregationSpec> aggregations,
                                   long memoryLimit,
                                   int numPartitions) {
        super("HashAggregation", "HashAggregation", 1,
                upstream, memoryLimit, Math.max(1, numPartitions));
        this.keyColumns = keyColumns;
        this.aggregations = aggregations;
        boolean anyCollect = false;
        for (AggregationSpec spec : aggregations) anyCollect |= spec.isCollect();
        if (anyCollect) {
            // collect-evaluate 型：本轮每 GROUP BY 只支持一个 collect 聚合，且不与增量型混用
            if (aggregations.size() != 1 || !aggregations.get(0).isCollect()) {
                throw new IllegalArgumentException(
                        "collect 型聚合本轮每个 GROUP BY 只支持一个（不混用增量/多 collect）: "
                                + aggregations);
            }
            collectMode = true;
        } else {
            collectMode = false;
        }

        this.distinctSpecFlag = new boolean[aggregations.size()];
        if (!collectMode) {
            int[] tmp = new int[aggregations.size()];
            int dc = 0;
            for (int i = 0; i < aggregations.size(); i++) {
                AggregationSpec spec = aggregations.get(i);
                if (spec.aggregate() != null
                        && "count_distinct".equals(spec.aggregate().functionName())) {
                    tmp[dc++] = i;
                    distinctSpecFlag[i] = true;
                }
            }
            this.distinctSpecs = Arrays.copyOf(tmp, dc);
        } else {
            this.distinctSpecs = new int[0];
        }
        this.distinctMode = distinctSpecs.length > 0;
    }

    public void setHavingConfig(AggregationHavingConfig havingConfig) {
        this.havingConfig = havingConfig;
    }

    /** GROUP BY 键列（供 Planner 列裁剪推算子节点需求）。 */
    public List<String> getKeyColumns() { return keyColumns; }

    /** 聚合定义（供 Planner 列裁剪推算子节点需求：输入列必须保留）。 */
    public List<AggregationSpec> getAggregations() { return aggregations; }

    // ==================== 抽象方法实现 ====================

    @Override
    protected Slot createSlot(BufferAllocator allocator) {
        if (collectMode) {
            // 实际收集列 = SQL 调用参数（按位），而非 CollectAggregateFunction 声明的参数位标签
            // （声明名可任意，如 stddev(discount_amount) 作用于声明 ("num",) 的函数）。
            collectValueColumns = aggregations.get(0).collectInputColumns();
            collectValueFields = new ArrayList<>(collectValueColumns.size());
            for (String col : collectValueColumns) {
                Field f = inputSchema.findField(col);
                if (f == null) {
                    throw new IllegalStateException(
                            "collect 聚合输入列 [" + col + "] 不存在于输入 schema");
                }
                collectValueFields.add(f);
            }
            return new CollectHashTable(allocator, keyColumns, groupKeyFields,
                    collectValueColumns, collectValueFields);
        }
        return new AggregationHashTable(allocator, keyColumns, groupKeyFields, aggregations);
    }

    @Override
    protected void addRow(Slot slot, Page input, int row) {
        slot.addRow(input, row);
    }

    @Override
    protected List<String> resolveGroupKeyColumns() {
        return keyColumns;
    }

    @Override
    protected List<Field> resolveGroupKeyFields(Schema schema) {
        List<Field> fields = new ArrayList<>();
        for (String col : keyColumns) {
            fields.add(schema.findField(col));
        }
        return fields;
    }

    /**
     * 溢写一个槽位。{@code distinctMode} 下拆成两条流写：
     * <ul>
     *   <li><b>标量流</b>（{@code p<pid>_b<idx>.arrow}）：每组一行，含 count/sum/min/max/avg 状态 ——
     *       与旧布局相同，只是不含 count_distinct 列；</li>
     *   <li><b>去重流</b>（{@code p<pid>_b<idx>_d<ord>.arrow}）：每个去重值一行 {@code [key..., value]}，
     *       按 {@code (key, value)} 有序，登记到 {@link PartitionState#distinctFiles}。</li>
     * </ul>
     * 其余算子（collect 型 / 纯标量聚合）走基类原实现。
     */
    @Override
    protected void spillSlotSync(PartitionState<Slot> pa, int slotIdx, Slot slot, int pid) {
        if (!distinctMode || !(slot instanceof AggregationHashTable ht) || !ht.hasDistinct()) {
            super.spillSlotSync(pa, slotIdx, slot, pid);
            return;
        }
        if (slot.isEmpty()) return;
        VectorSchemaRoot scalar = null;
        try {
            scalar = ht.buildSortedScalarSpillRoot();
            BatchWrite written = writeBatchToArrow(pa, scalar, pid);
            accountSpilledBytes(written.bytes());
            for (int ord = 0; ord < ht.distinctSpecCount(); ord++) {
                VectorSchemaRoot droot = null;
                try {
                    droot = ht.buildDistinctSpillRoot(ord);
                    if (droot == null || droot.getRowCount() == 0) continue;
                    // 与标量流同一批次号、加 _d<ordinal> 后缀：并发溢写线程的批次号由原子计数保证唯一。
                    Path file = spillDirForWrite().resolve(
                            "p" + pid + "_b" + written.batchIndex() + "_d" + ord + ".arrow");
                    long bytes = writeArrowFile(file, droot);
                    pa.distinctFilesFor(ord).add(file);
                    synchronized (pa) { pa.dataFileSize += bytes; }
                    accountSpilledBytes(bytes);
                } finally {
                    if (droot != null) droot.close();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write distinct spill for partition " + pid, e);
        } finally {
            if (scalar != null) scalar.close();
            try { slot.close(); } catch (Exception ignored) {}
            pa.slots.set(slotIdx, null);
        }
    }

    /**
     * 只有 collect 型 Phase 2 还按 key 随机回读（需要 {@link PartitionState#index}）；
     * 增量型（本轮 OOM 路径）改 K 路归并后不再建索引 —— 索引随去重键数增长，高基数分组下撑爆堆。
     */
    @Override
    protected boolean maintainSpillIndex() { return collectMode; }

    @Override
    protected void outputMemory(PartitionState<Slot> pa, int pid) throws Exception {
        for (int s = 0; s < SLOT_COUNT; s++) {
            checkCancelled();
            Slot slot = pa.slots.get(s);
            if (slot == null || slot.isEmpty()) continue;
            if (collectMode) {
                outputCollectMemory((CollectHashTable) slot, pid);
            } else {
                AggregationHashTable ht = (AggregationHashTable) slot;
                // 已构造但尚未 emit 的输出批次不登记在 PartitionState 里：取消检查点若在此处抛异常，
                // 必须由本方法自己收尾，否则这些 Arrow 批次会泄漏（实测取消后 2.3MB 不回落）。
                List<VectorSchemaRoot> pending = new ArrayList<>(ht.buildOutputBatches(AGG_PER_BATCH_KEYS));
                try {
                    for (int bi = 0; bi < pending.size(); bi++) {
                        checkCancelled();
                        VectorSchemaRoot batch = pending.get(bi);
                        pending.set(bi, null);      // 所有权已交出（emit 或 filterHaving 内部接管）
                        if (havingConfig != null) {
                            batch = filterHaving(batch);
                            if (batch == null) continue;
                        }
                        emitPage(Page.partitioned(batch, pid));
                    }
                } finally {
                    for (VectorSchemaRoot b : pending) {
                        if (b != null) { try { b.close(); } catch (Exception ignored) {} }
                    }
                }
            }
            slot.close();
            pa.slots.set(s, null);
        }
    }

    /**
     * 全局聚合（无 GROUP BY）空输入兜底：Phase 1 一页数据都没收到时补一行聚合初值
     * （{@code count(*)=0}、{@code avg/min/max=NULL}）—— SQL 语义要求无分组键的聚合
     * <b>恒返回 1 行</b>。分组聚合 / 去重（键列非空）空输入仍应 0 行，不补。
     *
     * <p>典型触发：{@code UNION ALL} 的某个分支过滤后为空，该分支的 {@code count(*)=0}
     * 行此前会凭空消失（用户观察到的「4 分支只回 3 行」）。HAVING 照常作用在这一行上
     * （如 {@code HAVING count(*) > 0} 会把它滤掉）。</p>
     *
     * <p>collect 型聚合（外部语言 evaluate）不走这里 —— 空批次交给后端求值的语义由
     * {@link CollectAggregateFunction} 实现自行定义，保持原有 0 行行为不动。</p>
     */
    @Override
    protected void emitEmptyInput(int pid) throws Exception {
        if (collectMode || !keyColumns.isEmpty() || aggregations.isEmpty()) {
            return;
        }
        AggregationHashTable ht = new AggregationHashTable(allocator, List.of(), List.of(), aggregations);
        try {
            // 没有输入页 ⇒ 输出列类型不能靠页 schema 解析（collect 的 List 子字段会退化成 Utf8）；
            // 用算子已知的上游 schema 对齐一次，保证空输入行的 schema 与规划期声明一致。
            ht.primeEmptyInputFields(inputSchema == null ? null : inputSchema.getFields());
            VectorSchemaRoot batch = ht.buildGlobalEmptyRow();
            if (havingConfig != null) {
                batch = filterHaving(batch);
                if (batch == null) return;
            }
            emitPage(Page.partitioned(batch, pid));
        } finally {
            ht.close();
        }
    }

    // ==================== Phase 2: 按 key 有序的 K 路归并 ====================

    /**
     * 增量型 Phase 2：对全部溢写文件做<b>按 key 有序的 K 路归并</b>。
     *
     * <p><b>为什么不建索引</b>：旧实现在 Phase 1 用 {@code PartitionState.index}（键 → 段列表）把同一键
     * 跨文件的行串起来，Phase 2 按键随机回读。该索引是 {@code O(去重键数)} 的<b>堆内</b>结构，且
     * {@code AggregationHashTable.estimateMemoryBytes()} 并不计它 ⇒ 高基数 GROUP BY（如
     * {@code group by session_id}）下索引先于槽位状态撑爆 JVM 堆（{@code OutOfMemoryError: Java heap space}）。</p>
     *
     * <p><b>本次改法</b>：每个溢写文件本就按 key 有序（{@code buildSortedSpillRoot}），且同一文件内键唯一
     * （一次溢写 = 一个槽位的哈希表，组键天然去重）。故按 key 顺序对 K 个文件做流式归并：优先队列每次取
     * 最小键，聚合所有「当前键 = 最小键」的文件游标后吐一行。峰值内存 = 文件数 × 单键状态 + 一个输出批，
     * <b>与去重键总数无关</b>。</p>
     */
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void mergeAndOutput(PartitionState<Slot> pa, int pid) throws Exception {
        if (collectMode) {
            mergeAndOutputCollect(pa, pid);
            return;
        }
        if (distinctMode) {
            mergeAndOutputDistinct(pa, pid);
            return;
        }
        int fileCount = pa.batchFiles.size();
        if (fileCount == 0) return;

        int[] stateColCount = new int[aggregations.size()];
        for (int si = 0; si < aggregations.size(); si++) {
            stateColCount[si] = aggregations.get(si).aggregate().functionName().equals("avg") ? 2 : 1;
        }

        // 高基数 GROUP BY 会溢写出成百上千个<b>单批</b>文件：旧实现 loadBatchFromArrow 把每个文件整批
        // 拷进查询分配器并常驻（files 列表）⇒ 峰值 ≈ 整份溢写状态，与溢写闸门同阶，直接 OOM
        // （线上 366MB 溢写、442MB 预算 → loadBatchFromArrow.allocateNew 抛 OutOfMemory）。
        // 先把文件数压到 MAX_SCALAR_MERGE_K 以内（多轮归并、每轮只同时打开 laneCount 路、每路只驻留一个批），
        // 再对 ≤ K 个顺串做流式 K 路归并输出，峰值 = 路数 × 单批 + 一个输出批，与总溢写量无关。
        List<Path> runPaths;
        boolean compacted;
        if (fileCount > MAX_SCALAR_MERGE_K) {
            runPaths = compactScalarRuns(pa.batchFiles, pa, pid, stateColCount);
            compacted = true;
        } else {
            runPaths = new ArrayList<>(pa.batchFiles);
            compacted = false;
        }

        List<ScalarRun> runs = new ArrayList<>(runPaths.size());
        List<Field> firstFields = null;
        // 在途输出批：指向它的是本方法（不是 PartitionState），取消检查点抛异常时必须由 finally 收尾，
        // 否则「停止任务」后这一批 Arrow 内存永不释放（实测 2.3MB 不回落）。
        List<FieldVector> outVectors = null;
        try {
            for (Path p : runPaths) {
                ScalarRun r = new ScalarRun(p);
                runs.add(r);
                if (firstFields == null && r.valid()) firstFields = r.fields();
            }

            List<Field> outFields = buildOutputFields(firstFields, stateColCount);

            PriorityQueue<Integer> pq = new PriorityQueue<>(Math.max(1, runs.size()),
                    (a, b) -> compareSpillKeys(runs.get(a).key(), runs.get(b).key()));
            for (int i = 0; i < runs.size(); i++) {
                if (runs.get(i).valid()) pq.add(i);
            }

            outVectors = allocOutputVectors(outFields);
            int outRow = 0;
            List<Integer> same = new ArrayList<>();

            while (!pq.isEmpty()) {
                checkCancelled();
                Object minKey = runs.get(pq.peek()).key();

                Object[] mergedStates = new Object[aggregations.size()];
                for (int si = 0; si < aggregations.size(); si++) {
                    mergedStates[si] = aggregations.get(si).aggregate().createState();
                }

                // 弹出所有「当前键 = 最小键」的顺串游标（同键可能分布在多个溢写文件里）
                same.clear();
                while (!pq.isEmpty() && keysEqual(runs.get(pq.peek()).key(), minKey)) {
                    same.add(pq.poll());
                }
                for (int fi : same) {
                    ScalarRun run = runs.get(fi);
                    mergeSpillRow(run.root(), run.row(), mergedStates, stateColCount);
                    run.advance();
                    if (run.valid()) pq.add(fi);
                }

                // HAVING 下推：不满足条件的组直接跳过
                if (havingConfig != null) {
                    int aggIdx = havingConfig.columnIndex() - keyColumns.size();
                    if (aggIdx >= 0 && aggIdx < aggregations.size()) {
                        AggregateFunction<Object> fn = (AggregateFunction<Object>) aggregations.get(aggIdx).aggregate();
                        Object result = fn.evaluate(mergedStates[aggIdx]);
                        long value = result instanceof Number n ? n.longValue() : 0;
                        if (!havingConfig.accept(result, value)) {
                            bumpMergeProgress();
                            continue;
                        }
                    }
                }

                writeKeyToOutput(outVectors, minKey, outRow);
                int outCol = keyColumns.size();
                for (int si = 0; si < aggregations.size(); si++) {
                    AggregateFunction<Object> fn = (AggregateFunction<Object>) aggregations.get(si).aggregate();
                    setVectorValue(outVectors.get(outCol), outRow, fn.evaluate(mergedStates[si]));
                    outCol++;
                }
                outRow++;
                bumpMergeProgress();

                if (outRow >= AGG_PER_BATCH_KEYS) {
                    List<FieldVector> batch = outVectors;
                    outVectors = null;                  // 所有权移交给输出页
                    for (FieldVector v : batch) v.setValueCount(outRow);
                    emitPage(Page.partitioned(new VectorSchemaRoot(outFields, batch, outRow), pid));
                    outVectors = allocOutputVectors(outFields);
                    outRow = 0;
                }
            }

            if (outRow > 0) {
                List<FieldVector> batch = outVectors;
                outVectors = null;                      // 所有权移交给输出页
                for (FieldVector v : batch) v.setValueCount(outRow);
                emitPage(Page.partitioned(new VectorSchemaRoot(outFields, batch, outRow), pid));
            }
        } finally {
            if (outVectors != null) {
                for (FieldVector v : outVectors) {
                    try { v.close(); } catch (Exception ignored) {}
                }
                outVectors = null;
            }
            for (ScalarRun r : runs) r.closeQuietly();
            // 归并产物不登记在 PartitionState（原始文件已在归并时删除），此处自行清理
            if (compacted) {
                for (Path p : runPaths) {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                }
            }
        }
    }

    /**
     * 把标量溢写流的多个顺串归并成更少的顺串（可含多个 record batch），直至文件数 ≤
     * {@link #MAX_SCALAR_MERGE_K}。每轮把 {@code laneCount} 个源并成一个文件，文件数至少按路数收敛。
     */
    private List<Path> compactScalarRuns(List<Path> input, PartitionState<Slot> pa, int pid,
                                         int[] stateColCount) throws IOException {
        List<Path> runs = new ArrayList<>(input);
        int round = 0;
        while (runs.size() > MAX_SCALAR_MERGE_K) {
            checkCancelled();
            List<Path> next = new ArrayList<>();
            int i = 0;
            while (i < runs.size()) {
                checkCancelled();
                int take = Math.min(runs.size() - i, laneCount(runs.get(i)));
                if (take <= 1) {
                    next.add(runs.get(i));
                    i++;
                    continue;
                }
                List<Path> chunk = new ArrayList<>(runs.subList(i, i + take));
                Path out = spillDirForWrite().resolve("p" + pid + "_m" + round + "_s" + i + ".arrow");
                Path produced = mergeScalarRuns(chunk, out, pa, pid, stateColCount);
                if (produced != null) next.add(produced);
                for (Path p : chunk) {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                }
                i += take;
            }
            runs = next;
            round++;
            bumpMergeProgress();
        }
        return runs;
    }

    /**
     * 单轮归并（同键合并状态），写出一个（多个 record batch 的）有序文件。
     *
     * @return 输出文件；所有源都为空时返回 {@code null}（不产生文件）
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private Path mergeScalarRuns(List<Path> sources, Path outFile, PartitionState<Slot> pa,
                                 int pid, int[] stateColCount) throws IOException {
        List<ScalarRun> runs = new ArrayList<>(sources.size());
        VectorSchemaRoot outRoot = null;
        try {
            for (Path p : sources) runs.add(new ScalarRun(p));
            PriorityQueue<Integer> pq = new PriorityQueue<>(Math.max(1, runs.size()),
                    (a, b) -> compareSpillKeys(runs.get(a).key(), runs.get(b).key()));
            for (int i = 0; i < runs.size(); i++) if (runs.get(i).valid()) pq.add(i);
            if (pq.isEmpty()) return null;

            List<Field> outFields = runs.get(pq.peek()).fields();
            if (outFields == null) return null;

            List<FieldVector> outVectors = new ArrayList<>(outFields.size());
            for (Field f : outFields) {
                FieldVector v = f.createVector(allocator);
                v.setInitialCapacity(AGG_SPILL_PER_BATCH_KEYS);
                v.allocateNew();
                outVectors.add(v);
            }
            final VectorSchemaRoot out = new VectorSchemaRoot(new ArrayList<>(outFields), outVectors, 0);
            outRoot = out;
            int outRows = 0;
            int outCapacity = minValueCapacity(outVectors);
            boolean wrote = false;
            List<Integer> same = new ArrayList<>();

            try (FileChannel outCh = FileChannel.open(outFile, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                 ArrowFileWriter writer = new ArrowFileWriter(out, null, outCh)) {
                while (!pq.isEmpty()) {
                    checkCancelled();
                    Object minKey = runs.get(pq.peek()).key();

                    Object[] mergedStates = new Object[aggregations.size()];
                    for (int si = 0; si < aggregations.size(); si++) {
                        mergedStates[si] = aggregations.get(si).aggregate().createState();
                    }

                    same.clear();
                    while (!pq.isEmpty() && keysEqual(runs.get(pq.peek()).key(), minKey)) {
                        same.add(pq.poll());
                    }
                    for (int fi : same) {
                        ScalarRun run = runs.get(fi);
                        mergeSpillRow(run.root(), run.row(), mergedStates, stateColCount);
                        run.advance();
                        if (run.valid()) pq.add(fi);
                    }

                    if (outRows >= outCapacity) {
                        for (FieldVector fv : outVectors) fv.reAlloc();
                        outCapacity = minValueCapacity(outVectors);
                    }
                    writeKeyToOutput(outVectors, minKey, outRows);
                    // ★归并压缩的产物必须保持「溢写状态列」布局（下游还要按状态读回）：
                    //   - count_distinct 在标量流里不占列 → 跳过；
                    //   - avg 占 2 列（sum, cnt）→ 必须写状态而不是 evaluate 后的单值，
                    //     否则后续聚合列整体错位（旧实现还会写出 outVectors 越界）。
                    int outCol = keyColumns.size();
                    for (int si = 0; si < aggregations.size(); si++) {
                        if (distinctSpecFlag[si]) continue;
                        Object state = mergedStates[si];
                        switch (aggregations.get(si).aggregate().functionName()) {
                            case "avg" -> {
                                Object[] avgState = (Object[]) state;
                                setVectorValue(outVectors.get(outCol), outRows, avgState[0]);
                                setVectorValue(outVectors.get(outCol + 1), outRows, avgState[1]);
                            }
                            case "count", "sum_int64" -> {
                                long[] longs = (long[]) state;
                                setVectorValue(outVectors.get(outCol), outRows, longs[0]);
                            }
                            case "collect_list", "collect_set" -> {
                                // 状态 = Collection：整组写成一格 List（空组 → NULL，与唯一输出语义一致）
                                if (state instanceof Collection<?> c && !c.isEmpty()) {
                                    setVectorValue(outVectors.get(outCol), outRows, c);
                                } else {
                                    setVectorValue(outVectors.get(outCol), outRows, null);
                                }
                            }
                            default -> {
                                // sum / min / max：Object[]{原始状态值}
                                Object[] objState = (Object[]) state;
                                setVectorValue(outVectors.get(outCol), outRows, objState[0]);
                            }
                        }
                        outCol += stateColCount[si];
                    }
                    outRows++;

                    if (outRows >= AGG_SPILL_PER_BATCH_KEYS) {
                        out.setRowCount(outRows);
                        writer.writeBatch();
                        wrote = true;
                        bumpMergeProgress();
                        for (FieldVector fv : outVectors) fv.setValueCount(0);
                        outRows = 0;
                    }
                }
                if (outRows > 0) {
                    out.setRowCount(outRows);
                    writer.writeBatch();
                    wrote = true;
                }
            }
            if (!wrote) {
                try { Files.deleteIfExists(outFile); } catch (IOException ignored) {}
                return null;
            }
            accountCompactionFile(pa, outFile);
            return outFile;
        } finally {
            if (outRoot != null) { try { outRoot.close(); } catch (Exception ignored) {} }
            for (ScalarRun run : runs) run.closeQuietly();
        }
    }

    /**
     * Phase 2 输出列：分组键 + 各聚合结果列。
     *
     * <p>MIN/MAX 与输入严格同型（数值保持 INT/FLOAT 原宽度；时间戳 / 日期 / 字符串 / 布尔 / Decimal）；
     * SUM/AVG 的 DECIMAL 状态列是 Decimal —— 都以溢写文件对应状态列的真实类型为准，
     * 否则输出会退化成 DOUBLE 丢精度 / 给整数带上小数点。</p>
     *
     * @param spillFields   溢写状态行的字段布局（键列 + 状态列），可为 {@code null}（无溢写文件）
     * @param stateColCount 每个聚合在标量溢写流里占的列数（count_distinct 为 0）
     */
    private List<Field> buildOutputFields(List<Field> spillFields, int[] stateColCount) {
        List<Field> outFields = new ArrayList<>(groupKeyFields);
        int stateCol = keyColumns.size();
        for (int si = 0; si < aggregations.size(); si++) {
            AggregationSpec spec = aggregations.get(si);
            if (spec.isCollectArray()) {
                // COLLECT_LIST / COLLECT_SET：结果列是 list<元素类型>。溢写路径取状态列的子字段，
                // 未溢写（纯内存）取输入列字段 —— 都必须在 Field 层级带上子字段，ArrowType.List 不够。
                Field child = null;
                Field spilled = (!distinctSpecFlag[si] && spillFields != null
                        && stateCol < spillFields.size()) ? spillFields.get(stateCol) : null;
                if (spilled != null && !spilled.getChildren().isEmpty()) {
                    child = spilled.getChildren().get(0);
                } else {
                    Field in = spec.inputColumn() == null
                            ? null : inputSchema.findField(spec.inputColumn());
                    if (in != null) child = new Field("item", in.getFieldType(), in.getChildren());
                }
                FieldType ft = spilled != null ? spilled.getFieldType()
                        : FieldType.nullable(new ArrowType.List());
                outFields.add(new Field(spec.outputColumn(), ft,
                        child != null ? List.of(child) : null));
                stateCol += stateColCount[si];
                continue;
            }
            ArrowType type = spec.outputArrowType();
            if (!distinctSpecFlag[si] && (isMinMax(spec) || isSumAvg(spec))) {
                if (spillFields != null && stateCol < spillFields.size()) {
                    type = spillFields.get(stateCol).getType();
                }
            }
            outFields.add(new Field(spec.outputColumn(), FieldType.nullable(type), null));
            stateCol += stateColCount[si];
        }
        return outFields;
    }

    private static boolean isMinMax(AggregationSpec spec) {
        if (spec.aggregate() == null) return false;
        String fn = spec.aggregate().functionName();
        return "min".equals(fn) || "max".equals(fn);
    }

    /** SUM/AVG 的输出类型可能跟随输入（DECIMAL 输入 → Decimal 列），需以溢写状态列类型为准。 */
    private static boolean isSumAvg(AggregationSpec spec) {
        if (spec.aggregate() == null) return false;
        String fn = spec.aggregate().functionName();
        return "sum".equals(fn) || "avg".equals(fn);
    }

    /** 分配一个输出批的向量组（初始容量 = 单批 key 数）。 */
    private List<FieldVector> allocOutputVectors(List<Field> outFields) {
        List<FieldVector> outVectors = new ArrayList<>(outFields.size());
        for (Field f : outFields) {
            FieldVector vec = f.createVector(allocator);
            vec.setInitialCapacity(AGG_PER_BATCH_KEYS);
            vec.allocateNew();
            outVectors.add(vec);
        }
        return outVectors;
    }

    /**
     * 把一个溢写行（key 归并后的一个来源）的各聚合状态并入 {@code mergedStates}
     * （列布局同 {@link AggregationHashTable#buildScalarSpillRoot()}）。
     *
     * <p>★{@code count_distinct} 在标量流里<b>不占列</b>（它走独立的逐值流），必须跳过；
     * 否则会把后面某个标量状态列（如 sum 的 Float8）当成去重集合的 VARCHAR 去解码 ——
     * 线上 {@code ClassCastException: Float8Vector cannot be cast to VarCharVector}
     * （{@code count_distinct} 不是最后一个聚合时必现）。</p>
     */
    @SuppressWarnings("unchecked")
    private void mergeSpillRow(VectorSchemaRoot root, int row, Object[] mergedStates, int[] stateColCount) {
        int col = keyColumns.size();
        for (int si = 0; si < aggregations.size(); si++) {
            if (distinctSpecFlag[si]) continue;   // 标量流不含 count_distinct 列
            Object otherState = readStateFromSpillCols(root, row, aggregations.get(si), col);
            AggregateFunction<Object> fn = (AggregateFunction<Object>) aggregations.get(si).aggregate();
            fn.merge(mergedStates[si], otherState);
            col += stateColCount[si];
        }
    }

    /**
     * 跨行键比较，与 {@link AggregationHashTable#compareValues} 同序 ⇒ K 路归并输出仍按键升序。
     *
     * <p>★变长键是 Arrow 的 {@link Text}（不实现 {@code Comparable}）：若落到
     * {@code toString().compareTo(...)}，<b>每次比较都要 UTF-8 解码出两个 String</b> ——
     * 统一 K 路归并的优先队列对每行做 log(路数) 次比较，几百万行就是几千万次解码，
     * 实测占 Phase 2 一半以上 CPU。{@code compareValues} 对 Text 直接按字节比较（零分配）。</p>
     */
    private static int compareSpillKeys(Object a, Object b) {
        if (a instanceof List<?> la && b instanceof List<?> lb) return ArrowComparators.compareLists(la, lb);
        return AggregationHashTable.compareValues(a, b);
    }

    // ==================== Phase 2（count_distinct）：两条流统一 K 路归并 ====================

    /**
     * 增量型 + {@code count_distinct} 的 Phase 2。
     *
     * <p><b>两条流</b>（溢写时形状不同，无法共用布局）：</p>
     * <ul>
     *   <li>标量流：每组一行，键唯一 —— 文件数 &gt; {@link #MAX_SCALAR_MERGE_K} 时先多轮归并压缩，
     *       再流式 K 路归并（与纯标量路径同构，内存有界）；</li>
     *   <li>去重流：每个值一行，按 {@code (key, value)} 有序 —— 先按 ordinal 归并成<b>一条</b>有序顺串
     *       （流式、路数受分配器预算约束），再逐键数「相邻不重复值的个数」。</li>
     * </ul>
     *
     * <p><b>关键</b>：去重侧全程不物化集合 —— 同一个键的几百万个值只会以「一行一个值」的顺序流过，
     * 峰值内存 = 归并路数 × 单批 + 一个输出批，与单组去重基数无关。这正是旧格式
     * （整组集合编码成一条 VARCHAR，编解码本身 O(值数) 且峰值 = 整组集合）的替代方案。</p>
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void mergeAndOutputDistinct(PartitionState<Slot> pa, int pid) throws Exception {
        // 1) 每条去重流先归并成一条有序顺串（多轮、流式、内存有界；consumed 原文件已删）
        Path[] distinctStreams = new Path[distinctSpecs.length];
        for (int ord = 0; ord < distinctSpecs.length; ord++) {
            List<Path> dfiles = new ArrayList<>(pa.distinctFilesFor(ord));
            if (!dfiles.isEmpty()) distinctStreams[ord] = compactDistinctRuns(dfiles, pa, pid, ord);
        }

        // 标量状态列宽度：count_distinct 在标量流里不占列
        int[] stateColCount = new int[aggregations.size()];
        for (int si = 0; si < aggregations.size(); si++) {
            stateColCount[si] = distinctSpecFlag[si] ? 0
                    : (aggregations.get(si).aggregate().functionName().equals("avg") ? 2 : 1);
        }

        // 2) 标量流（键 + 标量状态）同样做内存有界的多轮归并压缩 + 流式游标。此前只把「去重值流」
        //    改成流式，标量流仍全量 loadBatchFromArrow —— 高基数分组键（group by user_id, device_type）
        //    + count(distinct) 时键本身就能溢写几百个文件，照样与溢写闸门同阶地 OOM。与纯标量路径同修。
        int fileCount = pa.batchFiles.size();
        List<Path> runPaths;
        boolean compacted;
        if (fileCount > MAX_SCALAR_MERGE_K) {
            runPaths = compactScalarRuns(pa.batchFiles, pa, pid, stateColCount);
            compacted = true;
        } else {
            runPaths = new ArrayList<>(pa.batchFiles);
            compacted = false;
        }

        List<ScalarRun> scalarRuns = new ArrayList<>(runPaths.size());
        List<Field> firstFields = null;
        List<DistinctRun> runs = new ArrayList<>();
        // 在途输出批：取消检查点抛异常时由 finally 收尾（同 mergeAndOutput）。
        List<FieldVector> outVectors = null;
        try {
            for (Path p : runPaths) {
                ScalarRun r = new ScalarRun(p);
                scalarRuns.add(r);
                if (firstFields == null && r.valid()) firstFields = r.fields();
            }
            for (int ord = 0; ord < distinctStreams.length; ord++) {
                if (distinctStreams[ord] != null) {
                    runs.add(new DistinctRun(distinctStreams[ord], distinctSpecs[ord]));
                }
            }

            List<Field> outFields = buildOutputFields(firstFields, stateColCount);

            // 统一优先队列：编码 >= 0 = 标量顺串游标；< 0 = 去重流 (-i-1)
            PriorityQueue<Integer> pq = new PriorityQueue<>(Math.max(1, scalarRuns.size() + runs.size()),
                    (a, b) -> compareSpillKeys(cursorKey(a, scalarRuns, runs), cursorKey(b, scalarRuns, runs)));
            for (int i = 0; i < scalarRuns.size(); i++) {
                if (scalarRuns.get(i).valid()) pq.add(i);
            }
            for (int ri = 0; ri < runs.size(); ri++) {
                if (runs.get(ri).valid()) pq.add(-ri - 1);
            }

            outVectors = allocOutputVectors(outFields);
            int outRow = 0;
            List<Integer> same = new ArrayList<>();

            while (!pq.isEmpty()) {
                checkCancelled();
                Object minKey = cursorKey(pq.peek(), scalarRuns, runs);

                Object[] mergedStates = new Object[aggregations.size()];
                for (int si = 0; si < aggregations.size(); si++) {
                    mergedStates[si] = distinctSpecFlag[si]
                            ? new DistinctCountState()
                            : aggregations.get(si).aggregate().createState();
                }

                // 弹出所有「当前键 = 最小键」的游标：标量同键跨文件、去重流每 ordinal 至多一个
                same.clear();
                while (!pq.isEmpty() && keysEqual(cursorKey(pq.peek(), scalarRuns, runs), minKey)) {
                    same.add(pq.poll());
                }
                for (int ci : same) {
                    if (ci >= 0) {
                        ScalarRun run = scalarRuns.get(ci);
                        mergeScalarRow(run.root(), run.row(), mergedStates, stateColCount);
                        run.advance();
                        if (run.valid()) pq.add(ci);
                    } else {
                        int ri = -ci - 1;
                        DistinctRun run = runs.get(ri);
                        long cnt = consumeDistinctGroup(run, minKey);
                        ((DistinctCountState) mergedStates[run.specIndex()]).add(cnt);
                        if (run.valid()) pq.add(ci);
                    }
                }

                if (havingConfig != null) {
                    int aggIdx = havingConfig.columnIndex() - keyColumns.size();
                    if (aggIdx >= 0 && aggIdx < aggregations.size()) {
                        Object result = evaluateMerged(aggIdx, mergedStates[aggIdx]);
                        long value = result instanceof Number n ? n.longValue() : 0;
                        if (!havingConfig.accept(result, value)) {
                            bumpMergeProgress();
                            continue;
                        }
                    }
                }

                writeKeyToOutput(outVectors, minKey, outRow);
                int outCol = keyColumns.size();
                for (int si = 0; si < aggregations.size(); si++) {
                    setVectorValue(outVectors.get(outCol), outRow, evaluateMerged(si, mergedStates[si]));
                    outCol++;
                }
                outRow++;
                bumpMergeProgress();

                if (outRow >= AGG_PER_BATCH_KEYS) {
                    List<FieldVector> batch = outVectors;
                    outVectors = null;                  // 所有权移交给输出页
                    for (FieldVector v : batch) v.setValueCount(outRow);
                    emitPage(Page.partitioned(new VectorSchemaRoot(outFields, batch, outRow), pid));
                    outVectors = allocOutputVectors(outFields);
                    outRow = 0;
                }
            }

            if (outRow > 0) {
                List<FieldVector> batch = outVectors;
                outVectors = null;                      // 所有权移交给输出页
                for (FieldVector v : batch) v.setValueCount(outRow);
                emitPage(Page.partitioned(new VectorSchemaRoot(outFields, batch, outRow), pid));
            }
        } finally {
            if (outVectors != null) {
                for (FieldVector v : outVectors) {
                    try { v.close(); } catch (Exception ignored) {}
                }
                outVectors = null;
            }
            for (ScalarRun r : scalarRuns) r.closeQuietly();
            for (DistinctRun run : runs) run.closeQuietly();
            // 归并产物不登记在 PartitionState（原始文件已在归并时删除），此处自行清理
            for (Path p : distinctStreams) {
                if (p == null) continue;
                try { Files.deleteIfExists(p); } catch (IOException ignored) {}
            }
            if (compacted) {
                for (Path p : runPaths) {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                }
            }
        }
    }

    /** 优先队列元素的当前键：>=0 标量顺串游标；<0 去重流 (-i-1)。 */
    private static Object cursorKey(int code, List<ScalarRun> scalarRuns, List<DistinctRun> runs) {
        return code >= 0 ? scalarRuns.get(code).key() : runs.get(-code - 1).key();
    }

    /** 合并一个标量溢写行（只走非 count_distinct 规格；列布局与 buildScalarSpillRoot 对应）。 */
    @SuppressWarnings("unchecked")
    private void mergeScalarRow(VectorSchemaRoot root, int row, Object[] mergedStates, int[] stateColCount) {
        int col = keyColumns.size();
        for (int si = 0; si < aggregations.size(); si++) {
            if (distinctSpecFlag[si]) continue;
            Object otherState = readStateFromSpillCols(root, row, aggregations.get(si), col);
            AggregateFunction<Object> fn = (AggregateFunction<Object>) aggregations.get(si).aggregate();
            fn.merge(mergedStates[si], otherState);
            col += stateColCount[si];
        }
    }

    /**
     * 输出去重流中「键 = {@code key}」的全部行，返回<b>相邻不重复值</b>的个数。
     *
     * <p>去重流按 {@code (key, value)} 有序 ⇒ 同键的行连续、且值已升序 ⇒ 重复值必然相邻，
     * 只需保留「上一个值」即可 O(1) 去重，不需要集合。</p>
     */
    private long consumeDistinctGroup(DistinctRun run, Object key) {
        long count = 0;
        Object prev = null;
        boolean hasPrev = false;
        while (run.valid() && keysEqual(run.key(), key)) {
            Object v = run.value();
            if (!hasPrev || AggregationHashTable.compareValues(prev, v) != 0) {
                count++;
                prev = v;
                hasPrev = true;
            }
            run.advance();
        }
        return count;
    }

    /** 评估归并后的状态：count_distinct 的轻量计数态直接取 count，其余走聚合函数自身 evaluate。 */
    @SuppressWarnings("unchecked")
    private Object evaluateMerged(int si, Object state) {
        if (state instanceof DistinctCountState dc) return dc.count();
        return ((AggregateFunction<Object>) aggregations.get(si).aggregate()).evaluate(state);
    }

    /**
     * count_distinct 流式归并的轻量状态：只保存<b>去重后的个数</b>（不物化值集合）。
     * 字段是 {@code long}，规避 {@code Set.size()} 的 int 上限。
     */
    private static final class DistinctCountState extends AbstractSet<Object> {
        private long count;
        void add(long n) { count += n; }
        long count() { return count; }
        @Override public int size() { return (int) Math.min(Integer.MAX_VALUE, count); }
        @Override public boolean isEmpty() { return count == 0; }
        @Override public Iterator<Object> iterator() { return Collections.emptyIterator(); }
    }

    /**
     * 把同一条去重流的多个顺串归并成一条有序顺串（可含多个 record batch）。
     *
     * <p>多轮收敛：每轮把 {@code laneCount} 个源并成一个文件，文件数至少减半；路数按
     * 「分配器上限 / 4 ÷ 首个源文件大小」定，且至少 2 路（保证收敛）、至多
     * {@link #MAX_DISTINCT_MERGE_K}。输出按 {@code (key, value)} 有序，并<b>就地去掉相邻重复值</b>
     * —— 多个槽位对同一 (key, value) 各写一次很正常（它们来自不同的内存槽）。</p>
     */
    private Path compactDistinctRuns(List<Path> input, PartitionState<Slot> pa, int pid, int ordinal)
            throws IOException {
        List<Path> runs = new ArrayList<>(input);
        int round = 0;
        while (runs.size() > 1) {
            checkCancelled();
            List<Path> next = new ArrayList<>();
            int i = 0;
            while (i < runs.size()) {
                checkCancelled();
                int take = Math.min(runs.size() - i, laneCount(runs.get(i)));
                if (take <= 1) {
                    next.add(runs.get(i));
                    i++;
                    continue;
                }
                List<Path> chunk = new ArrayList<>(runs.subList(i, i + take));
                Path out = spillDirForWrite().resolve(
                        "p" + pid + "_m" + round + "_d" + ordinal + "_" + i + ".arrow");
                Path produced = mergeDistinctRuns(chunk, out, pa, ordinal);
                if (produced != null) next.add(produced);
                for (Path p : chunk) {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                }
                i += take;
            }
            runs = next;
            round++;
            bumpMergeProgress();
        }
        return runs.isEmpty() ? null : runs.get(0);
    }

    /** 单轮可同时打开的路数：按「分配器上限 / 4 ÷ 首个源文件大小」估算，至少 2、至多 MAX。 */
    private int laneCount(Path first) {
        long est;
        try {
            est = Math.max(1L, Files.size(first));
        } catch (IOException e) {
            est = 1L << 20;
        }
        long budget = Math.max(1L, allocator.getLimit() / 4);
        return (int) Math.max(2L, Math.min(MAX_DISTINCT_MERGE_K, budget / est));
    }

    /**
     * 单轮归并 + 相邻去重，写出一个（多个 record batch 的）有序文件。
     *
     * @return 输出文件；所有源都为空时返回 {@code null}（不产生文件）
     */
    private Path mergeDistinctRuns(List<Path> sources, Path outFile, PartitionState<Slot> pa, int ordinal)
            throws IOException {
        List<DistinctRun> runs = new ArrayList<>(sources.size());
        VectorSchemaRoot outRoot = null;
        try {
            for (Path p : sources) runs.add(new DistinctRun(p, distinctSpecs[ordinal]));
            PriorityQueue<Integer> pq = new PriorityQueue<>(runs.size(), (a, b) -> {
                int c = compareSpillKeys(runs.get(a).key(), runs.get(b).key());
                if (c != 0) return c;
                return AggregationHashTable.compareValues(runs.get(a).value(), runs.get(b).value());
            });
            for (int i = 0; i < runs.size(); i++) if (runs.get(i).valid()) pq.add(i);
            if (pq.isEmpty()) return null;

            List<Field> outFields = runs.get(pq.peek()).fields();
            if (outFields == null) return null;

            List<FieldVector> outVectors = new ArrayList<>(outFields.size());
            for (Field f : outFields) {
                FieldVector v = f.createVector(allocator);
                v.setInitialCapacity(AGG_SPILL_PER_BATCH_KEYS);
                v.allocateNew();
                outVectors.add(v);
            }
            final VectorSchemaRoot out = new VectorSchemaRoot(new ArrayList<>(outFields), outVectors, 0);
            outRoot = out;
            int outRows = 0;
            int outCapacity = minValueCapacity(outVectors);
            boolean wrote = false;
            TransferPair[][] tps = new TransferPair[runs.size()][];
            Object lastKey = null;
            Object lastValue = null;
            boolean hasLast = false;

            try (FileChannel outCh = FileChannel.open(outFile, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                 ArrowFileWriter writer = new ArrowFileWriter(out, null, outCh)) {
                while (!pq.isEmpty()) {
                    checkCancelled();
                    int ri = pq.poll();
                    DistinctRun run = runs.get(ri);
                    Object k = run.key();
                    Object v = run.value();
                    // 相邻重复值（同键同值）直接丢弃：同一 (key,value) 在多个槽位各出现一次
                    boolean dup = hasLast && keysEqual(lastKey, k)
                            && AggregationHashTable.compareValues(lastValue, v) == 0;
                    if (!dup) {
                        if (outRows >= outCapacity) {
                            for (FieldVector fv : outVectors) fv.reAlloc();
                            outCapacity = minValueCapacity(outVectors);
                            tps = new TransferPair[runs.size()][];   // reAlloc 换缓冲，旧映射失效
                        }
                        if (tps[ri] == null) {
                            List<FieldVector> srcVectors = run.vectors();
                            TransferPair[] pair = new TransferPair[outVectors.size()];
                            for (int c = 0; c < pair.length; c++) {
                                pair[c] = srcVectors.get(c).makeTransferPair(outVectors.get(c));
                            }
                            tps[ri] = pair;
                        }
                        for (TransferPair tp : tps[ri]) tp.copyValueSafe(run.row(), outRows);
                        outRows++;
                        hasLast = true;
                        lastKey = k;
                        lastValue = v;

                        if (outRows >= AGG_SPILL_PER_BATCH_KEYS) {
                            out.setRowCount(outRows);
                            writer.writeBatch();
                            wrote = true;
                            bumpMergeProgress();
                            for (FieldVector fv : outVectors) fv.setValueCount(0);
                            outRows = 0;
                        }
                    }
                    run.advance();
                    if (run.valid()) pq.add(ri);
                }
                if (outRows > 0) {
                    out.setRowCount(outRows);
                    writer.writeBatch();
                    wrote = true;
                }
            }
            if (!wrote) {
                try { Files.deleteIfExists(outFile); } catch (IOException ignored) {}
                return null;
            }
            accountCompactionFile(pa, outFile);
            return outFile;
        } finally {
            if (outRoot != null) { try { outRoot.close(); } catch (Exception ignored) {} }
            for (DistinctRun run : runs) run.closeQuietly();
        }
    }

    /**
     * 去重顺串的流式游标：文件内可能含多个 record batch（归并产物），逐批加载，
     * 任意时刻只驻留<b>一个批</b>。
     *
     * <p>★{@link ArrowFileReader#loadRecordBatch} 复用同一个 {@code VectorSchemaRoot} 的缓冲，
     * 换批后旧值失效；但 {@code getObject} 对变长列会复制字节（Text 自带副本），故缓存的
     * key/value 跨批仍然稳定 —— 与 Window 归并缓存 {@code curKeys} 的假设一致。</p>
     */
    /**
     * 标量溢写顺串（键 + 标量状态）的流式游标：文件内可能含多个 record batch（归并产物），逐批加载，
     * 任意时刻只驻留<b>一个批</b>。
     *
     * <p>与 {@link DistinctRun} 同构：{@link ArrowFileReader#loadRecordBatch} 复用同一个
     * {@code VectorSchemaRoot} 的缓冲，换批后旧值失效；但 {@code getObject} 对变长列会复制字节
     * （Text 自带副本），故缓存的 {@code key} 跨批仍然稳定。</p>
     */
    private final class ScalarRun implements AutoCloseable {
        private final Path path;
        private final FileChannel channel;
        private final ArrowFileReader reader;
        private final List<ArrowBlock> blocks;
        private int blockIdx;
        private VectorSchemaRoot root;
        private int row;
        private int rowCount;
        private Object key;
        private boolean valid;

        ScalarRun(Path path) throws IOException {
            this.path = path;
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            this.reader = new ArrowFileReader(channel, allocator);
            this.blocks = reader.getRecordBlocks();
            if (loadNextBlock()) positionAtCurrentRow();
            else valid = false;
        }

        private boolean loadNextBlock() {
            while (blockIdx < blocks.size()) {
                try {
                    reader.loadRecordBatch(blocks.get(blockIdx++));
                    root = reader.getVectorSchemaRoot();
                } catch (IOException e) {
                    throw new RuntimeException("读取标量溢写文件失败: " + path, e);
                }
                rowCount = root.getRowCount();
                row = 0;
                if (rowCount > 0) return true;
            }
            return false;
        }

        private void positionAtCurrentRow() {
            key = buildKeyFromSpill(root, row);
            valid = true;
        }

        boolean valid() { return valid; }
        Object key() { return key; }
        int row() { return row; }
        VectorSchemaRoot root() { return root; }
        List<Field> fields() { return root == null ? null : root.getSchema().getFields(); }

        void advance() {
            row++;
            if (row < rowCount) {
                positionAtCurrentRow();
                return;
            }
            if (loadNextBlock()) positionAtCurrentRow();
            else valid = false;
        }

        @Override public void close() {
            try { reader.close(); } catch (Exception ignored) {}
            try { channel.close(); } catch (Exception ignored) {}
        }

        void closeQuietly() {
            try { close(); } catch (Exception ignored) {}
        }
    }

    private final class DistinctRun implements AutoCloseable {
        private final Path path;
        private final FileChannel channel;
        private final ArrowFileReader reader;
        private final List<ArrowBlock> blocks;
        private int blockIdx;
        private VectorSchemaRoot root;
        private int row;
        private int rowCount;
        private final int specIndex;
        private final int valueCol;
        private Object key;
        private Object value;
        private boolean valid;

        DistinctRun(Path path, int specIndex) throws IOException {
            this.path = path;
            this.specIndex = specIndex;
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            this.reader = new ArrowFileReader(channel, allocator);
            this.blocks = reader.getRecordBlocks();
            this.valueCol = keyColumns.size();
            if (loadNextBlock()) positionAtCurrentRow();
            else valid = false;
        }

        private boolean loadNextBlock() {
            while (blockIdx < blocks.size()) {
                try {
                    reader.loadRecordBatch(blocks.get(blockIdx++));
                    root = reader.getVectorSchemaRoot();
                } catch (IOException e) {
                    throw new RuntimeException("读取去重溢写文件失败: " + path, e);
                }
                rowCount = root.getRowCount();
                row = 0;
                if (rowCount > 0) return true;
            }
            return false;
        }

        private void positionAtCurrentRow() {
            key = buildKeyFromSpill(root, row);
            value = AggregationHashTable.valueAt(root, valueCol, row);
            valid = true;
        }

        boolean valid() { return valid; }
        Object key() { return key; }
        Object value() { return value; }
        int row() { return row; }
        int specIndex() { return specIndex; }
        List<FieldVector> vectors() { return root.getFieldVectors(); }
        List<Field> fields() { return root == null ? null : root.getSchema().getFields(); }

        void advance() {
            row++;
            if (row < rowCount) {
                positionAtCurrentRow();
                return;
            }
            if (loadNextBlock()) positionAtCurrentRow();
            else valid = false;
        }

        @Override public void close() {
            try { reader.close(); } catch (Exception ignored) {}
            try { channel.close(); } catch (Exception ignored) {}
        }

        void closeQuietly() {
            try { close(); } catch (Exception ignored) {}
        }
    }

    // ==================== collect-evaluate 型 Phase 2 ====================

    /** 内存路径：CollectHashTable 攒齐后按 key 排序拼接交付（无溢写时走这里）。 */
    private void outputCollectMemory(CollectHashTable ht, int pid) {
        List<CollectHashTable.CollectSeg> segs = ht.sortedSegments();
        if (segs.isEmpty()) return;
        CollectAggregateFunction fn = (CollectAggregateFunction) aggregations.get(0).collect();
        String outName = aggregations.get(0).outputColumn();
        List<Field> valueFields = collectValueFields;

        // 分块输出：每块 AGG_PER_BATCH_KEYS 个 key，拼接 N-key batch 一次交付
        for (int start = 0; start < segs.size(); start += AGG_PER_BATCH_KEYS) {
            checkCancelled();
            int end = Math.min(start + AGG_PER_BATCH_KEYS, segs.size());
            List<CollectHashTable.CollectSeg> chunk = segs.subList(start, end);
            List<CollectHashTable.CollectGroup> groups = new ArrayList<>(chunk.size());
            for (CollectHashTable.CollectSeg s : chunk) {
                groups.add(new CollectHashTable.CollectGroup(s.key(),
                        List.of(new CollectHashTable.ValueFrag(s.valueCols(), s.rows()))));
            }
            VectorSchemaRoot deliver = CollectHashTable.buildDeliverBatch(
                    allocator, groupKeyFields, valueFields, groups);
            List<Object> keys = new ArrayList<>(groups.size());
            for (CollectHashTable.CollectGroup g : groups) keys.add(g.key());
            emitCollectOutput(deliver, keys, fn, outName, pid);
        }
    }

    /**
     * 溢写路径：从 pa.index 的 Segment(batchIndex, startRow, endRow) 读回 → 逐 key 提取值段 → 拼接交付。
     *
     * <p><b>内存模型（v4.1）</b>：按「值总数预算」切 key 批次，每批<b>逐文件流式</b>写入交付 batch，
     * 文件读完立即关闭 —— 峰值 = 一个交付 batch + 一个溢写文件，不再把全部分区溢写文件常驻缓存
     * （旧实现 cache 不淘汰 + deliver 再复制一份 ⇒ 峰值 ≈ 2× 数据量，几千万值的大组必然 OOM）。
     * 定长/变长值列走 {@link CollectHashTable.DeliverStream}；嵌套类型回退旧拼接路径。</p>
     */
    private void mergeAndOutputCollect(PartitionState<Slot> pa, int pid) {
        if (pa.index.isEmpty()) return;
        CollectAggregateFunction fn = (CollectAggregateFunction) aggregations.get(0).collect();
        String outName = aggregations.get(0).outputColumn();
        List<Field> valueFields = collectValueFields;
        boolean streaming = CollectHashTable.DeliverStream.supportsAll(valueFields);

        List<Object> allKeys = new ArrayList<>(pa.index.keySet());
        sortKeys(allKeys);

        int start = 0;
        while (start < allKeys.size()) {
            checkCancelled();
            // 按值总数预算切批（单 key 本身超预算也照常交付：collect 语义要求整组值一次性求值）
            long budgetValues = 0;
            int end = start;
            while (end < allKeys.size()) {
                checkCancelled();
                long sz = segmentRowCount(pa, allKeys.get(end));
                if (end > start && budgetValues + sz > AGG_COLLECT_DELIVER_VALUES) break;
                budgetValues += sz;
                end++;
            }
            List<Object> keyBatch = allKeys.subList(start, end);
            if (streaming) {
                deliverCollectStreaming(pa, pid, keyBatch, fn, outName, valueFields);
            } else {
                deliverCollectLegacy(pa, pid, keyBatch, fn, outName, valueFields);
            }
            bumpMergeProgress();   // 每交付完一批 key（读文件 + 逐 key 收集）→ 归并推进
            start = end;
        }
    }

    /** 一个 key 在全部溢写文件中的总行数。 */
    private static long segmentRowCount(PartitionState<?> pa, Object key) {
        List<Segment> segs = pa.index.get(key);
        if (segs == null) return 0;
        long n = 0;
        for (int i = 0; i < segs.size(); i++) {
            Segment seg = segs.get(i);
            n += Math.max(0, seg.endRow() - seg.startRow());
        }
        return n;
    }

    /**
     * 流式交付一个 key 批次：文件升序，一次只加载一个溢写文件，把各 key 的值段直接写入交付 List 列。
     * 变长值列先做一遍「只统计字节数」的扫描（定长列不需要），用于一次性分配 child data buffer。
     */
    private void deliverCollectStreaming(PartitionState<Slot> pa, int pid, List<Object> keyBatch,
                                         CollectAggregateFunction fn, String outName,
                                         List<Field> valueFields) {
        int groupCount = keyBatch.size();
        int[] groupSizes = new int[groupCount];
        // 文件 → 该文件内的段 [groupIdx, startRow, endRow]（文件升序 = 稳定的组内值顺序）
        TreeMap<Integer, List<int[]>> byFile = new TreeMap<>();
        boolean hasVarWidth = false;
        for (Field f : valueFields) hasVarWidth |= RowAccumulator.typeWidthOf(f.getType()) == 0;
        long[][] groupBytes = hasVarWidth ? new long[valueFields.size()][groupCount] : null;

        for (int g = 0; g < groupCount; g++) {
            checkCancelled();
            List<Segment> segs = pa.index.get(keyBatch.get(g));
            if (segs == null) continue;
            for (int i = 0; i < segs.size(); i++) {
                Segment seg = segs.get(i);
                int len = seg.endRow() - seg.startRow();
                if (len <= 0) continue;
                groupSizes[g] += len;
                byFile.computeIfAbsent(seg.batchIndex(), k -> new ArrayList<>())
                        .add(new int[]{g, seg.startRow(), seg.endRow()});
            }
        }

        try {
            // 变长列第一遍：统计每組字节数（决定 child data buffer 容量与组内起始字节）
            if (groupBytes != null) {
                for (Map.Entry<Integer, List<int[]>> e : byFile.entrySet()) {
                    checkCancelled();
                    VectorSchemaRoot r = loadBatchFromArrow(pa, e.getKey(), pid);
                    if (r == null) continue;
                    try {
                        for (int[] seg : e.getValue()) {
                            for (int v = 0; v < valueFields.size(); v++) {
                                FieldVector src = r.getVector(keyColumns.size() + v);
                                if (src instanceof BaseVariableWidthVector
                                        || src instanceof BaseLargeVariableWidthVector) {
                                    groupBytes[v][seg[0]] += varWidthBytes(src, seg[1], seg[2]);
                                }
                            }
                        }
                    } finally {
                        r.close();
                    }
                }
            }

            try (CollectHashTable.DeliverStream ds = CollectHashTable.DeliverStream.open(
                    allocator, groupKeyFields, valueFields, keyBatch, groupSizes, groupBytes)) {
                // 第二遍：文件升序逐个加载 → 直接写入交付列 → 立即关闭
                for (Map.Entry<Integer, List<int[]>> e : byFile.entrySet()) {
                    checkCancelled();
                    VectorSchemaRoot r = loadBatchFromArrow(pa, e.getKey(), pid);
                    if (r == null) continue;
                    try {
                        for (int[] seg : e.getValue()) {
                            int g = seg[0];
                            int len = seg[2] - seg[1];
                            for (int v = 0; v < valueFields.size(); v++) {
                                ds.copy(r.getVector(keyColumns.size() + v), v, seg[1], g, len);
                            }
                        }
                    } finally {
                        r.close();
                    }
                }
                emitCollectOutput(ds.finish(), keyBatch, fn, outName, pid);
            }
        } catch (org.apache.arrow.memory.OutOfMemoryException oom) {
            long values = 0;
            for (int sz : groupSizes) values += sz;
            throw new IllegalStateException(
                    "collect 聚合 [" + fn.functionName() + "] 待交付值过多（" + values + " 个），"
                            + "超出可用内存：collect 模型要求把每个分组的全部值一次性交给外挂函数求值。"
                            + "请改用四阶段增量聚合（Python: @aggregate_function / class Xxx(AggregateFunction)；"
                            + "Java: AggregateFunction 实现），其状态与数据量无关、可溢写。", oom);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "collect 聚合 [" + fn.functionName() + "] 溢写交付失败: " + e.getMessage(), e);
        }
    }

    /** 嵌套类型回退路径：按 key 批次缓存所需溢写文件，再整体拼接交付（与旧实现一致）。 */
    private void deliverCollectLegacy(PartitionState<Slot> pa, int pid, List<Object> keyBatch,
                                      CollectAggregateFunction fn, String outName,
                                      List<Field> valueFields) {
        Map<Integer, VectorSchemaRoot> cache = new LinkedHashMap<>();
        try {
            for (Object key : keyBatch) {
                checkCancelled();
                List<Segment> segs = pa.index.get(key);
                if (segs == null) continue;
                for (int i = 0; i < segs.size(); i++) {
                    int bi = segs.get(i).batchIndex();
                    if (!cache.containsKey(bi)) {
                        try {
                            VectorSchemaRoot r = loadBatchFromArrow(pa, bi, pid);
                            if (r != null) cache.put(bi, r);
                        } catch (Exception e) {
                            System.err.printf("[HashAggregation] Error loading batch %d: %s%n",
                                    bi, e.getMessage());
                        }
                    }
                }
            }

            List<CollectHashTable.CollectGroup> groups = new ArrayList<>();
            List<Object> keys = new ArrayList<>();
            for (Object key : keyBatch) {
                List<Segment> segs = pa.index.get(key);
                if (segs == null || segs.isEmpty()) continue;
                List<CollectHashTable.ValueFrag> frags = new ArrayList<>();
                for (int i = 0; i < segs.size(); i++) {
                    Segment seg = segs.get(i);
                    VectorSchemaRoot br = cache.get(seg.batchIndex());
                    if (br == null) continue;
                    int len = seg.endRow() - seg.startRow();
                    if (len <= 0) continue;
                    FieldVector[] vcols = new FieldVector[valueFields.size()];
                    for (int v = 0; v < valueFields.size(); v++) {
                        vcols[v] = br.getVector(keyColumns.size() + v);
                    }
                    int[] rows = new int[len];
                    for (int k = 0; k < len; k++) rows[k] = seg.startRow() + k;
                    frags.add(new CollectHashTable.ValueFrag(vcols, rows));
                }
                if (frags.isEmpty()) continue;
                groups.add(new CollectHashTable.CollectGroup(key, frags));
                keys.add(key);
            }
            if (groups.isEmpty()) return;

            VectorSchemaRoot deliver = CollectHashTable.buildDeliverBatch(
                    allocator, groupKeyFields, valueFields, groups);
            emitCollectOutput(deliver, keys, fn, outName, pid);
        } finally {
            for (VectorSchemaRoot r : cache.values()) {
                try { r.close(); } catch (Exception ignored) {}
            }
        }
    }

    /** 变长列一段（startRow..endRow）的字节总数。 */
    private static long varWidthBytes(FieldVector src, int startRow, int endRow) {
        org.apache.arrow.memory.ArrowBuf off = src.getOffsetBuffer();
        long n = 0;
        if (src instanceof BaseLargeVariableWidthVector) {
            for (int i = startRow; i < endRow; i++) {
                if (!src.isNull(i)) {
                    n += off.getLong((long) (i + 1) * 8) - off.getLong((long) i * 8);
                }
            }
        } else {
            for (int i = startRow; i < endRow; i++) {
                if (!src.isNull(i)) {
                    n += off.getInt((long) (i + 1) * 4) - off.getInt((long) i * 4);
                }
            }
        }
        return n;
    }

    /**
     * 交付：{@code deliver} = [key..., list<value>...]（每 key 一行，值列 = 该 key 攒齐的全部值，
     * 引擎侧已按 key 分组）→ evaluateBatch → N 行结果列 → 拼 [key..., result] 输出页入队。
     * deliver 由调用方持有、本方法负责 close。
     */
    private void emitCollectOutput(VectorSchemaRoot deliver, List<Object> keys,
                                   CollectAggregateFunction fn, String outName, int pid) {
        int keyCount = groupKeyFields.size();
        try (deliver) {
            FieldVector result;
            try {
                result = fn.evaluateBatch(deliver, keyCount, allocator, outName);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "collect 聚合 [" + fn.functionName() + "] evaluateBatch 失败: " + e.getMessage(), e);
            }
            int n = result.getValueCount();
            if (n != keys.size()) {
                throw new IllegalStateException(
                        "collect 聚合 [" + fn.functionName() + "] 结果行数 " + n + " != key 数 " + keys.size());
            }
            // 输出页 = [key 列..., result 列]
            List<Field> outFields = new ArrayList<>(groupKeyFields);
            Field resultField = new Field(outName,
                    result.getField().getFieldType(), result.getField().getChildren());
            outFields.add(resultField);
            List<FieldVector> outVectors = new ArrayList<>(outFields.size());
            for (Field f : outFields) {
                FieldVector v = f.createVector(allocator);
                v.setInitialCapacity(n);
                v.allocateNew();
                outVectors.add(v);
            }
            for (int i = 0; i < n; i++) writeKeyToOutput(outVectors, keys.get(i), i);
            FieldVector resVec = outVectors.get(keyCount);
            TransferPair tp = result.makeTransferPair(resVec);
            for (int i = 0; i < n; i++) tp.copyValueSafe(i, i);
            resVec.setValueCount(n);
            for (FieldVector v : outVectors) v.setValueCount(n);
            emitPage(Page.partitioned(new VectorSchemaRoot(outFields, outVectors, n), pid));
            result.close();
        }
    }

    // ==================== HAVING 过滤 ====================

    /** 过滤输出批中不满足 HAVING 条件的行，返回新 root 或 null（全被过滤）。 */
    private VectorSchemaRoot filterHaving(VectorSchemaRoot root) {
        int total = root.getRowCount();
        if (total == 0) return root;

        org.apache.arrow.vector.FieldVector havingCol = root.getVector(havingConfig.columnIndex());
        List<Integer> keep = new ArrayList<>(total);
        for (int r = 0; r < total; r++) {
            if (havingConfig.accept(havingCol, r)) keep.add(r);
        }
        if (keep.isEmpty()) { root.close(); return null; }
        if (keep.size() == total) return root;

        // 部分匹配：构建新 root，只包含符合条件的行
        VectorSchemaRoot filtered = VectorSchemaRoot.create(root.getSchema(), allocator);
        filtered.allocateNew();
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            org.apache.arrow.vector.FieldVector src = root.getVector(i);
            org.apache.arrow.vector.FieldVector dst = filtered.getVector(i);
            org.apache.arrow.vector.util.TransferPair tp = src.makeTransferPair(dst);
            for (int j = 0; j < keep.size(); j++) tp.copyValueSafe(keep.get(j), j);
            dst.setValueCount(keep.size());
        }
        filtered.setRowCount(keep.size());
        root.close();
        return filtered;
    }

    // ==================== 内部工具 ====================

    @SuppressWarnings("unchecked")
    private Object readStateFromSpillCols(VectorSchemaRoot root, int row,
                                           AggregationSpec spec, int startCol) {
        return switch (spec.aggregate().functionName()) {
            case "count", "sum_int64" -> {
                Object val = root.getVector(startCol).getObject(row);
                long v = val == null ? 0L : ((Number) val).longValue();
                yield new long[]{v};
            }
            case "sum" -> {
                Object val = root.getVector(startCol).getObject(row);
                // DECIMAL 状态列 getObject → BigDecimal（精确）；BIGINT 状态列 → Long（整型精确）；
                // 浮点状态列 → double（旧口径）。
                // ★NULL 状态原样保留（空集 / 全 NULL 组），不能回填 0.0。
                // ★必须用 if/else 而不是三元表达式：Long 与 double 两支会被数值提升统一成
                //   Double（结果是 9.007199254740992E15），sum(CAST(x AS BIGINT)) 溢写回读丢精度。
                Object pick;
                if (val == null) {
                    pick = null;
                } else if (val instanceof java.math.BigDecimal bd) {
                    pick = bd;
                } else if (val instanceof Long || val instanceof Integer
                        || val instanceof Short || val instanceof Byte) {
                    pick = ((Number) val).longValue();
                } else {
                    pick = ((Number) val).doubleValue();
                }
                yield new Object[]{pick};
            }
            case "min", "max" -> {
                // 状态列与输入列同型（时间戳 / 日期 / 字符串 / 数值…），原样读回再规范化
                Object val = root.getVector(startCol).getObject(row);
                yield new Object[]{com.dojao.core.function.aggregate.MinMaxSupport.normalize(val)};
            }
            case "avg" -> {
                Object sumVal = root.getVector(startCol).getObject(row);
                Object cntVal = root.getVector(startCol + 1).getObject(row);
                Object sum = sumVal instanceof java.math.BigDecimal bd ? bd
                        : (sumVal == null ? 0.0d : ((Number) sumVal).doubleValue());
                long cnt = cntVal == null ? 0L : ((Number) cntVal).longValue();
                yield new Object[]{sum, cnt};
            }
            case "count_distinct" -> {
                org.apache.arrow.vector.VarCharVector vv =
                        (org.apache.arrow.vector.VarCharVector) root.getVector(startCol);
                yield com.dojao.core.function.aggregate.CountDistinctAggregateFunction
                        .decode(vv.isNull(row) ? null : vv.get(row));
            }
            case "collect_list", "collect_set" -> {
                // List 列一格 = 整组值；NULL（空组 / 全 NULL 组）→ 空状态。
                Object val = root.getVector(startCol).getObject(row);
                java.util.Collection<?> vals = val instanceof java.util.Collection<?> c ? c : null;
                yield com.dojao.core.function.aggregate.CollectSupport
                        .fromValues(spec.aggregate().functionName(), vals);
            }
            default -> throw new UnsupportedOperationException(
                    "Cannot read spilt state for aggregate: " + spec.aggregate().functionName());
        };
    }

    @SuppressWarnings("unchecked")
    private void writeKeyToOutput(List<FieldVector> outVectors, Object key, int outRow) {
        if (keyColumns.size() == 1) {
            setVectorValue(outVectors.get(0), outRow, key);
        } else {
            List<Object> keyList = (List<Object>) key;
            for (int i = 0; i < keyColumns.size(); i++) {
                setVectorValue(outVectors.get(i), outRow, keyList.get(i));
            }
        }
    }

    // ==================== OperatorMemoryHandle ====================

    @Override
    public SpillResult spill(long targetBytes) {
        long totalFreed = 0;
        for (int pid = 0; pid < partitions.length; pid++) {
            PartitionState<Slot> pa = partitions[pid];
            if (pa == null) continue;
            Slot active = pa.activeSlot();
            if (active != null && !active.isEmpty()) {
                long before = active.estimateMemoryBytes();
                pa.submitSpill(spillExecutor, this, pid);
                pa.awaitIdleSlot();
                totalFreed += before;
            }
        }
        return totalFreed > 0 ? SpillResult.completed(totalFreed, 0) : SpillResult.noOp();
    }
}
