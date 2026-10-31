package com.dojao.core.operator;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 内部排序组件 —— 按 join key 分组排序，暴露 Arrow 文件 + 索引。
 *
 * <h3>与 {@link WindowJoinSortOperator} 的差异</h3>
 * <ul>
 *   <li>无 ORDER BY：只按 join key 分组，组内顺序任意</li>
 *   <li>不做页级输出：排序结果以 Arrow 文件 + 内存索引形式暴露给下游 MergeJoin</li>
 *   <li>{@link #doNext()} 返回 EOF —— MergeJoin 通过 {@link #getPartitionedData(int)} 直接读取</li>
 * </ul>
 *
 * <h3>流水线</h3>
 * <pre>
 *   Phase 1: 上游 Page → RowAccumulator（三槽溢写）→ Arrow 文件 + index
 *   Phase 2: 残留 slot → 溢写 → 所有数据统一为 Arrow 文件格式
 * </pre>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/23
 */
class JoinSortOperator extends AbstractSpillableOperator<RowAccumulator> {

    private final List<String> joinKeyColumns;
    private int[] keyIndices;
    private boolean isCompositeKey;
    private boolean pipelineDone;

    /** join 排序内部缓冲 / 输出批行数（独立于 agg/window 旋钮，固定 50000）。 */
    private static final int OUTPUT_BATCH_SIZE = 50000;

    // ==================== 构造函数 ====================

    public JoinSortOperator(Operator upstream,
                        List<String> joinKeyColumns,
                        long memoryLimit,
                        int numPartitions) {
        super("Sort", "Sort", 1, upstream, memoryLimit, numPartitions);
        this.joinKeyColumns = joinKeyColumns;
        this.pipelineDone = false;
    }

    public JoinSortOperator(Operator upstream,
                        List<String> joinKeyColumns,
                        long memoryLimit) {
        this(upstream, joinKeyColumns, memoryLimit, DEFAULT_PARTITIONS);
    }

    public JoinSortOperator(Operator upstream, String joinKeyColumn, long memoryLimit) {
        this(upstream, List.of(joinKeyColumn), memoryLimit, DEFAULT_PARTITIONS);
    }

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
        return joinKeyColumns;
    }

    @Override
    protected List<Field> resolveGroupKeyFields(Schema schema) {
        List<Field> fields = new ArrayList<>();
        for (String col : joinKeyColumns) fields.add(schema.findField(col));
        return fields;
    }

    /**
     * <b>★不建溢写索引</b>。基类默认 {@code true} 会为每个溢写批按行建「key → 段」堆内索引，
     * 其规模是 <b>O(去重键数)</b> 而非 O(溢写字节) —— 对高基数 join 键（如全局唯一的 event_id）
     * 一个分区就是数百万条 {@code Map} 节点 + {@code ArrayList} + 装箱键，且 {@code estimateMemoryBytes()}
     * 不计它 ⇒ 溢写无法缓解，Phase 1 期间堆单调膨胀直至 GC 抖动 / OOM。
     * <p>本算子的索引只被 {@code SortMergeJoinOperator.indexBasedMergeJoin} 使用，而该路径仅在
     * Phase 3 归并<b>失败</b>（{@code compacted == false}）时才会走到；正常路径由 K 路归并产出
     * key 连续文件后 {@code compactPartition} 会 {@code index.clear()} 直接丢弃。故这里不建索引；
     * 归并失败已是致命错误（见 {@link #pipeAll()}，不再吞异常）。</p>
     */
    @Override
    protected boolean maintainSpillIndex() {
        return false;
    }

    @Override
    protected void prepareSlot(RowAccumulator slot, Page input) {
        if (keyIndices == null) resolveKeyIndices();
        slot.initIfNeeded(inputSchema, allocator, OUTPUT_BATCH_SIZE);
        slot.ensureTransfers(input.getRoot(), keyIndices, isCompositeKey);
    }

    // ==================== Phase 2：内存/溢写统一为 Arrow 文件 ====================

    @Override
    protected void outputMemory(PartitionState<RowAccumulator> pa, int pid) {
        if (pa.batchFiles.isEmpty()) {
            // 纯内存路径：数据没溢写
            RowAccumulator theAcc = null;
            int slotCount = 0;
            for (int s = 0; s < SLOT_COUNT; s++) {
                RowAccumulator acc = pa.slots.get(s);
                if (acc == null || acc.isEmpty()) continue;
                if (theAcc == null) theAcc = acc;
                slotCount++;
            }
            if (slotCount == 0) return;

            if (slotCount == 1 && theAcc != null) {
                // ★ 零拷贝路径：只出排序索引，不拷贝数据
                pa.sortedIndices = buildSortedIndices(theAcc);
                pa.indexSource = theAcc;
                // 不调 acc.clear() — 向量保留给 P2 读取
                // 不设 inMemoryRoot — 保持 null
                pa.compacted = true;
            } else {
                // 多 slot：回退原有拷贝路径（罕见）
                List<VectorSchemaRoot> sortedRoots = new ArrayList<>();
                for (int s = 0; s < SLOT_COUNT; s++) {
                    RowAccumulator acc = pa.slots.get(s);
                    if (acc == null || acc.isEmpty()) continue;
                    sortedRoots.add(buildKeySortedOutput(acc));
                    acc.clear();
                    pa.slots.set(s, null);
                }
                if (sortedRoots.isEmpty()) return;
                pa.inMemoryRoot = sortedRoots.size() == 1
                        ? sortedRoots.getFirst()
                        : mergeSortedRoots(sortedRoots);
                pa.compacted = true;
                for (VectorSchemaRoot r : sortedRoots) {
                    if (r != pa.inMemoryRoot) r.close();
                }
            }
        } else {
            // 有溢写文件：剩余 slot 数据写成 Arrow 文件（补充到 batchFiles），
            // 后续 compactPartition 归并成 key 连续文件
            for (int s = 0; s < SLOT_COUNT; s++) {
                RowAccumulator acc = pa.slots.get(s);
                if (acc == null || acc.isEmpty()) continue;
                writeAccumulatorToArrow(pa, acc, pid);
                acc.clear();
                pa.slots.set(s, null);
            }
            if (pa.batchFiles.size() <= 1) {
                pa.compacted = true;
            }
        }
    }

    /** 多 slot 都没溢写时的 K 路归并到内存（罕見场景，slot 阈值大 + 数据跨多 slot 但都不大） */
    private VectorSchemaRoot mergeSortedRoots(List<VectorSchemaRoot> roots) {
        VectorSchemaRoot result = roots.getFirst();
        for (int i = 1; i < roots.size(); i++) {
            result = mergeTwoRoots(result, roots.get(i));
        }
        return result;
    }

    private VectorSchemaRoot mergeTwoRoots(VectorSchemaRoot r1, VectorSchemaRoot r2) {
        int total = r1.getRowCount() + r2.getRowCount();
        List<Field> fields = r1.getSchema().getFields();
        List<FieldVector> out = createOutputVectors(r1.getSchema(), total);

        int i = 0, j = 0, pos = 0;
        while (i < r1.getRowCount() && j < r2.getRowCount()) {
            if (compareKey(keyAt(r1, i), keyAt(r2, j)) <= 0) {
                copyRow(r1, i++, out, pos++);
            } else {
                copyRow(r2, j++, out, pos++);
            }
        }
        while (i < r1.getRowCount()) copyRow(r1, i++, out, pos++);
        while (j < r2.getRowCount()) copyRow(r2, j++, out, pos++);

        for (FieldVector v : out) v.setValueCount(total);
        r1.close();
        r2.close();
        return new VectorSchemaRoot(fields, out, total);
    }

    @Override
    protected void mergeAndOutput(PartitionState<RowAccumulator> pa, int pid) {
        // 溢写数据已在 Arrow 文件 + index 中，flushRemainingSlots 已处理残留
        // 这里不需要额外产出 Page
    }

    // ==================== 溢写：覆盖 spillSlotSync（基类实现对本算子会 NPE） ====================

    /**
     * Phase 1 溢写一个槽位：按 join key 排序后写成 Arrow 文件并登记「key → 段」索引。
     *
     * <p><b>★必须覆盖基类</b>：基类 {@code spillSlotSync} 调用 {@link RowAccumulator#buildSortedSpillRoot()}，
     * 而该实现只服务于 {@link WindowSortOperator}、对本算子恒返回 {@code null}（本算子由 {@link #buildKeySortedOutput}
     * 产出排序快照）⇒ 溢写时 {@code new ArrowFileWriter(null, …)} 直接 NPE。之前从未触发是因为
     * 排序归并 join 直到遇到「build 侧超预算」的大表才第一次真正溢写。</p>
     */
    @Override
    protected void spillSlotSync(PartitionState<RowAccumulator> pa, int slotIdx,
                                  RowAccumulator acc, int pid) {
        if (acc.isEmpty()) return;
        try {
            writeAccumulatorToArrow(pa, acc, pid);
        } finally {
            // 无论成败都释放槽位：写失败时不 close 会泄漏 Arrow 缓冲
            acc.clear();
            pa.slots.set(slotIdx, null);
        }
    }

    /** 将 RowAccumulator 排序后写入 Arrow 文件（key 有序，组内无序）。 */
    private void writeAccumulatorToArrow(PartitionState<RowAccumulator> pa,
                                          RowAccumulator acc, int pid) {
        if (acc.isEmpty()) return;
        // ★ 只累加「本次新写入的字节」：直接累加分区累计 dataFileSize 会让统计量随溢写次数
        //   平方级虚增（同 WindowSortOperator.spillSlotSync 的教训）。
        VectorSchemaRoot sorted = buildKeySortedOutput(acc);
        try {
            BatchWrite written = writeBatchToArrow(pa, sorted, pid);
            if (maintainSpillIndex()) {
                addToIndex(pa, sorted, written.batchIndex());
            }
            accountSpilledBytes(written.bytes());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Arrow batch for partition " + pid, e);
        } finally {
            sorted.close();
        }
    }

    /** 按 key 排序输出（只在组间排序，组内保持追加顺序） */
    private VectorSchemaRoot buildKeySortedOutput(RowAccumulator acc) {
        int[] outIndices = buildSortedIndices(acc);

        // 按索引构建输出 root：固定宽列直接 buffer copy，变长列用 TransferPair
        Schema schema = inputSchema;
        List<Field> fields = schema.getFields();
        int nCols = fields.size();
        FieldVector[] srcVecs = acc.vectors.toArray(new FieldVector[0]);
        FieldVector[] dstVecs = new FieldVector[nCols];
        int[] widths = new int[nCols];
        boolean[] isFixed = new boolean[nCols];
        TransferPair[] tps = new TransferPair[nCols];
        for (int col = 0; col < nCols; col++) {
            dstVecs[col] = fields.get(col).createVector(allocator);
            dstVecs[col].setInitialCapacity(acc.rowCount);
            dstVecs[col].allocateNew();
            int tw = RowAccumulator.typeWidthOf(fields.get(col).getType());
            widths[col] = tw;
            isFixed[col] = tw > 0;
            if (!isFixed[col]) tps[col] = srcVecs[col].makeTransferPair(dstVecs[col]);
        }
        for (int i = 0; i < acc.rowCount; i++) {
            int srcIdx = outIndices[i];
            for (int col = 0; col < nCols; col++) {
                if (isFixed[col]) {
                    int tw = widths[col];
                    dstVecs[col].getDataBuffer().setBytes((long) i * tw,
                            srcVecs[col].getDataBuffer(), (long) srcIdx * tw, tw);
                    // ★ memcpy 不搬 validity：新缓冲零值（= 全 NULL），非空行必须显式置位
                    if (srcVecs[col].isNull(srcIdx)) {
                        dstVecs[col].setNull(i);
                    } else {
                        ArrowBuf val = dstVecs[col].getValidityBuffer();
                        if (val != null) BitVectorHelper.setBit(val, i);
                    }
                } else {
                    tps[col].copyValueSafe(srcIdx, i);
                }
            }
        }
        for (FieldVector v : dstVecs) v.setValueCount(acc.rowCount);
        return new VectorSchemaRoot(fields, Arrays.asList(dstVecs), acc.rowCount);
    }

    /** 构建排序索引：INT 键用 primitive map 快速路径，其他键用 LinkedHashMap */
    private int[] buildSortedIndices(RowAccumulator acc) {
        int[] outIndices = new int[acc.rowCount];
        int pos = 0;
        if (acc.hasIntKeys()) {
            int[] keys = acc.sortedIntKeys();
            for (int key : keys) {
                for (int idx : acc.getIntRowIndices(key)) {
                    outIndices[pos++] = idx;
                }
            }
        } else {
            List<Object> keys = new ArrayList<>(acc.keyToRowIndices.keySet());
            sortKeys(keys);
            for (Object key : keys) {
                for (int idx : acc.keyToRowIndices.get(key)) {
                    outIndices[pos++] = idx;
                }
            }
        }
        return outIndices;
    }

    // ==================== 生命周期 ====================

    @Override
    protected Page doNext() throws Exception {
        if (pipelineDone) return Page.eof();

        // 走基类的 Phase 1 + 2 流水线，但只到排序完成为止
        pipeAll();

        pipelineDone = true;
        return Page.eof();
    }

    /** 耗尽上游、完成三槽溢写、等待 Phase 2 完成（不产生输出 Page） */
    private void pipeAll() throws Exception {
        // Phase 1: 消费上游
        long phase1Start = System.currentTimeMillis();
        long nextTotal = 0, accTotal = 0, closeTotal = 0;
        int pageCount = 0;
        long totalRows = 0;
        while (!upstreamExhausted) {
            long tNext = System.currentTimeMillis();
            Page input = upstream.next();
            nextTotal += System.currentTimeMillis() - tNext;
            if (input.isEOF()) {
                upstreamExhausted = true;
                break;
            }
            totalRows += input.getRowCount();
            recordInput(input);

            long tAcc = System.currentTimeMillis();
            accumulatePage(input);
            accTotal += System.currentTimeMillis() - tAcc;

            long tClose = System.currentTimeMillis();
            input.close();
            releaseInput(input);
            closeTotal += System.currentTimeMillis() - tClose;
            pageCount++;
        }
        long phase1Elapsed = System.currentTimeMillis() - phase1Start;
        System.err.printf("[Sort] Phase1 upstream.next=%dms accumulate=%dms close=%dms total=%dms pages=%d rows=%d%n",
                nextTotal, accTotal, closeTotal, phase1Elapsed, pageCount, totalRows);

        // Phase 2: 分区级并行处理（溢写残留、内存转 Arrow）
        long t2 = System.currentTimeMillis();
        System.err.println("[Sort] Flushing remaining data to Arrow files...");

        for (int pid = 0; pid < partitions.length; pid++) {
            if (partitions[pid] != null) partitions[pid].awaitPendingSpills();
        }

        int activeCount = 0;
        for (PartitionState<RowAccumulator> pa : partitions) {
            if (pa != null && pa.hasData()) activeCount++;
        }

        // ★ 失败必须上抛：任一分区刷盘/Copy 失败都会丢行，静默吞掉会产出错误结果
        AtomicReference<Throwable> failure = new AtomicReference<>();
        if (activeCount > 0) {
            List<Thread> threads = new ArrayList<>();
            for (int pid = 0; pid < partitions.length; pid++) {
                final int p = pid;
                PartitionState<RowAccumulator> pa = partitions[pid];
                if (pa == null || !pa.hasData()) continue;
                Thread t = new Thread(() -> {
                    try {
                        if (pa.dataFilePath != null) {
                            flushRemainingSlots(pa, p);
                            mergeAndOutput(pa, p);
                        } else {
                            outputMemory(pa, p);
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                        System.err.printf("[Sort] Partition %d failed: %s%n", p, e);
                    }
                });
                t.start();
                threads.add(t);
            }
            for (Thread t : threads) t.join();
        }
        rethrowIfFailed(failure, "Phase 2 flush");

        System.err.printf("[Sort] Flush done: %d ms%n", System.currentTimeMillis() - t2);

        // Phase 3: K 路归并 — 把 key 交错的 Arrow 文件归并成 key 连续文件
        long t3 = System.currentTimeMillis();
        System.err.println("[Sort] Compacting partition files (K-way merge)...");

        List<Thread> compactThreads = new ArrayList<>();
        for (int pid = 0; pid < partitions.length; pid++) {
            final int p = pid;
            PartitionState<RowAccumulator> pa = partitions[pid];
            if (pa == null || pa.batchFiles.size() <= 1) {
                if (pa != null && pa.batchFiles.size() <= 1) pa.compacted = true;
                continue;
            }
            Thread t = new Thread(() -> {
                try { compactPartition(p); }
                catch (Throwable e) {
                    // ★ 不能吞：compacted 保持 false 会退化到 indexBasedMergeJoin，
                    //   而归并中途已删除源文件、索引也不再维护 ⇒ 必然丢行/空结果。
                    failure.compareAndSet(null, e);
                    System.err.printf("[Sort] Compact partition %d failed: %s%n", p, e);
                }
            });
            t.start();
            compactThreads.add(t);
        }
        for (Thread t : compactThreads) t.join();
        rethrowIfFailed(failure, "Phase 3 compaction");

        System.err.printf("[Sort] Compact done: %d ms%n", System.currentTimeMillis() - t3);
    }

    /** 并行阶段失败上抛：Error 原样抛，受检异常包装为 RuntimeException（pipeAll 调用方只处理 Exception 语义）。 */
    private static void rethrowIfFailed(AtomicReference<Throwable> failure, String phase) {
        Throwable t = failure.get();
        if (t == null) return;
        if (t instanceof Error e) throw e;
        if (t instanceof RuntimeException re) throw re;
        throw new RuntimeException("[Sort] " + phase + " failed", t);
    }

    // ==================== Phase 3: K 路归并 ====================

    private static final int MAX_MERGE_K = 16;

    /**
     * 对指定分区做 K 路归并，把 key 交错的 batch 文件归并为 key 连续文件。
     *
     * <p><b>扇入自适应</b>：上界 {@link #MAX_MERGE_K}，但真正的闸门是<b>实测堆外占用</b> —— 每装载一份源
     * 就查一次 {@code allocator.getAllocatedMemory()}，逼近 {@link #mergeInputCap()} 即停。★不能用
     * 「按 footer 估字节」：Arrow 每次分配**向上取到 2 的幂**（实测 2.2MB 估算 → 实分 4MB），估算必偏小 ⇒
     * 16 路固定扇入必 OOM。多轮归并直到分区只剩一个文件。</p>
     */
    private void compactPartition(int pid) {
        PartitionState<RowAccumulator> pa = partitions[pid];
        if (pa == null) return;

        List<Path> sources = new ArrayList<>(pa.batchFiles);
        if (sources.size() <= 1) {
            pa.compacted = true;  // ≤1 个文件天然 key 连续，标记已归并
            pa.index.clear();
            return;
        }

        int initialFiles = sources.size();
        long inputCap = mergeInputCap();
        int round = 0;
        while (sources.size() > 1) {
            List<Path> next = new ArrayList<>();
            int i = 0;
            while (i < sources.size()) {
                int[] consumedOut = new int[1];
                next.addAll(mergeOneRound(sources, i, pid, round, inputCap, consumedOut));
                i += Math.max(1, consumedOut[0]);
            }
            for (Path old : sources) {
                if (!next.contains(old)) {
                    try { java.nio.file.Files.deleteIfExists(old); } catch (Exception ignored) {}
                }
            }
            sources = next;
            round++;
        }

        pa.batchFiles.clear();
        pa.batchFiles.addAll(sources);
        pa.index.clear();
        pa.compacted = true;
        System.err.printf("[Sort] Compact partition %d: %d -> %d files / %d rounds%n",
                pid, initialFiles, sources.size(), round);
    }

    /** 单次归并的堆外输入上限：分配器上限按并发分区数均摊后取一半（给输出缓冲留余量）。 */
    private long mergeInputCap() {
        int concurrent = 0;
        for (PartitionState<RowAccumulator> p : partitions) if (p != null) concurrent++;
        long perMerge = allocator.getLimit() / Math.max(1, concurrent);
        return Math.max(1, perMerge / 2);
    }

    /**
     * 单轮 K 路归并：自 {@code sources[start]} 起按内存闸门取若干源，归并为<b>一个 key 连续文件</b>。
     *
     * <p>★<b>一个 key 组一个文件 + 多 record batch 追加</b>（文件名带 {@code _s<start>} 区分各趟）——
     * 若沿用「一个 batch 一个文件」，2 源归并仍产出 2 个文件、文件数不下降 ⇒ <b>死循环</b>；且各趟
     * 输出名重开序号会**互相覆盖**（曾致结果丢行/串行）。一文件一组则文件数**必然收敛**。</p>
     *
     * @param consumedOut 出参：本趟实际消费的源文件数（调用方据此推进游标）
     */
    private List<Path> mergeOneRound(List<Path> sources, int start, int pid, int round,
                                     long inputCap, int[] consumedOut) {
        List<Path> outputFiles = new ArrayList<>();
        int n = sources.size();
        ArrowFileReader[] readers = new ArrowFileReader[n];
        FileChannel[] channels = new FileChannel[n];
        @SuppressWarnings("unchecked")
        List<ArrowBlock>[] blockLists = new List[n];
        VectorSchemaRoot[] roots = new VectorSchemaRoot[n];
        int[] blockIdx = new int[n];
        int[] positions = new int[n];
        int[] rowCounts = new int[n];
        Path[] loadedPaths = new Path[n];
        int loaded = 0;
        int cursor = start;
        Path outFile = null;
        FileChannel outCh = null;
        ArrowFileWriter outWriter = null;
        VectorSchemaRoot outRoot = null;
        boolean wroteAny = false;
        try {
            // 装载源：至少 1 个；已有 ≥2 个且逼近堆外闸门就停
            while (cursor < n && loaded < MAX_MERGE_K) {
                if (loaded >= 2 && allocator.getAllocatedMemory() >= inputCap) break;
                Path srcPath = sources.get(cursor++);
                FileChannel ch = FileChannel.open(srcPath, StandardOpenOption.READ);
                ArrowFileReader r = new ArrowFileReader(ch, allocator);
                List<ArrowBlock> blocks = r.getRecordBlocks();
                if (blocks.isEmpty()) { r.close(); ch.close(); continue; }
                blockLists[loaded] = blocks;
                r.loadRecordBatch(blocks.get(0));
                readers[loaded] = r; channels[loaded] = ch;
                roots[loaded] = r.getVectorSchemaRoot();
                loadedPaths[loaded] = srcPath;
                rowCounts[loaded] = roots[loaded].getRowCount();
                blockIdx[loaded] = 1;
                loaded++;
            }
            consumedOut[0] = cursor - start;
            if (loaded <= 1) {
                return loaded == 1 ? new ArrayList<>(List.of(loadedPaths[0])) : outputFiles;
            }

            final int sCount = loaded;
            PriorityQueue<Integer> heap = new PriorityQueue<>((a, b) ->
                    compareKey(keyAt(roots[a], positions[a]), keyAt(roots[b], positions[b])));
            for (int i = 0; i < sCount; i++) {
                if (positions[i] < rowCounts[i]
                        || advanceBlock(i, readers, blockLists, blockIdx, roots, positions, rowCounts)) {
                    heap.offer(i);
                }
            }

            Schema schema = inputSchema;
            List<FieldVector> outVectors = null;
            int outRowCount = 0, outCapacity = 0;
            Object lastKey = null;
            while (!heap.isEmpty()) {
                int si = heap.poll();
                VectorSchemaRoot src = roots[si];
                int row = positions[si];

                if (outVectors == null) {
                    outVectors = createOutputVectors(schema, OUTPUT_BATCH_SIZE);
                    // ★ 闸门必须是「所有列的最小容量」：Arrow 按字节取整后各列换算回的行数不同
                    //   （TINYINT 一次 reAlloc 只有 116504 行，INT 有 127100 行）。用首列会在窄列上越界。
                    outCapacity = minValueCapacity(outVectors);
                    outRoot = new VectorSchemaRoot(new ArrayList<>(outVectors));
                    outFile = spillDirForWrite().resolve(
                            "p" + pid + "_c" + round + "_s" + start + ".arrow");
                    outCh = FileChannel.open(outFile, StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                    outWriter = new ArrowFileWriter(outRoot, null, outCh);
                }

                Object curKey = keyAt(src, row);
                // 缓冲满且 key 换组 → 落一个 batch（保证 key 组不跨 batch，便于下游顺序扫描）
                if (outRowCount >= OUTPUT_BATCH_SIZE && lastKey != null && !keysEqual(lastKey, curKey)) {
                    outRoot.setRowCount(outRowCount);
                    outWriter.writeBatch();
                    wroteAny = true;
                    for (FieldVector v : outVectors) v.setValueCount(0);
                    outRowCount = 0;
                }
                // 扩容（同一 key 组超过 batch 大小时）
                if (outRowCount >= outCapacity) {
                    for (FieldVector v : outVectors) v.reAlloc();
                    outCapacity = minValueCapacity(outVectors);
                }

                copyRow(src, row, outVectors, outRowCount);
                lastKey = curKey;
                outRowCount++;

                positions[si]++;
                if (positions[si] >= rowCounts[si]) {
                    if (advanceBlock(si, readers, blockLists, blockIdx, roots, positions, rowCounts)) {
                        heap.offer(si);
                    }
                } else {
                    heap.offer(si);
                }
            }
            if (outWriter != null && outRowCount > 0) {
                outRoot.setRowCount(outRowCount);
                outWriter.writeBatch();
                wroteAny = true;
            }
            if (wroteAny) outputFiles.add(outFile);
            return outputFiles;
        } catch (IOException e) {
            throw new RuntimeException("Compact partition " + pid + " round " + round + " failed", e);
        } finally {
            if (outWriter != null) { try { outWriter.close(); } catch (Exception ignored) {} }
            if (outCh != null) { try { outCh.close(); } catch (Exception ignored) {} }
            if (outRoot != null) { try { outRoot.close(); } catch (Exception ignored) {} }
            if (wroteAny && outFile != null) {
                // 归并重写也是真实溢写 I/O：文件已关闭，按实际大小计入（否则监控明显偏低）。
                accountCompactionFile(partitions[pid], outFile);
            } else if (outFile != null) {
                try { java.nio.file.Files.deleteIfExists(outFile); } catch (Exception ignored) {}
            }
            for (int i = 0; i < loaded; i++) {
                if (roots[i] != null) { try { roots[i].close(); } catch (Exception ignored) {} }
                if (readers[i] != null) { try { readers[i].close(); } catch (Exception ignored) {} }
                if (channels[i] != null) { try { channels[i].close(); } catch (Exception ignored) {} }
            }
        }
    }

    /** 读源的下一个 record block（多 batch 文件必需，否则丢行）；无后续返回 false。 */
    private boolean advanceBlock(int si, ArrowFileReader[] readers, List<ArrowBlock>[] blockLists,
                                 int[] blockIdx, VectorSchemaRoot[] roots, int[] positions, int[] rowCounts) {
        List<ArrowBlock> blocks = blockLists[si];
        while (blockIdx[si] < blocks.size()) {
            int rc;
            try {
                readers[si].loadRecordBatch(blocks.get(blockIdx[si]++));
                roots[si] = readers[si].getVectorSchemaRoot();
                rc = roots[si].getRowCount();
            } catch (IOException e) {
                throw new RuntimeException("Compact read next batch failed", e);
            }
            positions[si] = 0;
            rowCounts[si] = rc;
            if (rc > 0) return true;
        }
        return false;
    }

    private List<FieldVector> createOutputVectors(Schema schema, int capacity) {
        List<FieldVector> vecs = new ArrayList<>();
        for (Field f : schema.getFields()) {
            FieldVector v = f.createVector(allocator);
            v.setInitialCapacity(capacity);
            v.allocateNew();
            vecs.add(v);
        }
        return vecs;
    }

    /**
     * 拷贝一行（归并输出用）。定长列走 memcpy 快路径 —— <b>必须自己管 validity</b>：Arrow 新缓冲是
     * 零值（= 全 NULL），只拷数据不置位，读回时会报 {@code Value at index is null} / 值全变 NULL。
     * 有效则 {@link BitVectorHelper#setBit}，NULL 则 {@code setNull}。
     */
    private void copyRow(VectorSchemaRoot src, int srcRow, List<FieldVector> dst, int dstRow) {
        List<FieldVector> srcVecs = src.getFieldVectors();
        for (int c = 0; c < srcVecs.size(); c++) {
            FieldVector sv = srcVecs.get(c);
            FieldVector dv = dst.get(c);
            int tw = RowAccumulator.typeWidthOf(sv.getField().getType());
            if (tw > 0) {
                dv.getDataBuffer().setBytes((long) dstRow * tw,
                        sv.getDataBuffer(), (long) srcRow * tw, tw);
                if (sv.isNull(srcRow)) {
                    dv.setNull(dstRow);
                } else {
                    ArrowBuf val = dv.getValidityBuffer();
                    if (val != null) BitVectorHelper.setBit(val, dstRow);
                }
            } else {
                sv.makeTransferPair(dv).copyValueSafe(srcRow, dstRow);
            }
        }
    }

    private Object keyAt(VectorSchemaRoot root, int row) {
        return buildKeyFromSpill(root, row);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareKey(Object a, Object b) {
        if (a == RowAccumulator.NULL_KEY && b == RowAccumulator.NULL_KEY) return 0;
        if (a == RowAccumulator.NULL_KEY) return -1;
        if (b == RowAccumulator.NULL_KEY) return 1;
        if (a instanceof Comparable ca && b instanceof Comparable) return ca.compareTo(b);
        if (a instanceof WindowSortOperator.CompositeKey cka
                && b instanceof WindowSortOperator.CompositeKey ckb)
            return compareCompositeKeys(cka, ckb);
        return String.valueOf(a).compareTo(String.valueOf(b));
    }

    // ==================== 索引 ====================

    @Override
    protected Object buildKeyFromSpill(VectorSchemaRoot sorted, int row) {
        if (isCompositeKey) {
            Object[] vals = new Object[keyIndices.length];
            for (int i = 0; i < keyIndices.length; i++) {
                FieldVector vec = sorted.getVector(keyIndices[i]);
                vals[i] = RowAccumulator.readKeyValue(vec, row);
            }
            return new WindowSortOperator.CompositeKey(vals);
        }
        FieldVector vec = sorted.getVector(keyIndices[0]);
        return RowAccumulator.readKeyValue(vec, row);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void sortKeys(List<Object> keys) {
        keys.sort((a, b) -> {
            if (a == RowAccumulator.NULL_KEY && b == RowAccumulator.NULL_KEY) return 0;
            if (a == RowAccumulator.NULL_KEY) return -1;
            if (b == RowAccumulator.NULL_KEY) return 1;
            if (a instanceof Comparable ca && b instanceof Comparable) return ca.compareTo(b);
            if (a instanceof WindowSortOperator.CompositeKey cka
                    && b instanceof WindowSortOperator.CompositeKey ckb)
                return compareCompositeKeys(cka, ckb);
            return String.valueOf(a).compareTo(String.valueOf(b));
        });
    }

    // ==================== 工具 ====================

    private void resolveKeyIndices() {
        keyIndices = new int[joinKeyColumns.size()];
        for (int i = 0; i < joinKeyColumns.size(); i++) {
            for (int j = 0; j < inputSchema.getFields().size(); j++) {
                if (inputSchema.getFields().get(j).getName().equals(joinKeyColumns.get(i))) {
                    keyIndices[i] = j;
                    break;
                }
            }
        }
        isCompositeKey = keyIndices.length > 1;
    }

    private static int compareCompositeKeys(WindowSortOperator.CompositeKey a,
                                             WindowSortOperator.CompositeKey b) {
        int len = Math.min(a.values().length, b.values().length);
        for (int i = 0; i < len; i++) {
            Object va = a.values()[i], vb = b.values()[i];
            boolean na = isNullKey(va), nb = isNullKey(vb);
            if (na && nb) continue;
            if (na) return -1;
            if (nb) return 1;
            if (va instanceof Comparable<?> ca && vb instanceof Comparable<?>) {
                @SuppressWarnings({"rawtypes", "unchecked"})
                int cmp = ((Comparable) ca).compareTo(vb);
                if (cmp != 0) return cmp;
            } else if (va instanceof List<?> la && vb instanceof List<?> lb) {
                int cmp = ArrowComparators.compareLists(la, lb);
                if (cmp != 0) return cmp;
            }
        }
        return a.values().length - b.values().length;
    }

    private static boolean isNullKey(Object v) {
        return v == null || v == RowAccumulator.NULL_KEY;
    }

    // ==================== 对外 API（供 MergeJoin 读取） ====================

    /** 把 inputSchema 同步到基类字段（MergeJoin 需要 schema 来构建输出向量） */
    Schema getSchema() {
        if (inputSchema != null) return inputSchema;
        // 兜底：从第一个 Arrow 文件读嵌入的 schema
        for (PartitionState<RowAccumulator> pa : partitions) {
            if (pa != null && !pa.batchFiles.isEmpty()) {
                try {
                    java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(
                            pa.batchFiles.get(0), java.nio.file.StandardOpenOption.READ);
                    org.apache.arrow.vector.ipc.ArrowFileReader rdr =
                            new org.apache.arrow.vector.ipc.ArrowFileReader(ch, allocator);
                    Schema s = rdr.getVectorSchemaRoot().getSchema();
                    rdr.close(); ch.close();
                    return s;
                } catch (Exception ignored) {}
            }
        }
        return null;
    }

    int getPartitionCount() { return partitions.length; }

    /**
     * 返回指定分区的排序数据。如果没有溢写（dataFilePath == null），
     * 将内存 slot 写入 Arrow 文件后再返回。
     */
    PartitionState<RowAccumulator> getPartitionedData(int pid) {
        return partitions[pid];
    }
}
