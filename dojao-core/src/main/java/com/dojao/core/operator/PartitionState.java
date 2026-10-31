package com.dojao.core.operator;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单个分区的状态：3 个槽位、Arrow 文件、内存索引。
 *
 * @param <S> 槽位类型
 * @author jiaxin
 * @since 2.0
 */
public class PartitionState<S extends Slot> {
    private final int slotCount;
    final List<S> slots;
    volatile int activeIdx;

    Path dataFilePath;   // 首个溢写文件路径（null = 无溢写）

    /**
     * 溢写批次计数与批次文件表 —— <b>由并发的溢写线程写</b>（3 槽位流水线允许最多 2 个溢写任务同时在跑）：
     * 普通 {@code int++} 会丢更新 ⇒ 两个线程分到同一个 batchIndex ⇒ 文件互相覆盖、索引错位
     * ⇒ Phase 2 {@code batchFiles.get(batchIndex)} 越界 + <b>静默丢行</b>（实测 200 行只剩 190/195，
     * 每次还不一样）。故计数用原子、文件表用写时复制（读侧全是按序 get/迭代）。
     */
    final AtomicInteger batchCount = new AtomicInteger(0);
    final List<Path> batchFiles = new CopyOnWriteArrayList<>();
    long dataFileSize;

    /**
     * {@code count_distinct} 的「逐值」溢写文件，按 distinct ordinal 分列：每个文件是一条
     * 按 {@code (key, value)} 有序的顺串（{@code [key..., value]}），与 {@link #batchFiles}
     * （标量流，每组一行）并行存在。Phase 2 先归并成单条有序流，再与标量流统一 K 路归并。
     *
     * <p>外层用 COW，内层也是 COW：3 槽位流水线允许两个溢写线程并发追加。</p>
     */
    final List<List<Path>> distinctFiles = new CopyOnWriteArrayList<>();

    /** 取（必要时创建）第 {@code ordinal} 条去重流的文件表。 */
    List<Path> distinctFilesFor(int ordinal) {
        synchronized (distinctFiles) {
            while (distinctFiles.size() <= ordinal) {
                distinctFiles.add(new CopyOnWriteArrayList<>());
            }
        }
        return distinctFiles.get(ordinal);
    }

    public final Map<Object, List<Segment>> index = new ConcurrentHashMap<>();
    boolean compacted;  // 归并后为 true，表示 batchFiles 已 key 连续

    /** 纯内存路径：未溢写时存储排好序的 VectorSchemaRoot，P2 直接使用，不写盘 */
    VectorSchemaRoot inMemoryRoot;

    /** 零拷贝索引模式：逻辑排序位置 → accumulator 物理行号（单 slot 无溢写，不拷贝数据） */
    int[] sortedIndices;

    /** 零拷贝索引模式：索引对应的 accumulator 引用，由 PartitionState.close() 统一清理 */
    RowAccumulator indexSource;

    final AtomicInteger pendingSpills = new AtomicInteger(0);
    final ReentrantLock slotLock = new ReentrantLock();
    final Condition slotAvailable = slotLock.newCondition();
    final Set<Integer> spillingSlots = ConcurrentHashMap.newKeySet();

    /**
     * 首个 Phase 1 异步溢写失败的异常（如磁盘写满 / {@code SpillSpaceException}）。
     *
     * <p>为什么必须存下来：溢写跑在 {@link #submitSpill} 提交的任务里，其 {@code Future} 无人读取 ⇒
     * 抛出的异常会被<b>静默吞掉</b>，而该槽位的数据已被 drop —— 结果是查询「成功」但少数据。
     * 记在这里，由消费线程在 {@link #checkSpillFailure()} 处原样上抛，让查询明确失败。</p>
     */
    volatile Throwable spillFailure;

    public PartitionState(int slotCount) {
        this.slotCount = slotCount;
        this.slots = new ArrayList<>(Collections.nCopies(slotCount, null));
    }

    public S activeSlot() { return slots.get(activeIdx); }

    public boolean hasData() {
        for (S slot : slots) if (slot != null && !slot.isEmpty()) return true;
        return !batchFiles.isEmpty();
    }

    void submitSpill(ExecutorService executor, AbstractSpillableOperator<S> owner, int pid) {
        slotLock.lock();
        try {
            int spillIdx = activeIdx;
            S slot = slots.get(spillIdx);
            if (slot == null || slot.isEmpty()) return;
            pendingSpills.incrementAndGet();
            spillingSlots.add(spillIdx);
            switchActiveSlot();
            executor.submit(() -> {
                try {
                    owner.spillSlotAsync(this, spillIdx, slot, pid);
                } catch (Throwable t) {
                    // 必须在 decrementAndGet 之前记下：awaitPendingSpills() 见到 pendingSpills==0
                    // 即认为本轮溢写结束，此刻 spillFailure 必须已可见（volatile）。
                    if (spillFailure == null) spillFailure = t;
                } finally {
                    spillingSlots.remove(spillIdx);
                    pendingSpills.decrementAndGet();
                    slotLock.lock();
                    try { slotAvailable.signalAll(); } finally { slotLock.unlock(); }
                }
            });
        } finally {
            slotLock.unlock();
        }
    }

    /**
     * 有 Phase 1 溢写失败就原样抛出去（消费线程调用）。
     *
     * <p>调用点两处：① 触发溢写后的 {@code awaitIdleSlot()} 之后（尽早失败）；
     * ② Phase 2 开始处 {@code awaitPendingSpills()} 之后 —— Phase 2 是唯一的产出源头，
     * 此处检查保证「一行都不会吐出去才失败」，绝不会出现半份结果。</p>
     */
    void checkSpillFailure() {
        Throwable t = spillFailure;
        if (t == null) {
            return;
        }
        if (t instanceof RuntimeException re) throw re;
        if (t instanceof Error e) throw e;
        throw new RuntimeException("溢写失败：" + t.getMessage(), t);
    }

    void awaitIdleSlot() {
        slotLock.lock();
        try {
            while (spillingSlots.size() >= slotCount - 1) {
                try { slotAvailable.await(10, TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }
            while (spillingSlots.contains(activeIdx)) {
                activeIdx = (activeIdx + 1) % slotCount;
            }
        } finally {
            slotLock.unlock();
        }
    }

    void awaitPendingSpills() {
        while (pendingSpills.get() > 0) {
            try { Thread.sleep(10); } catch (InterruptedException e) { break; }
        }
    }

    private void switchActiveSlot() {
        int tried = 0;
        do { activeIdx = (activeIdx + 1) % slotCount; tried++; }
        while (spillingSlots.contains(activeIdx) && tried < slotCount);
    }

    void close() {
        for (int i = 0; i < slotCount; i++) {
            S slot = slots.get(i);
            if (slot != null) { slot.close(); slots.set(i, null); }
        }
        if (inMemoryRoot != null) {
            inMemoryRoot.close();
            inMemoryRoot = null;
        }
        for (Path f : batchFiles) {
            try { Files.deleteIfExists(f); } catch (IOException ignored) {}
        }
        batchFiles.clear();
        for (List<Path> files : distinctFiles) {
            for (Path f : files) {
                try { Files.deleteIfExists(f); } catch (IOException ignored) {}
            }
            files.clear();
        }
        distinctFiles.clear();
        index.clear();
    }
}
