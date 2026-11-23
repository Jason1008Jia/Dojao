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
 * 单个分区的状态：若干卡槽（默认 3；单槽预算过小时算子会降为 2，见
 * {@code AbstractSpillableOperator#selectSlotCount()}）、Arrow 文件、内存索引。
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

    /**
     * {@code collect_list} / {@code collect_set} 的「逐值」溢写文件，按 collect ordinal 分列：
     * 每个文件是按键有序的顺串（{@code [key..., value]}，一个值一行），与 {@link #batchFiles}
     * （标量流，每组一行且不含 collect 状态）并行存在。
     *
     * <p><b>为什么不再「一格 = 整组」</b>：整组塞进一格 Arrow List 时，单次溢写的编解码 +
     * 排序复制 + reAlloc 都是 O(单组值数) 且峰值 = 整组集合；低基数分组 + 几百万值时会把这
     * 一步拖到几分钟（甚至看起来像卡死）。扁平逐值格式让溢写恒定 O(槽内行数)、峰值与单组无关
     * —— 与 {@link #distinctFiles} 同一思路。</p>
     */
    final List<List<Path>> collectFiles = new CopyOnWriteArrayList<>();

    /** 取（必要时创建）第 {@code ordinal} 条 collect 值流的文件表。 */
    List<Path> collectFilesFor(int ordinal) {
        synchronized (collectFiles) {
            while (collectFiles.size() <= ordinal) {
                collectFiles.add(new CopyOnWriteArrayList<>());
            }
        }
        return collectFiles.get(ordinal);
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

    /**
     * 算子已关闭 / 查询被取消。
     *
     * <p>关闭时由 {@code AbstractSpillableOperator.doClose()} 置位：等待溢写的消费线程必须<b>立刻收手</b>，
     * 绝不能像旧实现那样把 {@link InterruptedException} 当成「等完了」继续起 Phase 2 ——
     * 溢写线程还在跑、状态又不完整，结果是取消后整轮归并照烧 CPU、内存也不回落。</p>
     */
    volatile boolean cancelled;

    void markCancelled() { cancelled = true; }

    public PartitionState(int slotCount) {
        this.slotCount = slotCount;
        this.slots = new ArrayList<>(Collections.nCopies(slotCount, null));
    }

    public S activeSlot() { return slots.get(activeIdx); }

    public boolean hasData() {
        for (S slot : slots) if (slot != null && !slot.isEmpty()) return true;
        return !batchFiles.isEmpty() || !collectFiles.isEmpty();
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
                } catch (CancellationException ce) {
                    // 取消 ≠ 失败：不记 spillFailure（否则会变成一条「溢写失败」的假告警）。
                    // 槽位本身由 spillSlotSync 的 finally 关闭。
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
            // 等待条件 = 全部卡槽都在写（而不是「只剩一个空闲」）：
            // 3 槽 ⇒ 最多 2 写 + 1 收（设计峰值）；2 槽 ⇒ 1 写 + 1 收。
            // 若沿用 >= slotCount - 1，2 槽会在每次提交后立刻等待刚提交的那次溢写 ——
            // 「异步」退化成逐槽同步；而拉取线程是跨分区共享的（窗口算子 5 个分区一个线程），
            // 单个分区的同步化会把整条流水线一起停摆。
            while (spillingSlots.size() >= slotCount) {
                if (cancelled) throw new CancellationException("溢写等待槽位时被取消");
                try { slotAvailable.await(1, TimeUnit.SECONDS); }
                catch (InterruptedException e) {
                    // 取消必须原样抛出：吞掉中断会让消费线程带着不完整状态继续累积 / 起 Phase 2
                    Thread.currentThread().interrupt();
                    throw new CancellationException("溢写等待槽位时被中断");
                }
            }
            while (spillingSlots.contains(activeIdx)) {
                activeIdx = (activeIdx + 1) % slotCount;
            }
        } finally {
            slotLock.unlock();
        }
    }

    /**
     * 等本分区全部 Phase 1 异步溢写结束（Phase 2 开始前的门）。
     *
     * <p>溢写跑在虚拟线程上（{@code jstack} 看不见栈），卡住时外部无从判断卡在哪 —— 不能默认静默。
     * 超过 30s 就每 30s 打一行心跳：pendingSpills / 进行中槽位 / 已落盘批次 / 已等待秒数，
     * 让「Phase 2 长时间无输出」能一眼区分是「还在等溢写」还是「归并本身慢」。</p>
     *
     * <p><b>取消必须抛出，不能 break</b>：旧实现在 {@link InterruptedException} 时直接 {@code break}
     * 走出循环（中断标记还被 sleep 清掉了），于是「用户杀任务」被当成「溢写等完了」→ 消费线程照常
     * 进入 Phase 2，对一份不完整的数据做整轮归并；溢写虚拟线程 meanwhile 还在跑 ⇒ 任务已停
     * （WebUI Running=0）但 Worker CPU 一直 100%。</p>
     */
    void awaitPendingSpills(int pid) {
        long startNanos = System.nanoTime();
        long lastReportNanos = startNanos;
        while (pendingSpills.get() > 0) {
            if (cancelled) {
                throw new CancellationException("分区 " + pid + " 等待溢写收尾时被取消");
            }
            try { Thread.sleep(10); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CancellationException("分区 " + pid + " 等待溢写收尾时被中断");
            }
            long now = System.nanoTime();
            if (now - lastReportNanos >= 30_000_000_000L) {
                lastReportNanos = now;
                System.err.printf("[溢写] 分区 %d 仍在等 Phase 1 溢写收尾：pendingSpills=%d，"
                                + "进行中槽位=%s，已落盘批次=%d，已等待 %d 秒%n",
                        pid, pendingSpills.get(), spillingSlots, batchFiles.size(),
                        (now - startNanos) / 1_000_000_000L);
            }
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
        for (List<Path> files : collectFiles) {
            for (Path f : files) {
                try { Files.deleteIfExists(f); } catch (IOException ignored) {}
            }
            files.clear();
        }
        collectFiles.clear();
        index.clear();
    }
}
