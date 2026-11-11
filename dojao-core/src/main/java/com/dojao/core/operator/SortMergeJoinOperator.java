package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Sort-Merge Join 算子。
 *
 * <h3>执行流程</h3>
 * <pre>
 *   Phase 1: 左/右 JoinSortOperator 独立排序（三槽溢写 → Arrow 文件 + 内存索引）
 *   Phase 2: 分区级双指针归并
 *            ├── 每分区一个线程
 *            ├── 1000 keys / batch
 *            ├── 加载涉及的 Arrow 文件（每文件只开一次）
 *            ├── 同 key 行提取 → cross join → 输出
 *            └── 输出进「有界」outputQueue，doNext() 并发排空（背压）
 * </pre>
 *
 * <h3>内存口径（"内存满了就溢写"）</h3>
 * <p>两侧的排序阶段各自走 {@link AbstractSpillableOperator} 的三槽溢写（超阈值即落 Arrow 文件），
 * 因此<b>单侧数据量不受内存限制</b>；归并阶段输出<b>边产边消费</b>（有界队列 + 背压），
 * 峰值内存与 join 结果总行数无关。这正是 {@link HashJoinOperator} 做不到的 —— 它的 build 侧
 * 必须整份装进「每分区内存预算」，超了就报错。</p>
 *
 * <h3>支持 Join 类型</h3>
 * <ul>
 *   <li>INNER：只输出匹配 key</li>
 *   <li>LEFT：左表全部保留，右表无匹配填 NULL</li>
 *   <li>RIGHT：右表全部保留，左表无匹配填 NULL</li>
 *   <li>FULL：两侧全部保留，无匹配侧填 NULL</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.2
 * @date 2026/7/23
 */
public class SortMergeJoinOperator extends AbstractMeasurableOperator implements MergeProgress, Explainable {

    /** EXPLAIN 明细：算法（shuffle / broadcast / sort_merge）+ 是否可溢写 + 连接类型 / 连接键 / 分区数。 */
    @Override
    public List<String> explainLines() {
        return List.of("algorithm: " + JoinExplain.localSortMerge(memoryLimit, numPartitions)
                + " type=" + joinType
                + " keys=" + leftKeyColumns + "=" + rightKeyColumns + " partitions=" + numPartitions);
    }

    private static final int OUTPUT_BATCH_SIZE = 50000;
    private static final int KEYS_PER_BATCH = 5000;

    /**
     * 输出队列容量（页）。Phase 2 各分区线程是<b>生产者</b>、{@code doNext()} 是<b>消费者</b>，
     * 队列有界 → 生产者被消费者拖住（背压），归并结果<b>流过</b>本算子而不在本算子内积攒。
     * 旧实现是 {@code ConcurrentLinkedQueue} 无界 + 「全部分区归并完才吐第一页」，
     * 大表 join 的输出（可上亿行）会整份停在堆里 → OOM。
     */
    private static final int OUTPUT_QUEUE_CAPACITY = 8;

    private final Operator leftUpstream;
    private final Operator rightUpstream;
    private JoinSortOperator leftSort;
    private JoinSortOperator rightSort;
    private final List<String> leftKeyColumns;
    private final List<String> rightKeyColumns;
    private final JoinType joinType;
    private final long memoryLimit;
    private final BufferAllocator allocator;

    /**
     * 右（build）侧<b>声明列</b>（Master 规划期下发的 {@code JoinStep.declaredRightFields}）。
     *
     * <p><b>为什么 SORT_MERGE 也需要它</b>：右表在某个分区<b>一页都没收到</b>时（空表 / 谓词全滤掉），
     * {@link JoinSortOperator#getSchema()} 返回 {@code null}，而 LEFT/FULL 归并要输出「左行 + 右列全
     * NULL」——没有右列类型就<b>造不出输出 schema</b>（{@code initFromLeft} NPE）。此时退回声明列。
     * 见 {@code ArrowSchemaIpc}。（修复前：这类分区落到 index 分支、静默吐 0 行 —— 实测
     * {@code a LEFT JOIN b}、b 侧 0 页 ⇒ 结果集整个是空的。）</p>
     */
    private final List<Field> declaredRightFields;

    /**
     * 左（probe）侧<b>声明列</b>（{@code JoinStep.declaredLeftFields}），与右表声明列对称：
     * RIGHT / FULL join 的左表某分区一页未收时，「右行 + 左列全 NULL」的孤儿行需要左列类型。
     * 缺失时旧行为是<b>响亮报错</b>（{@link #requireSchema}），绝不静默少行。
     */
    private final List<Field> declaredLeftFields;

    private Schema leftSchema;
    private Schema rightSchema;
    private List<Field> outputFields;

    // ---- 两阶段流水线状态 ----
    private volatile boolean phase1Done;
    private volatile boolean phase2Started;
    /**
     * Phase 1 / Phase 2 初始化锁。<b>本地计划里多个 HashPartition 分发线程会并发 next()</b>，
     * 两个线程同时看到 {@code phase2Started == false} 会各自起一轮 Phase 2（覆盖池、重置
     * {@code phase2Remaining}）→ 计数永远归不了零、消费者永久等待。初始化必须只做一次。
     */
    private final Object phaseInitLock = new Object();
    private ExecutorService phase2Pool;
    private final List<Future<?>> phase2Futures = new ArrayList<>();
    /**
     * 尚未归并完成的分区数（0 且输出队列空 = 真 EOF）。
     *
     * <p><b>必须是原子计数</b>：同一 Worker 常own 多个活跃分区，各分区归并线程在各自 finally
     * 里递减。旧的 {@code volatile int} + {@code phase2Remaining--} 是读-改-写三步骤，两个线程
     * 同时收尾会丢失一次递减 → 计数永远停在 1，{@code doNext()} 永久等待（Small data 下两个分区
     * 近乎同时完成，几乎必现）。</p>
     */
    private final java.util.concurrent.atomic.AtomicInteger phase2Remaining =
            new java.util.concurrent.atomic.AtomicInteger();
    /** 首个分区归并失败（fail-fast；旧实现把它印到 stderr 后当成正常结束 → 静默少行）。 */
    private final AtomicReference<Throwable> phase2Failure = new AtomicReference<>();
    private volatile boolean closed;
    private final BlockingQueue<Page> outputQueue = new ArrayBlockingQueue<>(OUTPUT_QUEUE_CAPACITY);
    private final int numPartitions;

    /**
     * 分区级<b>输出</b>计数 / 完成标记（遥测用）。
     *
     * <p>WebUI 上 SortMergeJoin 片段只有 1 个 task，且 Phase 1/2 期间全局 stats 增长缓慢 ——
     * 用户只能看到「Join 框一直是 0」。这里按分区记行数 / 字节，Phase 2 分区线程退出时置 done，
     * 由 {@code FragmentExecutor.trackJoinPartitions} 注册成 {@code <fragmentId>#p<k>} 子任务，
     * 使排序归并 join 与哈希 join 一样逐分区可见。</p>
     *
     * <p>输入侧计数在两侧 {@link JoinSortOperator} 里（Phase 1 按页分区号累加），见
     * {@link #partitionRowsIn(int)} / {@link #partitionBytesIn(int)}。</p>
     */
    private final AtomicLongArray partitionRowsOut;
    private final AtomicLongArray partitionBytesOut;
    private final AtomicIntegerArray partitionDone;

    // ==================== 构造函数 ====================

    public SortMergeJoinOperator(Operator leftUpstream,
                                  Operator rightUpstream,
                                  List<String> leftKeyColumns,
                                  List<String> rightKeyColumns,
                                  JoinType joinType,
                                  long memoryLimit,
                                  int numPartitions) {
        this(leftUpstream, rightUpstream, leftKeyColumns, rightKeyColumns, joinType, memoryLimit,
                numPartitions, null);
    }

    /**
     * 带<b>右表声明列</b>的构造（分布式路径）：右表某分区<b>一页都没收到</b>时用它兜底输出 schema。
     * 为空 = 不声明（单机路径 / 调用方拿不到连接器元数据），行为与旧版逐字节一致。
     */
    public SortMergeJoinOperator(Operator leftUpstream,
                                  Operator rightUpstream,
                                  List<String> leftKeyColumns,
                                  List<String> rightKeyColumns,
                                  JoinType joinType,
                                  long memoryLimit,
                                  int numPartitions,
                                  List<Field> declaredRightFields) {
        this(leftUpstream, rightUpstream, leftKeyColumns, rightKeyColumns, joinType, memoryLimit,
                numPartitions, declaredRightFields, null);
    }

    /**
     * @param declaredRightFields 右（build）侧声明列（空 build 分区输出「左行 + 右列全 NULL」用）
     * @param declaredLeftFields  左（probe）侧声明列（空 probe 分区输出「右行 + 左列全 NULL」用，
     *                            RIGHT / FULL join 必需 —— 修复前该场景直接抛错）
     */
    public SortMergeJoinOperator(Operator leftUpstream,
                                  Operator rightUpstream,
                                  List<String> leftKeyColumns,
                                  List<String> rightKeyColumns,
                                  JoinType joinType,
                                  long memoryLimit,
                                  int numPartitions,
                                  List<Field> declaredRightFields,
                                  List<Field> declaredLeftFields) {
        super("SortMergeJoin", "SortMergeJoin", 2);
        this.leftUpstream = leftUpstream;
        this.rightUpstream = rightUpstream;
        this.leftKeyColumns = leftKeyColumns;
        this.rightKeyColumns = rightKeyColumns;
        this.joinType = joinType;
        this.memoryLimit = memoryLimit;
        this.numPartitions = numPartitions;
        this.partitionRowsOut = new AtomicLongArray(numPartitions);
        this.partitionBytesOut = new AtomicLongArray(numPartitions);
        this.partitionDone = new AtomicIntegerArray(numPartitions);
        this.declaredRightFields =
                (declaredRightFields == null || declaredRightFields.isEmpty()) ? null : declaredRightFields;
        this.declaredLeftFields =
                (declaredLeftFields == null || declaredLeftFields.isEmpty()) ? null : declaredLeftFields;
        // 分配器是进程级共享根（NodeMemory）的子 —— 归并/溢写缓冲的堆外占用进统一账本，受共享根硬上限约束。
        this.allocator = NodeMemory.shared()
                .newOperatorAllocator("sortmergejoin", ResourceConfig.queryDirectLimit());
    }

    public SortMergeJoinOperator(Operator leftUpstream,
                                  Operator rightUpstream,
                                  List<String> leftKeyColumns,
                                  List<String> rightKeyColumns,
                                  JoinType joinType,
                                  long memoryLimit) {
        this(leftUpstream, rightUpstream, leftKeyColumns, rightKeyColumns,
                joinType, memoryLimit, AbstractSpillableOperator.DEFAULT_PARTITIONS);
    }

    public SortMergeJoinOperator(Operator leftUpstream,
                                  Operator rightUpstream,
                                  String leftKeyColumn,
                                  String rightKeyColumn,
                                  JoinType joinType,
                                  long memoryLimit) {
        this(leftUpstream, rightUpstream,
                List.of(leftKeyColumn), List.of(rightKeyColumn), joinType, memoryLimit);
    }

    // ==================== 生命周期 ====================

    @Override
    protected void doOpen() throws Exception {
        this.leftSort = new JoinSortOperator(leftUpstream, leftKeyColumns, memoryLimit, numPartitions);
        this.rightSort = new JoinSortOperator(rightUpstream, rightKeyColumns, memoryLimit, numPartitions);
        // ★真正溢写的是两侧 JoinSortOperator：把溢写量转发到本算子统计。监控注册的是这个包装
        //   算子，不转发的话它的 spilledBytes 恒 0 —— 排序归并 join 无论落了多少 GB 都显示「无溢写」。
        leftSort.setSpillBytesListener(this::addSpilledBytes);
        rightSort.setSpillBytesListener(this::addSpilledBytes);
        leftSort.open();   // JoinSortOperator.doOpen() 会 auto-open upstream
        rightSort.open();
    }

    @Override
    protected Page doNext() throws Exception {
        if (!phase1Done || !phase2Started) {
            synchronized (phaseInitLock) {
                if (!phase1Done) {
                    runPhase1();
                    phase1Done = true;
                }
                if (!phase2Started) {
                    startPhase2();
                    phase2Started = true;
                }
            }
        }

        // 边归并边消费：Phase 2 分区线程把页 put 进有界队列（满则阻塞在本线程），这里同时排空。
        // 数据「流过」本算子 —— 峰值内存 = 队列容量 × 页大小，与 join 结果总行数无关。
        while (true) {
            Page p = outputQueue.poll(100, TimeUnit.MILLISECONDS);
            if (p != null) return p;
            if (closed) return Page.eof();
            Throwable f = phase2Failure.get();
            if (f != null) throw asRuntime(f);   // fail-fast：分区失败绝不静默当成功（曾少行）
            if (phase2Remaining.get() == 0) return Page.eof();   // 全部分区归并完 + 队列空 = 真 EOF
        }
    }

    @Override
    protected void doClose() throws Exception {
        // 先立旗：生产者的 offer 循环见到 closed 立即退出并释放页（否则会阻塞等着无人消费的队列）
        closed = true;
        if (phase2Pool != null) phase2Pool.shutdownNow();
        for (Future<?> f : phase2Futures) {
            try { f.cancel(true); } catch (Exception ignored) {}
        }
        Page p;
        while ((p = outputQueue.poll()) != null) {
            try { p.close(); } catch (Exception ignored) {}
        }
        if (leftSort != null) leftSort.close();
        if (rightSort != null) rightSort.close();
        leftUpstream.close();
        rightUpstream.close();
        allocator.close();
    }

    /**
     * 归并进度 = 两侧 {@link JoinSortOperator}（各自的溢写归并）之和。
     *
     * <p>排序归并 join 的重头戏是 Phase 1「两侧各自排序 + 溢写」—— 本算子在此期间不吐一页，
     * 上游 shuffle 收页也可能早已结束。若心跳只看收页数，长排序会被 Master 结果看门狗误杀。
     */
    @Override
    public long mergeProgressTicks() {
        long ticks = 0L;
        if (leftSort != null) ticks += leftSort.mergeProgressTicks();
        if (rightSort != null) ticks += rightSort.mergeProgressTicks();
        return ticks;
    }

    // ==================== 分区级遥测（WebUI 逐分区行数 / 字节 / 完成） ====================

    /** 分区数（与 Master 下发的 shuffle 分区数一致）。 */
    public int numPartitions() { return numPartitions; }

    /** 指定分区已 join 输出的行数。 */
    public long partitionRowsOut(int pid) {
        return pid >= 0 && pid < partitionRowsOut.length() ? partitionRowsOut.get(pid) : 0L;
    }

    /** 指定分区已 join 输出的字节数。 */
    public long partitionBytesOut(int pid) {
        return pid >= 0 && pid < partitionBytesOut.length() ? partitionBytesOut.get(pid) : 0L;
    }

    /** 指定分区从两侧 shuffle 读入的行数（Phase 1 计数）。 */
    public long partitionRowsIn(int pid) {
        long n = 0L;
        if (leftSort != null) n += leftSort.partitionRowsIn(pid);
        if (rightSort != null) n += rightSort.partitionRowsIn(pid);
        return n;
    }

    /** 指定分区从两侧 shuffle 读入的字节数（Phase 1 计数）。 */
    public long partitionBytesIn(int pid) {
        long n = 0L;
        if (leftSort != null) n += leftSort.partitionBytesIn(pid);
        if (rightSort != null) n += rightSort.partitionBytesIn(pid);
        return n;
    }

    /** 指定分区的归并线程是否已结束。 */
    public boolean partitionDone(int pid) {
        return pid >= 0 && pid < partitionDone.length() && partitionDone.get(pid) != 0;
    }

    /** 全部分区的 join 输出行数之和（片段级遥测在窗口归并期间的行数兜底）。 */
    public long rowsProduced() {
        long sum = 0L;
        for (int k = 0; k < partitionRowsOut.length(); k++) sum += partitionRowsOut.get(k);
        return sum;
    }

    private static RuntimeException asRuntime(Throwable t) {
        if (t instanceof RuntimeException re) return re;
        if (t instanceof Error err) throw err;
        return new RuntimeException("SortMergeJoin 分区归并失败", t);
    }

    /**
     * 取消检查点 —— Phase 1 排序 / Phase 2 分区归并全是纯 CPU 循环，没有任何阻塞点。
     * 任务取消时 {@code WorkerManager.cancelTask} 只打断任务线程并摘掉槽位（WebUI 立刻显示 0 个运行中），
     * 若循环不抬头，线程会一直烧 CPU、溢写目录与 Arrow 内存也永不释放。
     *
     * <p>用非受检的 {@link CancellationException}：各归并方法签名各异（有的 throws IOException、有的无 throws），
     * 统一在 {@link #startPhase2()} / 线程体收口，取消 ≠ 失败（绝不记 phase2Failure）。</p>
     */
    private void checkCancelled() {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new CancellationException("SortMergeJoin cancelled");
        }
    }

    // ==================== Phase 1 ====================

    /** 两侧排序独立线程并行推进（各自溢写），Phase 1 期间本算子不吐页。 */
    private void runPhase1() throws InterruptedException {
        long t1 = System.currentTimeMillis();
        System.err.println("[SortMergeJoin] Phase 1: sorting both sides...");

        AtomicReference<Throwable> sortFailure = new AtomicReference<>();
        Thread leftThread = new Thread(() -> {
            try { drainOperator(leftSort); }
            catch (CancellationException ce) { /* 取消：静默收手，不算失败 */ }
            catch (Throwable t) { sortFailure.compareAndSet(null, new RuntimeException("Left sort failed", t)); }
        }, "sort-merge-left-sort");
        Thread rightThread = new Thread(() -> {
            try { drainOperator(rightSort); }
            catch (CancellationException ce) { /* 取消：静默收手，不算失败 */ }
            catch (Throwable t) { sortFailure.compareAndSet(null, new RuntimeException("Right sort failed", t)); }
        }, "sort-merge-right-sort");
        leftThread.start(); rightThread.start();
        try {
            leftThread.join();  rightThread.join();
        } catch (InterruptedException ie) {
            // Future.cancel(true) 只打断任务线程；两个裸排序线程必须显式打断，否则会跑完整个排序。
            leftThread.interrupt();
            rightThread.interrupt();
            throw ie;
        }
        if (sortFailure.get() != null) throw asRuntime(sortFailure.get());

        System.err.printf("[SortMergeJoin] Phase 1 done: %d ms%n",
                System.currentTimeMillis() - t1);
    }

    /** Phase 2：每活跃分区一个归并线程；失败记进 {@link #phase2Failure} 由消费者 fail-fast 抛出。 */
    private void startPhase2() {
        long t1 = System.currentTimeMillis();
        System.err.println("[SortMergeJoin] Phase 2: partition-level merge join...");

        leftSchema = leftSort.getSchema();
        // 左（probe）侧一页都没收到 → 用 Master 下发的声明列兜底（RIGHT/FULL 要输出
        // 「右行 + 左列全 NULL」，没有左列类型就造不出输出 schema）。
        if (leftSchema == null && declaredLeftFields != null) {
            leftSchema = new Schema(declaredLeftFields);
        }
        // 右（build）侧一页都没收到 → 排序算子没有 schema，用 Master 下发的声明列兜底
        // （否则 LEFT/FULL 输出「左行 + 右列全 NULL」时造不出右列向量）。
        rightSchema = rightSort.getSchema();
        if (rightSchema == null && declaredRightFields != null) {
            rightSchema = new Schema(declaredRightFields);
        }

        int[] activePids = collectActivePartitions();
        if (activePids.length == 0) {
            phase2Remaining.set(0);
            return;
        }
        phase2Remaining.set(activePids.length);
        phase2Pool = Executors.newFixedThreadPool(activePids.length, r -> {
            Thread t = new Thread(r, "sort-merge-merge-join");
            t.setDaemon(true);
            return t;
        });
        for (int pid : activePids) {
            final int p = pid;
            phase2Futures.add(phase2Pool.submit(() -> {
                try {
                    mergeJoinPartition(p);
                } catch (CancellationException ce) {
                    // checkCancelled() 的收口点：取消 ≠ 失败，静默收手（绝不记 phase2Failure）。
                } catch (Throwable t) {
                    phase2Failure.compareAndSet(null, t);
                } finally {
                    // 分区归并线程退出 = 该分区不再产页（done 先于 remaining 递减，采样器不会读到「未完成但已停产」）
                    if (p >= 0 && p < partitionDone.length()) partitionDone.set(p, 1);
                    phase2Remaining.decrementAndGet();
                }
            }));
        }
        phase2Pool.shutdown();
        System.err.printf("[SortMergeJoin] Phase 2 started: %d partitions, %d ms%n",
                activePids.length, System.currentTimeMillis() - t1);
    }

    private void drainOperator(Operator op) {
        Page p;
        while (true) {
            checkCancelled();
            if ((p = op.next()).isEOF()) break;
            p.close();
        }
    }

    /**
     * 有界入队 —— 队列满则等待消费者排空（背压）；算子已关闭则丢弃并立即释放本页。
     *
     * <p>Phase 2 分区线程与 {@code doNext()} 消费者并发，故必须<b>不能无限阻塞</b>：查询取消 /
     * 失败时消费者不再排空队列，{@code offer(超时)} + {@code closed} 复查保证线程能退出。
     */
    private void emit(Page page) {
        // 分区级输出计数：所有 SortMerge 输出页都带 pid（Page.partitioned），按页归属累加。
        int pid = page.getPartitionId().orElse(-1);
        if (pid >= 0 && pid < partitionRowsOut.length()) {
            partitionRowsOut.addAndGet(pid, page.getRowCount());
            partitionBytesOut.addAndGet(pid, page.getSizeInBytes());
        }
        while (!closed) {
            try {
                if (outputQueue.offer(page, 100, TimeUnit.MILLISECONDS)) return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        try { page.close(); } catch (Exception ignored) {}
    }

    // ==================== Phase 2 ====================

    private void mergeJoinPartition(int pid) {
        PartitionState<RowAccumulator> leftPa = leftSort.getPartitionedData(pid);
        PartitionState<RowAccumulator> rightPa = rightSort.getPartitionedData(pid);

        if (leftPa == null && rightPa == null) return;  // 两侧都无数据才跳过

        // 单侧为空（另一侧有数据）且该侧孤儿必须输出 → 走流式分支把有数据侧整份当孤儿吐出来。
        //   LEFT/FULL 缺右 → 输出全部左行（右列 NULL）；RIGHT/FULL 缺左 → 输出全部右行（左列 NULL）。
        // ★ 为什么不能落到 index 分支：Phase 1 结束时<b>每个非空分区恒 compacted=true</b>
        //   （纯内存路径只留 sortedIndices/inMemoryRoot、K 路归并后 pa.index.clear()）⇒ 空侧 key 集为空
        //   ⇒ indexBasedMergeJoin 静默吐 0 行。实测 `a LEFT JOIN b`（b 侧 0 页）结果集整个是空的，
        //   而 LEFT JOIN 必须输出全部 275000 行左数据 + NULL 右列。
        boolean needsEmptySideOrphans = (rightPa == null && leftPa != null
                    && (joinType == JoinType.LEFT || joinType == JoinType.FULL))
                || (leftPa == null && rightPa != null
                    && (joinType == JoinType.RIGHT || joinType == JoinType.FULL));

        boolean leftCompacted = leftPa != null && leftPa.compacted;
        boolean rightCompacted = rightPa != null && rightPa.compacted;
        if (needsEmptySideOrphans || (leftCompacted && rightCompacted)) {
            streamingMergeJoin(pid, leftPa, rightPa);
        } else {
            indexBasedMergeJoin(pid, leftPa, rightPa);
        }
    }

    // ==================== 流式双指针归并 join（归并后的 key 连续文件） ====================

    private void streamingMergeJoin(int pid, PartitionState<RowAccumulator> leftPa,
                                     PartitionState<RowAccumulator> rightPa) {
        JoinOutputBuffer buf = new JoinOutputBuffer(pid);
        FileIterator left = null;
        FileIterator right = null;
        boolean ok = false;
        try {
            // 单侧无数据 → 有数据侧全部当孤儿输出。走到这里说明 join 类型确实需要孤儿
            // （判定在 mergeJoinPartition：LEFT/FULL 缺右、RIGHT/FULL 缺左）——此处不再重复判断。
            if (leftPa == null) {
                requireSchema(leftSchema, "左（probe）", pid);
                dumpAllAsOrphan(rightPa, findFieldIndex(rightSchema, rightKeyColumns.getFirst()),
                        false, pid, buf);
                ok = true;
                return;
            }
            if (rightPa == null) {
                requireSchema(rightSchema, "右（build）", pid);
                dumpAllAsOrphan(leftPa, findFieldIndex(leftSchema, leftKeyColumns.getFirst()),
                        true, pid, buf);
                ok = true;
                return;
            }

            int leftKeyIdx = findFieldIndex(leftSchema, leftKeyColumns.getFirst());
            int rightKeyIdx = findFieldIndex(rightSchema, rightKeyColumns.getFirst());
            left = createIterator(leftPa, leftKeyIdx);
            right = createIterator(rightPa, rightKeyIdx);

            boolean leftHasMore = left.nextBatch();
            boolean rightHasMore = right.nextBatch();

            while (leftHasMore && rightHasMore) {
                checkCancelled();
                Object lk = left.currentKey();
                Object rk = right.currentKey();
                int cmp = compareKeys(lk, rk);

                if (cmp < 0) {
                    if (joinType == JoinType.LEFT || joinType == JoinType.FULL) {
                        leftHasMore = consumeKeyGroup(left, (it, chunk) -> outputOrphanRows(it, chunk, buf));
                    } else {
                        leftHasMore = discardKeyGroup(left); // 丢弃（INNER/RIGHT 不需要输出）
                    }
                } else if (cmp > 0) {
                    if (joinType == JoinType.RIGHT || joinType == JoinType.FULL) {
                        rightHasMore = consumeKeyGroup(right, (it, chunk) -> outputOrphanRight(it, chunk, buf));
                    } else {
                        rightHasMore = discardKeyGroup(right); // 丢弃（INNER/LEFT 不需要输出）
                    }
                } else {
                    // key 匹配 → 逐块 cross join（组可能跨多个 record batch）
                    crossJoinKeyGroup(left, right, buf);
                    leftHasMore = left.hasCurrentRow();
                    rightHasMore = right.hasCurrentRow();
                }
            }
            // 剩余孤儿
            while (leftHasMore) {
                checkCancelled();
                if (joinType == JoinType.LEFT || joinType == JoinType.FULL)
                    leftHasMore = consumeKeyGroup(left, (it, chunk) -> outputOrphanRows(it, chunk, buf));
                else
                    leftHasMore = discardKeyGroup(left);
            }
            while (rightHasMore) {
                checkCancelled();
                if (joinType == JoinType.RIGHT || joinType == JoinType.FULL)
                    rightHasMore = consumeKeyGroup(right, (it, chunk) -> outputOrphanRight(it, chunk, buf));
                else
                    rightHasMore = discardKeyGroup(right);
            }

            ok = true;
        } finally {
            // 取消 / 失败：迭代器占着回读的 Arrow 文件批，在途输出批也未成页 —— 都必须显式释放。
            if (left != null) { try { left.close(); } catch (Exception ignored) {} }
            if (right != null) { try { right.close(); } catch (Exception ignored) {} }
            if (ok) buf.flush();
            else buf.discard();
        }
    }

    /**
     * 消费<b>一个完整 key 组</b>（可能跨多个 record batch），逐块交给 {@code writer}。
     *
     * <p>旧实现把「本 batch 内连续同 key 的行」当作整组（组不跨批）。{@link JoinSortOperator#mergeOneRound}
     * 原先也确实只在 key 换组时切批 —— 代价是单个热键组 &gt; 一批时会无限扩容输出向量直至 OOM。
     * 现在切批只按缓冲大小，组可以跨批，消费端必须按「组跨批」推进：块内跑到批尾且下一批仍是同一 key
     * 就继续。</p>
     *
     * @return 组消费完后迭代器是否还有当前行（false = 该侧已无数据）
     */
    private boolean consumeKeyGroup(FileIterator it, BiConsumer<FileIterator, List<int[]>> writer) {
        Object key = it.currentKey();
        while (true) {
            checkCancelled();
            writer.accept(it, it.collectKeyChunk());
            if (!it.atBatchEnd()) break;                    // 同批内已到下一个 key
            if (!it.nextBatch()) break;                     // 无更多数据
            if (!keysEqual(key, it.currentKey())) break;    // 下一批已经是新 key
        }
        return it.hasCurrentRow();
    }

    /** 丢弃一个完整 key 组（INNER/RIGHT 不需要左孤儿、INNER/LEFT 不需要右孤儿）。 */
    private boolean discardKeyGroup(FileIterator it) {
        return consumeKeyGroup(it, (i, chunk) -> { });
    }

    /**
     * 匹配 key 组的 cross join，两侧的组都可能跨多个 batch。
     *
     * <p>左组按块流式推进；每换一个左块，就把右迭代器 rewind 回右组起点、重新逐块扫描右组
     * （跨批时从磁盘重读切片）。这样任一时刻只驻留左右各一个回读切片，内存与组大小无关。</p>
     *
     * <p>结束时 left 已越过左组、right 停在右组末尾（两者各自指向下一组起点或耗尽）。</p>
     */
    private void crossJoinKeyGroup(FileIterator left, FileIterator right, JoinOutputBuffer buf) {
        Object key = left.currentKey();
        FileIterator.Mark rightStart = right.mark();
        while (true) {
            checkCancelled();
            List<int[]> lChunk = left.collectKeyChunk();
            right.rewind(rightStart);
            while (true) {
                checkCancelled();
                List<int[]> rChunk = right.collectKeyChunk();
                crossJoinInBuffer(left, lChunk, right, rChunk, buf);
                if (!right.atBatchEnd()) break;                     // 同批内已到下一个 key
                if (!right.nextBatch()) break;                      // 右组结束（无更多数据）
                if (!keysEqual(key, right.currentKey())) break;     // 下一批已经是新 key
            }
            if (!left.atBatchEnd()) break;              // 同批内已到下一个 key → 左组结束
            if (!left.nextBatch()) break;               // 无更多数据
            if (!keysEqual(key, left.currentKey())) break;
        }
    }

    /** 流式迭代器：文件模式逐批加载，内存模式直接用已排序的 root，索引模式零拷贝 */
    private static class FileIterator {
        private final PartitionState<RowAccumulator> pa;  // null = 内存/索引模式
        private final BufferAllocator allocator;
        private final int keyColIdx;
        private final boolean inMemory;
        private final int[] sortedIndices;      // null = 文件/内存模式，非 null = 索引模式
        private final List<FieldVector> sourceVectors; // 零拷贝索引模式：直接引用 accumulator 向量
        private int fileIdx;
        private int nextBlock;      // 当前文件内下一个待读 record block
        private int sliceFirstBlock; // 当前切片起始 block（rewind 复用/重载切片用）
        private boolean fileDone;   // 当前文件的 block 已读完
        private VectorSchemaRoot currentRoot;
        private int row;
        private int rowCount;

        /** 迭代器位置快照：文件模式下按 (文件, 切片起始 block, 行偏移) 定位，供跨批 key 组重扫。 */
        record Mark(int fileIdx, int sliceFirstBlock, int row) { }

        /** 文件模式：逐批加载 Arrow 文件 */
        FileIterator(PartitionState<RowAccumulator> pa, BufferAllocator allocator, int keyColIdx) {
            this.pa = pa;
            this.allocator = allocator;
            this.keyColIdx = keyColIdx;
            this.fileIdx = -1;
            this.nextBlock = 0;
            this.fileDone = false;
            this.inMemory = false;
            this.sortedIndices = null;
            this.sourceVectors = null;
        }

        /** 内存模式：直接用已排序的 VectorSchemaRoot，不加载文件 */
        FileIterator(VectorSchemaRoot root, int keyColIdx) {
            this.pa = null;
            this.allocator = null;
            this.keyColIdx = keyColIdx;
            this.fileIdx = 0;
            this.currentRoot = root;
            this.row = 0;
            this.rowCount = root.getRowCount();
            this.inMemory = true;
            this.sortedIndices = null;
            this.sourceVectors = null;
        }

        /** 索引模式：零拷贝，直接持有 accumulator 向量引用 + 排序索引，不包装 VectorSchemaRoot */
        FileIterator(int[] sortedIndices, List<FieldVector> vectors, int keyColIdx) {
            this.pa = null;
            this.allocator = null;
            this.keyColIdx = keyColIdx;
            this.sortedIndices = sortedIndices;
            this.sourceVectors = vectors;
            this.fileIdx = 0;
            this.inMemory = false;
            this.row = 0;
            this.rowCount = sortedIndices.length;
            this.currentRoot = null;
        }

        boolean nextBatch() {
            if (currentRoot != null && row < rowCount) return true;
            if (sortedIndices != null) return row < rowCount;  // 索引模式：数据已在内存，检查是否还有行
            if (currentRoot != null) {
                if (!inMemory) currentRoot.close();
                currentRoot = null;
            }
            if (inMemory || pa == null) return false;
            // 每次只读「一个输出批」大小的 block 切片（key 组可能跨切片，不能假设组不跨批），
            // 绝不再把一个文件的所有 block 拼成一个大 root（联邦全量 join 的单文件可达千万行，
            // setInitialCapacity(totalRows) 会一次申请 >100MB 连续 buffer 而撞分配器上限）。
            while (true) {
                if (fileIdx < 0) {
                    fileIdx = 0;
                    nextBlock = 0;
                    fileDone = false;
                } else if (fileDone) {
                    fileIdx++;
                    nextBlock = 0;
                    fileDone = false;
                }
                if (fileIdx >= pa.batchFiles.size()) return false;

                int firstBlock = nextBlock;
                FileSlice slice = loadArrowFileSlice(
                        pa.batchFiles.get(fileIdx), allocator, firstBlock, LOAD_BATCH_ROWS);
                if (slice == null) {   // 读取失败：跳过该文件
                    fileIdx++;
                    nextBlock = 0;
                    fileDone = false;
                    continue;
                }
                sliceFirstBlock = firstBlock;
                nextBlock = slice.nextBlock();
                fileDone = slice.done();
                if (slice.root() != null && slice.root().getRowCount() > 0) {
                    currentRoot = slice.root();
                    row = 0;
                    rowCount = currentRoot.getRowCount();
                    return true;
                }
                // 空切片：同文件还有后续 block 就继续，否则下一轮换文件
            }
        }

        Object currentKey() {
            int phys = (sortedIndices != null) ? sortedIndices[row] : row;
            return RowAccumulator.readKeyValue(getVector(keyColIdx), phys);
        }

        /**
         * 收集当前 key 在<b>当前 batch 内</b>的连续行并前进游标（不跨批）。
         * 返回 [start, row) 的逻辑行范围；组是否还有后续行由调用方按「批尾 + 同 key」判断。
         */
        List<int[]> collectKeyChunk() {
            int start = row;
            Object k = currentKey();
            while (row < rowCount && readAndEqual(k)) row++;
            List<int[]> ranges = new ArrayList<>(1);
            ranges.add(new int[]{start, row});
            return ranges;
        }

        /** 当前 batch 是否已消费到尾（尾后接下一批或已结束）。 */
        boolean atBatchEnd() {
            return row >= rowCount;
        }

        /** 迭代器是否还有当前行（尚未耗尽）。 */
        boolean hasCurrentRow() {
            if (sortedIndices != null) return row < rowCount;
            return currentRoot != null && row < rowCount;
        }

        /** 记录当前位置（跨批 key 组重扫用）。 */
        Mark mark() {
            return new Mark(fileIdx, sliceFirstBlock, row);
        }

        /**
         * 回到 {@link #mark()}：文件模式下若当前切片就是 mark 所在切片，直接复位行游标（不读盘）；
         * 否则关闭当前切片并从 mark 的起始 block 重新加载（组跨批时右组需要被重扫）。
         */
        void rewind(Mark mark) {
            if (inMemory || sortedIndices != null) {
                row = mark.row;
                return;
            }
            if (currentRoot != null && fileIdx == mark.fileIdx
                    && sliceFirstBlock == mark.sliceFirstBlock) {
                row = mark.row;
                return;
            }
            if (currentRoot != null) {
                currentRoot.close();
                currentRoot = null;
            }
            fileIdx = mark.fileIdx;
            nextBlock = mark.sliceFirstBlock;
            fileDone = false;
            FileSlice slice = loadArrowFileSlice(
                    pa.batchFiles.get(fileIdx), allocator, mark.sliceFirstBlock, LOAD_BATCH_ROWS);
            if (slice == null || slice.root() == null) {
                row = 0;
                rowCount = 0;
                return;
            }
            sliceFirstBlock = mark.sliceFirstBlock;
            nextBlock = slice.nextBlock();
            fileDone = slice.done();
            currentRoot = slice.root();
            row = mark.row;
            rowCount = currentRoot.getRowCount();
        }

        private boolean readAndEqual(Object k) {
            int phys = (sortedIndices != null) ? sortedIndices[row] : row;
            return keysEqual(k, RowAccumulator.readKeyValue(getVector(keyColIdx), phys));
        }

        /** 获取第 col 列向量（索引模式直接返回 accumulator 向量，文件/内存模式返回 root 中的向量） */
        FieldVector getVector(int col) {
            if (sourceVectors != null) return sourceVectors.get(col);
            return currentRoot != null ? currentRoot.getVector(col) : null;
        }

        /** 列数 */
        int cols() {
            if (sourceVectors != null) return sourceVectors.size();
            return currentRoot != null ? currentRoot.getFieldVectors().size() : 0;
        }

        /** 翻译逻辑行 → 物理行（索引模式查 sortedIndices，文件/内存模式原样返回） */
        int physicalRow(int logical) { return (sortedIndices != null) ? sortedIndices[logical] : logical; }

        void close() {
            if (currentRoot != null) {
                if (!inMemory && sortedIndices == null) currentRoot.close();
                currentRoot = null;
            }
        }
    }

    /** 输出左孤儿行（LEFT/FULL join） */
    private void outputOrphanRows(FileIterator left, List<int[]> lRows, JoinOutputBuffer buf) {
        for (int[] range : lRows) {
            for (int li = range[0]; li < range[1]; li++) {
                buf.appendLeftOrphanRow(left, left.physicalRow(li));
            }
        }
    }

    /**
     * 孤儿输出需要「另一侧」的列类型来造 NULL 列 —— 拿不到就<b>响亮报错</b>，绝不静默少行/丢列。
     *
     * <p>何时会拿不到：该侧<b>整张表一页都没有</b>（连声明列也没下发）。已知缺口是 SORT_MERGE 的
     * RIGHT/FULL join 且 probe（左）侧为空 —— 左表声明列目前未随 {@code JoinStep} 下发
     * （与 HASH 路径的同类缺口一致）；报错让用户看到明确原因，而不是拿到一份「成功但少数据」的结果。</p>
     */
    private static void requireSchema(Schema schema, String side, int pid) {
        if (schema == null) {
            throw new IllegalStateException("SortMergeJoin 分区 " + pid + "：" + side
                    + "侧无数据且无声明列，无法构造输出 schema（无法输出该侧全 NULL 的孤儿行）");
        }
    }

    /** 单侧无数据时, dump 有数据侧的所有行为孤儿 (LEFT/FULL 时右为空, RIGHT/FULL 时左为空) */
    private void dumpAllAsOrphan(PartitionState<RowAccumulator> pa, int keyIdx,
                                  boolean isLeft, int pid, JoinOutputBuffer buf) {
        FileIterator fi = createIterator(pa, keyIdx);
        while (fi.nextBatch()) {
            checkCancelled();
            while (fi.row < fi.rowCount) {
                checkCancelled();
                int phys = fi.physicalRow(fi.row);
                if (isLeft) buf.appendLeftOrphanRow(fi, phys);
                else buf.appendRightOrphanRow(fi, phys);
                fi.row++;
            }
        }
        fi.close();
        buf.flush();
    }

    /** 三路分发：索引模式 > 内存模式 > 文件模式 */
    private FileIterator createIterator(PartitionState<RowAccumulator> pa, int keyIdx) {
        if (pa.sortedIndices != null && pa.indexSource != null) {
            return new FileIterator(pa.sortedIndices, pa.indexSource.vectors, keyIdx);
        }
        if (pa.inMemoryRoot != null) {
            return new FileIterator(pa.inMemoryRoot, keyIdx);
        }
        return new FileIterator(pa, allocator, keyIdx);
    }

    /** 输出右孤儿行（RIGHT/FULL join） */
    private void outputOrphanRight(FileIterator right, List<int[]> rRows, JoinOutputBuffer buf) {
        for (int[] range : rRows) {
            for (int ri = range[0]; ri < range[1]; ri++) {
                buf.appendRightOrphanRow(right, right.physicalRow(ri));
            }
        }
    }

    /** cross join 一组匹配的 key */
    private void crossJoinInBuffer(FileIterator left, List<int[]> lRanges,
                                    FileIterator right, List<int[]> rRanges,
                                    JoinOutputBuffer buf) {
        for (int[] lr : lRanges) {
            for (int li = lr[0]; li < lr[1]; li++) {
                int pl = left.physicalRow(li);
                for (int[] rr : rRanges) {
                    for (int ri = rr[0]; ri < rr[1]; ri++) {
                        buf.appendMatchedRow(left, pl, right, right.physicalRow(ri));
                    }
                }
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareKeys(Object a, Object b) {
        if (a == RowAccumulator.NULL_KEY && b == RowAccumulator.NULL_KEY) return 0;
        if (a == RowAccumulator.NULL_KEY) return -1;
        if (b == RowAccumulator.NULL_KEY) return 1;
        if (a instanceof Comparable ca && b instanceof Comparable cb) return ca.compareTo(cb);
        if (a instanceof WindowSortOperator.CompositeKey cka
                && b instanceof WindowSortOperator.CompositeKey ckb)
            return compareComposite(cka, ckb);
        return String.valueOf(a).compareTo(String.valueOf(b));
    }

    // ---- 流式 join 的静态工具 ----

    @SuppressWarnings({"rawtypes", "unchecked"})
    static boolean keysEqual(Object a, Object b) {
        if (a == RowAccumulator.NULL_KEY && b == RowAccumulator.NULL_KEY) return true;
        if (a == RowAccumulator.NULL_KEY || b == RowAccumulator.NULL_KEY) return false;
        if (a instanceof Comparable ca && b instanceof Comparable cb) return ca.compareTo(cb) == 0;
        return a.equals(b);
    }

    static int findFieldIndex(Schema schema, String name) {
        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(name)) return i;
        }
        return 0;
    }

    /**
     * 单次回读的最大行数（≈ 一个 {@link JoinSortOperator} 输出批）。
     *
     * <p>JoinSort 的 block 只按缓冲大小切出（{@code mergeOneRound}），<b>同一个 join key 组可以跨
     * 多个 block / 多个切片</b>（热键组的行数不设上限）。消费端必须用 {@code collectKeyChunk} +
     * 「批尾且下一批同 key 则继续」的游标，不能假设组不跨批。</p>
     */
    private static final int LOAD_BATCH_ROWS = 50_000;

    /** 一个溢写文件的切片：{@code root} 为若干连续 record block 的拼接；{@code done}=文件中已无后续 block。 */
    record FileSlice(VectorSchemaRoot root, int nextBlock, boolean done) { }

    /**
     * 从 {@code firstBlock} 起读取连续 record block，直到累计行数达到 {@code maxRows}
     * （至少读一个 block），<b>不做整文件拼接</b>。
     *
     * <p>★这是「溢写回读」的内存闸门：compacted join 文件可能承载整个分区的上千万行，
     * 旧实现按总行数 {@code setInitialCapacity(totalRows)} 一次申请 >100MB 连续 buffer，
     * 在联邦全量 join（375MB 已占用 / 443MB 上限）直接 OOM。切片后峰值 = 单切片行数。</p>
     */
    static FileSlice loadArrowFileSlice(Path file, BufferAllocator alloc, int firstBlock, int maxRows) {
        List<FieldVector> copied = new ArrayList<>();
        try {
            FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
            ArrowFileReader reader = new ArrowFileReader(ch, alloc);
            try {
                List<ArrowBlock> blocks = reader.getRecordBlocks();
                if (firstBlock >= blocks.size()) return new FileSlice(null, blocks.size(), true);

                // 选定 [firstBlock, end)：至少一个 block，之后累计到 maxRows 为止
                Schema schema = null;
                int dstRow = 0;
                int bi = firstBlock;
                // one pass: read each block once, copy into the target root as we go
                while (bi < blocks.size() && (bi == firstBlock || dstRow < maxRows)) {
                    reader.loadRecordBatch(blocks.get(bi));
                    VectorSchemaRoot loaded = reader.getVectorSchemaRoot();
                    int rc = loaded.getRowCount();
                    bi++;
                    if (rc == 0) continue;
                    if (schema == null) {
                        schema = loaded.getSchema();
                        for (int c = 0; c < loaded.getFieldVectors().size(); c++) {
                            FieldVector v = loaded.getVector(c).getField().createVector(alloc);
                            v.setInitialCapacity(Math.max(1, maxRows));
                            v.allocateNew();
                            copied.add(v);
                        }
                    }
                    for (int c = 0; c < loaded.getFieldVectors().size(); c++) {
                        RowAccumulator.copyColumnBatch(loaded.getVector(c), copied.get(c), 0, dstRow, rc);
                    }
                    dstRow += rc;
                }
                boolean done = bi >= blocks.size();
                if (dstRow == 0) return new FileSlice(null, bi, done);
                for (FieldVector v : copied) v.setValueCount(dstRow);
                return new FileSlice(new VectorSchemaRoot(schema.getFields(), copied, dstRow), bi, done);
            } finally {
                reader.close();
                ch.close();
            }
        } catch (IOException e) {
            // unreadable file: skip it (pre-existing semantics)
            closeQuietly(copied);
            return null;
        } catch (RuntimeException e) {
            // OOM / corruption must NEVER be swallowed into "skip the file" (silent data loss)
            closeQuietly(copied);
            throw e;
        }
    }

    private static void closeQuietly(List<FieldVector> vectors) {
        for (FieldVector v : vectors) {
            try { v.close(); } catch (Exception ignored) { }
        }
    }

    /**
     * 读回一个溢写文件的<b>全部</b> record block 并纵向拼接（索引路径用；正常流式路径走
     * {@link #loadArrowFileSlice}，避免整文件一次性分配）。
     *
     * <p>compacted 归并文件按 {@code OUTPUT_BATCH_SIZE} 在 key 组边界切出多个 record block
     * （见 {@link JoinSortOperator#mergeOneRound}）—— 只读首个 block 会把 &gt;50000 行的分区静默截断。
     * 第一遍量出总行数、按总行数一次性分配目标向量，第二遍逐 block 拷贝（峰值内存 = 总行数 + 单 block，
     * 不随 block 数增长）。</p>
     */
    static VectorSchemaRoot loadArrowFile(Path file, BufferAllocator alloc) {
        try {
            FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
            ArrowFileReader reader = new ArrowFileReader(ch, alloc);
            try {
                List<ArrowBlock> blocks = reader.getRecordBlocks();
                if (blocks.isEmpty()) return null;

                // 第一遍：量总行数（逐 block 读入即计数，reader 每次只驻留一个 batch）
                int totalRows = 0;
                for (ArrowBlock b : blocks) {
                    reader.loadRecordBatch(b);
                    totalRows += reader.getVectorSchemaRoot().getRowCount();
                }
                if (totalRows == 0) return null;

                // 第二遍：按总行数一次性分配目标向量，逐 block 拷贝
                Schema schema = null;
                List<FieldVector> copied = new ArrayList<>();
                int dstRow = 0;
                for (ArrowBlock b : blocks) {
                    reader.loadRecordBatch(b);
                    VectorSchemaRoot loaded = reader.getVectorSchemaRoot();
                    int rc = loaded.getRowCount();
                    if (schema == null) {
                        schema = loaded.getSchema();
                        for (int c = 0; c < loaded.getFieldVectors().size(); c++) {
                            FieldVector v = loaded.getVector(c).getField().createVector(alloc);
                            v.setInitialCapacity(totalRows);
                            v.allocateNew();
                            copied.add(v);
                        }
                    }
                    for (int c = 0; c < loaded.getFieldVectors().size(); c++) {
                        RowAccumulator.copyColumnBatch(loaded.getVector(c), copied.get(c), 0, dstRow, rc);
                    }
                    dstRow += rc;
                }
                for (FieldVector v : copied) v.setValueCount(totalRows);
                return new VectorSchemaRoot(schema.getFields(), copied, totalRows);
            } finally {
                reader.close();
                ch.close();
            }
        } catch (IOException e) {
            return null;
        }
    }

    // ==================== 索引路径 join（归并前的 key 交错文件） ====================

    private void indexBasedMergeJoin(int pid, PartitionState<RowAccumulator> leftPa,
                                      PartitionState<RowAccumulator> rightPa) {

        Map<Object, List<Segment>> leftIdx = leftPa != null ? leftPa.index : Collections.emptyMap();
        Map<Object, List<Segment>> rightIdx = rightPa != null ? rightPa.index : Collections.emptyMap();

        Set<Object> keySet = buildKeySet(leftIdx, rightIdx);
        List<Object> allKeys = new ArrayList<>(keySet);
        sortKeys(allKeys);

        // ★ 懒加载缓存：首次用到某 batch 文件时才从磁盘加载，全部分区处理完后统一关闭
        //    由于 slot 溢写后 key 在文件间可能交错（如 file0: [1,3,5], file1: [2,4,6]），
        //    同一文件可能被非连续的 keyBatch 引用，所以不淘汰，只增不减
        Map<Integer, VectorSchemaRoot> leftCache = new LinkedHashMap<>();
        Map<Integer, VectorSchemaRoot> rightCache = new LinkedHashMap<>();

        JoinOutputBuffer buf = new JoinOutputBuffer(pid);
        boolean ok = false;
        try {
            for (int start = 0; start < allKeys.size(); start += KEYS_PER_BATCH) {
                checkCancelled();
                int end = Math.min(start + KEYS_PER_BATCH, allKeys.size());
                List<Object> keyBatch = allKeys.subList(start, end);

                // 加载当前 keyBatch 需要但尚未缓存的文件
                for (Object key : keyBatch) {
                    ensureLoaded(leftPa, leftIdx.get(key), leftCache);
                    ensureLoaded(rightPa, rightIdx.get(key), rightCache);
                }

                for (Object key : keyBatch) {
                    checkCancelled();
                    List<Segment> ls = leftIdx.get(key);
                    List<Segment> rs = rightIdx.get(key);

                    boolean leftHas = ls != null && !ls.isEmpty();
                    boolean rightHas = rs != null && !rs.isEmpty();

                    if (leftHas && rightHas) {
                        buf.appendCrossJoinSegments(leftCache, ls, rightCache, rs);
                    } else if (leftHas) {
                        // LEFT/FULL: 左有右无 → 左孤儿
                        if (joinType == JoinType.LEFT || joinType == JoinType.FULL)
                            outputIndexOrphan(leftCache, ls, buf, true);
                    } else if (rightHas) {
                        // RIGHT/FULL: 右有左无 → 右孤儿
                        if (joinType == JoinType.RIGHT || joinType == JoinType.FULL)
                            outputIndexOrphan(rightCache, rs, buf, false);
                    }
                }
                buf.flushIfNeeded();
            }
            buf.flush();
            ok = true;
        } finally {
            if (!ok) buf.discard();   // 取消 / 失败：在途输出批未成页，必须显式关闭
            for (VectorSchemaRoot r : leftCache.values()) closeQuietly(r);
            for (VectorSchemaRoot r : rightCache.values()) closeQuietly(r);
        }
    }

    /** 把 segments 引用的 batch 文件按需加载到缓存 */
    private void ensureLoaded(PartitionState<RowAccumulator> pa,
                              List<Segment> segs,
                              Map<Integer, VectorSchemaRoot> cache) {
        if (pa == null || segs == null) return;
        for (Segment s : segs) {
            int bi = s.batchIndex();
            if (!cache.containsKey(bi)) {
                VectorSchemaRoot r = loadBatch(pa, bi);
                if (r != null) cache.put(bi, r);
            }
        }
    }

    private static void closeQuietly(VectorSchemaRoot r) {
        try { r.close(); } catch (Exception ignored) {}
    }

    /** 从缓存的 batch 文件输出孤儿行（索引路径的 LEFT/RIGHT/FULL 孤儿） */
    private void outputIndexOrphan(Map<Integer, VectorSchemaRoot> cache,
                                   List<Segment> segs, JoinOutputBuffer buf,
                                   boolean isLeft) {
        for (Segment s : segs) {
            VectorSchemaRoot root = cache.get(s.batchIndex());
            if (root == null) continue;
            for (int ri = s.startRow(); ri < s.endRow(); ri++) {
                if (isLeft) buf.appendLeftOrphanRow(root, ri);
                else buf.appendRightOrphanRow(root, ri);
            }
        }
    }

    private Set<Object> buildKeySet(Map<Object, List<Segment>> l, Map<Object, List<Segment>> r) {
        return switch (joinType) {
            case INNER -> { Set<Object> s = new LinkedHashSet<>(l.keySet()); s.retainAll(r.keySet()); yield s; }
            case LEFT -> new LinkedHashSet<>(l.keySet());
            case RIGHT -> new LinkedHashSet<>(r.keySet());
            case FULL -> { Set<Object> s = new LinkedHashSet<>(l.keySet()); s.addAll(r.keySet()); yield s; }
            default -> Collections.emptySet();
        };
    }

    // ==================== 笛卡尔积 ====================

    private void crossJoinAndOutput(VectorSchemaRoot left, VectorSchemaRoot right, int pid) {
        int lRows = left.getRowCount(), rRows = right.getRowCount();
        if (lRows == 0 || rRows == 0) return;
        long total = (long) lRows * rRows;
        int lCols = left.getFieldVectors().size(), rCols = right.getFieldVectors().size();

        List<Field> outFields = new ArrayList<>(left.getSchema().getFields());
        outFields.addAll(right.getSchema().getFields());
        if (outputFields == null) outputFields = outFields;

        long offset = 0;
        while (offset < total) {
            int batch = (int) Math.min(OUTPUT_BATCH_SIZE, total - offset);

            // 左列：用源向量创建 TransferPair 得到完全兼容的目标向量
            List<TransferPair> ltps = new ArrayList<>();
            List<FieldVector> lOut = new ArrayList<>();
            for (int c = 0; c < lCols; c++) {
                TransferPair tp = left.getVector(c).makeTransferPair(
                        left.getVector(c).getField().createVector(allocator));
                tp.getTo().setInitialCapacity(batch);
                tp.getTo().allocateNew();
                ltps.add(tp);
                lOut.add((FieldVector) tp.getTo());
            }

            // 右列
            List<TransferPair> rtps = new ArrayList<>();
            List<FieldVector> rOut = new ArrayList<>();
            for (int c = 0; c < rCols; c++) {
                TransferPair tp = right.getVector(c).makeTransferPair(
                        right.getVector(c).getField().createVector(allocator));
                tp.getTo().setInitialCapacity(batch);
                tp.getTo().allocateNew();
                rtps.add(tp);
                rOut.add((FieldVector) tp.getTo());
            }

            for (int i = 0; i < batch; i++) {
                int pi = (int) (offset + i), li = pi / rRows, ri = pi % rRows;
                for (TransferPair tp : ltps) tp.copyValueSafe(li, i);
                for (TransferPair tp : rtps) tp.copyValueSafe(ri, i);
            }

            List<FieldVector> allOut = new ArrayList<>(lOut);
            allOut.addAll(rOut);
            for (FieldVector v : allOut) v.setValueCount(batch);
            emit(Page.partitioned(new VectorSchemaRoot(outFields, allOut, batch), pid));
            offset += batch;
        }
    }

    private void outputOrphan(VectorSchemaRoot left, VectorSchemaRoot right,
                               int nullLeftCols, int nullRightCols, int pid) {
        int rows = left != null ? left.getRowCount() : right.getRowCount();
        if (rows == 0) return;

        // 构建输出 schema
        List<Field> outFields = List.of();
        if (left != null && right != null) {
            outFields = new ArrayList<>(left.getSchema().getFields());
            outFields.addAll(right.getSchema().getFields());
        } else if (left != null) {
            outFields = new ArrayList<>(left.getSchema().getFields());
            for (int i = 0; i < nullRightCols; i++) outFields.add(outputFields.get(outFields.size()));
        } else {
            for (int i = 0; i < nullLeftCols; i++) outFields.add(outputFields.get(i));
            outFields.addAll(right.getSchema().getFields());
        }

        int offset = 0;
        while (offset < rows) {
            int batch = Math.min(OUTPUT_BATCH_SIZE, rows - offset);
            List<FieldVector> out = new ArrayList<>();

            if (left != null) {
                for (int c = 0; c < left.getFieldVectors().size(); c++) {
                    FieldVector dst = left.getVector(c).getField().createVector(allocator);
                    dst.setInitialCapacity(batch); dst.allocateNew();
                    out.add(dst);
                    TransferPair tp = left.getVector(c).makeTransferPair(dst);
                    for (int r = 0; r < batch; r++) tp.copyValueSafe(offset + r, r);
                }
            } else {
                for (int c = 0; c < nullLeftCols; c++) {
                    Field f = outputFields != null ? outputFields.get(c) : null;
                    FieldVector v = (f != null ? f.createVector(allocator) : null);
                    if (v != null) {
                        v.setInitialCapacity(batch); v.allocateNew();
                        for (int r = 0; r < batch; r++) v.setNull(r);
                    }
                    out.add(v);
                }
            }
            if (right != null) {
                for (int c = 0; c < right.getFieldVectors().size(); c++) {
                    FieldVector dst = right.getVector(c).getField().createVector(allocator);
                    dst.setInitialCapacity(batch); dst.allocateNew();
                    out.add(dst);
                    TransferPair tp = right.getVector(c).makeTransferPair(dst);
                    for (int r = 0; r < batch; r++) tp.copyValueSafe(offset + r, r);
                }
            } else {
                int startCol = left != null ? left.getFieldVectors().size() : nullLeftCols;
                for (int c = 0; c < nullRightCols; c++) {
                    Field f = outputFields != null ? outputFields.get(startCol + c) : null;
                    FieldVector v = (f != null ? f.createVector(allocator) : null);
                    if (v != null) {
                        v.setInitialCapacity(batch); v.allocateNew();
                        for (int r = 0; r < batch; r++) v.setNull(r);
                    }
                    out.add(v);
                }
            }
            for (FieldVector v : out) v.setValueCount(batch);
            emit(Page.partitioned(new VectorSchemaRoot(outFields, out, batch), pid));
            offset += batch;
        }
    }

    // ==================== 行工具 ====================

    private VectorSchemaRoot loadAndMerge(List<Segment> segs, Map<Integer, VectorSchemaRoot> loaded) {
        List<VectorSchemaRoot> roots = new ArrayList<>();
        for (Segment seg : segs) {
            VectorSchemaRoot r = loaded.get(seg.batchIndex());
            if (r == null) continue;
            VectorSchemaRoot part = extractRows(r, seg.startRow(), seg.endRow());
            if (part != null) roots.add(part);
        }
        VectorSchemaRoot merged = mergeRoots(roots);
        // ★ 如果 merged 就是 roots 里某一个 (如只有 1 个 root) 不要关它
        for (VectorSchemaRoot r : roots) {
            if (r != merged) r.close();
        }
        return merged;
    }

    private VectorSchemaRoot extractRows(VectorSchemaRoot root, int s, int e) {
        int n = e - s;
        if (n <= 0) return null;
        List<Field> fields = root.getSchema().getFields();
        List<FieldVector> out = new ArrayList<>();
        List<TransferPair> tps = new ArrayList<>();
        for (Field f : fields) {
            FieldVector v = f.createVector(allocator); v.setInitialCapacity(n); v.allocateNew();
            out.add(v);
            tps.add(root.getVector(out.size() - 1).makeTransferPair(v));
        }
        for (int i = 0; i < n; i++)
            for (TransferPair tp : tps) tp.copyValueSafe(s + i, i);
        for (FieldVector v : out) v.setValueCount(n);
        return new VectorSchemaRoot(fields, out, n);
    }

    private VectorSchemaRoot mergeRoots(List<VectorSchemaRoot> roots) {
        if (roots.isEmpty()) return null;
        if (roots.size() == 1) return roots.getFirst();
        int total = roots.stream().mapToInt(VectorSchemaRoot::getRowCount).sum();
        if (total == 0) return null;
        Schema schema = roots.getFirst().getSchema();
        List<Field> fields = schema.getFields();
        List<FieldVector> out = new ArrayList<>();
        for (Field f : fields) {
            FieldVector v = f.createVector(allocator); v.setInitialCapacity(total); v.allocateNew();
            out.add(v);
        }
        int pos = 0;
        for (VectorSchemaRoot r : roots) {
            for (int col = 0; col < fields.size(); col++) {
                TransferPair tp = r.getVector(col).makeTransferPair(out.get(col));
                for (int row = 0; row < r.getRowCount(); row++) tp.copyValueSafe(row, pos);
            }
            pos += r.getRowCount();
        }
        for (FieldVector v : out) v.setValueCount(total);
        return new VectorSchemaRoot(fields, out, total);
    }

    // ==================== Arrow IO ====================

    private VectorSchemaRoot loadBatch(PartitionState<RowAccumulator> pa, int batchIndex) {
        // 与流式路径同口径：读回文件的全部 record block，否则多 block 的 compacted 文件会被截断
        return loadArrowFile(pa.batchFiles.get(batchIndex), allocator);
    }

    // ==================== 工具 ====================

    private void buildOutputSchema() {
        if (leftSchema == null) {
            throw new IllegalStateException(
                    "Left sort schema not captured — no data flowed through left side. " +
                    "Ensure JVM flag: --add-opens=java.base/java.nio=ALL-UNNAMED");
        }
        if (rightSchema == null) {
            throw new IllegalStateException(
                    "Right sort schema not captured — no data flowed through right side. " +
                    "Ensure JVM flag: --add-opens=java.base/java.nio=ALL-UNNAMED");
        }
        outputFields = new ArrayList<>(leftSchema.getFields());
        outputFields.addAll(rightSchema.getFields());
    }

    private void sortKeys(List<Object> keys) {
        keys.sort((a, b) -> {
            if (a == null && b == null) return 0;
            if (a == null) return -1;
            if (b == null) return 1;
            if (a instanceof List<?> la && b instanceof List<?> lb) {
                int cmp = ArrowComparators.compareLists(la, lb);
                if (cmp != 0) return cmp;
            }
            if (a instanceof Comparable<?> ca && b instanceof Comparable<?> cb) {
                @SuppressWarnings({"rawtypes", "unchecked"})
                int cmp = ((Comparable) ca).compareTo(cb);
                if (cmp != 0) return cmp;
            }
            if (a instanceof WindowSortOperator.CompositeKey cka
                    && b instanceof WindowSortOperator.CompositeKey ckb)
                return compareComposite(cka, ckb);
            return a.toString().compareTo(b.toString());
        });
    }

    private static int compareComposite(WindowSortOperator.CompositeKey a,
                                         WindowSortOperator.CompositeKey b) {
        Object[] av = a.values(), bv = b.values();
        int len = Math.min(av.length, bv.length);
        for (int i = 0; i < len; i++) {
            Object va = av[i], vb = bv[i];
            boolean na = isNullKey(va), nb = isNullKey(vb);
            if (na && nb) continue;
            if (na) return -1;
            if (nb) return 1;
            if (va instanceof Comparable<?> ca && vb instanceof Comparable<?> cb) {
                @SuppressWarnings({"rawtypes", "unchecked"})
                int cmp = ((Comparable) ca).compareTo(cb);
                if (cmp != 0) return cmp;
            } else if (va instanceof List<?> la && vb instanceof List<?> lb) {
                int cmp = ArrowComparators.compareLists(la, lb);
                if (cmp != 0) return cmp;
            }
        }
        return av.length - bv.length;
    }

    private static boolean isNullKey(Object v) {
        return v == null || v == RowAccumulator.NULL_KEY;
    }

    private int[] collectActivePartitions() {
        int max = Math.max(leftSort.getPartitionCount(), rightSort.getPartitionCount());
        List<Integer> act = new ArrayList<>();
        for (int p = 0; p < max; p++)
            if (leftSort.getPartitionedData(p) != null || rightSort.getPartitionedData(p) != null)
                act.add(p);
        int[] r = new int[act.size()];
        for (int i = 0; i < act.size(); i++) r[i] = act.get(i);
        return r;
    }


    private static void forEachSeg(List<Segment> segs, Consumer<Segment> fn) {
        if (segs != null) for (Segment s : segs) fn.accept(s);
    }

    // ===================================================================
    // 跨 key 输出缓冲：攒满 OUTPUT_BATCH_SIZE 行再 flush
    // ===================================================================

    private class JoinOutputBuffer {
        private final int pid;
        private final List<FieldVector> outVectors;
        private int filled;

        JoinOutputBuffer(int pid) {
            this.pid = pid;
            this.outVectors = new ArrayList<>();
            this.filled = 0;
        }

        /**
         * 直接从 loaded batch + segments 拷贝到输出缓冲，不分配临时 VectorSchemaRoot。
         * 跳过 extractRows + mergeRoots，消除 per-key 的向量分配/销毁开销。
         */
        void appendCrossJoinSegments(Map<Integer, VectorSchemaRoot> leftLoaded,
                                      List<Segment> lSegs,
                                      Map<Integer, VectorSchemaRoot> rightLoaded,
                                      List<Segment> rSegs) {
            for (Segment ls : lSegs) {
                VectorSchemaRoot lRoot = leftLoaded.get(ls.batchIndex());
                if (lRoot == null) continue;
                for (int li = ls.startRow(); li < ls.endRow(); li++) {
                    for (Segment rs : rSegs) {
                        VectorSchemaRoot rRoot = rightLoaded.get(rs.batchIndex());
                        if (rRoot == null) continue;
                        for (int ri = rs.startRow(); ri < rs.endRow(); ri++) {
                            if (filled >= OUTPUT_BATCH_SIZE) flush();
                            if (outVectors.isEmpty()) initOutputVectors(lRoot, rRoot);
                            copyRow(lRoot, li, rRoot, ri);
                            filled++;
                        }
                    }
                }
            }
        }

        // ---- 初始化 + 快速拷贝 ----

        private int lCols, rCols;
        private int[] lWidths, rWidths;
        private TransferPair[] lTPs, rTPs;  // 变长列的 TransferPair 缓存（避免逐行 makeTransferPair）
        // ★每个变长列 TP 当前绑定的源向量：跨溢写批文件时向量对象会变，变了必须重绑（见 copyCell）
        private FieldVector[] lTpSrc, rTpSrc;

        private void initOutputVectors(VectorSchemaRoot lRoot, VectorSchemaRoot rRoot) {
            lCols = lRoot.getFieldVectors().size();
            rCols = rRoot.getFieldVectors().size();
            lWidths = new int[lCols]; lTPs = new TransferPair[lCols]; lTpSrc = new FieldVector[lCols];
            rWidths = new int[rCols]; rTPs = new TransferPair[rCols]; rTpSrc = new FieldVector[rCols];
            for (int c = 0; c < lCols; c++) {
                FieldVector src = lRoot.getVector(c);
                FieldVector v = src.getField().createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                int tw = RowAccumulator.typeWidthOf(src.getField().getType());
                lWidths[c] = tw;
                if (tw <= 0) { lTPs[c] = src.makeTransferPair(v); lTpSrc[c] = src; }  // 缓存变长列 TP
            }
            for (int c = 0; c < rCols; c++) {
                FieldVector src = rRoot.getVector(c);
                FieldVector v = src.getField().createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                int tw = RowAccumulator.typeWidthOf(src.getField().getType());
                rWidths[c] = tw;
                if (tw <= 0) { rTPs[c] = src.makeTransferPair(v); rTpSrc[c] = src; }
            }
        }

        private void copyRow(VectorSchemaRoot lRoot, int li, VectorSchemaRoot rRoot, int ri) {
            for (int c = 0; c < lCols; c++) copyCell(lRoot.getVector(c), outVectors.get(c), lWidths[c], li, lTPs, lTpSrc, c);
            for (int c = 0; c < rCols; c++) copyCell(rRoot.getVector(c), outVectors.get(lCols + c), rWidths[c], ri, rTPs, rTpSrc, c);
        }

        // ---- FileIterator 重载（索引模式不需要 VectorSchemaRoot） ----

        private void initOutputVectors(FileIterator left, FileIterator right) {
            lCols = left.cols();
            rCols = right.cols();
            lWidths = new int[lCols]; lTPs = new TransferPair[lCols]; lTpSrc = new FieldVector[lCols];
            rWidths = new int[rCols]; rTPs = new TransferPair[rCols]; rTpSrc = new FieldVector[rCols];
            for (int c = 0; c < lCols; c++) {
                FieldVector src = left.getVector(c);
                FieldVector v = src.getField().createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                int tw = RowAccumulator.typeWidthOf(src.getField().getType());
                lWidths[c] = tw;
                if (tw <= 0) { lTPs[c] = src.makeTransferPair(v); lTpSrc[c] = src; }
            }
            for (int c = 0; c < rCols; c++) {
                FieldVector src = right.getVector(c);
                FieldVector v = src.getField().createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                int tw = RowAccumulator.typeWidthOf(src.getField().getType());
                rWidths[c] = tw;
                if (tw <= 0) { rTPs[c] = src.makeTransferPair(v); rTpSrc[c] = src; }
            }
        }

        private void copyRow(FileIterator left, int li, FileIterator right, int ri) {
            for (int c = 0; c < lCols; c++) copyCell(left.getVector(c), outVectors.get(c), lWidths[c], li, lTPs, lTpSrc, c);
            for (int c = 0; c < rCols; c++) copyCell(right.getVector(c), outVectors.get(lCols + c), rWidths[c], ri, rTPs, rTpSrc, c);
        }

        private void initFromLeft(FileIterator left) {
            lCols = left.cols();
            rCols = rightSchema.getFields().size();
            lWidths = new int[lCols]; lTPs = new TransferPair[lCols]; lTpSrc = new FieldVector[lCols];
            rWidths = new int[rCols]; rTPs = new TransferPair[rCols]; rTpSrc = new FieldVector[rCols];
            for (int c = 0; c < lCols; c++) {
                FieldVector src = left.getVector(c);
                FieldVector v = src.getField().createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                int tw = RowAccumulator.typeWidthOf(src.getField().getType());
                lWidths[c] = tw;
                if (tw <= 0) { lTPs[c] = src.makeTransferPair(v); lTpSrc[c] = src; }
            }
            for (int c = 0; c < rCols; c++) {
                Field f = rightSchema.getFields().get(c);
                FieldVector v = f.createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                rWidths[c] = RowAccumulator.typeWidthOf(f.getType());
            }
        }

        private void initFromRight(FileIterator right) {
            lCols = leftSchema.getFields().size();
            rCols = right.cols();
            lWidths = new int[lCols]; lTPs = new TransferPair[lCols]; lTpSrc = new FieldVector[lCols];
            rWidths = new int[rCols]; rTPs = new TransferPair[rCols]; rTpSrc = new FieldVector[rCols];
            for (int c = 0; c < lCols; c++) {
                Field f = leftSchema.getFields().get(c);
                FieldVector v = f.createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                lWidths[c] = RowAccumulator.typeWidthOf(f.getType());
            }
            for (int c = 0; c < rCols; c++) {
                FieldVector src = right.getVector(c);
                FieldVector v = src.getField().createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                int tw = RowAccumulator.typeWidthOf(src.getField().getType());
                rWidths[c] = tw;
                if (tw <= 0) { rTPs[c] = src.makeTransferPair(v); rTpSrc[c] = src; }
            }
        }

        void appendMatchedRow(FileIterator left, int li, FileIterator right, int ri) {
            if (filled >= OUTPUT_BATCH_SIZE) flush();
            if (outVectors.isEmpty()) initOutputVectors(left, right);
            copyRow(left, li, right, ri);
            filled++;
        }

        void appendLeftOrphanRow(FileIterator left, int li) {
            if (filled >= OUTPUT_BATCH_SIZE) flush();
            if (outVectors.isEmpty()) initFromLeft(left);
            for (int c = 0; c < lCols; c++)
                copyCell(left.getVector(c), outVectors.get(c), lWidths[c], li, lTPs, lTpSrc, c);
            for (int c = 0; c < rCols; c++) outVectors.get(lCols + c).setNull(filled);
            filled++;
        }

        void appendRightOrphanRow(FileIterator right, int ri) {
            if (filled >= OUTPUT_BATCH_SIZE) flush();
            if (outVectors.isEmpty()) initFromRight(right);
            for (int c = 0; c < lCols; c++) outVectors.get(c).setNull(filled);
            for (int c = 0; c < rCols; c++)
                copyCell(right.getVector(c), outVectors.get(lCols + c), rWidths[c], ri, rTPs, rTpSrc, c);
            filled++;
        }

        private void copyCell(FieldVector src, FieldVector dst, int tw, int srcIdx,
                              TransferPair[] tps, FieldVector[] tpSrc, int c) {
            if (tw > 0) {
                dst.getDataBuffer().setBytes((long) filled * tw,
                        src.getDataBuffer(), (long) srcIdx * tw, tw);
                // validity 在 init* 中已预初始化为全 1，只对 null 行显式清零
                if (src.isNull(srcIdx)) dst.setNull(filled);
            } else {
                // 变长列：缓存的 TransferPair 绑定的是「建缓存那一刻」的源向量。输出缓冲会跨多个溢写
                // 批文件累积（一个 flush 窗口内首次见到的 root 决定初始化），后续行的 src 可能来自另一个
                // 批文件 —— 旧 TP 会从旧批文件读取 ⇒ 越界(IOOBE)或静默读错行。故按 src 身份惰性重绑
                // （src 仅在跨批时变化，重绑代价可忽略）。与 BroadcastHashJoinOperator.copyCell 同口径。
                TransferPair tp = tps[c];
                if (tp == null || tpSrc[c] != src) {
                    tp = src.makeTransferPair(dst);
                    tps[c] = tp;
                    tpSrc[c] = src;
                }
                tp.copyValueSafe(srcIdx, filled);
            }
        }

        /** 将向量的 validity 缓冲区全部初始化为 1（非 null），用 8 字节块快速填充 */
        private static void initValidityAllValid(FieldVector v) {
            org.apache.arrow.memory.ArrowBuf vb = v.getValidityBuffer();
            if (vb == null) return;
            long cap = vb.capacity();
            long pos = 0;
            while (pos + 8 <= cap) {
                vb.setLong(pos, -1L);
                pos += 8;
            }
            while (pos < cap) {
                vb.setByte(pos, (byte) 0xFF);
                pos++;
            }
        }

        // ---- 流式 join 的单行追加 ----

        void appendMatchedRow(VectorSchemaRoot lRoot, int li,
                              VectorSchemaRoot rRoot, int ri) {
            if (filled >= OUTPUT_BATCH_SIZE) flush();
            if (outVectors.isEmpty()) initOutputVectors(lRoot, rRoot);
            copyRow(lRoot, li, rRoot, ri);
            filled++;
        }

        void appendLeftOrphanRow(VectorSchemaRoot lRoot, int li) {
            if (filled >= OUTPUT_BATCH_SIZE) flush();
            if (outVectors.isEmpty()) initFromLeft(lRoot);
            for (int c = 0; c < lCols; c++)
                copyCell(lRoot.getVector(c), outVectors.get(c), lWidths[c], li, lTPs, lTpSrc, c);
            // 右侧列显式设为 NULL (Arrow allocateNew 默认 validity=全1, 不是全0!)
            for (int c = 0; c < rCols; c++) outVectors.get(lCols + c).setNull(filled);
            filled++;
        }

        void appendRightOrphanRow(VectorSchemaRoot rRoot, int ri) {
            if (filled >= OUTPUT_BATCH_SIZE) flush();
            if (outVectors.isEmpty()) initFromRight(rRoot);
            // 左侧列设为 NULL
            for (int c = 0; c < lCols; c++) outVectors.get(c).setNull(filled);
            for (int c = 0; c < rCols; c++)
                copyCell(rRoot.getVector(c), outVectors.get(lCols + c), rWidths[c], ri, rTPs, rTpSrc, c);
            filled++;
        }

        /** 从左孤儿行的实际 root 创建输出向量（只有左列是实际数据，右列从 schema） */
        private void initFromLeft(VectorSchemaRoot lRoot) {
            lCols = lRoot.getFieldVectors().size();
            rCols = rightSchema.getFields().size();
            lWidths = new int[lCols]; lTPs = new TransferPair[lCols]; lTpSrc = new FieldVector[lCols];
            rWidths = new int[rCols]; rTPs = new TransferPair[rCols]; rTpSrc = new FieldVector[rCols];
            for (int c = 0; c < lCols; c++) {
                FieldVector src = lRoot.getVector(c);
                FieldVector v = src.getField().createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                int tw = RowAccumulator.typeWidthOf(src.getField().getType());
                lWidths[c] = tw;
                if (tw <= 0) { lTPs[c] = src.makeTransferPair(v); lTpSrc[c] = src; }
            }
            for (int c = 0; c < rCols; c++) {
                Field f = rightSchema.getFields().get(c);
                FieldVector v = f.createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                rWidths[c] = RowAccumulator.typeWidthOf(f.getType());
            }
        }

        /** 从右孤儿行的实际 root 创建输出向量（只有右列是实际数据，左列从 schema） */
        private void initFromRight(VectorSchemaRoot rRoot) {
            lCols = leftSchema.getFields().size();
            rCols = rRoot.getFieldVectors().size();
            lWidths = new int[lCols]; lTPs = new TransferPair[lCols]; lTpSrc = new FieldVector[lCols];
            rWidths = new int[rCols]; rTPs = new TransferPair[rCols]; rTpSrc = new FieldVector[rCols];
            for (int c = 0; c < lCols; c++) {
                Field f = leftSchema.getFields().get(c);
                FieldVector v = f.createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                lWidths[c] = RowAccumulator.typeWidthOf(f.getType());
            }
            for (int c = 0; c < rCols; c++) {
                FieldVector src = rRoot.getVector(c);
                FieldVector v = src.getField().createVector(allocator);
                v.setInitialCapacity(OUTPUT_BATCH_SIZE); v.allocateNew();
                initValidityAllValid(v);
                outVectors.add(v);
                int tw = RowAccumulator.typeWidthOf(src.getField().getType());
                rWidths[c] = tw;
                if (tw <= 0) { rTPs[c] = src.makeTransferPair(v); rTpSrc[c] = src; }
            }
        }

        void flushIfNeeded() {
            if (filled >= OUTPUT_BATCH_SIZE) flush();
        }

        void flush() {
            if (outVectors.isEmpty() || filled == 0) return;
            for (FieldVector v : outVectors) v.setValueCount(filled);
            List<Field> fields = new ArrayList<>(outVectors.size());
            for (FieldVector v : outVectors) fields.add(v.getField());
            emit(Page.partitioned(
                    new VectorSchemaRoot(fields, new ArrayList<>(outVectors), filled), pid));
            outVectors.clear();
            filled = 0;
        }

        /**
         * 丢弃在途输出批（取消 / 失败路径）—— 这些向量不属于任何 Page，也不登记在分区状态里，
         * 不走 flush 就必须显式关闭，否则「停止任务」后这块 Arrow 内存永不释放。
         */
        void discard() {
            for (FieldVector v : outVectors) {
                try { v.close(); } catch (Exception ignored) {}
            }
            outVectors.clear();
            filled = 0;
        }
    }
}
