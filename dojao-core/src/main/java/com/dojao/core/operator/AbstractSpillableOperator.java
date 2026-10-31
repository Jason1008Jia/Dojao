package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import com.dojao.spi.util.NestedTypeSupport;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.holders.NullableTimeStampMicroTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampSecTZHolder;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;

/**
 * 三槽异步溢写算子基类 —— 抽取 {@link HashAggregationOperator} 和
 * {@link WindowSortOperator} 共有管线。
 *
 * <h3>子类需实现的差异点</h3>
 * <ol>
 *   <li>{@link #createSlot(BufferAllocator)} — 创建槽位</li>
 *   <li>{@link #addRow(Slot, Page, int)} — 单行数据写入槽位（accumulate 或 append）</li>
 *   <li>{@link #mergeAndOutput(PartitionState, int)} — Phase 2 溢写归并 + 输出</li>
 *   <li>{@link #outputMemory(PartitionState, int)} — Phase 2 内存路径输出</li>
 *   <li>{@link #()} — 分组键列名</li>
 *   <li>{@link #resolveGroupKeyFields(Schema)} — 从 schema 解析键字段</li>
 * </ol>
 *
 * @param <S> 槽位类型（AggregationHashTable 或 RowAccumulator）
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/22
 */
public abstract class AbstractSpillableOperator<S extends Slot>
        extends AbstractMeasurableOperator implements OperatorMemoryHandle, MergeProgress {

    private static final Logger log = LoggerFactory.getLogger(AbstractSpillableOperator.class);

    // ==================== 常量 ====================

    /** 聚合算子 Phase 2 每批聚合的 key 数（query.agg.perBatchKeys 可配）：collect-evaluate 每批交付给 Python 的 key 数。 */
    protected static final int AGG_PER_BATCH_KEYS = ResourceConfig.aggPerBatchKeys();
    protected static final int SLOT_COUNT = 3;
    /** 溢写/拉取批次不在此处定义：聚合用 query.agg.spill.perBatchKeys，窗口用 query.window.spill.perBatchRows（写）与 query.window.spill.fetchPerKeys（读），各子类独立。 */
    protected static final int DEFAULT_PARTITIONS = 4;
    /**
     * Phase 2 输出中转队列容量（页数）。有界 ⇒ 生产者（归并任务）满了阻塞 = 背压传到下游，
     * 峰值内存 = 容量 × 页大小，<b>与结果总行数无关</b>。
     */
    private static final int OUTPUT_QUEUE_CAPACITY = 8;

    /** 同 JVM 内算子实例序号，配合 PID 保证溢写目录跨进程、跨实例唯一。 */
    private static final AtomicLong INSTANCE_SEQUENCE = new AtomicLong();

    /**
     * 归并进度 tick（{@link MergeProgress}）：Phase 2 每推进一步自增。心跳线程据此判断「不吐页但仍在
     * 归并」的算子未挂起 —— 否则长归并（远超上游收页结束）会被 Master 结果看门狗误杀。
     */
    private final AtomicLong mergeTicks = new AtomicLong();

    /** 溢写根目录轮转游标：query.spill.dir 配了多目录时，逐个实例摊到不同盘。 */
    private static final AtomicInteger SPILL_ROOT_SEQUENCE = new AtomicInteger();

    // ==================== 实例字段 ====================

    protected Operator upstream;
    protected final long memoryLimit;
    protected final long slotBytesThreshold;
    protected final BufferAllocator allocator;
    protected final int numPartitions;

    @SuppressWarnings("unchecked")
    protected PartitionState<S>[] partitions;

    protected Schema inputSchema;
    protected List<Field> groupKeyFields;
    protected List<String> groupKeyColumns;

    protected boolean upstreamExhausted;
    /**
     * Phase 2 是否已启动（旧字段 {@code mergePhaseDone} 的「已跑完」语义在流式改造后不再成立 ——
     * 归并边跑边吐页，不能等「全部完成」才认为 Phase 2 结束，见 {@link #doNext()}）。
     */
    protected boolean phase2Started;
    /**
     * Phase 2 输出页的<b>有界</b>中转队列。归并任务（生产者）{@link #emitPage} 入队，消费者
     * （{@link #doNext()}）并发排空；队列满则生产者阻塞 ⇒ 峰值内存 = 队列容量 × 页大小，与结果
     * 总行数无关。
     *
     * <p><b>为什么必须改</b>：旧实现是无界 {@code ConcurrentLinkedQueue}，且「全部分区归并完才吐
     * 第一页」（{@code f.get(120s)}）⇒ 整个结果集（可上亿行）整份停在算子堆外内存里，释放与下游
     * 消费速度脱钩，正是 worker 内存不释放、机器被拖死的根因。</p>
     */
    protected final BlockingQueue<Page> outputQueue;
    /** Phase 2 仍在跑的归并任务数（归零且队列排空 = 真 EOF）。 */
    private final AtomicInteger phase2Remaining = new AtomicInteger();
    /** 首个分区归并失败（fail-fast：绝不静默当成功吐残缺结果）。 */
    private final AtomicReference<Throwable> phase2Failure = new AtomicReference<>();
    /**
     * Phase 2 生产者<b>被背压挡住</b>的累计时长（{@link #emitPage} 里 {@code offer} 超时的部分）。
     * 排障用：该值占 Phase 2 大头 ⇒ 瓶颈在<b>下游消费</b>（结果回传链路），不在读/归并本身；
     * 该值很小而 Phase 2 仍很慢 ⇒ 瓶颈在本算子的归并/磁盘。默认不打印（仅 &gt;0 时于 {@code doClose} 打一行）。
     */
    private final AtomicLong emitBlockNanos = new AtomicLong();
    /** 已关闭标记：生产者 / 消费者的阻塞等待据此退出（取消 / 失败后无人排空队列）。 */
    protected volatile boolean closed;
    protected ExecutorService spillExecutor;

    /**
     * Phase 2 分区归并线程池（每次 Phase 2 新建、提交完立即 shutdown；保留引用供 {@link #doClose()} 收尾）。
     *
     * <p><b>为什么必须留字段</b>：取消路径下 {@link #doNext()} 在 {@code outputQueue.poll()} 收到
     * {@code InterruptedException} 就立刻抛出返回，<b>已提交的归并任务还在跑</b>；而调用方
     * （{@code FragmentExecutor.runAndStream}）的 finally 紧接着调 {@code close()} —— 后者靠
     * {@code closed} 立旗 + {@code shutdownNow()} 收拢这些任务。若此时直接清空
     * 槽位状态（{@code groupStates} / {@code keyVectors}）并关掉分配器，正在跑的
     * {@code buildOutputBatches} 就会读空列表 → {@code Index 125238 out of bounds for length 0}
     * （实测集群日志），半成品批次 / 已入队的页无人回收 → Arrow 泄漏（{@code Memory was leaked by query}）。</p>
     */
    private volatile ExecutorService phase2Executor;

    /**
     * 当前溢写目录（{@code <root>/<spillDirName>}）。<b>可变</b>：所在盘达到使用率上限时，
     * {@link #spillDirForWrite()} 会换到 {@code query.spill.dir} 里另一块合格的盘（见 {@link SpillDirs}）。
     */
    protected volatile Path spillDir;

    /** 本实例的目录名（{@code <算子类型>-p<PID>-<实例序号>}）；换盘时只换父目录，名字不变。 */
    protected final String spillDirName;

    /** 本实例用过的全部溢写实例子目录（换盘后不止一个）—— 关闭时逐个删干净。 */
    protected final Set<Path> spillDirsUsed = ConcurrentHashMap.newKeySet();

    /**
     * 溢写字节转发回调（可空）。包装算子（{@link SortMergeJoinOperator} 汇总两侧
     * {@link JoinSortOperator}）据此把子算子的溢写量计入自己的 {@link OperatorStats} ——
     * 否则监控注册的是包装算子，它的 {@code spilledBytes} 恒 0，而真正的溢写全在子算子里。
     */
    private volatile LongConsumer spillBytesListener;

    /** 累积超内存预算时的溢写策略（默认 {@link #SPILL} 沿用旧行为）。 */
    public enum SpillPolicy {SPILL, WARN, ERROR}

    /** 本算子实例的溢写策略。默认 SPILL；Planner 对「顶层 ORDER BY 无 LIMIT 的全局排序」设 ERROR/WARN。 */
    protected volatile SpillPolicy spillPolicy = SpillPolicy.SPILL;
    private volatile boolean spillWarned;

    // ==================== 构造函数 ====================

    protected AbstractSpillableOperator(String operatorName, String operatorType, int stages,
                                        Operator upstream, long memoryLimit, int numPartitions) {
        super(operatorName, operatorType, stages);
        this.upstream = upstream;
        this.memoryLimit = memoryLimit;
        // 单槽阈值 = 算子预算 × 溢写比例 ÷ 分区数：本算子同时持有 numPartitions 个分区，每个分区各 3 个槽
        // —— 预算得<b>摊到各分区</b>，否则 P 个分区各按「整个算子预算」累积，总占用 = P × 3槽 × 阈值，
        // 会先撞「按单分区派生的」分配器硬上限（query.direct.limit），此时任一单槽都还没到阈值 ⇒
        // 触发的是 Arrow OOM 而非溢写（表现：row_number() over(partition by …) 直接把 Worker 撑挂）。
        this.slotBytesThreshold =
                (long) (memoryLimit * ResourceConfig.spillRatio() / Math.max(1, numPartitions));
        // 实例序号：溢写目录名要按实例唯一（同 JVM 内多个同类算子并发时，目录重名会互相覆盖/误删）。
        // 子分配器的 Arrow 名字由 NodeMemory 内部按序自增，无需外部保证唯一。
        long instanceSeq = INSTANCE_SEQUENCE.incrementAndGet();
        // Arrow 向量走堆外内存：分配器是<b>进程级共享根</b>（NodeMemory）的子分配器 ——
        // 算子有独立上限（query.direct.limit），但全进程堆外总量受节点预算硬约束；超限抛 OOM
        // （查询失败）而非无限增长被内核 OOM-killer 杀进程。类别按算子类型归组（监控可见）。见 NodeMemory。
        this.allocator = NodeMemory.shared()
                .newOperatorAllocator(getStats().getOperatorType(), ResourceConfig.queryDirectLimit());
        this.numPartitions = numPartitions;
        @SuppressWarnings("unchecked")
        PartitionState<S>[] pa = (PartitionState<S>[]) new PartitionState[numPartitions];
        this.partitions = pa;
        this.outputQueue = new ArrayBlockingQueue<>(OUTPUT_QUEUE_CAPACITY);
        this.upstreamExhausted = false;
        this.phase2Started = false;
        // 溢写根目录：query.spill.dir（默认 <DOJAO_HOME>/spill），配多个时按实例轮转摊到各块盘。
        // spillDir 必须按实例唯一（PID + 实例序号）：若只按 operatorId（=类型名）建目录，同 JVM 内
        // 多个 HashAggregation/WindowSort 实例会互相覆盖溢写文件，且先关闭者递归删除整个共享目录
        // → 其余实例 Phase 2 写文件 NoSuchFileException。
        List<Path> spillRoots = ResourceConfig.spillDirs();
        Path spillRoot = spillRoots.get(Math.floorMod(SPILL_ROOT_SEQUENCE.getAndIncrement(), spillRoots.size()));
        this.spillDirName = getStats().getOperatorId() + "-p" + ProcessHandle.current().pid()
                + "-" + instanceSeq;
        this.spillDir = spillRoot.resolve(spillDirName);
    }

    /** 更新上游算子引用（列裁剪优化使用，须在 {@link #doOpen()} 前调用）。 */
    public void setUpstream(Operator upstream) { this.upstream = upstream; }

    // ==================== 归并进度（MergeProgress） ====================

    /**
     * 归并推进一步：Phase 2 每写完一个输出批 / 每吐出一页 / 每处理完一个 key 批时由子类调用。
     * 多线程安全（Phase 2 分区归并任务并发跑，tick 用原子量）。
     */
    protected void bumpMergeProgress() { mergeTicks.incrementAndGet(); }

    /**
     * 本算子（连同其上游链）的累计归并进度。向上游透传 —— 链式片段（join→窗口/聚合）或
     * 顶层排序尾链里，真正在归并的可能是下游任一个溢写算子，心跳只看链顶即可覆盖全部。
     */
    @Override
    public long mergeProgressTicks() {
        long own = mergeTicks.get();
        Operator up = upstream;
        return up instanceof MergeProgress mp ? own + mp.mergeProgressTicks() : own;
    }

    // ==================== Phase 2 输出（有界中转 + 背压） ====================

    /**
     * 把一页输出放进有界中转队列 —— <b>Phase 2 归并任务（生产者）</b>调用，队列满则阻塞等待消费者
     * 排空（背压）。算子已关闭 / 被取消则丢弃本页并立即释放。
     *
     * <p>与 {@link SortMergeJoinOperator} 的 emit 同构：{@code offer(超时)} + {@code closed}
     * 复查，保证取消路径下生产线程能退出（消费者不再排空时不会无限阻塞）。</p>
     */
    protected void emitPage(Page page) {
        while (!closed) {
            long t0 = System.nanoTime();
            try {
                if (outputQueue.offer(page, 100, TimeUnit.MILLISECONDS)) {
                    emitBlockNanos.addAndGet(System.nanoTime() - t0);
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            // offer 超时 = 队列满 = 下游没排空 ⇒ 这一段就是「背压阻塞」时长
            emitBlockNanos.addAndGet(System.nanoTime() - t0);
        }
        try { page.close(); } catch (Exception ignored) {}
    }

    /**
     * 分区归并失败 → 套上算子级上下文上抛（Error 直通）。
     *
     * <p>旧实现只给一句「Phase 2 分区归并失败」，外层再包成「Operator Limit next failed」——
     * 用户看到的是一整条算子链，<b>看不出失败是哪一类</b>（堆外 OOM / 磁盘写满 / 溢写读回异常），
     * 也就无法判断该调内存、清磁盘还是改 SQL。这里把现场信息直接写进 message。</p>
     */
    private RuntimeException phase2Failed(Throwable t) {
        if (t instanceof Error err) throw err;
        return new Phase2Exception("Phase 2 分区归并失败：" + describePhase2Failure(t), t);
    }

    /**
     * Phase 2 分区归并失败（带现场信息）。
     *
     * <p>单独成型（而不是普通 {@code RuntimeException}）有两个用处：</p>
     * <ul>
     *   <li>{@link AbstractMeasurableOperator#rootCauseSummary} 认得它，把这整条现场信息带进
     *       「Operator Limit next failed: …」外壳 —— 否则上游只把它当普通 cause，退回只显示最深一层；</li>
     *   <li>上层可按类型判断「这是溢写/归并阶段失败」而不用解析字符串（缺陷报告要求保留原始
     *       cause 类型以便降级/重试；原始异常仍完整挂在 {@code getCause()} 上）。</li>
     * </ul>
     */
    public static final class Phase2Exception extends RuntimeException {
        public Phase2Exception(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Phase 2 归并失败的现场信息 —— 让「Operator Limit next failed」这类外壳自带可诊断上下文。
     *
     * <p>内容对齐缺陷报告 §7.1 的要求：</p>
     * <ul>
     *   <li><b>算子</b>：类型 + 溢写目录名（{@code <算子>-p<PID>-<序号>}，与 worker 日志里
     *       {@code [溢写] ... 目录 ...} 那行逐字对应，据此定位到具体实例）；</li>
     *   <li><b>溢写量</b>：累计溢写字节 + 已写批次文件数（判断是否走到了溢写路径、溢写了多少）；</li>
     *   <li><b>磁盘</b>：溢写目录 + 剩余/总空间 + 使用率上限（区分「盘满了」与「内存不够」）；</li>
     *   <li><b>根因</b>：异常链最深一层的类型 + 首行 message（完整栈仍在 cause 里）；</li>
     *   <li><b>处置建议</b>：按根因分类给可操作动作，而不是丢给用户一堆算子名。</li>
     * </ul>
     */
    String describePhase2Failure(Throwable t) {
        StringBuilder sb = new StringBuilder();
        sb.append("operator=").append(getStats().getOperatorId())
                .append(", 实例=").append(spillDirName)
                .append(", pid=").append(ProcessHandle.current().pid())
                .append(", spillDir=").append(spillDir)
                .append("（").append(SpillDirs.describeFreeSpace(spillDir))
                .append("，上限 ").append(SpillDirs.describeUsageLimit()).append("）")
                .append(", spillBytes=").append(formatBytes(getStats().getSpilledBytes()))
                .append(", spillFiles=").append(spillFileCount())
                .append(", 根因=").append(rootCauseSummary(t))
                .append("。").append(failureHint(t));
        return sb.toString();
    }

    /** 本算子各分区已写出的溢写文件总数（含归并重写产生的批次文件）。 */
    private long spillFileCount() {
        long files = 0;
        PartitionState<S>[] pa = partitions;
        if (pa == null) {
            return 0;
        }
        for (PartitionState<S> p : pa) {
            if (p != null) {
                files += p.batchCount.get();
            }
        }
        return files;
    }

    /** 按根因分类给处置建议（各分支都指向「用户能马上做的一件事」）。 */
    private static String failureHint(Throwable t) {
        Throwable root = t;
        for (int depth = 0; root.getCause() != null && root.getCause() != root && depth < 32; depth++) {
            root = root.getCause();
        }
        String msg = root.getMessage() == null ? "" : root.getMessage();
        String cls = root.getClass().getSimpleName();
        if (root instanceof Error || cls.contains("OutOfMemory") || msg.contains("OutOfMemory")
                || msg.contains("Unable to allocate")) {
            return "溢写/归并阶段堆外内存不足（分配失败，不是算错结果）：可调小 "
                    + "query.window.spill.perBatchRows / query.agg.spill.perBatchKeys（单批更小），"
                    + "或调大单算子堆外预算 query.direct.limit；若为 ORDER BY 无 LIMIT 的全量全局排序，"
                    + "优先加 LIMIT 缩小结果集。";
        }
        if (root instanceof SpillDirs.SpillSpaceException
                || msg.contains("No space left on device") || msg.contains("空间不够")) {
            return "溢写磁盘空间不足：清理 spillDir 所在盘，或把 query.spill.dir 指到大盘；"
                    + "若为 ORDER BY 无 LIMIT 的全量全局排序，加 LIMIT 可显著降低溢写量。";
        }
        if (root instanceof java.io.IOException) {
            return "溢写文件读写失败（读回自己写下的溢写段时出错）：请检查 spillDir 的读写权限与盘状态；"
                    + "若为 ORDER BY 无 LIMIT 的全量全局排序，优先加 LIMIT 或缩小数据范围。";
        }
        return "请按「根因」分类处理；若为 ORDER BY 无 LIMIT 的全量全局排序，"
                + "加 LIMIT 或缩小数据范围可规避单分区全量排序。";
    }

    /**
     * 设置累积超内存预算时的策略（须在 {@link #doOpen()} 前调用；默认 {@link SpillPolicy#SPILL}）。
     *
     * <p>Planner 对<b>顶层 ORDER BY 无 LIMIT 的全量全局排序</b>设 ERROR/WARN：此类查询会把整个结果集
     * 收拢到 Master 单点排序，超预算溢写会写满 Master 磁盘、极慢——宁可拒绝并提示加 LIMIT，也不静默溢写。
     * 窗口分区排序 / 哈希聚合 / 带 LIMIT 的 ORDER BY 保持默认 SPILL，不受影响。</p>
     */
    public void setSpillPolicy(SpillPolicy policy) {
        if (policy != null) this.spillPolicy = policy;
    }

    /** 溢写被策略拒绝时的错误文案（含放开的 conf 指引）。 */
    private String describeSpillRejected() {
        return getClass().getSimpleName() + " 累积数据超过内存预算（threshold≈"
                + formatBytes(slotBytesThreshold) + " / memoryLimit=" + formatBytes(memoryLimit)
                + "）但溢写被禁用（spillPolicy=ERROR）。若为顶层 ORDER BY 无 LIMIT 的全量全局排序："
                + "会把整表收拢到 Master 并整表排序，超预算溢写将占满 Master 磁盘、极慢且危险，故默认拒绝。"
                + "请加 LIMIT（如 ORDER BY ... LIMIT n）或缩小数据范围；确需全量排序可在 conf/dojao.conf 设 "
                + "query.sort.topSpillPolicy=warn（告警后溢写）或 spill（静默溢写）。";
    }

    // ==================== 子类抽象方法 ====================

    /** 创建新槽位 */
    protected abstract S createSlot(BufferAllocator allocator);

    /** 单行写入（子类决定是 accumulate 还是 append） */
    protected abstract void addRow(S slot, Page input, int row);

    /** 页前准备（默认 no-op，WindowSort 子类覆盖以初始化 accumulator） */
    protected void prepareSlot(S slot, Page input) {}

    /** 分组键列名 */
    protected abstract List<String> resolveGroupKeyColumns();

    /** 从 schema 解析键字段 */
    protected abstract List<Field> resolveGroupKeyFields(Schema schema);

    /** Phase 2 溢写路径：归并溢写文件 → 输出 */
    protected abstract void mergeAndOutput(PartitionState<S> pa, int pid) throws Exception;

    /** Phase 2 内存路径：从槽位直接输出 */
    protected abstract void outputMemory(PartitionState<S> pa, int pid) throws Exception;

    // ==================== 生命周期 ====================

    /**
     * 取本次溢写的目标目录，并在需要时换盘 —— <b>每次真正写溢写文件前都调用</b>。
     *
     * <p>当前盘使用率合格 → 原样返回（绝大多数情况，代价只有一次 statfs）。超上限
     * （{@code query.spill.disk.maxUsage}，默认 0.8）→ 换 {@code query.spill.dir} 里另一块合格的盘，
     * 换了就打日志并建目录、登记进 {@link #spillDirsUsed}（关闭时连同旧目录一起删）。所有候选盘都
     * 超上限 → 抛 {@link SpillDirs.SpillSpaceException}（查询失败并列出全部候选目录与使用率），
     * 而不是把盘写爆。</p>
     */
    protected Path spillDirForWrite() throws IOException {
        Path dir = spillDir;
        if (SpillDirs.hasRoom(dir)) {
            return dir;
        }
        Path other = SpillDirs.pickUsableDir(dir, spillDirName);
        log.warn("[溢写] {} 当前目录 {} 所在盘已用 {}（上限 {}）→ 换到 {}",
                getStats().getOperatorId(), dir, SpillDirs.describeUsage(dir),
                SpillDirs.describeUsageLimit(), other);
        Files.createDirectories(other);
        spillDirsUsed.add(other);
        SpillDirs.register(other);
        spillDir = other;
        return other;
    }

    @Override
    protected void doOpen() throws Exception {
        upstream.open();
        this.spillExecutor = Executors.newVirtualThreadPerTaskExecutor();
        Files.createDirectories(spillDir);
        spillDirsUsed.add(spillDir);
        // 登记到 JVM 关闭钩子：Ctrl+C / SIGTERM（systemd stop）时钩子会把这些目录删掉
        SpillDirs.register(spillDir);
        // 溢写目录直接打到日志：用户最常问「溢写到哪个盘了」，一目了然（query.spill.dir 可配）
        log.info("[溢写] {} 目录 {}", getStats().getOperatorId(), spillDir);
        this.groupKeyColumns = resolveGroupKeyColumns();
    }

    @Override
    protected synchronized Page doNext() throws Exception {
        // ====== Phase 1: 消费上游，三缓冲异步溢写 ======
        // synchronized：本算子可能被多线程并发消费（如 HashPartition 的 N 个 distributor
        // 线程共享拉取 → 聚合/窗口累积 + Phase 2 归并 + outputQueue 取页必须串行）。
        // 否则 ensureTransfers 的 keyTransfers.clear()/refill、groupStates.add() 等在并发下
        // 竞争 → IndexOutOfBoundsException（"Index 0 out of bounds for length 0"）。
        // 聚合累积本质串行，锁只让消费线程排队，无死锁（上游 scan 生产线程独立、phase2
        // 任务不重入 next()）。
        while (!upstreamExhausted && outputQueue.isEmpty()) {
            Page input = upstream.next();
            if (input.isEOF()) { upstreamExhausted = true; break; }
            recordInput(input);
            // ★accumulatePage 可能抛（例如 prepareSlot 里的嵌套键/窗口键闸门）——失败路径同样要关页，
            //   否则异常退出后这张页永不释放，分配器关闭时报 Memory was leaked。
            try {
                accumulatePage(input);
            } finally {
                input.close();
                releaseInput(input);
            }
        }

        // ====== Phase 2: 分区级并行归并（流式，不在本方法内等完成） ======
        if (upstreamExhausted && !phase2Started) {
            startPhase2();
        }

        // 边归并边消费：Phase 2 任务把页 emitPage 进有界队列，这里同时排空 —— 数据「流过」本算子，
        // 峰值内存 = 队列容量 × 页大小，与结果总行数无关（旧实现把整份结果攒在无界队列里）。
        while (true) {
            if (closed) return Page.eof();                                 // 已关闭：尽快收手
            Throwable f = phase2Failure.get();
            if (f != null) throw phase2Failed(f);                          // fail-fast：绝不静默少行
            // 生产者全退（remaining 归零是其最后动作 ⇒ 不会再 emit）且队列已空 = 真 EOF。
            // 放在 poll 之前判：重复 next() 收尾时不必每次白等 100ms。
            if (phase2Started && phase2Remaining.get() == 0 && outputQueue.isEmpty()) return Page.eof();
            Page p;
            try {
                p = outputQueue.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                // 被取消（Master 收尾 / 反向 cancel）≠ 失败：poll 会清掉中断标记，上层
                // （FragmentExecutor）靠它区分「取消」与「真失败」⇒ 补回标记后原样上抛。
                Thread.currentThread().interrupt();
                throw ie;
            }
            if (p != null) return p;
        }
    }

    /**
     * 启动 Phase 2 分区归并（每个活跃分区一个归并任务）—— <b>不在本方法内等待完成</b>：任务是生产者，
     * 边归并边 {@link #emitPage} 到有界队列，由 {@link #doNext()} 并发排空。
     *
     * <p>失败记入 {@link #phase2Failure}（不吞），由消费者 fail-fast 抛出；每个任务在 finally 里递减
     * {@link #phase2Remaining}，归零且队列排空即真 EOF。</p>
     */
    private void startPhase2() {
        phase2Started = true;
        long t1 = System.currentTimeMillis();
        System.err.printf("[%s] Phase 2: key-level concurrent merge...%n", getClass().getSimpleName());

        for (int pid = 0; pid < partitions.length; pid++) {
            if (partitions[pid] != null) {
                partitions[pid].awaitPendingSpills();
                // 溢写在别的线程跑，异常没人读 Future ⇒ 必须在这里（产出任何页之前）检查并上抛，
                // 否则失败=静默丢数据（查询"成功"但少行）。Phase 2 是唯一产出源头，此处最稳妥。
                partitions[pid].checkSpillFailure();
                bumpMergeProgress();   // 每等完一个分区的落盘 → 归并推进（长溢写期间的心跳存活信号）
            }
        }
        // ★与监控同一口径：直接读统计数据（Σ dataFileSize 曾是另一套口径，归并重写后会对不上）。
        long spilledBytes = getStats().getSpilledBytes();
        System.err.printf("[%s] Spilled: %s%n", getClass().getSimpleName(),
                spilledBytes > 0 ? formatBytes(spilledBytes) : "0 (all in-memory)");

        int activeCount = 0;
        for (PartitionState<S> pa : partitions) {
            if (pa != null && pa.hasData()) activeCount++;
        }
        phase2Remaining.set(activeCount);
        if (activeCount == 0) {
            // 空输入收尾：默认不产出（分组聚合 / 去重 / 排序空输入本就该是空结果）；
            // 全局聚合（无分组键）必须补一行 —— SQL 语义要求它恒返回 1 行。见 emitEmptyInput。
            try {
                emitEmptyInput(0);
            } catch (Throwable e) {
                phase2Failure.compareAndSet(null, e);
            }
            System.err.printf("[%s] Phase 2 started: 0 partitions, %d ms%n",
                    getClass().getSimpleName(), System.currentTimeMillis() - t1);
            return;
        }

        ExecutorService exec = Executors.newFixedThreadPool(activeCount);
        phase2Executor = exec;
        for (int pid = 0; pid < partitions.length; pid++) {
            final int p = pid;
            PartitionState<S> pa = partitions[pid];
            if (pa == null || !pa.hasData()) continue;
            exec.submit(() -> {
                try {
                    // 分区任务开跑 → 归并推进。子类在归并循环内还有更细粒度的 bump；
                    // 这里兜住「子类未细分」的实现（如 JoinSortOperator），保证心跳不断档。
                    bumpMergeProgress();
                    if (pa.dataFilePath != null) {
                        flushRemainingSlots(pa, p);
                        mergeAndOutput(pa, p);
                    } else {
                        outputMemory(pa, p);
                    }
                    bumpMergeProgress();
                } catch (InterruptedException ie) {
                    // 被取消（shutdownNow / future.cancel(true)）≠ 失败：不记 failure、不打失败日志，
                    // 恢复中断标记后收手（emitPage 内部吞中断，这里兜住子类归并循环抛出的中断）。
                    Thread.currentThread().interrupt();
                } catch (Throwable e) {
                    System.err.printf("[%s] Partition %d failed: %s%n",
                            getClass().getSimpleName(), p, e.getMessage());
                    phase2Failure.compareAndSet(null, e);
                } finally {
                    try { pa.close(); } catch (Exception ignored) {}
                    partitions[p] = null;
                    phase2Remaining.decrementAndGet();   // 最后一个动作：归零即「不会再 emit」
                }
            });
        }
        exec.shutdown();   // 不再接新任务（已提交的照跑）
        System.err.printf("[%s] Phase 2 started: %d partitions, %d ms%n",
                getClass().getSimpleName(), activeCount, System.currentTimeMillis() - t1);
    }

    @Override
    protected void doClose() throws Exception {
        // ★ 先立旗：阻塞在有界队列 emitPage 上的归并任务、以及 doNext() 的 poll 等待都要靠它退出
        //   —— 关闭后无人再排空队列，不立旗生产者会一直等下去（与 SortMergeJoinOperator 同构）。
        closed = true;
        // 排障：Phase 2 有多少时间是「被下游背压挡住」而非自己算/读（见 emitBlockNanos 注释）。
        long blockedMs = emitBlockNanos.get() / 1_000_000;
        if (blockedMs > 0) {
            System.err.printf("[%s] Phase 2 背压阻塞：%dms（生产者等下游排空队列，非本算子在读/归并）%n",
                    getClass().getSimpleName(), blockedMs);
        }
        // ★ 再等 Phase 2 的并行归并任务<b>真正退出</b>（与下面的 spillExecutor 同理，之前只等了溢写、漏了归并）。
        //   取消路径下 doNext 已抛出、归并任务仍在跑；此处若径直 groupStates/keyVectors.clear() +
        //   allocator.close()，正在跑的 buildOutputBatches 会读到空列表
        //   → "Index N out of bounds for length 0"，半成品批次/页泄漏（实测 5.5MB）。故所有状态清理与
        //   allocator.close() 必须排在此之后。
        ExecutorService phase2 = phase2Executor;
        if (phase2 != null) {
            // ★ 先清掉本线程的中断标记再等：取消路径下它<b>恒为真</b>（doNext 抛出前刻意补回的），
            //   而 awaitTermination 底层是 Condition.awaitNanos —— 一见中断标记就立刻抛
            //   InterruptedException，于是「等」退化成「不等」，任务照样被并发清状态（实测复现）。
            boolean interrupted = Thread.interrupted();
            try {
                phase2.shutdownNow();   // 中断仍在跑的归并任务（其分批循环里有中断检查 → 迅速收手）
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (!phase2.awaitTermination(1, TimeUnit.SECONDS)) {
                    if (System.nanoTime() >= deadline) {
                        log.warn("[{}] Phase 2 归并任务 60s 未退出，仍继续关闭（其未回收的批次可能泄漏）",
                                getStats().getOperatorId());
                        break;
                    }
                }
            } catch (InterruptedException ie) {
                // 等待期间又被取消（另一路 interrupt）：不再等待，交给 finally 恢复标记后照常收尾
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
        if (spillExecutor != null) {
            spillExecutor.shutdownNow();
            try { spillExecutor.awaitTermination(5, TimeUnit.SECONDS); } catch (Exception ignored) {}
        }
        Page p;
        while ((p = outputQueue.poll()) != null) {
            try { p.close(); } catch (Exception ignored) {}
        }
        for (PartitionState<S> pa : partitions) {
            if (pa != null) pa.close();
        }
        upstream.close();
        // 清掉本实例用过的全部溢写目录（换过盘就可能有多个），并从关闭钩子名单摘除 ——
        // 算子自己关干净后，钩子不必再走一遍。
        for (Path dir : List.copyOf(spillDirsUsed)) {
            SpillDirs.deleteRecursively(dir);
            SpillDirs.unregister(dir);
        }
        allocator.close();
    }

    // ==================== Phase 1: 累积 & 溢写 ====================

    protected void accumulatePage(Page input) {
        // Worker 级背压：本节点堆外用量吃紧时，先短暂等待（让别的算子溢写把内存还回来）再累积本页。
        // 放在「累积」这个「即将占用更多内存」的入口，绝不放溢写/释放路径（否则无法解除背压）。
        NodeMemory.shared().throttle();

        int pid = input.getPartitionId().orElse(0);

        if (inputSchema == null) {
            inputSchema = input.getRoot().getSchema();
            groupKeyFields = resolveGroupKeyFields(inputSchema);
            // 分组 / 分区 / join 排序键：嵌套列没有定义相等性与排序语义，入口显式拒绝
            NestedTypeSupport.requireSupported(groupKeyFields, getStats().getOperatorId() + " 的分组/排序键");
        }

        ensurePartition(pid);
        PartitionState<S> pa = partitions[pid];
        // 上一轮的异步溢写可能已失败（如磁盘写满）→ 立刻上抛，不再继续吞数据
        pa.checkSpillFailure();

        S active = pa.activeSlot();
        if (active == null) {
            active = createSlot(allocator);
            pa.slots.set(pa.activeIdx, active);
        }
        if (!active.isEmpty() && active.estimateMemoryBytes() >= slotBytesThreshold) {
            // 策略门：ERROR → 拒绝溢写直接抛错；WARN → 告警一次后照常溢写；SPILL（默认）→ 静默溢写
            SpillPolicy policy = spillPolicy;
            if (policy == SpillPolicy.ERROR) {
                throw new IllegalStateException(describeSpillRejected());
            }
            if (policy == SpillPolicy.WARN && !spillWarned) {
                spillWarned = true;
                System.err.printf("[%s] ⚠ 累积数据超过内存预算（threshold≈%s）将开始溢写；若为顶层 ORDER BY "
                        + "无 LIMIT 的全量排序，建议加 LIMIT 或缩小数据量，或设 query.sort.topSpillPolicy=error "
                        + "直接拒绝%n", getClass().getSimpleName(), formatBytes(slotBytesThreshold));
            }
            pa.submitSpill(spillExecutor, this, pid);
            pa.awaitIdleSlot();
            pa.checkSpillFailure();
            active = pa.activeSlot();
            if (active == null) {
                active = createSlot(allocator);
                pa.slots.set(pa.activeIdx, active);
            }
        }
        prepareSlot(active, input);
        active.addPage(input);
    }

    /**
     * 空输入收尾钩子：Phase 1 没有累积到任何数据（上游一页都没有 / 页全空）时，在 Phase 2
     * 开跑前调用一次，仅供算子<b>直接 {@link #emitPage}</b> 补出必须存在的收尾行。
     *
     * <p>默认不产出 —— 分组聚合、去重、排序的空输入本就该是空结果。全局聚合（无 GROUP BY）
     * 必须覆盖它产出一行聚合初值：{@code SELECT count(*) FROM 空输入} 的 SQL 语义是
     * <b>恒 1 行 count=0</b>，而不是 0 行。否则 UNION ALL 里某个分支恰好过滤干净时，
     * 该分支的 0 行会凭空消失（表现为「4 分支只回 3 行」）。</p>
     *
     * @param pid 分区号（全局聚合恒 0）
     */
    protected void emitEmptyInput(int pid) throws Exception {
    }

    protected void ensurePartition(int pid) {
        if (partitions[pid] == null) {
            partitions[pid] = new PartitionState<>(SLOT_COUNT);
        }
    }

    protected void spillSlotSync(PartitionState<S> pa, int slotIdx, S slot, int pid) {
        if (slot.isEmpty()) return;
        VectorSchemaRoot sorted = null;
        try {
            sorted = slot.buildSortedSpillRoot();
            BatchWrite written = writeBatchToArrow(pa, sorted, pid);
            if (maintainSpillIndex()) addToIndex(pa, sorted, written.batchIndex());
            // ★只累加本次文件写入量：直接累加分区累计 dataFileSize 会统计成 Σ(k×file) ≈ O(N²)
            // —— 线上曾出现「磁盘 129MB、监控 10.5GB」。
            accountSpilledBytes(written.bytes());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Arrow batch for partition " + pid, e);
        } finally {
            // 无论成败都释放：写失败时 sorted/slot 若不 close 会泄漏 Arrow 缓冲（曾见 40MB 泄漏）
            if (sorted != null) { try { sorted.close(); } catch (Exception ignored) {} }
            try { slot.close(); } catch (Exception ignored) {}
            pa.slots.set(slotIdx, null);
        }
    }

    /**
     * 是否在溢写时维护 {@link PartitionState#index 段索引}。默认 {@code true}（Phase 2 按 key 随机回读）。
     *
     * <p>哈希聚合的<b>非 collect</b> Phase 2 改用「按 key 有序的 K 路归并」（见
     * {@code HashAggregationOperator.mergeAndOutput}）后不再需要该索引 —— 索引是
     * {@code O(去重键数)} 的堆内结构（CHM 条目 + 键对象 + 段容器），在高基数 GROUP BY 下会先于
     * 槽位状态撑爆 JVM 堆。子类覆盖返回 {@code false} 即可跳过建索引，让内存只随「文件数 × 单键状态」增长。</p>
     */
    protected boolean maintainSpillIndex() { return true; }

    void spillSlotAsync(PartitionState<S> pa, int slotIdx, S slot, int pid) {
        spillSlotSync(pa, slotIdx, slot, pid);
    }

    /**
     * 输出批能安全容纳的行数 = <b>所有列 {@code valueCapacity} 的最小值</b>。
     *
     * <p>★不能只取首列：Arrow 分配器按<b>字节数</b>向上取整，再换算回行数时不同类型的容量并不相等
     * （实测 {@code allocateNew(50000)} 后一次 {@code reAlloc}：TINYINT = 116504 行、INT = 127100、
     * BIGINT = 129055、BIT = 131072）。若首列恰好比窄列宽，用首列当统一闸门就会让
     * {@code copyRow} 在窄列上越界（线上 {@code index: 116504, length: 1 (expected: range(0, 116504))}）。</p>
     */
    protected static int minValueCapacity(List<FieldVector> vectors) {
        int min = Integer.MAX_VALUE;
        for (FieldVector v : vectors) min = Math.min(min, v.getValueCapacity());
        return min == Integer.MAX_VALUE ? 0 : min;
    }

    // ==================== Arrow IPC ====================

    /**
     * 一次溢写文件写入结果。
     *
     * @param batchIndex 批次号（分区内递增）
     * @param bytes      本文件实际字节数 —— ★统计口径必须用「本次写入量」，不能用
     *                   {@link PartitionState#dataFileSize} 的累计值（那会让 N 次溢写累加成
     *                   Σ(k×file) ≈ O(N²)）；也不能用「前后累计值相减」——3 槽位流水线允许
     *                   两个溢写线程并发，非原子的 {@code +=} 会丢更新，差值随之失真。
     */
    protected record BatchWrite(int batchIndex, long bytes) { }

    /**
     * 设置溢写字节转发回调（{@code null} = 清除）。
     *
     * @see #accountSpilledBytes(long)
     */
    public void setSpillBytesListener(LongConsumer listener) {
        this.spillBytesListener = listener;
    }

    /**
     * 记账一次<b>本次实际写入溢写文件</b>的字节数 —— 所有写盘点（三槽溢写、按批写入、
     * 归并 compaction 重写）都必须经过这里，口径恒为「本次写入量」。
     *
     * <p>转发回调让包装算子把子算子的溢写量汇总进自己的统计（如
     * {@link SortMergeJoinOperator} 的两侧 {@link JoinSortOperator}）。</p>
     */
    protected final void accountSpilledBytes(long bytes) {
        if (bytes <= 0) return;
        addSpilledBytes(bytes);
        LongConsumer l = spillBytesListener;
        if (l != null) l.accept(bytes);
    }

    /**
     * 归并（compaction / K 路归并）输出文件写完后记账。
     *
     * <p>归并把多个溢写批重写成 key 连续文件，这同样是真实磁盘写入。此前既不进
     * {@link OperatorStats#getSpilledBytes()} 也不更新 {@link PartitionState#dataFileSize} ⇒
     * 大排序 / 窗口场景下监控「溢写总量」明显偏低（归并往往是 I/O 大头）。</p>
     *
     * <p>★必须在文件关闭后调用（{@code Files.size} 才能拿到完整长度），且只记一次。</p>
     */
    protected void accountCompactionFile(PartitionState<S> pa, Path file) {
        if (pa == null || file == null) return;
        try {
            long bytes = Files.size(file);
            // 并发归并线程共享该字段：同步累加（与 writeBatchToArrow 同口径）。
            synchronized (pa) { pa.dataFileSize += bytes; }
            accountSpilledBytes(bytes);
        } catch (IOException e) {
            log.warn("统计归并输出文件大小失败: {}", file, e);
        }
    }

    protected BatchWrite writeBatchToArrow(PartitionState<S> pa, VectorSchemaRoot root, int pid)
            throws IOException {
        int batchIndex = pa.batchCount.getAndIncrement();
        // 每次写溢写文件前查一次所在盘容量（超上限换盘，全超上限抛 SpillSpaceException）
        Path batchFile = spillDirForWrite().resolve("p" + pid + "_b" + batchIndex + ".arrow");
        pa.batchFiles.add(batchFile);
        if (pa.dataFilePath == null) pa.dataFilePath = batchFile;
        long bytes = writeArrowFile(batchFile, root);
        // 并发溢写线程共享该字段：原子累加，避免一次写入丢失（Phase 2 的 Spilled 汇总口径）。
        synchronized (pa) { pa.dataFileSize += bytes; }
        return new BatchWrite(batchIndex, bytes);
    }

    /**
     * 写一个单批 Arrow IPC 文件并返回实际字节数。
     *
     * <p>写失败（磁盘满 / 中途异常）时也必须关掉 writer 与文件句柄：
     * 溢写是高频路径，任何一次泄漏的 handle 都会在长时间查询里累积到 fd 上限。</p>
     */
    public static long writeArrowFile(Path file, VectorSchemaRoot root) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             ArrowFileWriter w = new ArrowFileWriter(root, null, ch)) {
            w.writeBatch();
        }
        return Files.size(file);
    }

    protected VectorSchemaRoot loadBatchFromArrow(PartitionState<S> pa, int batchIndex, int pid)
            throws IOException {
        Path batchFile = pa.batchFiles.get(batchIndex);
        FileChannel ch = FileChannel.open(batchFile, StandardOpenOption.READ);
        ArrowFileReader reader = null;
        VectorSchemaRoot loaded = null;
        List<FieldVector> copied = new ArrayList<>();
        boolean ok = false;
        try {
            reader = new ArrowFileReader(ch, allocator);
            List<ArrowBlock> blocks = reader.getRecordBlocks();
            if (blocks.isEmpty()) return null;
            reader.loadRecordBatch(blocks.getFirst());
            loaded = reader.getVectorSchemaRoot();
            int rc = loaded.getRowCount();
            for (int col = 0; col < loaded.getFieldVectors().size(); col++) {
                FieldVector src = loaded.getVector(col);
                FieldVector dst = src.getField().createVector(allocator);
                dst.setInitialCapacity(rc);
                dst.allocateNew();
                RowAccumulator.copyColumnBatch(src, dst, 0, 0, rc);
                dst.setValueCount(rc);
                copied.add(dst);
            }
            Schema schema = loaded.getSchema();
            reader.close();
            reader = null;
            loaded.close();
            loaded = null;
            ok = true;
            return new VectorSchemaRoot(schema.getFields(), copied, rc);
        } finally {
            // 异常路径：已分配到查询分配器上的列 + reader/源批 + 文件句柄全部归还，
            // 否则一次读盘失败就会把整批 buffer 永久挂在查询 allocator 上直到查询结束。
            if (!ok) {
                for (FieldVector v : copied) {
                    try { v.close(); } catch (Throwable ignored) { }
                }
            }
            if (reader != null) { try { reader.close(); } catch (Throwable ignored) { } }
            if (loaded != null) { try { loaded.close(); } catch (Throwable ignored) { } }
            try { ch.close(); } catch (Throwable ignored) { }
        }
    }

    // ==================== 索引 ====================

    protected void addToIndex(PartitionState<S> pa, VectorSchemaRoot sorted, int batchIndex) {
        int rc = sorted.getRowCount();
        if (rc == 0) return;
        int segStart = 0;
        Object segKey = buildKeyFromSpill(sorted, 0);
        for (int r = 1; r < rc; r++) {
            Object cur = buildKeyFromSpill(sorted, r);
            if (!keysEqual(segKey, cur)) {
                addSegment(pa, segKey, new Segment(batchIndex, segStart, r));
                segStart = r;
                segKey = cur;
            }
        }
        addSegment(pa, segKey, new Segment(batchIndex, segStart, rc));
    }

    /**
     * 追加一个 key 的段索引 —— <b>可能被两个并发的溢写线程同时调用</b>（同一个 key 出现在两个槽位）。
     *
     * <p>用 {@link SegmentList}（整型平行数组 + {@code synchronized} 追加）而非每 key 一个
     * {@code CopyOnWriteArrayList}：后者在高基数分组下会退化出与行数同阶的容器/段对象，索引内存
     * 反超数据本身（详见 {@link SegmentList} 类注释）。{@code computeIfAbsent} 对同一 key 原子，
     * 只会存一个实例。</p>
     */
    private static void addSegment(PartitionState<?> pa, Object key, Segment seg) {
        List<Segment> list = pa.index.computeIfAbsent(key, k -> new SegmentList());
        ((SegmentList) list).addSegment(seg.batchIndex(), seg.startRow(), seg.endRow());
    }

    protected Object buildKeyFromSpill(VectorSchemaRoot sorted, int row) {
        List<String> keyCols = groupKeyColumns;
        if (keyCols.size() == 1) {
            FieldVector vec = sorted.getVector(0);
            if (vec.isNull(row)) return null;
            return toKeyValue(vec.getObject(row));
        }
        Object[] vals = new Object[keyCols.size()];
        for (int i = 0; i < keyCols.size(); i++) {
            FieldVector vec = sorted.getVector(i);
            vals[i] = vec.isNull(row) ? null : toKeyValue(vec.getObject(row));
        }
        return Arrays.asList(vals);
    }

    protected boolean keysEqual(Object a, Object b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        if (a instanceof List<?> la && b instanceof List<?> lb) {
            if (la.size() != lb.size()) return false;
            for (int i = 0; i < la.size(); i++) {
                Object va = la.get(i), vb = lb.get(i);
                if (va == null && vb == null) continue;
                if (va == null || vb == null) return false;
                if (!va.equals(vb)) return false;
            }
            return true;
        }
        return a.equals(b);
    }

    protected void flushRemainingSlots(PartitionState<S> pa, int pid) {
        for (int s = 0; s < SLOT_COUNT; s++) {
            S slot = pa.slots.get(s);
            if (slot != null && !slot.isEmpty()) {
                spillSlotSync(pa, s, slot, pid);
            }
        }
    }

    // ==================== 工具方法 ====================

    protected static Object toKeyValue(Object raw) {
        if (raw == null) return null;
        if (raw instanceof byte[] b) return new String(b, StandardCharsets.UTF_8);
        return raw;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void sortKeys(List<Object> keys) {
        keys.sort((a, b) -> {
            if (a == null && b == null) return 0;
            if (a == null) return -1;
            if (b == null) return 1;
            if (a instanceof List<?> la && b instanceof List<?> lb) return ArrowComparators.compareLists(la, lb);
            if (a instanceof Comparable ca && b instanceof Comparable cb) return ca.compareTo(cb);
            return a.toString().compareTo(b.toString());
        });
    }

    /**
     * 把一个 Java 值写入任意 Arrow 向量（写不进去就置 NULL 兜底）。
     *
     * <p>★MIN/MAX 的状态值现在与输入列同型（时间戳 / 日期 / 时间 / 字符串 / 布尔 / Decimal /
     * 二进制 / Duration），溢写与结果输出都要能写回对应向量。日期时间在 Arrow 里以 epoch 为单位
     * 存储（{@code getObject} 用 UTC 还原成 {@code LocalDateTime}），这里按 UTC 反向换算。</p>
     */
    protected static void setVectorValue(FieldVector vec, int row, Object val) {
        if (val == null) { vec.setNull(row); return; }
        switch (vec) {
            // ★数值列除 Number 外也接受数字字符串：窗口函数三参默认值写 '0' / '-1' 时
            //   规划层按字面量保留成 String，强转会 ClassCastException ⇒ 被调用方兜成 NULL。
            case BigIntVector v -> v.setSafe(row, toLongValue(val));
            case IntVector v -> v.setSafe(row, (int) toLongValue(val));
            case SmallIntVector v -> v.setSafe(row, (int) toLongValue(val));
            case TinyIntVector v -> v.setSafe(row, (int) toLongValue(val));
            case Float8Vector v -> v.setSafe(row, toDoubleValue(val));
            case Float4Vector v -> v.setSafe(row, (float) toDoubleValue(val));
            case BitVector v -> v.setSafe(row, toBit(val));
            // ★ Arrow 15 的 DecimalVector.setSafe 要求 BigDecimal 的 scale 与向量声明严格相等
            //   （否则抛 UnsupportedOperationException）⇒ 写入前按向量 scale 归一。
            case DecimalVector v -> v.setSafe(row, toBigDecimal(val, v.getScale()));
            case VarCharVector v -> v.setSafe(row, toBytes(val));
            case VarBinaryVector v -> v.setSafe(row, toBytes(val));
            case DateDayVector v -> v.setSafe(row, ((Number) val).intValue());
            case DateMilliVector v -> v.setSafe(row, toEpochMilli(val));
            case TimeSecVector v -> v.setSafe(row, toSecondOfDay(val));
            case TimeMilliVector v -> v.setSafe(row, toMillisOfDay(val));
            case TimeMicroVector v -> v.setSafe(row, toMicrosOfDay(val));
            case TimeNanoVector v -> v.setSafe(row, toNanosOfDay(val));
            case TimeStampSecVector v -> v.setSafe(row, toEpochSecond(val));
            case TimeStampMilliVector v -> v.setSafe(row, toEpochMilli(val));
            case TimeStampMicroVector v -> v.setSafe(row, toEpochMicro(val));
            case TimeStampNanoVector v -> v.setSafe(row, toEpochNano(val));
            case TimeStampSecTZVector v -> {
                NullableTimeStampSecTZHolder h = new NullableTimeStampSecTZHolder();
                h.isSet = 1; h.value = toEpochSecond(val); h.timezone = timestampZone(vec);
                v.setSafe(row, h);
            }
            case TimeStampMilliTZVector v -> {
                NullableTimeStampMilliTZHolder h = new NullableTimeStampMilliTZHolder();
                h.isSet = 1; h.value = toEpochMilli(val); h.timezone = timestampZone(vec);
                v.setSafe(row, h);
            }
            case TimeStampMicroTZVector v -> {
                NullableTimeStampMicroTZHolder h = new NullableTimeStampMicroTZHolder();
                h.isSet = 1; h.value = toEpochMicro(val); h.timezone = timestampZone(vec);
                v.setSafe(row, h);
            }
            case TimeStampNanoTZVector v -> {
                NullableTimeStampNanoTZHolder h = new NullableTimeStampNanoTZHolder();
                h.isSet = 1; h.value = toEpochNano(val); h.timezone = timestampZone(vec);
                v.setSafe(row, h);
            }
            case DurationVector v -> v.setSafe(row, durationUnits(v, val));
            default -> vec.setNull(row);
        }
    }

    private static long toLongValue(Object val) {
        if (val instanceof Number n) return n.longValue();
        return Long.parseLong(val.toString().trim());
    }

    private static double toDoubleValue(Object val) {
        if (val instanceof Number n) return n.doubleValue();
        return Double.parseDouble(val.toString().trim());
    }

    private static byte[] toBytes(Object val) {
        if (val instanceof byte[] b) return b;
        return val.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static int toBit(Object val) {
        if (val instanceof Boolean b) return b ? 1 : 0;
        if (val instanceof Number n) return n.longValue() != 0 ? 1 : 0;
        return Boolean.parseBoolean(val.toString()) ? 1 : 0;
    }

    private static BigDecimal toBigDecimal(Object val, int scale) {
        BigDecimal b = (val instanceof BigDecimal v) ? v : new BigDecimal(val.toString());
        return b.scale() == scale ? b : b.setScale(scale, java.math.RoundingMode.HALF_UP);
    }

    private static String timestampZone(FieldVector vec) {
        return vec.getField().getType() instanceof ArrowType.Timestamp ts ? ts.getTimezone() : null;
    }

    private static long toEpochSecond(Object val) {
        if (val instanceof LocalDateTime dt) return dt.toEpochSecond(ZoneOffset.UTC);
        return ((Number) val).longValue();
    }

    private static long toEpochMilli(Object val) {
        if (val instanceof LocalDateTime dt) return dt.toInstant(ZoneOffset.UTC).toEpochMilli();
        return ((Number) val).longValue();
    }

    private static long toEpochMicro(Object val) {
        if (val instanceof LocalDateTime dt) {
            return Math.addExact(Math.multiplyExact(dt.toEpochSecond(ZoneOffset.UTC), 1_000_000L),
                    dt.getNano() / 1_000);
        }
        return ((Number) val).longValue();
    }

    private static long toEpochNano(Object val) {
        if (val instanceof LocalDateTime dt) {
            return Math.addExact(Math.multiplyExact(dt.toEpochSecond(ZoneOffset.UTC), 1_000_000_000L),
                    dt.getNano());
        }
        return ((Number) val).longValue();
    }

    private static int toSecondOfDay(Object val) {
        if (val instanceof LocalDateTime dt) return dt.toLocalTime().toSecondOfDay();
        return ((Number) val).intValue();
    }

    private static int toMillisOfDay(Object val) {
        if (val instanceof LocalDateTime dt) {
            LocalTime t = dt.toLocalTime();
            return t.toSecondOfDay() * 1000 + t.getNano() / 1_000_000;
        }
        return ((Number) val).intValue();
    }

    private static long toMicrosOfDay(Object val) {
        if (val instanceof LocalDateTime dt) {
            LocalTime t = dt.toLocalTime();
            return t.toSecondOfDay() * 1_000_000L + t.getNano() / 1_000;
        }
        return ((Number) val).longValue();
    }

    private static long toNanosOfDay(Object val) {
        if (val instanceof LocalDateTime dt) {
            LocalTime t = dt.toLocalTime();
            return t.toSecondOfDay() * 1_000_000_000L + t.getNano();
        }
        return ((Number) val).longValue();
    }

    private static long durationUnits(DurationVector vec, Object val) {
        if (!(val instanceof Duration d)) return ((Number) val).longValue();
        return switch (((ArrowType.Duration) vec.getField().getType()).getUnit()) {
            case SECOND -> d.getSeconds();
            case MILLISECOND -> d.toMillis();
            case MICROSECOND -> d.toNanos() / 1_000;
            case NANOSECOND -> d.toNanos();
            default -> d.toMillis();
        };
    }

    protected static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        // 溢写量动辄上 GB：只到 MB 会打出「12345.67 MB」，多一层换算更直观
        if (bytes < 1024L * 1024 * 1024) return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    // ==================== OperatorMemoryHandle ====================
    // 基类提供默认实现，子类可覆盖

    @Override public long getSpilledBytes() { return getStats().getSpilledBytes(); }
    @Override public long getMemoryBudget() { return memoryLimit; }
    @Override public void setMemoryBudget(long b) {}
    @Override public SpillResult spill(long t) { return SpillResult.noOp(); }
    @Override public boolean isSpilling() { return false; }
}
