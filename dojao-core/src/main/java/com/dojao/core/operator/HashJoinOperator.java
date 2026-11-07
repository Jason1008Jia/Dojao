package com.dojao.core.operator;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import com.dojao.core.operator.expr.ExprEvaluator;
import com.dojao.core.operator.expr.ExprParser;
import com.dojao.spi.util.NestedTypeSupport;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.BitVectorHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Hash Join 算子（并行，唯一 hash join）—— 每分区一个工作线程，独立完成 build/probe/send。
 *
 * <h3>执行形态</h3>
 * <pre>
 *   ParallelTableScan → [HashPartition: 分发线程 + N 个每分区队列] ──┐
 *                                                                ├→ N 个 join 工作线程 → 有界结果队列 → 下游
 *   ParallelTableScan → [HashPartition: 分发线程 + N 个每分区队列] ──┘
 * </pre>
 *
 * <p>两侧上游是 {@link PartitionedSource}（恒定并行）：单机 = {@link HashPartitionOperator}
 * （每分区独立有界队列 + 后台分发线程）；分布式 = {@link ShuffleReader}（跨机灌入本地有界队列）。
 * 相同 join key 按同一哈希函数落同一分区 → 分区间互不相关。每个工作线程只消费自己的分区流：
 * <ol>
 *   <li><b>Build</b>：从右表分区队列建 {@link JoinHashTable}，超过 {@code memoryLimit / N} 报错</li>
 *   <li><b>Probe</b>：从左表分区队列逐行探测，匹配对攒满 {@link #JOIN_SEND_BATCH_SIZE}(5W) 即组装输出；
 *       收到该分区 {@link Page#partitionEnd(int)} 立即把残余也发走（小分区不阻塞）</li>
 *   <li><b>Unmatched</b>（RIGHT/FULL）：补发该分区未匹配的右表行，左列置 NULL</li>
 * </ol>
 * 完成后每个工作线程向共享结果队列塞一个 {@code Page.eof()} 终止标记；下游数满 N 个终止标记
 * 即认为整个 join 结束。
 *
 * <p><b>背压闭环</b>：结果队列有界 → 工作线程阻塞 → 分区队列有界 → 分发线程阻塞 → 扫描线程阻塞，
 * 任一环节内存都被队列容量约束，不会无界膨胀。
 *
 * <h3>输出 Schema</h3>
 * 左表所有列 + 右表非 join key 列（右表 join key 若与左表同名则排除）。
 *
 * @author jiaxin
 * @version 3.0
 * @date 2026/8/5
 */
public class HashJoinOperator extends AbstractMeasurableOperator implements Explainable {

    /** EXPLAIN 明细：算法（shuffle / broadcast / sort_merge）+ 是否可溢写 + 连接类型 / 连接键 / 分区数。 */
    @Override
    public List<String> explainLines() {
        List<String> lines = new ArrayList<>();
        lines.add("algorithm: " + JoinExplain.localHash(memoryLimit, numPartitions)
                + " type=" + joinType + " keys=" + leftKeyColumns + "=" + rightKeyColumns
                + " partitions=" + numPartitions);
        if (onLeftFilter != null) lines.add("on-left-filter: " + onLeftFilter);
        if (onRightFilter != null) lines.add("on-right-filter: " + onRightFilter);
        return lines;
    }

    /** 发送批大小：每凑满 5W 行即组装输出，不再全量攒批。 */
    private static final int JOIN_SEND_BATCH_SIZE = 50000;
    /** 单分区持有源页上限：稀疏匹配时的内存安全阀，超过即强制发送。 */
    private static final int MAX_HELD_PAGES = 32;
    /** 共享结果队列容量（页数上限，第二道安全阀；主背压见 {@link #outputByteBudget} 的字节预算）。 */
    private static final int RESULT_QUEUE_CAPACITY = 32;
    /** 工作线程完成标记。 */
    private static final Page POISON = Page.eof();

    /**
     * 输出缓冲字节预算占本算子 allocator 上限的比例（其余留给 build 哈希表 + 组装瞬时峰值）。
     *
     * <p>为什么不能只按「页数」限流：{@link #RESULT_QUEUE_CAPACITY} 限的是<b>页数</b>，而每页可达
     * {@link #JOIN_SEND_BATCH_SIZE}(5W) 行 —— 宽行 + 下游（如窗口算子）消费慢时，排队的 32 页
     * 就能把 allocator 顶爆（线上 {@code assemble} 处 435MB/416MB 的 Arrow OOM 根因）。改为按
     * <b>字节</b>限流，输出缓冲占用才与本算子的堆外预算真正挂钩。</p>
     */
    private static final double OUTPUT_BUDGET_FRACTION = 0.5;
    /** 输出缓冲字节预算下限（分配器上限极小或未派生时兜底，避免把预算算成 0 而卡死）。 */
    private static final long MIN_OUTPUT_BUDGET_BYTES = 16L * 1024 * 1024;

    // 分区源（PartitionedSource）：单机 = HashPartitionOperator；分布式 = ShuffleReader（跨机灌入）
    private final PartitionedSource leftPart;   // probe 侧
    private final PartitionedSource rightPart;  // build 侧
    private final List<String> leftKeyColumns;
    private final List<String> rightKeyColumns;
    private final JoinType joinType;
    private final boolean needNullRight;
    private final long memoryLimit;
    private final int numPartitions;
    private final BufferAllocator allocator;
    /**
     * 右表（build 侧）<b>声明列</b>，来自计划期（{@code JoinStep.declaredRightFields}）；null/空 = 未声明。
     *
     * <p><b>为什么必须有</b>：输出 schema 原来只在 build 侧<b>收到过页</b>时才能确定
     * （{@link #ensureOutputSchema} 从首个右表页拿 {@code rightSchema}）。哈希分发会让<b>没有行的桶</b>
     * 一页都收不到（{@code HashPartitionOperator.flushMyBuffers} 跳过空桶，只发 partitionEnd）
     * ⇒ 该分区退化成「只有左列」的<b>短 schema</b>。于是同一算子的不同分区吐出的页列数不同，
     * 下游（{@code HashPartitionOperator} 按首个页 initialize、之后按 {@code vectors.size()} 取值）
     * 当场 {@code IndexOutOfBoundsException}，顺序相反时则<b>静默丢列</b>。
     *
     * <p>计划期已知两侧输入列 ⇒ 由 Master 随 {@code JoinStep} 下发，<b>所有分区输出 schema 恒一致</b>，
     * 与 build 侧是否有数据无关。右表整表为空（probe 侧谓词过滤光）时也能正确产出 NULL 右列。
     */
    private final List<Field> declaredRightFields;

    /**
     * 左表（probe 侧）<b>声明列</b>，来自计划期（{@code JoinStep.declaredLeftFields}）；null/空 = 未声明。
     *
     * <p>对称于 {@link #declaredRightFields}：RIGHT / FULL join 必须输出「右行 + 左列全 NULL」的孤儿行，
     * 而左表某分区可能<b>一页都收不到</b> —— 没有左列类型就造不出输出 schema。旧行为在这些分区
     * 静默跳过孤儿行（结果少数据）。有声明列后所有分区输出 schema 恒一致。</p>
     */
    private final List<Field> declaredLeftFields;

    /**
     * ON 子句中的<b>单侧残差谓词</b>（只引用一侧的列），null = 无。
     *
     * <p>为什么不能下推到扫描：OUTER JOIN 的 ON 谓词为假时行<b>不是被过滤掉</b>，而是「未匹配」——
     * LEFT/FULL 的左行仍要 NULL 扩展输出，RIGHT/FULL 的右行仍要 NULL 扩展输出。下推到任一侧扫描
     * 都会永久丢掉保留侧应输出的孤儿行（旧实现因此直接拒绝 FULL JOIN 的此类 ON 条件）。
     * 正确语义在本算子内实现：谓词为假的左行不参与探测（按未匹配处理），为假的右行不参与匹配
     * （{@code rightRowMatched} 保持 false，RIGHT/FULL 收尾时作为孤儿输出）。</p>
     */
    private final String onLeftFilter;
    private final String onRightFilter;
    /** 预编译的残差谓词求值器（构造期解析，运行期按<b>页</b>批求值，逐候选只查布尔位）。 */
    private final ExprEvaluator leftFilterEvaluator;
    private final ExprEvaluator rightFilterEvaluator;
    /** 残差谓词引用的列名（列裁剪必须保留，否则求值页里找不到列）。 */
    private final java.util.Set<String> onLeftFilterColumns;
    private final java.util.Set<String> onRightFilterColumns;

    /** 变长列占位源：空行/跳过行经 setSafe(k, EMPTY_BYTES) 写 0 长度，保持 offset 连续。 */
    private static final byte[] EMPTY_BYTES = new byte[0];

    // ==================== 结果汇合 ====================
    private final BlockingQueue<Page> resultQueue = new LinkedBlockingQueue<>(RESULT_QUEUE_CAPACITY);
    private final AtomicInteger poisonCount = new AtomicInteger();
    /**
     * 输出缓冲字节预算：结果队列中<b>排队</b>的 + 各分区正在<b>组装</b>的输出页字节合计不得超过它。
     *
     * <p>取值 = 本算子 allocator 上限 × {@link #OUTPUT_BUDGET_FRACTION} − {@link #memoryLimit}(build 预算)，
     * 再兜下限 {@link #MIN_OUTPUT_BUDGET_BYTES}。分配器<b>无上限</b>（离线探针）时置 {@link Long#MAX_VALUE}
     * （不设闸）。</p>
     */
    private final long outputByteBudget;
    /**
     * 结果队列中<b>排队</b>输出页的字节合计。生产端 {@link #assembleAndEnqueue} 组装前检查总量是否
     * 越过 {@link #outputByteBudget}，越过即等待；消费端 {@link #doNext} 取出页时递减。
     */
    private final AtomicLong pendingOutputBytes = new AtomicLong();
    private volatile boolean shuttingDown;
    private volatile boolean eofReturned;
    private volatile Throwable failure;
    private ExecutorService workers;

    /**
     * 每分区<b>已发出行数</b> + 该分区工作线程是否已收尾（监控用，非执行路径）。
     *
     * <p>本算子固定 {@code numPartitions} 个分区线程并行 build/probe/send，但上层把它包在<b>一个</b>
     * 片段任务里（跨机时一个 Worker 一个片段）→ 监控只看到 1 个 task，串行假象。这两个数组把片段内部
     * 的分区并行度暴露给 Worker 遥测：每分区行数 = 该分区真实 join 输出行数（各分区之和 = 算子总输出），
     * done 由该分区线程 finish 时置位 → 监控可逐分区独立变绿。执行路径零依赖。</p>
     */
    private final AtomicLong[] partitionRowsOut;
    /**
     * 每分区<b>读入 / 发出字节</b>（监控用，非执行路径）：与 {@link #partitionRowsOut} 同源同步 ——
     * 一个片段任务里跑着 n 个分区线程，只有分区级计数才能让监控框的流量卡显示真实数据。
     */
    private final AtomicLong[] partitionBytesIn;
    private final AtomicLong[] partitionBytesOut;
    private final boolean[] partitionDone;
    /**
     * 输出页是否携带分区号（{@link Page#partitioned}）。
     *
     * <p>默认 false：单机 HashJoin → HashAggregation（分区数 1）链路里，输出页无分区号即落到槽 0，
     * 带号反而可能让下游按 pid 建槽越界。链式分布式 join（join 输出直接喂窗口排序）必须置 true：
     * 否则 N 个分区的 join 结果全部挤进窗口排序的槽 0，单槽归并整份数据 → 长时间不吐页。</p>
     */
    private final boolean tagPartitionIds;

    // ==================== 构造 ====================

    /** 分区数从左侧分区源推断。 */
    public HashJoinOperator(PartitionedSource leftPart, PartitionedSource rightPart,
                            List<String> leftKeyColumns, List<String> rightKeyColumns,
                            JoinType joinType, long memoryLimit) {
        this(leftPart, rightPart, leftKeyColumns, rightKeyColumns, joinType, memoryLimit,
                Math.max(1, leftPart.numPartitions()));
    }

    /** 单 key 便捷构造。 */
    public HashJoinOperator(PartitionedSource leftPart, PartitionedSource rightPart,
                            String leftKeyColumn, String rightKeyColumn,
                            JoinType joinType, long memoryLimit) {
        this(leftPart, rightPart, List.of(leftKeyColumn), List.of(rightKeyColumn), joinType, memoryLimit);
    }

    public HashJoinOperator(PartitionedSource leftPart, PartitionedSource rightPart,
                            List<String> leftKeyColumns, List<String> rightKeyColumns,
                            JoinType joinType, long memoryLimit, int numPartitions) {
        this(leftPart, rightPart, leftKeyColumns, rightKeyColumns, joinType, memoryLimit, numPartitions, null);
    }

    /**
     * @param declaredRightFields 右表（build 侧）全列（计划期下发，见 {@link #declaredRightFields}）；
     *                            null/空 = 未声明 → 退回「按 build 侧实收页推断」（旧行为）
     */
    public HashJoinOperator(PartitionedSource leftPart, PartitionedSource rightPart,
                            List<String> leftKeyColumns, List<String> rightKeyColumns,
                            JoinType joinType, long memoryLimit, int numPartitions,
                            List<Field> declaredRightFields) {
        this(leftPart, rightPart, leftKeyColumns, rightKeyColumns, joinType, memoryLimit, numPartitions,
                declaredRightFields, null);
    }

    /**
     * @param declaredRightFields 右表（build 侧）全列（见 {@link #declaredRightFields}）
     * @param declaredLeftFields  左表（probe 侧）全列（见 {@link #declaredLeftFields}）
     */
    public HashJoinOperator(PartitionedSource leftPart, PartitionedSource rightPart,
                            List<String> leftKeyColumns, List<String> rightKeyColumns,
                            JoinType joinType, long memoryLimit, int numPartitions,
                            List<Field> declaredRightFields, List<Field> declaredLeftFields) {
        this(leftPart, rightPart, leftKeyColumns, rightKeyColumns, joinType, memoryLimit, numPartitions,
                declaredRightFields, declaredLeftFields, false);
    }

    /**
     * @param tagPartitionIds 输出页是否带分区号：分布式链式 join（join 输出直接喂多分区窗口排序）必须
     *                        true，否则各分区结果全落窗口排序槽 0；单机链路保持 false（下游按 1 分区建槽）。
     */
    public HashJoinOperator(PartitionedSource leftPart, PartitionedSource rightPart,
                            List<String> leftKeyColumns, List<String> rightKeyColumns,
                            JoinType joinType, long memoryLimit, int numPartitions,
                            List<Field> declaredRightFields, List<Field> declaredLeftFields,
                            boolean tagPartitionIds) {
        this(leftPart, rightPart, leftKeyColumns, rightKeyColumns, joinType, memoryLimit, numPartitions,
                declaredRightFields, declaredLeftFields, tagPartitionIds, null, null);
    }

    /**
     * @param onLeftFilter  ON 子句中只引用左表的残差谓词（已去限定符），null = 无
     * @param onRightFilter ON 子句中只引用右表的残差谓词（已去限定符），null = 无
     */
    public HashJoinOperator(PartitionedSource leftPart, PartitionedSource rightPart,
                            List<String> leftKeyColumns, List<String> rightKeyColumns,
                            JoinType joinType, long memoryLimit, int numPartitions,
                            List<Field> declaredRightFields, List<Field> declaredLeftFields,
                            boolean tagPartitionIds, String onLeftFilter, String onRightFilter) {
        super("HashJoin", "HashJoin", Math.max(1, numPartitions));
        this.leftPart = leftPart;
        this.rightPart = rightPart;
        this.leftKeyColumns = leftKeyColumns;
        this.rightKeyColumns = rightKeyColumns;
        this.joinType = joinType;
        this.needNullRight = (joinType == JoinType.LEFT || joinType == JoinType.FULL);
        this.memoryLimit = memoryLimit;
        this.numPartitions = Math.max(1, numPartitions);
        this.tagPartitionIds = tagPartitionIds;
        this.declaredRightFields = (declaredRightFields == null || declaredRightFields.isEmpty())
                ? null : List.copyOf(declaredRightFields);
        this.declaredLeftFields = (declaredLeftFields == null || declaredLeftFields.isEmpty())
                ? null : List.copyOf(declaredLeftFields);
        this.onLeftFilter = blankToNull(onLeftFilter);
        this.onRightFilter = blankToNull(onRightFilter);
        // 构造期解析：表达式非法时立刻失败（而不是跑起来后在每个分区里抛同样的错）。
        this.leftFilterEvaluator = this.onLeftFilter == null
                ? null : new ExprEvaluator(ExprParser.parse(this.onLeftFilter));
        this.rightFilterEvaluator = this.onRightFilter == null
                ? null : new ExprEvaluator(ExprParser.parse(this.onRightFilter));
        this.onLeftFilterColumns = this.leftFilterEvaluator == null
                ? java.util.Set.of() : java.util.Set.copyOf(this.leftFilterEvaluator.columnArgs());
        this.onRightFilterColumns = this.rightFilterEvaluator == null
                ? java.util.Set.of() : java.util.Set.copyOf(this.rightFilterEvaluator.columnArgs());
        // 分配器是进程级共享根（NodeMemory）的子 —— build 侧哈希表全部堆外占用进统一账本（背压水位可见），
        // 并受共享根硬上限约束：build 表超预算时抛 OOM 让该查询失败，而非被内核 OOM-killer 杀进程。
        this.allocator = NodeMemory.shared()
                .newOperatorAllocator("hashjoin", ResourceConfig.queryDirectLimit());
        // 输出缓冲字节预算：allocator 上限扣掉 build 预算（buildSide 已按 memoryLimit/N 逐分区强制，
        // 总和 ≤ memoryLimit）后按比例留给输出。分配器无上限时不设闸（离线探针）。
        this.outputByteBudget = computeOutputByteBudget(this.allocator.getLimit());
        // 分区源恒定并行（单机每分区队列 + 后台分发线程；分布式跨机灌入），本算子的执行形态依赖它
        this.partitionRowsOut = new AtomicLong[this.numPartitions];
        this.partitionBytesIn = new AtomicLong[this.numPartitions];
        this.partitionBytesOut = new AtomicLong[this.numPartitions];
        this.partitionDone = new boolean[this.numPartitions];
        for (int i = 0; i < this.numPartitions; i++) {
            partitionRowsOut[i] = new AtomicLong();
            partitionBytesIn[i] = new AtomicLong();
            partitionBytesOut[i] = new AtomicLong();
        }
    }

    public List<String> getLeftKeyColumns() { return leftKeyColumns; }
    public List<String> getRightKeyColumns() { return rightKeyColumns; }
    public JoinType getJoinType() { return joinType; }

    /** ON 左（probe 侧）残差谓词引用的列 —— 列裁剪必须保留（否则 probe 页缺列，求值即失败）。 */
    public java.util.Set<String> onLeftFilterColumns() { return onLeftFilterColumns; }

    /** ON 右（build 侧）残差谓词引用的列 —— 列裁剪必须保留。 */
    public java.util.Set<String> onRightFilterColumns() { return onRightFilterColumns; }

    /**
     * 计算输出缓冲字节预算（{@link #outputByteBudget}）—— 由构造器调用。
     *
     * <p>默认 = {@code allocLimit × }{@link #OUTPUT_BUDGET_FRACTION}{@code  − }{@link #memoryLimit}
     * （扣掉 build 预算 + 给组装瞬时峰值留余量），再兜下限 {@link #MIN_OUTPUT_BUDGET_BYTES}；
     * {@code allocLimit} 无效（≤0 / {@link Long#MAX_VALUE}）时返回 {@link Long#MAX_VALUE}（不设闸，离线探针用）。</p>
     *
     * <p><b>声明为 {@code protected}</b>：离线探针可覆写成 {@link Long#MAX_VALUE} 复现「无字节预算」的
     * 旧行为，做判别力自证。<b>覆写实现不得依赖子类自身状态</b>（构造期即被调用，仅 {@code allocLimit}
     * 与已赋值的 {@link #memoryLimit} 可用）。</p>
     */
    protected long computeOutputByteBudget(long allocLimit) {
        if (allocLimit <= 0 || allocLimit == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return Math.max(MIN_OUTPUT_BUDGET_BYTES,
                (long) (allocLimit * OUTPUT_BUDGET_FRACTION) - memoryLimit);
    }

    // ==================== 分区并行度可观测（监控） ====================

    /** 分区数 = 内部分区工作线程数（上层片段的任务数只有 1，监控据此展开）。 */
    public int numPartitions() { return numPartitions; }

    /** 分区 {@code pid} 已发出的行数（并发安全；越界/未开始 → 0）。 */
    public long partitionRowsOut(int pid) {
        return (pid >= 0 && pid < numPartitions) ? partitionRowsOut[pid].get() : 0L;
    }

    /** 分区 {@code pid} 的工作线程是否已收尾（build+probe+unmatched 全做完）。 */
    public boolean partitionDone(int pid) {
        return pid >= 0 && pid < numPartitions && partitionDone[pid];
    }

    /** 分区 {@code pid} 已读入的字节（build + probe 两侧从 shuffle 收的页大小之和）。 */
    public long partitionBytesIn(int pid) {
        return (pid >= 0 && pid < numPartitions) ? partitionBytesIn[pid].get() : 0L;
    }

    /** 分区 {@code pid} 已发出的字节（join 输出页大小之和）。 */
    public long partitionBytesOut(int pid) {
        return (pid >= 0 && pid < numPartitions) ? partitionBytesOut[pid].get() : 0L;
    }

    // ==================== 生命周期 ====================

    @Override
    protected void doOpen() throws Exception {
        leftPart.open();
        rightPart.open();
        workers = Executors.newFixedThreadPool(numPartitions, r -> {
            Thread t = new Thread(r, "hash-join-worker");
            t.setDaemon(true);
            return t;
        });
        for (int pid = 0; pid < numPartitions; pid++) {
            final int p = pid;
            workers.submit(() -> runPartition(p));
        }
        System.err.printf("[HashJoin] %d worker(s) started, joinType=%s, partitions=%d%n",
                numPartitions, joinType, numPartitions);
    }

    @Override
    protected Page doNext() throws Exception {
        while (!eofReturned) {
            checkFailure();
            Page p = resultQueue.poll(100, TimeUnit.MILLISECONDS);
            if (p == null) {
                continue;
            }
            if (p == POISON) {
                if (poisonCount.incrementAndGet() >= numPartitions) {
                    checkFailure();
                    eofReturned = true;
                    return Page.eof();
                }
                continue;
            }
            pendingOutputBytes.addAndGet(-p.getSizeInBytes());   // 取出即从输出缓冲预算中扣减，解除生产端背压
            return p;
        }
        return Page.eof();
    }

    @Override
    protected void doClose() throws Exception {
        shuttingDown = true;   // 让在 awaitOutputBudget() 中等待的工作线程尽快退出
        if (workers != null) {
            workers.shutdownNow();
            try { workers.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }
        Page p;
        while ((p = resultQueue.poll()) != null) {
            if (p != POISON) { try { p.close(); } catch (Exception ignored) {} }
        }
        try { leftPart.close(); } catch (Exception ignored) {}
        try { rightPart.close(); } catch (Exception ignored) {}
        allocator.close();
    }

    // ==================== 工作线程：每分区独立 build/probe/send ====================

    /**
     * 单个分区的 join 执行上下文（每个工作线程一份，状态完全本地化，线程间零共享）。
     */
    private class PartitionJoin {
        final int pid;
        Schema leftSchema;
        Schema rightSchema;
        Schema outputSchema;
        int[] rightColumnOutputMapping;
        /** probe 侧 join 键列在左表页 schema 中的下标（复合键全部列出；交叉连接为空）。 */
        int[] probeKeyIndices;
        JoinHashTable table;
        boolean[] rightRowMatched;
        /**
         * build 行是否通过右残差谓词（null = 无右残差，全部通过）。
         * 谓词为假的 build 行<b>仍然入表</b>（否则 RIGHT/FULL 收尾时无从输出该孤儿行），
         * 只是不参与匹配、{@code rightRowMatched} 保持 false。
         */
        boolean[] rightRowOk;
        // 跨页攒批（扁平 int 数组，替代逐对 int[3] 对象分配，降低 GC 压力）：
        int[] batchPage = new int[1024];   // 左行来源持有页下标
        int[] batchLRow = new int[1024];   // 左行号（-1 = 该侧 NULL）
        int[] batchRRow = new int[1024];   // build 行号（-1 = 该侧 NULL）
        int batchSize;
        final List<Page> heldPages = new ArrayList<>();
        long matches;
        long nulls;

        PartitionJoin(int pid) { this.pid = pid; }
    }

    /** 追加一对 [pageIdx, leftRow, rightRow] 到扁平批数组（超长自动扩容）。 */
    private void addPair(PartitionJoin j, int pageIdx, int lRow, int rRow) {
        int size = j.batchSize;
        if (size == j.batchPage.length) {
            int newCap = size * 2;
            j.batchPage = Arrays.copyOf(j.batchPage, newCap);
            j.batchLRow = Arrays.copyOf(j.batchLRow, newCap);
            j.batchRRow = Arrays.copyOf(j.batchRRow, newCap);
        }
        j.batchPage[size] = pageIdx;
        j.batchLRow[size] = lRow;
        j.batchRRow[size] = rRow;
        j.batchSize = size + 1;
    }

    private void runPartition(int pid) {
        PartitionJoin j = new PartitionJoin(pid);
        try {
            buildSide(j);
            // 左表该分区可能一页都收不到（哈希分发下空桶是常态）：用计划期声明列先建输出 schema，
            // RIGHT / FULL 的「右行 + 左列全 NULL」孤儿行才不会因缺左列类型被静默跳过。
            if (j.leftSchema == null && declaredLeftFields != null) {
                j.leftSchema = new Schema(declaredLeftFields);
                ensureOutputSchema(j);
            }
            probeSide(j);
            emitUnmatched(j);
            resultQueue.put(POISON);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();   // 正常关闭
        } catch (Throwable t) {
            recordFailure(t);
            // 确保 doNext 不会因缺少终止标记而挂起
            try { resultQueue.offer(POISON); } catch (Exception ignored) {}
        } finally {
            if (j.table != null) {
                try { j.table.close(); } catch (Exception ignored) {}
            }
            for (Page hp : j.heldPages) {
                try { hp.close(); } catch (Exception ignored) {}
            }
            j.heldPages.clear();
            partitionDone[pid] = true;   // 监控：该分区线程已收尾（无论正常/异常，供监控区分完成度）
        }
    }

    /** Build：消费右表分区队列，建该分区的 {@link JoinHashTable}。 */
    private void buildSide(PartitionJoin j) throws Exception {
        BlockingQueue<Page> rightQ = rightPart.partitionQueue(j.pid);
        long memBudget = memoryLimit / Math.max(1, numPartitions);
        while (true) {
            Page page = rightQ.take();
            if (page.isEndOfPartition()) {
                checkFailure();   // 分发线程若失败，这里退出本 worker
                break;
            }
            // 节点级背压（读取端）：本节点堆外吃紧时放缓读入 —— build 表 + 输出缓冲 + 上游页都算在
            // 同一本堆外账上，读慢一点让溢写/消费追上来，把「内存不够」变成「读得慢一点」而不是 OOM。
            NodeMemory.shared().throttle();
            partitionBytesIn[j.pid].addAndGet(page.getSizeInBytes());   // 监控：该分区读入字节
            // ★闸门/建表/插入任一步失败都必须关页（runPartition 的 finally 只管 build 表与 heldPages），
            //   否则 Worker 线程异常退出后这张页永远不释放，分配器关闭时报 Memory was leaked。
            try {
                if (j.table == null) {
                    j.rightSchema = page.getSchema();
                    List<Field> keyFields = new ArrayList<>();
                    for (String col : rightKeyColumns) {
                        Field kf = j.rightSchema.findField(col);
                        NestedTypeSupport.requireSupported(kf, "HashJoin 的 join 键");
                        keyFields.add(kf);
                    }
                    j.table = new JoinHashTable(allocator, rightKeyColumns, keyFields, j.rightSchema);
                }
                int rows = page.getRowCount();
                // 右残差谓词：每页求值一次得到布尔掩码；为假的行同样入表（RIGHT/FULL 收尾要作为孤儿输出），
                // 只是不参与匹配。
                boolean[] pageOk = evaluateMask(rightFilterEvaluator, page.getRoot(), rows, "__on_right");
                if (pageOk != null) {
                    ensureRightOkCapacity(j, j.table.buildRowCount() + rows);
                }
                for (int r = 0; r < rows; r++) {
                    int buildRow = j.table.buildRowCount();
                    j.table.insertOrAppend(page, r);
                    if (pageOk != null) j.rightRowOk[buildRow] = pageOk[r];
                }
                long mem = j.table.estimateMemoryBytes();
                if (mem > memBudget) {
                    throw new RuntimeException(String.format(
                            "HashJoin partition %d build side too large: %s exceeds per-partition budget %s",
                            j.pid, formatBytes(mem), formatBytes(memBudget)));
                }
            } finally {
                page.close();
            }
        }

        // RIGHT/FULL：初始化该分区匹配追踪
        if (joinType == JoinType.RIGHT || joinType == JoinType.FULL) {
            if (j.table != null && !j.table.isEmpty()) {
                j.rightRowMatched = new boolean[j.table.buildRowCount()];
            }
        }
    }

    /**
     * Probe：消费左表分区队列，逐行探测；攒满 5W 即组装发送，收到该分区结束标记立即发残余。
     */
    private void probeSide(PartitionJoin j) throws Exception {
        BlockingQueue<Page> leftQ = leftPart.partitionQueue(j.pid);
        while (true) {
            Page page = leftQ.take();
            if (page.isEndOfPartition()) {
                checkFailure();
                break;
            }
            // ★先挂进 heldPages：随后的 schema / 键列解析可能抛（嵌套键闸门），
            //   runPartition 的 finally 会统一关闭 heldPages，避免失败路径漏页。
            int pageIdx = j.heldPages.size();
            j.heldPages.add(page);
            // 节点级背压（读取端）：同 buildSide —— 节点堆外吃紧时放缓读入，让下游消费追上来。
            NodeMemory.shared().throttle();
            partitionBytesIn[j.pid].addAndGet(page.getSizeInBytes());   // 监控：该分区读入字节
            if (j.leftSchema == null) {
                j.leftSchema = page.getSchema();
                ensureOutputSchema(j);
            }
            // 键列下标必须按<b>实收页</b>的 schema 解析一次（复合键全部列出；probe 热路径逐行按名查找太贵）。
            // 不能只在 leftSchema == null 分支里做：分布式路径在 runPartition 里已用计划期声明列
            // 预置了 leftSchema（空 probe 分区要建输出 schema），此时 leftSchema != null 但下标尚未解析。
            if (j.probeKeyIndices == null && !leftKeyColumns.isEmpty()) {
                j.probeKeyIndices = resolveKeyIndices(page.getSchema(), leftKeyColumns);
            }
            VectorSchemaRoot root = page.getRoot();
            int rowCount = root.getRowCount();
            // 空键 = 交叉连接（build 侧只有唯一分组，probe 每行与 build 全部行配对）—— 用于
            // 「非相关标量子查询」的单值广播：build 侧是恒 1 行的全局聚合派生表，被复制到每台
            // Worker（REPLICATE），本机与左表就地做交叉连接。此时没有 probe 键向量可解析。
            boolean crossJoin = leftKeyColumns.isEmpty();
            boolean emptyBuild = (j.table == null || j.table.isEmpty());

            // 左残差谓词：每页求值一次（逐候选只查布尔位）。
            boolean[] leftPageOk = evaluateMask(leftFilterEvaluator, root, rowCount, "__on_left");
            for (int r = 0; r < rowCount; r++) {
                boolean leftOk = leftPageOk == null || leftPageOk[r];
                int groupIdx = (!leftOk || emptyBuild) ? -1
                        : (crossJoin ? 0 : j.table.probe(page, j.probeKeyIndices, r));
                int matchedPairs = 0;
                if (groupIdx >= 0) {
                    for (int rr : j.table.getMatchingRows(groupIdx)) {
                        if (j.rightRowOk != null && !j.rightRowOk[rr]) continue;   // 右残差为假 ⇒ 该 build 行不匹配
                        addPair(j, pageIdx, r, rr);
                        j.matches++;
                        matchedPairs++;
                        if (j.rightRowMatched != null) j.rightRowMatched[rr] = true;
                        if (j.batchSize >= JOIN_SEND_BATCH_SIZE) {
                            flush(j, true);
                            // flush(true) 后当前页被移到 heldPages[0]，后续配对的下标随之复位
                            pageIdx = 0;
                        }
                    }
                }
                // ON 谓词为假 / 键不匹配 / 所有候选 build 行都被右残差否决 ⇒ 该左行「未匹配」。
                // 只有 LEFT/FULL 需要 NULL 扩展输出它；INNER/RIGHT 无输出（右侧由收尾阶段补发）。
                if (matchedPairs == 0 && needNullRight) {
                    addPair(j, pageIdx, r, -1);
                    j.nulls++;
                    if (j.batchSize >= JOIN_SEND_BATCH_SIZE) {
                        flush(j, true);
                        pageIdx = 0;
                    }
                }
            }
            // 当前页耗尽：攒满 5W 或持有页过多（稀疏匹配）触发内存安全阀
            if (j.batchSize >= JOIN_SEND_BATCH_SIZE || j.heldPages.size() > MAX_HELD_PAGES) {
                flush(j, false);
            }
        }
        // 该分区结束：残余无论多少立即发送
        flush(j, false);
    }

    /**
     * 组装该分区累积的匹配对并放入结果队列。
     *
     * @param keepCurrent true 表示当前页仍有未探测行，需保留（关闭其之前的持有页）；
     *                    false 表示该分区所有持有页均已耗尽，全部关闭。
     */
    private void flush(PartitionJoin j, boolean keepCurrent) throws InterruptedException {
        if (j.batchSize == 0) {
            if (!keepCurrent) {
                closeHeldPages(j);
            }
            return;
        }
        assembleAndEnqueue(j, () -> assemble(j));
        j.batchSize = 0;

        List<Page> held = j.heldPages;
        int closeEnd = keepCurrent ? held.size() - 1 : held.size();
        for (int i = 0; i < closeEnd; i++) {
            try { held.get(i).close(); } catch (Exception ignored) {}
        }
        if (keepCurrent) {
            Page current = held.get(held.size() - 1);
            held.clear();
            held.add(current);
        } else {
            held.clear();
        }
    }

    /**
     * 组装一页输出并放入结果队列 —— <b>生产端唯一出口</b>，统一走输出字节预算闸门与排队记账。
     *
     * <p>probe 残余页（{@link #flush}）与 RIGHT/FULL 补发行（{@link #emitUnmatched}）共用此路径，
     * 保证任何输出生成点都受同一份背压约束，不会出现「某条路径绕过限流」。组装前先过
     * {@link #awaitOutputBudget()}（排队输出字节越预算即等待），组装后按真实字节入账，{@link #doNext}
     * 取出时递减。</p>
     *
     * @param builder 真正组装该页的逻辑（由调用方给出，本方法负责限流与记账）
     */
    private void assembleAndEnqueue(PartitionJoin j, Supplier<VectorSchemaRoot> builder)
            throws InterruptedException {
        awaitOutputBudget();
        VectorSchemaRoot outRoot = builder.get();
        Page out = tagPartitionIds ? Page.partitioned(outRoot, j.pid) : Page.fromRoot(outRoot);
        long bytes = out.getSizeInBytes();
        pendingOutputBytes.addAndGet(bytes);                 // 入账：排队中，doNext 取出时递减
        partitionRowsOut[j.pid].addAndGet(out.getRowCount());   // 监控：该分区已发出行数
        partitionBytesOut[j.pid].addAndGet(bytes);              // 监控：该分区已发出字节
        resultQueue.put(out);
    }

    /**
     * 输出水位闸门 —— 组装输出前的背压入口。
     *
     * <p>本算子的 build 哈希表、各分区<b>在制</b>输出页、结果队列中<b>排队</b>的输出页，全部从同一个
     * allocator（上限 = {@link ResourceConfig#queryDirectLimit()}）分配。原先只按「结果队列最多
     * {@link #RESULT_QUEUE_CAPACITY} 页」限流，而每页可达 {@link #JOIN_SEND_BATCH_SIZE}(5W) 行 ——
     * 宽行 + 下游（窗口算子等）消费慢时，排队页字节数足以把 allocator 顶到上限，再组装下一页即
     * {@code Arrow OutOfMemoryException}（线上 {@code assemble} 处 435MB / 416MB）。</p>
     *
     * <p>闸门语义：排队输出字节越过 {@link #outputByteBudget} 就等待，让下游消费把内存还回来。
     * <b>这个等待必定能推进</b>（因此无需超时）：条件 {@code pendingOutputBytes > budget} 意味着结果
     * 队列里确有排队页，而排队页只能由下游 {@link #doNext} 取出递减；一旦越预算，所有生产线程都在此
     * 等待、不再新增 → 总量只减不增，直到回落。若查询取消，工作线程被 {@code shutdownNow} 中断，
     * {@code sleep} 抛 {@link InterruptedException} 即退出；{@link #doClose} 置 {@link #shuttingDown} 亦可
     * 提前放行。分配器<b>无上限</b>（离线探针）时 {@link #outputByteBudget} 为 {@link Long#MAX_VALUE}，不设闸。</p>
     */
    private void awaitOutputBudget() throws InterruptedException {
        if (outputByteBudget == Long.MAX_VALUE) {
            return;
        }
        while (!shuttingDown && pendingOutputBytes.get() > outputByteBudget) {
            Thread.sleep(5);
        }
    }

    private void closeHeldPages(PartitionJoin j) {
        for (Page hp : j.heldPages) {
            try { hp.close(); } catch (Exception ignored) {}
        }
        j.heldPages.clear();
    }

    /** RIGHT/FULL：补发该分区未匹配的右表行，左列置 NULL。 */
    private void emitUnmatched(PartitionJoin j) throws Exception {
        if (joinType != JoinType.RIGHT && joinType != JoinType.FULL) return;
        if (j.table == null || j.table.isEmpty()) return;
        if (j.outputSchema == null) return;   // 左表为空且从未见过左页 → 无 schema 可组装，保守跳过

        boolean[] matched = j.rightRowMatched;
        int buildRows = j.table.buildRowCount();
        List<int[]> batch = new ArrayList<>();
        for (int rr = 0; rr < buildRows; rr++) {
            if (matched != null && matched[rr]) continue;
            batch.add(new int[]{-1, rr});
            if (batch.size() >= JOIN_SEND_BATCH_SIZE) {
                List<int[]> page = new ArrayList<>(batch);
                assembleAndEnqueue(j, () -> assembleUnmatched(j, page));
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            assembleAndEnqueue(j, () -> assembleUnmatched(j, batch));
        }
    }

    // ==================== 输出组装 ====================

    /**
     * 首次看到左表数据后构建输出 schema：左表所有列 + 右表非重复 join key 列。
     *
     * <p>右表列优先取 build 侧<b>实收页</b>的 schema；该分区 build 侧一页都没收到时（哈希分发下
     * 空桶是常态）退回计划期下发的 {@link #declaredRightFields} —— 两者的列集合相同（同一张表、
     * 同一套全表列扫描），但退回声明值才能保证<b>所有分区输出同一 schema</b>。
     */
    private void ensureOutputSchema(PartitionJoin j) {
        if (j.outputSchema != null) return;
        List<Field> outFields = new ArrayList<>();
        for (Field f : j.leftSchema.getFields()) {
            outFields.add(f);
        }
        List<Field> rightFields = (j.rightSchema != null) ? j.rightSchema.getFields() : declaredRightFields;
        if (rightFields != null) {
            j.rightColumnOutputMapping = new int[rightFields.size()];
            for (int i = 0; i < rightFields.size(); i++) {
                Field rf = rightFields.get(i);
                // ★仅在 INNER 连接时剔除「与左键同名的右键」（如 a.id=b.id 避免输出两个 id）：
                //   内连接两侧键值恒等，去重不改变语义。外连接（LEFT/RIGHT/FULL）**必须保留右键列** ——
                //   未匹配行的右键为 NULL、左键非 NULL，两者语义不同；剔掉后外层 b.key 会按裸列名
                //   静默绑到左键列（恒非 NULL），b.key IS NULL / count(b.key) 全错。
                // 左表存在同名非 join key 列（如 a.user_id=b.id 时左表也有 id 列）不排除，select * 需保留该列。
                if (joinType == JoinType.INNER && rightKeyColumns.contains(rf.getName())
                        && leftKeyColumns.contains(rf.getName())) {
                    j.rightColumnOutputMapping[i] = -1;
                } else {
                    j.rightColumnOutputMapping[i] = outFields.size();
                    outFields.add(rf);
                }
            }
        } else {
            j.rightColumnOutputMapping = new int[0];
        }
        j.outputSchema = new Schema(outFields);
    }

    /**
     * 从跨页累积的扁平批组装输出根（组装时所有持有页都未关闭）。
     *
     * <p>替代逐对逐格 copyFromSafe：定长列按输出行号直接 setBytes + 逐位 validity；
     * 变长列按输出行序单遍写 offset/data（data 按行序连续打包），均走直接 buffer 操作，
     * 消除 copyFromSafe 的 handleSafe + 重复扩容开销。
     */
    private VectorSchemaRoot assemble(PartitionJoin j) {
        int outRows = j.batchSize;
        List<Field> fields = j.outputSchema.getFields();
        List<FieldVector> outVectors = new ArrayList<>(fields.size());
        for (Field f : fields) {
            FieldVector vec = f.createVector(allocator);
            vec.setInitialCapacity(outRows);
            vec.allocateNew();
            // 预置全 valid，之后只对 NULL 行 unset
            ArrowBuf val = vec.getValidityBuffer();
            if (val != null) {
                int valBytes = (outRows + 7) / 8;
                for (int b = 0; b < valBytes; b++) val.setByte(b, (byte) 0xFF);
            }
            outVectors.add(vec);
        }
        int leftCols = j.leftSchema.getFields().size();
        int[] pageArr = j.batchPage;
        int[] lRowArr = j.batchLRow;
        int[] rRowArr = j.batchRRow;
        int nPages = j.heldPages.size();

        // 左列：源行跨持有页，逐列按输出行序 gather
        for (int c = 0; c < leftCols; c++) {
            FieldVector[] pageVecs = new FieldVector[nPages];
            for (int p = 0; p < nPages; p++) pageVecs[p] = j.heldPages.get(p).getRoot().getVector(c);
            FieldVector dst = outVectors.get(c);
            if (dst.getField().getType() instanceof ArrowType.FixedSizeList) {
                copyLeftFixedList(pageVecs, pageArr, lRowArr, dst, outRows);
            } else if (RowAccumulator.typeWidthOf(dst.getField().getType()) > 0) {
                copyLeftFixed(pageVecs, pageArr, lRowArr, dst, outRows);
            } else if (dst instanceof BaseVariableWidthVector) {
                copyLeftVar(pageVecs, pageArr, lRowArr, dst, outRows);
            } else {
                // 嵌套列（LIST/MAP/STRUCT）作为 join 载荷列：定长/变长快路径都不适用，
                // 交给 Arrow 的递归 copyFromSafe（child 由它自己扩容）。
                copyLeftNested(pageVecs, pageArr, lRowArr, dst, outRows);
            }
        }

        // 右列：build 表单源 gather
        if (j.table != null) {
            List<FieldVector> buildVectors = j.table.getBuildVectors();
            int[] map = j.rightColumnOutputMapping;
            for (int c = 0; c < map.length; c++) {
                int oc = map[c];
                if (oc < 0) continue;
                FieldVector dst = outVectors.get(oc);
                if (dst.getField().getType() instanceof ArrowType.FixedSizeList) {
                    copyRightFixedList(buildVectors.get(c), dst, rRowArr, outRows);
                } else if (RowAccumulator.typeWidthOf(dst.getField().getType()) > 0) {
                    copyRightFixed(buildVectors.get(c), dst, rRowArr, outRows);
                } else if (dst instanceof BaseVariableWidthVector) {
                    copyRightVar(buildVectors.get(c), dst, rRowArr, outRows);
                } else {
                    copyRightNested(buildVectors.get(c), dst, rRowArr, outRows);
                }
            }
        }

        // 补 NULL：左 NULL 行（RIGHT/FULL 未匹配）与右 NULL 行（LEFT/FULL 未匹配）
        int[] map = j.rightColumnOutputMapping;
        for (int k = 0; k < outRows; k++) {
            if (lRowArr[k] < 0) {
                for (int c = 0; c < leftCols; c++) outVectors.get(c).setNull(k);
            }
            if (rRowArr[k] < 0) {
                for (int c = 0; c < map.length; c++) {
                    int oc = map[c];
                    if (oc >= 0) outVectors.get(oc).setNull(k);
                }
            }
        }

        // 统一收尾：直接写 buffer 的 var 拷贝（copyLeftVar/copyRightVar）不会推进 Arrow lastSet
        // （allocateNew 后恒为 -1），下游任一 setValueCount 的 fillHoles 会从 lastSet+1 起把
        // offset 压平成 offset[0]=0 → var 列全部变空串。这里先把 lastSet 置为 outRows-1，再
        // setValueCount → fillHoles 变 no-op；同时补上 valueCount，保证 transfer()/transferTo
        // 拷贝的是真实行数。
        for (FieldVector v : outVectors) {
            if (v instanceof BaseVariableWidthVector dv) dv.setLastSet(outRows - 1);
            v.setValueCount(outRows);
        }

        return new VectorSchemaRoot(fields, outVectors, outRows);
    }

    /** 定长列左表 gather：源行来自不同持有页，按输出行序写 data buffer + 逐位 validity。 */
    private static void copyLeftFixed(FieldVector[] pageVecs, int[] pageArr, int[] lRowArr,
                                      FieldVector dst, int outRows) {
        int tw = RowAccumulator.typeWidthOf(dst.getField().getType());
        ArrowBuf dstData = dst.getDataBuffer();
        if (dstData.capacity() < (long) outRows * tw) { dst.reAlloc(); dstData = dst.getDataBuffer(); }
        ArrowBuf dstVal = dst.getValidityBuffer();
        boolean[] hasNulls = new boolean[pageVecs.length];
        for (int p = 0; p < pageVecs.length; p++) hasNulls[p] = pageVecs[p].getNullCount() > 0;
        for (int k = 0; k < outRows; k++) {
            int lr = lRowArr[k];
            if (lr < 0) continue;   // 补 NULL 阶段处理
            int p = pageArr[k];
            FieldVector src = pageVecs[p];
            dstData.setBytes((long) k * tw, src.getDataBuffer(), (long) lr * tw, tw);
            if (dstVal != null && hasNulls[p] && src.isNull(lr)) BitVectorHelper.unsetBit(dstVal, k);
        }
    }

    /** 变长列左表 gather：源行跨持有页，按输出行序经 setSafe 追加。
     *
     * <p>不用直接写 data/offset buffer：allocateNew 后 valueCount 恒为 0，Arrow 的
     * {@code reAlloc()} 在 valueCount==0 时不会扩容 data buffer（128 字节封顶，跨页数据超限即
     * IndexOutOfBoundsException）。setSafe 内部 handleSafe 以显式目标大小 {@code reallocDataBuffer(need)}
     * 扩容，与 {@link RowAccumulator#copyColumnBatch} 变长分支同款、全链路验证可用。
     * 空行/跳过行写 0 长度占位，offset 连续；assemble 末尾统一 setLastSet+setValueCount 收尾。 */
    private static void copyLeftVar(FieldVector[] pageVecs, int[] pageArr, int[] lRowArr,
                                    FieldVector dst, int outRows) {
        BaseVariableWidthVector dv = (BaseVariableWidthVector) dst;
        ArrowBuf dstVal = dv.getValidityBuffer();
        for (int k = 0; k < outRows; k++) {
            int lr = lRowArr[k];
            if (lr < 0) {
                dv.setSafe(k, EMPTY_BYTES);
                if (dstVal != null) BitVectorHelper.unsetBit(dstVal, k);
                continue;
            }
            BaseVariableWidthVector sv = (BaseVariableWidthVector) pageVecs[pageArr[k]];
            if (sv.isNull(lr)) {
                dv.setSafe(k, EMPTY_BYTES);
                if (dstVal != null) BitVectorHelper.unsetBit(dstVal, k);
            } else {
                ArrowBuf srcOff = sv.getOffsetBuffer();
                int start = srcOff.getInt((long) lr * 4);
                int len = srcOff.getInt((long) (lr + 1) * 4) - start;
                dv.setSafe(k, start, len, sv.getDataBuffer());
            }
        }
    }

    /** 定长列右表 gather：单源 build 向量，按输出行序写。 */
    private static void copyRightFixed(FieldVector src, FieldVector dst, int[] rRowArr, int outRows) {
        int tw = RowAccumulator.typeWidthOf(dst.getField().getType());
        ArrowBuf dstData = dst.getDataBuffer();
        if (dstData.capacity() < (long) outRows * tw) { dst.reAlloc(); dstData = dst.getDataBuffer(); }
        ArrowBuf dstVal = dst.getValidityBuffer();
        boolean hasNulls = src.getNullCount() > 0;
        ArrowBuf srcData = src.getDataBuffer();
        for (int k = 0; k < outRows; k++) {
            int rr = rRowArr[k];
            if (rr < 0) continue;   // 补 NULL 阶段处理
            dstData.setBytes((long) k * tw, srcData, (long) rr * tw, tw);
            if (dstVal != null && hasNulls && src.isNull(rr)) BitVectorHelper.unsetBit(dstVal, k);
        }
    }

    /** 变长列右表 gather：单源 build 向量，按输出行序经 setSafe 追加（理由同 {@link #copyLeftVar}）。 */
    private static void copyRightVar(FieldVector src, FieldVector dst, int[] rRowArr, int outRows) {
        BaseVariableWidthVector sv = (BaseVariableWidthVector) src;
        BaseVariableWidthVector dv = (BaseVariableWidthVector) dst;
        ArrowBuf srcOff = sv.getOffsetBuffer();
        ArrowBuf srcData = sv.getDataBuffer();
        ArrowBuf dstVal = dv.getValidityBuffer();
        for (int k = 0; k < outRows; k++) {
            int rr = rRowArr[k];
            if (rr < 0) {
                dv.setSafe(k, EMPTY_BYTES);
                if (dstVal != null) BitVectorHelper.unsetBit(dstVal, k);
                continue;
            }
            if (sv.isNull(rr)) {
                dv.setSafe(k, EMPTY_BYTES);
                if (dstVal != null) BitVectorHelper.unsetBit(dstVal, k);
            } else {
                int start = srcOff.getInt((long) rr * 4);
                int len = srcOff.getInt((long) (rr + 1) * 4) - start;
                dv.setSafe(k, start, len, srcData);
            }
        }
    }

    /**
     * 嵌套列（LIST/MAP/STRUCT）左表 gather。
     *
     * <p>这类列既没有定长 stride（不能 memcpy data buffer），也不是 {@link BaseVariableWidthVector}
     * （不能按 offset/data 快写），只能走 Arrow 自带的递归 {@code copyFromSafe} —— 它会把 offset、
     * validity 与 child 向量一并拷贝，并按需扩容目标 child。join 键本身仍拒绝嵌套
     * （{@link NestedTypeSupport#requireSupported}），这里只承担<b>载荷列透传</b>。</p>
     */
    private static void copyLeftNested(FieldVector[] pageVecs, int[] pageArr, int[] lRowArr,
                                       FieldVector dst, int outRows) {
        for (int k = 0; k < outRows; k++) {
            int lr = lRowArr[k];
            if (lr < 0) continue;   // 补 NULL 阶段处理
            FieldVector src = pageVecs[pageArr[k]];
            if (src.isNull(lr)) dst.setNull(k);
            else dst.copyFromSafe(lr, k, src);
        }
    }

    /** 嵌套列（LIST/MAP/STRUCT）右表 gather：单源 build 向量。 */
    private static void copyRightNested(FieldVector src, FieldVector dst, int[] rRowArr, int outRows) {
        for (int k = 0; k < outRows; k++) {
            int rr = rRowArr[k];
            if (rr < 0) continue;   // 补 NULL 阶段处理
            if (src.isNull(rr)) dst.setNull(k);
            else dst.copyFromSafe(rr, k, src);
        }
    }

    /** FixedSizeList（Vector 类型）左表 gather：child 逐行拷贝（整行一次 setBytes）+ 父级 NULL unset + child validity 置有效。 */
    private static void copyLeftFixedList(FieldVector[] pageVecs, int[] pageArr, int[] lRowArr,
                                          FieldVector dst, int outRows) {
        ArrowType.FixedSizeList fl = (ArrowType.FixedSizeList) dst.getField().getType();
        int listSize = fl.getListSize();
        FieldVector dstC = ((FixedSizeListVector) dst).getDataVector();
        int elementTw = RowAccumulator.typeWidthOf(dstC.getField().getType());
        long childBytes = (long) listSize * elementTw;
        ArrowBuf dstData = dstC.getDataBuffer();
        if (dstData.capacity() < (long) outRows * childBytes) { dst.reAlloc(); dstData = dstC.getDataBuffer(); }
        // child validity：有效行元素全 valid（getObject 逐元素读 child）
        ArrowBuf dstCVal = dstC.getValidityBuffer();
        if (dstCVal != null) {
            int cValBytes = (int) (((long) outRows * listSize + 7) / 8);
            for (int b = 0; b < cValBytes; b++) dstCVal.setByte(b, (byte) 0xFF);
        }
        boolean[] hasNulls = new boolean[pageVecs.length];
        for (int p = 0; p < pageVecs.length; p++) hasNulls[p] = pageVecs[p].getNullCount() > 0;
        for (int k = 0; k < outRows; k++) {
            int lr = lRowArr[k];
            if (lr < 0) continue;   // 补 NULL 阶段处理
            int p = pageArr[k];
            FieldVector src = pageVecs[p];
            FieldVector srcC = ((FixedSizeListVector) src).getDataVector();
            dstData.setBytes((long) k * childBytes, srcC.getDataBuffer(), (long) lr * childBytes, childBytes);
            if (hasNulls[p] && src.isNull(lr)) dst.setNull(k);
        }
    }

    /** FixedSizeList（Vector 类型）右表 gather：单源 build 向量，child 逐行拷贝。 */
    private static void copyRightFixedList(FieldVector src, FieldVector dst, int[] rRowArr, int outRows) {
        ArrowType.FixedSizeList fl = (ArrowType.FixedSizeList) dst.getField().getType();
        int listSize = fl.getListSize();
        FieldVector dstC = ((FixedSizeListVector) dst).getDataVector();
        int elementTw = RowAccumulator.typeWidthOf(dstC.getField().getType());
        long childBytes = (long) listSize * elementTw;
        ArrowBuf dstData = dstC.getDataBuffer();
        if (dstData.capacity() < (long) outRows * childBytes) { dst.reAlloc(); dstData = dstC.getDataBuffer(); }
        ArrowBuf dstCVal = dstC.getValidityBuffer();
        if (dstCVal != null) {
            int cValBytes = (int) (((long) outRows * listSize + 7) / 8);
            for (int b = 0; b < cValBytes; b++) dstCVal.setByte(b, (byte) 0xFF);
        }
        boolean hasNulls = src.getNullCount() > 0;
        FieldVector srcC = ((FixedSizeListVector) src).getDataVector();
        ArrowBuf srcData = srcC.getDataBuffer();
        for (int k = 0; k < outRows; k++) {
            int rr = rRowArr[k];
            if (rr < 0) continue;   // 补 NULL 阶段处理
            dstData.setBytes((long) k * childBytes, srcData, (long) rr * childBytes, childBytes);
            if (hasNulls && src.isNull(rr)) dst.setNull(k);
        }
    }

    /** 组装未匹配右行输出根（左列全部 NULL）。 */
    private VectorSchemaRoot assembleUnmatched(PartitionJoin j, List<int[]> pairs) {
        int outRows = pairs.size();
        VectorSchemaRoot outRoot = VectorSchemaRoot.create(j.outputSchema, allocator);
        outRoot.allocateNew();
        List<FieldVector> outVectors = outRoot.getFieldVectors();
        int leftCols = j.leftSchema.getFields().size();
        JoinHashTable table = j.table;

        for (int outR = 0; outR < outRows; outR++) {
            int rRow = pairs.get(outR)[1];
            for (int c = 0; c < leftCols; c++) {
                outVectors.get(c).setNull(outR);
            }
            if (rRow >= 0 && table != null) {
                for (int c = 0; c < j.rightColumnOutputMapping.length; c++) {
                    int oc = j.rightColumnOutputMapping[c];
                    if (oc >= 0) {
                        table.copyBuildCell(rRow, c, outVectors.get(oc), outR);
                    }
                }
            }
        }

        outRoot.setRowCount(outRows);
        return outRoot;
    }

    // ==================== 失败传播 ====================

    private synchronized void recordFailure(Throwable t) {
        if (failure == null) failure = t;
    }

    /** 检查本算子或任一分发线程是否失败，失败则抛出。 */
    private void checkFailure() {
        Throwable f = failure;
        if (f == null) f = leftPart.distributorFailure();
        if (f == null) f = rightPart.distributorFailure();
        if (f != null) {
            if (f instanceof RuntimeException re) throw re;
            if (f instanceof Error err) throw err;
            throw new RuntimeException("HashJoin failed", f);
        }
    }

    // ==================== 工具 ====================

    /**
     * 对一页求值 ON 残差谓词，返回逐行布尔掩码；{@code evaluator == null}（无残差）或空页 → null
     * （语义为「全部通过」，避免无谓分配）。
     *
     * <p>SQL 三值逻辑：NULL（未知）按不通过处理，与 {@code FilterOperator} 同口径。</p>
     */
    private boolean[] evaluateMask(ExprEvaluator evaluator, VectorSchemaRoot root, int rows, String outName) {
        if (evaluator == null || rows == 0) return null;
        boolean[] mask = new boolean[rows];
        try (FieldVector v = evaluator.evaluate(root, allocator, outName)) {
            for (int r = 0; r < rows; r++) {
                mask[r] = !v.isNull(r) && ExprEvaluator.truthy(v.getObject(r));
            }
        }
        return mask;
    }

    /** 扩容 {@link PartitionJoin#rightRowOk}（只在右残差存在时调用；首次分配即全 false，随后逐行覆盖）。 */
    private static void ensureRightOkCapacity(PartitionJoin j, int needed) {
        if (j.rightRowOk == null) {
            j.rightRowOk = new boolean[Math.max(16, needed)];
        } else if (j.rightRowOk.length < needed) {
            j.rightRowOk = Arrays.copyOf(j.rightRowOk, Math.max(needed, j.rightRowOk.length * 2));
        }
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }

    /**
     * 解析键列在给定 schema 中的下标（保持 {@code keyColumns} 顺序）。
     * probe 侧用：复合键必须逐列解析，不能只看首列。
     */
    private static int[] resolveKeyIndices(Schema schema, List<String> keyColumns) {
        List<Field> fields = schema.getFields();
        int[] idx = new int[keyColumns.size()];
        for (int i = 0; i < keyColumns.size(); i++) {
            String name = keyColumns.get(i);
            idx[i] = -1;
            for (int j = 0; j < fields.size(); j++) {
                if (fields.get(j).getName().equals(name)) {
                    NestedTypeSupport.requireSupported(fields.get(j), "HashJoin 的 join 键");
                    idx[i] = j;
                    break;
                }
            }
            if (idx[i] < 0) {
                StringBuilder avail = new StringBuilder();
                for (Field f : fields) avail.append(avail.isEmpty() ? "" : ", ").append(f.getName());
                throw new UnsupportedOperationException("HashJoin probe 键列 '" + name
                        + "' 不在输入列中（可用列: " + avail + "）");
            }
        }
        return idx;
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
    }
}
