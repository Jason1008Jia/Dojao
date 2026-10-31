package com.dojao.core.operator;

import com.dojao.common.config.CommonConfig;
import com.dojao.common.data.Page;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.ScanSlice;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.ScanReader;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 并行 TableScan 算子 — 自动探查表数据量，选择合适的并行分区策略。
 *
 * <h3>分区策略决策</h3>
 * <ol>
 *   <li>行数 &lt; 阈值（默认 10K）→ 单分区</li>
 *   <li>有单列数值型主键 → <b>主键范围分区</b>，每个线程读一段</li>
 *   <li>无合适主键 → 单分区（降级）</li>
 * </ol>
 *
 * <h3>使用方式</h3>
 * <pre>{@code
 *   // 单连接器 = 单分区（不并行）
 *   new ParallelTableScanOperator(connector, request);
 *
 *   // 多连接器 = 每分区一个独立连接器 = 真正并行
 *   new ParallelTableScanOperator(connectors, request);
 * }</pre>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/12
 */
public class ParallelTableScanOperator extends AbstractMeasurableOperator {

    private final ReadRequest baseRequest;
    private final int parallelism;

    /** 列裁剪：只读取这些列（空 = 全部列）。 */
    private final List<String> selectedColumns = new ArrayList<>();
    /** 谓词下推：要交给数据源执行的过滤条件。 */
    private final List<String> predicates = new ArrayList<>();
    /** 应用列裁剪/谓词下推后的基础请求（doOpen 时计算）。 */
    private ReadRequest effectiveRequest;

    /** 每个读线程独占一个 Connector 实例（保证游标独立）。 */
    private final List<DojaoConnector> connectors;

    private final BlockingQueue<Page> pageQueue = new LinkedBlockingQueue<>(2000);
    private final AtomicInteger poisonCount = new AtomicInteger(0);
    private volatile boolean eofReturned;
    /** 停止标志：下游（如 LIMIT 提前收尾）关闭本算子时置位，读线程随即退出，不再读数据源。 */
    private volatile boolean stopped;
    /**
     * 顶层 LIMIT 的截断下推（行数上限，0 = 不下推）—— 只对「扫描行与最终输出一一对应」的
     * 单表直通链由 Planner 设置。每片读满这么多行即停（各片之和是结果的超集，上方
     * {@link LimitOperator} 照常按 offset/limit 裁剪）。
     */
    private long fetchLimit;
    /**
     * 全部扫描切片（已列裁剪/下推 + 截断下推；连接器说分不了时会退化为单切片整表请求）。
     * <b>切片一个都不能少</b>：切片数可以多于连接器实例数，多出的切片由同一读线程顺序处理。
     */
    private List<ScanSlice> slices = List.of();
    /** 各读线程当前打开着的切片游标（doClose 逐个关闭，解开卡在阻塞读上的线程）。 */
    private final List<ScanReader> readers = Collections.synchronizedList(new ArrayList<>());
    private ExecutorService executor;
    /** 读线程数（= doNext 里等到的 POISON_PILL 个数）。 */
    private int totalPartitions;

    private static final Page POISON_PILL = Page.eof();

    /**
     * 单连接器模式 — 探查分区后，剩余分区也复用该连接器（串行），
     * 但主分区范围仍然会让每个线程读不同数据段。
     */
    public ParallelTableScanOperator(DojaoConnector connector, ReadRequest request) {
        this(Collections.singletonList(connector), request, CommonConfig.JDBC_DEFAULT_PARALLELISM);
    }

    /**
     * 多连接器模式 — 每个分区使用独立的 Connector 实例（真正并行）。
     *
     * @param connectors 每个分区对应一个 Connector（索引 0 用于探查，其余用于并行读取）
     * @param request    原始读请求
     */
    public ParallelTableScanOperator(List<DojaoConnector> connectors, ReadRequest request) {
        this(connectors, request, CommonConfig.JDBC_DEFAULT_PARALLELISM);
    }

    /**
     * @param connectors  每个分区对应一个 Connector
     * @param request     原始读请求
     * @param parallelism 并行线程数
     */
    public ParallelTableScanOperator(List<DojaoConnector> connectors, ReadRequest request, int parallelism) {
        super("ParallelTableScan", "ParallelTableScan", parallelism);
        this.connectors = new ArrayList<>(connectors);
        this.baseRequest = request;
        this.parallelism = Math.max(1, parallelism);
    }

    // ==================== 列裁剪 & 谓词下推 ====================

    /** 只读取指定列（可多次调用，追加）。列裁剪由连接器的 {@link DojaoConnector#pruneColumns} 完成。 */
    public ParallelTableScanOperator selectColumns(String... columns) {
        if (columns != null) Collections.addAll(this.selectedColumns, columns);
        return this;
    }

    /** 只读取指定列（替换已有列清单）。列裁剪优化使用，避免 append 语义导致列数膨胀。 */
    public ParallelTableScanOperator setColumns(List<String> columns) {
        this.selectedColumns.clear();
        if (columns != null) this.selectedColumns.addAll(columns);
        return this;
    }

    /** 谓词下推：把过滤条件交给数据源执行（可多次调用，以 AND 合入）。 */
    public ParallelTableScanOperator pushdown(String... predicates) {
        if (predicates != null) Collections.addAll(this.predicates, predicates);
        return this;
    }

    /** 谓词下推：把过滤条件交给数据源执行（可多次调用，以 AND 合入）。 */
    public ParallelTableScanOperator pushdown(List<String> predicates) {
        if (predicates != null) this.predicates.addAll(predicates);
        return this;
    }

    /**
     * 截断下推：顶层 LIMIT 的「够用行数」（= offset + limit，0 = 不下推）。见 {@link #fetchLimit}。
     * 必须在 {@link #open()} 前调用（切分请求在 doOpen 里生成）。
     */
    public void setFetchLimit(long rows) {
        this.fetchLimit = Math.max(0, rows);
    }

    // ==================== 只读访问（分布式算子的基表探测用） ====================

    /** 原始读请求（未列裁剪 / 未下推）。 */
    public ReadRequest baseRequest() { return baseRequest; }

    /** 已裁剪的列清单（空 = 全部列）。 */
    public List<String> selectedColumns() { return List.copyOf(selectedColumns); }

    /** 已下推的过滤条件（空 = 无）。 */
    public List<String> predicates() { return List.copyOf(predicates); }

    // ==================== 生命周期 ====================

    @Override
    protected void doOpen() throws Exception {
        String schema = baseRequest.schema();
        String table = baseRequest.table();

        // ---- 0. 列裁剪 + 谓词下推：让连接器把列清单与过滤条件写进请求 ----
        DojaoConnector probeConn = connectors.getFirst();
        ReadRequest effective = baseRequest;
        if (!selectedColumns.isEmpty()) {
            effective = probeConn.pruneColumns(effective, selectedColumns);
        }
        for (String p : predicates) {
            if (p == null || p.isBlank()) continue;
            effective = probeConn.pushdownPredicate(effective, p);
        }
        this.effectiveRequest = effective;
        System.err.printf("[ParallelScan] Probing table %s.%s for partition strategy...%n", schema, table);

        // 切片策略完全交给连接器：主键范围优先，无主键由连接器按方言兜底
        // （PG → ctid 物理行号；MySQL → 数值列范围 / LIMIT-OFFSET；Hive → 文件分组）。
        // fetchLimit 一并交给连接器：文件分片型连接器据此只列「够前缀」的文件（一个切片），
        // 不去列举整表；不理会它的连接器走默认实现，行为完全不变。
        List<ScanSlice> planned = probeConn.planSplits(effective, parallelism, fetchLimit);
        if (planned.isEmpty()) {
            System.err.println("[ParallelScan] No partition strategy, using single partition");
            planned = List.of(ScanSlice.whole(effective));
        } else {
            System.err.printf("[ParallelScan] Planned %d parallel slices%n", planned.size());
        }

        // 截断下推写进各切片请求（切分与截断都由连接器生成的请求除外，见 applyFetchLimit）
        List<ScanSlice> withLimit = new ArrayList<>(planned.size());
        for (ScanSlice s : planned) {
            withLimit.add(s.withRequest(applyFetchLimit(s.request())));
        }
        this.slices = withLimit;

        // ---- 2. 确定读线程数：每线程独占一个连接器实例（游标独立），切片多于实例时同线程顺序处理 ----
        //     绝不按实例数丢弃切片 —— 丢切片 = 静默少读数据（查询照常成功，只是结果少行）。
        totalPartitions = Math.min(slices.size(), Math.max(1, connectors.size()));
        System.err.printf("[ParallelScan] %d slice(s) / %d read thread(s) ready%n",
                slices.size(), totalPartitions);

        // ---- 3. 启动并行读取线程 ----
        executor = Executors.newFixedThreadPool(totalPartitions, r -> {
            Thread t = new Thread(r, "parallel-scan-" + UUID.randomUUID().toString().substring(0, 8));
            t.setDaemon(true);
            return t;
        });

        for (int i = 0; i < totalPartitions; i++) {
            final int idx = i;
            executor.submit(() -> scanPartition(idx));
        }
        System.err.printf("[ParallelScan] %d scan threads started, open() complete%n", totalPartitions);
    }

    /**
     * 把截断下推写进切片请求：{@code LIMIT fetchLimit OFFSET 0}。只在本片请求本身没有分页语义时
     * 生效（{@code offset==0 && limit==0}）—— 连接器自己生成的 LIMIT/OFFSET 切片（MySQL 无主键
     * 兜底策略）不能被覆盖，否则各片读到的是同一段数据的副本。
     *
     * <p>offset 一律给 0：本片返回的是 {@code [0, offset+limit)} 超集，跳过 offset 的工作由上方
     * {@link LimitOperator} 照常完成。</p>
     */
    private ReadRequest applyFetchLimit(ReadRequest part) {
        if (fetchLimit <= 0 || part.offset() > 0 || part.limit() > 0) return part;
        return part.withLimit(0L, fetchLimit);
    }

    /**
     * 读线程体：本线程独占 {@code connectors.get(t)}，顺序消费落在自己身上的切片
     * （{@code t, t+T, t+2T, ...}），每片开一个游标、读到 EOF、关掉再取下一片。
     */
    private void scanPartition(int t) {
        DojaoConnector conn = connectors.get(t);
        try {
            for (int i = t; i < slices.size() && !stopped; i += totalPartitions) {
                ScanSlice slice = slices.get(i);
                ScanReader reader = conn.openSplit(slice.request(), slice.split());
                readers.add(reader);
                try {
                    while (!stopped) {
                        Page page = reader.next();
                        if (page == null || page.isEOF()) break;
                        // 有界超时投放：队列满时周期性醒来检查停止标志。不依赖 shutdownNow 的中断 ——
                        // JDBC 阻塞读不响应中断，真正解开它的是关闭游标（见 doClose）。
                        boolean handedOff = false;
                        while (!stopped) {
                            if (pageQueue.offer(page, 200, TimeUnit.MILLISECONDS)) { handedOff = true; break; }
                        }
                        if (!handedOff) {
                            // 未交出所有权（停止标志置位）→ 本线程负责释放，避免泄漏
                            try { page.close(); } catch (Exception ignored) {}
                            break;
                        }
                    }
                } finally {
                    readers.remove(reader);
                    try { reader.close(); } catch (Throwable ignored) {}
                }
            }
        } catch (InterruptedException ie) {
            // shutdownNow 的正常停止路径：不是失败
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // 已置停止标志时的异常（多为关游标打断 JDBC 读）不算失败，不必刷 stderr 噪声
            if (!stopped) {
                throw new RuntimeException("Scan thread " + t + " failed", e);
            }
        } finally {
            if (stopped) {
                pageQueue.offer(POISON_PILL);   // 收尾阶段消费者已停：投不进就算了
            } else {
                try { pageQueue.put(POISON_PILL); } catch (InterruptedException ignored) {}
            }
        }
    }

    @Override
    protected Page doNext() throws Exception {
        if (eofReturned) return Page.eof();

        // 所有线程已结束，排空队列中残留的数据页
        if (poisonCount.get() >= totalPartitions) {
            Page page = pageQueue.poll();
            if (page == null || page == POISON_PILL) {
                eofReturned = true;
                return Page.eof();
            }
            recordInput(page);
            return page;
        }

        // 正常消费，统计 POISON_PILL 判断线程结束
        while (!eofReturned) {
            Page page = pageQueue.poll(100, TimeUnit.MILLISECONDS);
            if (page == null) continue;
            if (page == POISON_PILL) {
                if (poisonCount.incrementAndGet() >= totalPartitions) {
                    // 最后一个线程刚结束，检查队列是否有残余数据页
                    Page remaining = pageQueue.poll();
                    if (remaining == null || remaining == POISON_PILL) {
                        eofReturned = true;
                        return Page.eof();
                    }
                    recordInput(remaining);
                    return remaining;
                }
                continue;
            }
            recordInput(page);
            return page;
        }
        return Page.eof();
    }

    /**
     * 向下游发来的「停止」信号落地：置停止标志 → 中断阻塞在队列投放上的读线程 → <b>关游标</b>
     * （截断下推已让数据源读满即停，这里兜住没下推成的场景：JDBC 阻塞读只认 closeRead）。
     * 两步之间只留短等待 —— 卡在 JDBC 读上的线程要靠 closeRead 才能解开。
     */
    @Override
    protected void doClose() throws Exception {
        stopped = true;
        if (executor != null) {
            executor.shutdownNow();
            try { executor.awaitTermination(1, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }
        // 关闭各线程打开着的切片游标 —— JDBC 阻塞读不响应中断，这是唯一能当场解开它的手段
        for (ScanReader r : new ArrayList<>(readers)) {
            try { r.close(); } catch (Throwable ignored) {}
        }
        // 兜底：没有活跃 reader 的连接器（如停顿在探查/建连上）同样释放游标
        for (DojaoConnector c : connectors) {
            try { c.closeRead(); } catch (Exception ignored) {}
        }
        if (executor != null) {
            try { executor.awaitTermination(3, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }
        // 下游失败 / 提前关闭时队列里可能还有已读好但没人消费的数据页 —— 所有权在本算子，必须释放，
        // 否则这批 Arrow 向量会随查询结束泄漏（探针用 RootAllocator.close 时会直接报 Memory was leaked）。
        Page leftover;
        while ((leftover = pageQueue.poll()) != null) {
            if (leftover == POISON_PILL || leftover.isEOF()) continue;
            try { leftover.close(); } catch (Throwable ignored) {}
        }
    }

}
