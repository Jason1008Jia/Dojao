package com.dojao.server.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JobManager —— Master 侧全局 Job 调度器（单例），管理无穷多个并发提交的查询。
 *
 * <p>地位类似 YARN 资源管理器 / 线程池：
 * <ul>
 *   <li>所有渠道（CLI / JDBC / WEB）提交的查询统一进入 JobManager，每个查询一个 {@link Job}；</li>
 *   <li>底层是固定并行度线程池 + 无界队列 —— 队列即"YARN 队列"，堆积待执行的 Job；</li>
 *   <li>Job 级别管理用户资源（函数等，见 {@link JobResources}）；</li>
 *   <li>控制查询生命周期：用户 cancel 时把 Job / 底层 QueryManager 一并 Kill（见 {@link Job#cancel()}）；</li>
 *   <li>管理集群资源：分配策略走 {@link ResourceAllocator}，v1 为 {@link DefaultResourceAllocator} 占位。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public final class JobManager {

    private static final Logger log = LoggerFactory.getLogger(JobManager.class);

    /** 集群总并行度：-DDojaoClusterParallelism 覆盖，默认取 CPU 核数（>=2）。 */
    private static final int DEFAULT_PARALLELISM =
            Integer.getInteger("DojaoClusterParallelism", Math.max(2, Runtime.getRuntime().availableProcessors()));

    /** 最近完成 Job 缓存上限（LRU）。 */
    private static final int RECENT_CAPACITY = 100;

    private static final JobManager INSTANCE = new JobManager();

    /** 底层线程池：core=max=集群并行度，无界队列兜底，等价于固定并行度的 YARN 队列。 */
    private final ThreadPoolExecutor executor;
    private final ClusterResources cluster;
    private final ResourceAllocator allocator;

    /** 排队中的 Job（已提交未开始运行）。 */
    private final ConcurrentHashMap<String, Job> queued = new ConcurrentHashMap<>();

    /** 运行中的 Job。 */
    private final ConcurrentHashMap<String, Job> running = new ConcurrentHashMap<>();

    /** 最近完成的 Job（LRU，容量受 RECENT_CAPACITY 约束）。 */
    private final Map<String, Job> recent =
            Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Job> eldest) {
                    return size() > RECENT_CAPACITY;
                }
            });

    public JobManager(int parallelism) {
        int p = Math.max(1, parallelism);
        this.cluster = new ClusterResources(p);
        this.allocator = new DefaultResourceAllocator();
        this.executor = new ThreadPoolExecutor(
                p, p, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("dojao-job"));
    }

    private JobManager() {
        this(DEFAULT_PARALLELISM);
    }

    /** 全局单例（生产环境各渠道统一入口）。 */
    public static JobManager get() {
        return INSTANCE;
    }

    // ==================== 提交 ====================

    /**
     * 提交查询：创建 Job 并进入调度队列，返回后即可用 jobId 跟踪 / 取消。
     * 执行体（校验 + 计划 + 执行 + 挂载 QueryManager）由提交方通过 {@link JobExecution} 提供。
     */
    public Job submit(JobRequest request, JobExecution execution) {
        Job job = new Job(request);
        queued.put(job.jobId(), job);
        executor.execute(new JobTask(job, execution));
        log.info("Job 提交: jobId={}, user={}, channel={}, sql={}",
                job.jobId(), request.user(), request.channel(), request.sql());
        return job;
    }

    // ==================== 取消 ====================

    /** 取消 Job（排队中或运行中均可）；返回是否成功取消（终态返回 false）。 */
    public boolean cancel(String jobId) {
        Job job = queued.get(jobId);
        if (job == null) job = running.get(jobId);
        if (job == null) {
            synchronized (recent) {
                job = recent.get(jobId);
            }
        }
        return job != null && job.cancel();
    }

    // ==================== 查询 ====================

    /** 按 jobId 取 Job（先排队，后运行，再最近完成）。 */
    public Job get(String jobId) {
        Job job = queued.get(jobId);
        if (job != null) return job;
        job = running.get(jobId);
        if (job != null) return job;
        synchronized (recent) {
            return recent.get(jobId);
        }
    }

    /** 排队中的 Job。 */
    public List<Job> queuedJobs() {
        return List.copyOf(queued.values());
    }

    /** 运行中的 Job。 */
    public List<Job> activeJobs() {
        return List.copyOf(running.values());
    }

    /** 最近完成的 Job（LRU 顺序）。 */
    public List<Job> completedJobs() {
        synchronized (recent) {
            return List.copyOf(recent.values());
        }
    }

    public int queuedCount() { return queued.size(); }
    public int activeCount() { return running.size(); }
    public int completedCount() { return recent.size(); }
    public ClusterResources clusterResources() { return cluster; }

    // ==================== 内部：任务执行 ====================

    private final class JobTask implements Runnable {
        private final Job job;
        private final JobExecution execution;

        JobTask(Job job, JobExecution execution) {
            this.job = job;
            this.execution = execution;
        }

        @Override
        public void run() {
            job.setRunningThread(Thread.currentThread());
            queued.remove(job.jobId());

            // 排队期间被 cancel：直接收尾，不启动执行
            if (job.state() == JobState.CANCELED) {
                recordCompleted();
                return;
            }

            running.put(job.jobId(), job);
            job.markRunning();
            ResourceAllocation alloc = allocator.allocate(job.request(), cluster);
            job.setAllocatedParallelism(alloc.parallelism());
            try {
                execution.execute(job);
                job.markFinished();
            } catch (Throwable t) {
                job.markFailed(t);
            } finally {
                allocator.release(job, alloc, cluster);
                running.remove(job.jobId());
                recordCompleted();
            }
        }

        private void recordCompleted() {
            synchronized (recent) {
                recent.put(job.jobId(), job);
            }
            job.resources().closeAll();
            // 清除本线程的中断标志（cancel 会 interrupt 工作线程），避免污染线程池后续任务
            Thread.interrupted();
        }
    }

    /** 守护线程工厂。 */
    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger seq = new AtomicInteger();

        NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
