package com.dojao.server.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job —— JobManager 的调度单元，一个用户查询一个 Job。
 *
 * <p>jobId 复用 {@link QueryManager#newQueryId()} 的可排序时间戳_UUID，
 * 与查询链路中的 queryId 保持一致，WebUI / 客户端可用同一个 ID 追踪。
 *
 * <p>生命周期：QUEUED → RUNNING → FINISHED / FAILED / CANCELED。
 * {@link #cancel()} 是控制查询生命周期的核心入口：置 CANCELED、中断执行线程，
 * 并把取消信号下发给已挂载的 {@link QueryManager}，由执行循环检测后停止拉取。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public final class Job {

    private static final Logger log = LoggerFactory.getLogger(Job.class);

    private final String jobId;
    private final JobRequest request;
    private final JobResources resources = new JobResources();
    private final long submitTimeNanos;

    private volatile JobState state = JobState.QUEUED;
    private volatile long startTimeNanos;
    private volatile long endTimeNanos;
    private volatile String failureMessage;
    private volatile QueryManager queryManager;
    private volatile Thread runningThread;
    private volatile int allocatedParallelism;

    /** 便捷构造：自动生成可排序 jobId（yyyyMMddHHmmss_UUID）。 */
    public Job(JobRequest request) {
        this(QueryManager.newQueryId(), request);
    }

    public Job(String jobId, JobRequest request) {
        this.jobId = jobId;
        this.request = request;
        this.submitTimeNanos = System.nanoTime();
    }

    // ==================== 生命周期（由 JobManager 调用） ====================

    void markRunning() {
        this.state = JobState.RUNNING;
        this.startTimeNanos = System.nanoTime();
    }

    /** 正常完成；若已被取消则保持 CANCELED，不被覆盖。 */
    void markFinished() {
        if (state != JobState.CANCELED) {
            this.state = JobState.FINISHED;
            this.endTimeNanos = System.nanoTime();
            log.info("Job {} finished, wall={}ms", jobId, wallMs());
        }
    }

    /** 执行失败；若已被取消则不覆盖 CANCELED。 */
    void markFailed(Throwable t) {
        if (state == JobState.CANCELED) {
            return;
        }
        this.state = JobState.FAILED;
        this.endTimeNanos = System.nanoTime();
        this.failureMessage = t == null ? "unknown" : t.toString();
        log.error("Job {} failed: {}", jobId, failureMessage);
    }

    // ==================== 取消 ====================

    /**
     * 取消 Job（线程安全）：终态（FINISHED / FAILED / CANCELED）不可取消。
     * 排队中只置 CANCELED（任务启动时会检测并跳过）；运行中额外中断执行线程，
     * 并把取消下发给 QueryManager（执行循环检测到后停止拉取，close() 释放后台线程）。
     *
     * @return 是否成功取消（false 表示已处于终态）
     */
    public boolean cancel() {
        JobState s = state;
        if (s == JobState.FINISHED || s == JobState.FAILED || s == JobState.CANCELED) {
            return false;
        }
        this.state = JobState.CANCELED;
        this.endTimeNanos = System.nanoTime();
        QueryManager qm = queryManager;
        if (qm != null) {
            qm.cancel();
        }
        Thread t = runningThread;
        if (t != null) {
            t.interrupt();
        }
        log.warn("Job {} canceled", jobId);
        return true;
    }

    // ==================== 挂载 ====================

    /** 执行体创建好 QueryManager 后挂载，使 cancel 能下发给查询链路。 */
    public void attachQueryManager(QueryManager qm) {
        this.queryManager = qm;
    }

    // ==================== 指标 ====================

    public long wallMs() {
        if (startTimeNanos == 0L) return 0L;
        long end = endTimeNanos > 0L ? endTimeNanos : System.nanoTime();
        return (end - startTimeNanos) / 1_000_000L;
    }

    // ==================== Getter ====================

    public String jobId() { return jobId; }
    public JobRequest request() { return request; }
    public JobResources resources() { return resources; }
    public JobState state() { return state; }
    public String failureMessage() { return failureMessage; }
    public long submitTimeNanos() { return submitTimeNanos; }
    public long startTimeNanos() { return startTimeNanos; }
    public long endTimeNanos() { return endTimeNanos; }
    public QueryManager queryManager() { return queryManager; }
    public int allocatedParallelism() { return allocatedParallelism; }

    void setAllocatedParallelism(int n) { this.allocatedParallelism = n; }
    void setRunningThread(Thread t) { this.runningThread = t; }
}
