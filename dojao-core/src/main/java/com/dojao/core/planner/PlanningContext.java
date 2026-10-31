package com.dojao.core.planner;

import com.dojao.core.runtime.master.WorkerSelector;
import com.dojao.core.runtime.worker.WorkerGateway;
import com.dojao.spi.service.DojaoConnector;

/**
 * 物理计划构建上下文 —— 承载 Planner 所需的连接器提供者与执行配置。
 *
 * @author jiaxin
 * @date 2026/8/2
 */
public record PlanningContext(com.dojao.core.planner.PlanningContext.ConnectorProvider connectorProvider,
                              com.dojao.core.planner.PlanningContext.JoinAlgorithm joinAlgorithm, long memoryLimit,
                              int parallelism, WorkerGateway workerGateway, WorkerSelector workerSelector,
                              long workerHealthTimeoutMs, boolean distributedExecution,
                              boolean localScanOnly) {

    /**
     * Join 实现策略（可配置，默认 HASH）。
     */
    public enum JoinAlgorithm {HASH, BROADCAST, SORT_MERGE, PARALLEL_HASH}

    /**
     * 连接器提供者：按 catalog 取一个可供本次扫描独立使用的连接器。
     */
    @FunctionalInterface
    public interface ConnectorProvider {
        DojaoConnector get(String catalog);
    }

    /** 默认 Worker 心跳超时（ms）：健康检查窗口，超时视为节点失活。 */
    public static final long DEFAULT_WORKER_HEALTH_TIMEOUT_MS = 10_000L;

    public PlanningContext(ConnectorProvider connectorProvider,
                           JoinAlgorithm joinAlgorithm,
                           long memoryLimit,
                           int parallelism,
                           WorkerGateway workerGateway,
                           WorkerSelector workerSelector,
                           long workerHealthTimeoutMs,
                           boolean distributedExecution,
                           boolean localScanOnly) {
        this.connectorProvider = connectorProvider;
        this.joinAlgorithm = joinAlgorithm != null ? joinAlgorithm : JoinAlgorithm.HASH;
        this.memoryLimit = memoryLimit > 0 ? memoryLimit : 256L * 1024 * 1024;
        this.parallelism = parallelism > 0 ? parallelism : 4;
        this.workerGateway = workerGateway;
        this.workerSelector = workerSelector;
        this.workerHealthTimeoutMs = workerHealthTimeoutMs > 0 ? workerHealthTimeoutMs : DEFAULT_WORKER_HEALTH_TIMEOUT_MS;
        this.distributedExecution = distributedExecution;
        this.localScanOnly = localScanOnly;
    }

    public static Builder builder(ConnectorProvider connectorProvider) {
        return new Builder(connectorProvider);
    }

    public static final class Builder {
        private final ConnectorProvider connectorProvider;
        private JoinAlgorithm joinAlgorithm = JoinAlgorithm.HASH;
        private long memoryLimit = 256L * 1024 * 1024;
        private int parallelism = 4;
        private WorkerGateway workerGateway;
        private WorkerSelector workerSelector;
        private long workerHealthTimeoutMs = DEFAULT_WORKER_HEALTH_TIMEOUT_MS;
        /** 分布式执行开关：true 时单 join 的算子链整体派发到 Worker 执行（默认关，现有测试零影响）。 */
        private boolean distributedExecution = false;
        /**
         * 强制本地（Master JVM 内）扫描：true 时即使注入了 WorkerGateway 也不派发 Worker。
         * 用于「小 LIMIT 的简单单表查询」优化 —— 只需前几百行时，派发 Worker 的往返开销
         * 远大于本地直读，且数据不必跨进程搬运。
         */
        private boolean localScanOnly = false;

        Builder(ConnectorProvider connectorProvider) {
            this.connectorProvider = connectorProvider;
        }

        public Builder joinAlgorithm(JoinAlgorithm algorithm) {
            this.joinAlgorithm = algorithm;
            return this;
        }

        public Builder memoryLimit(long bytes) {
            this.memoryLimit = bytes;
            return this;
        }

        public Builder parallelism(int n) {
            this.parallelism = n;
            return this;
        }

        /** 注入 Master → Worker 派发通道；null → 无分布式扫描，走本地 ParallelTableScan。 */
        public Builder workerGateway(WorkerGateway gateway) {
            this.workerGateway = gateway;
            return this;
        }

        /** 注入任务落点选择策略；null → 默认 {@link com.dojao.core.runtime.master.DefaultWorkerSelector}。 */
        public Builder workerSelector(WorkerSelector selector) {
            this.workerSelector = selector;
            return this;
        }

        /** Worker 心跳健康超时（ms）。 */
        public Builder workerHealthTimeoutMs(long timeoutMs) {
            this.workerHealthTimeoutMs = timeoutMs;
            return this;
        }

        /** 开启分布式执行（单 join 的算子链整体派发到 Worker 执行）。默认关。 */
        public Builder distributedExecution(boolean on) {
            this.distributedExecution = on;
            return this;
        }

        /** 强制本地扫描（不派发 Worker）。默认关。 */
        public Builder localScanOnly(boolean on) {
            this.localScanOnly = on;
            return this;
        }

        public PlanningContext build() {
            return new PlanningContext(connectorProvider, joinAlgorithm, memoryLimit, parallelism,
                    workerGateway, workerSelector, workerHealthTimeoutMs, distributedExecution,
                    localScanOnly);
        }
    }
}
