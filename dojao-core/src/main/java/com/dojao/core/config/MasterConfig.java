package com.dojao.core.config;

/**
 * Master 服务器配置。
 *
 * @param host        绑定的主机名/IP，默认 {@code 0.0.0.0}
 * @param port        Arrow Flight 监听端口，默认 {@code 6530}
 * @param parallelism 查询并行分区数（扫描 / hash 分区 / 分布式聚合共用的并行度），默认 {@code 4}
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/07 15:30
 */
public record MasterConfig(
        String host,
        int port,
        int parallelism) {

    /** 兼容 (host, port) 构造：并行度取配置文件 {@code system.master.parallelism}。 */
    public MasterConfig(String host, int port) {
        this(host, port, ResourceConfig.masterParallelism());
    }

    public static MasterConfig defaults() {
        return new MasterConfig("0.0.0.0", 6530, ResourceConfig.masterParallelism());
    }
}
