package com.dojao.core.runtime.worker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * Worker 静态身份信息 —— 注册时提交给 Master，此后心跳一直携带。
 *
 * @param workerId       节点唯一 ID（UUID）
 * @param host           节点地址（主机名 / IP）
 * @param port           Worker 服务端口
 * @param version        Dojao Worker 版本
 * @param startTimeNanos 节点启动时刻（nanoTime）
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/11
 */
public record WorkerNode(String workerId, String host, int port, String version, long startTimeNanos) {

    /** 手工指定 host/port 创建（测试 / 多节点模拟）。 */
    public static WorkerNode create(String host, int port, String version) {
        return new WorkerNode(UUID.randomUUID().toString(), host, port, version, System.nanoTime());
    }

    /** 自动取本机主机名创建。 */
    public static WorkerNode local(int port, String version) {
        String host = "localhost";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ignored) {
            // 取不到就用 localhost
        }
        return create(host, port, version);
    }
}
