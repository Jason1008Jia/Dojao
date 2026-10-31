package com.dojao.core.runtime.master;

import com.dojao.core.runtime.worker.ResourceSummary;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点资源时间序列（Master 单例）—— 每收到一次心跳（或 Master 自身采样）就追加一个点，
 * 供监控页画 CPU / 内存 / 堆 / 线程数折线图。
 *
 * <p>内存型环形缓冲，每节点上限 {@link #PER_NODE_CAP} 个点（心跳 2s ≈ 6 分钟窗口）。
 * 线程安全：每个节点一把锁，只在追加 / 读取该节点的序列时短暂持有。
 *
 * <p>注意：<b>不做持久化，也不回填</b> —— 时间序列从进程启动后开始积累，
 * Master 重启即清空（当前快照卡片不受影响）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/29
 */
public final class ResourceHistory {

    /** 每节点保留的最大样本数（2s 心跳 ≈ 6 分钟）。 */
    private static final int PER_NODE_CAP = 180;

    private final Map<String, List<Sample>> series = new ConcurrentHashMap<>();

    /** 单个采样点：时刻（wall clock 毫秒，前端画横轴用）+ 当次资源摘要。 */
    public record Sample(long tsMillis, ResourceSummary resource) {
    }

    /** 追加一个采样点；超过上限从头丢弃（FIFO）。 */
    public void append(String nodeId, ResourceSummary resource) {
        if (nodeId == null || resource == null) return;
        List<Sample> list = series.computeIfAbsent(nodeId, k -> new ArrayList<>());
        synchronized (list) {
            list.add(new Sample(System.currentTimeMillis(), resource));
            while (list.size() > PER_NODE_CAP) {
                list.remove(0);
            }
        }
    }

    /** 某节点最近 max 个点（按时间升序）；未知节点返回空列表。 */
    public List<Sample> recent(String nodeId, int max) {
        List<Sample> list = series.get(nodeId);
        if (list == null) return List.of();
        synchronized (list) {
            int n = list.size();
            int from = Math.max(0, n - Math.max(0, max));
            return List.copyOf(list.subList(from, n));
        }
    }

    /** 当前有历史的节点 id（顺序不保证）。 */
    public List<String> nodeIds() {
        return List.copyOf(series.keySet());
    }

    /** 节点被注销 / 淘汰时清掉它的序列（避免陈旧节点长期占内存）。 */
    public void remove(String nodeId) {
        if (nodeId != null) series.remove(nodeId);
    }
}
