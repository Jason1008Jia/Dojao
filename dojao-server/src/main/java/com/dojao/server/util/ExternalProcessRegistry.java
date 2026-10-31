package com.dojao.server.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * 外部进程生命周期注册表 —— JVM 管理不了的外挂进程（python sidecar / 未来其他语言后端）统一由这里登记 + 清理。
 *
 * <p><b>职责</b>（进程拓扑改造中用户明确要求）：</p>
 * <ul>
 *   <li>每个 worker 内维护一张注册表；<b>每启动一个外部服务就登记一条</b>，记录：唯一 UUID、类型、
 *       {@link Process} 句柄、监听 host:port、绑定的 queryId（null = worker 级共享，不随任务结束回收）；</li>
 *   <li><b>任务结束</b> → worker 主动 {@link #killByQuery(String)} 杀掉该 query 绑定的进程；</li>
 *   <li><b>worker 被强制关闭</b>（kill / 红方块）→ shutdown 前先 {@link #killAll()} 清掉全部外部进程再退出；
 *       若没有外部进程则 no-op。</li>
 * </ul>
 *
 * <p><b>kill 策略</b>：先 {@link Process#destroy()}（优雅，Windows 上是 TerminateProcess）等 300ms，
 * 未退出再 {@link Process#destroyForcibly()}（强杀）。已死进程 no-op —— 全部幂等，可重复调用。</p>
 *
 * <p><b>线程安全</b>：{@link ConcurrentHashMap}（记录表）+ {@link CopyOnWriteArrayList}（查询索引），
 * 多线程（spawn / 任务结束 / shutdown）并发登记清理安全。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/30
 */
public final class ExternalProcessRegistry implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ExternalProcessRegistry.class);

    /** 单个外部服务登记。 */
    public record Record(
            String uuid,          // 唯一 ID（UUID v4）
            String type,          // 服务类型（如 "python-sidecar"）
            Process process,      // OS 进程句柄
            String host,          // 服务监听地址
            int port,             // 服务监听端口（0 = 不适用）
            String queryId,       // 绑定的 queryId（null = worker 级共享，不随任务结束回收）
            Instant startedAt     // 启动时间
    ) {
    }

    /** 优雅等待超时：destroy 后等这么久仍存活才 destroyForcibly。 */
    private static final long GRACE_MILLIS = 300;

    /** 登记表：uuid → Record。 */
    private final Map<String, Record> records = new ConcurrentHashMap<>();

    /** 查询索引：queryId → [uuid]。仅绑定 query 的进程入索引（null queryId 不进）。 */
    private final Map<String, List<String>> byQuery = new ConcurrentHashMap<>();

    private volatile boolean closed;

    /**
     * 登记一个外部服务。幂等：同一进程重复登记返回原 uuid（不重复记录）。
     *
     * @param type    服务类型描述
     * @param process OS 进程句柄（不允许 null）
     * @param host    服务监听地址
     * @param port    服务监听端口
     * @param queryId 绑定的 queryId；null = worker 级共享
     * @return 登记的 uuid（唯一 ID）
     */
    public String register(String type, Process process, String host, int port, String queryId) {
        if (process == null) {
            throw new IllegalArgumentException("process 不允许为 null");
        }
        // 幂等：同一进程已登记 → 返回既有 uuid（并补齐 queryId 绑定）
        for (Record r : records.values()) {
            if (r.process() == process) {
                if (queryId != null && !queryId.equals(r.queryId())) {
                    bindQuery(r.uuid(), queryId);
                }
                return r.uuid();
            }
        }
        String uuid = UUID.randomUUID().toString();
        Record rec = new Record(uuid, type, process, host, port, queryId, Instant.now());
        records.put(uuid, rec);
        if (queryId != null) {
            bindQuery(uuid, queryId);
        }
        log.info("外部服务已登记: uuid={} type={} host={}:{} queryId={}",
                uuid, type, host, port, queryId);
        return uuid;
    }

    /** 解除登记（进程已被外部清理 / 正常关闭）。幂等：未知 uuid 或已删 no-op。 */
    public void unregister(String uuid) {
        Record r = records.remove(uuid);
        if (r == null) {
            return;
        }
        if (r.queryId() != null) {
            byQuery.getOrDefault(r.queryId(), List.of()).remove(uuid);
        }
        log.info("外部服务已解除登记: uuid={} type={}", uuid, r.type());
    }

    /** 查某 query 绑定的全部记录（含已登记但进程已死的，调用方自行判断）。 */
    public List<Record> byQuery(String queryId) {
        return byQuery.getOrDefault(queryId, List.of()).stream()
                .map(records::get)
                .filter(r -> r != null)
                .toList();
    }

    /** 全部记录（快照）。 */
    public List<Record> all() {
        return List.copyOf(records.values());
    }

    /** 注册表条目数。 */
    public int size() {
        return records.size();
    }

    /**
     * 任务结束回调：杀掉该 query 绑定的全部外部进程并解除登记。
     * 无绑定进程时 no-op（不抛异常，不打断正常收尾）。
     */
    public void killByQuery(String queryId) {
        if (queryId == null || closed) {
            return;
        }
        for (Record r : byQuery(queryId)) {
            killAndUnregister(r);
        }
    }

    /**
     * 关闭注册表（shutdown 前调用）：杀掉全部登记的外部进程并清空。
     * 无外部进程时 no-op。幂等：第二次调用 no-op。
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        int n = records.size();
        if (n > 0) {
            log.warn("关闭注册表，清理 {} 个外部进程...", n);
        }
        for (Record r : all()) {
            killAndUnregister(r);
        }
        records.clear();
        byQuery.clear();
    }

    // ==================== 内部 ====================

    private void bindQuery(String uuid, String queryId) {
        byQuery.computeIfAbsent(queryId, k -> new CopyOnWriteArrayList<>()).add(uuid);
    }

    /** 杀进程（幂等）+ 解除登记。 */
    private void killAndUnregister(Record r) {
        Process p = r.process();
        String uuid = r.uuid();
        // 先解除登记（避免 kill 期间重入 / 重复清理）
        unregister(uuid);
        if (!p.isAlive()) {
            return;   // 已死 no-op
        }
        log.info("kill 外部进程: uuid={} type={} host={}:{} queryId={}",
                uuid, r.type(), r.host(), r.port(), r.queryId());
        try {
            p.destroy();
            if (!p.waitFor(GRACE_MILLIS, TimeUnit.MILLISECONDS)) {
                p.destroyForcibly();
                p.waitFor(GRACE_MILLIS, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            p.destroyForcibly();
        }
    }
}
