package com.dojao.common.memory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * <b>节点（进程）级共享内存分配器 + 背压闸门 + 按类别统计</b> —— 把「每个算子 / 连接器 / 传输各自拿一个
 * 整机级分配器」收敛成「全进程共享一个根，各方挂子分配器」，并在内存吃紧时对<b>读取端</b>施加有界背压。
 *
 * <p><b>为什么在 {@code dojao-common}</b>：连接器是<b>插件</b>，只依赖 {@code dojao-spi} → {@code dojao-common}，
 * 不能反向依赖 {@code dojao-core}；传输 / 网关在 {@code dojao-server}。要让「算子 + 连接器 + 传输」
 * 共用同一本堆外账，共享根必须放在依赖图的最低公共分母 {@code dojao-common} 上。</p>
 *
 * <p><b>要解决什么</b>：改造前每处各自 {@code new RootAllocator(Long.MAX_VALUE)}（无上限）或
 * {@code new RootAllocator(整机堆外)}，N 个并发消费方 × 各自以为能吃整机 ⇒ 越过 JVM
 * {@code -XX:MaxDirectMemorySize} ⇒ 轻则 Arrow OOM，重则内核 OOM-killer 杀进程（<b>日志无 Java 异常</b>，
 * 表现为「一跑开窗/聚合 worker 就莫名挂了」）。</p>
 *
 * <h3>两级保护</h3>
 * <ol>
 *   <li><b>硬上限（兜底）</b>：所有消费方共享 {@link #root}，其上限 = 本节点堆外总量
 *       <b>扣除传输层预留</b>（见 {@link #PROP_DIRECT_BYTES}）。
 *       任何消费方都只是它的<b>子分配器</b>，总量一旦逼近上限即抛 Arrow OOM ⇒ <b>该 query 失败，
 *       节点存活</b>，绝不会拖垮整机；传输层另有额度，不会被 Arrow 挤到 JVM 级 direct OOM。</li>
 *   <li><b>软背压（治本）</b>：{@link #throttle()} 在读取端检查<b>堆外用量水位</b>：越过高水位就短暂等待，
 *       回落到低水位或超时再放行 —— 读取变慢 ⇒ 溢写追上来 ⇒ 内存回落，把「内存不够」变成
 *       「读得慢一点」而不是「OOM 挂掉」。</li>
 * </ol>
 *
 * <p><b>为什么用「用量」而不是「预留」做背压信号</b>：用量是<b>真实占用</b>，溢写/释放会实时把它降下来，
 * 故等待一定能被解除；若改用「预留额度」，消费者持有预留、生产者排队等待 ⇒ 跨查询循环等待死锁。
 * 这里<b>只按用量</b>、<b>只施加有界等待</b>，从根上避免死锁。</p>
 *
 * <p><b>预算 / 水位从系统属性读</b>（{@code dojao.node.direct.bytes} 等）：本类不 import {@code dojao-core}，
 * 由 {@code ResourceConfig} 在类初始化时把派生值回填成系统属性（见 {@code ResourceConfig} 静态块）。
 * 连接器 / 传输在 {@code ResourceConfig} 之后才创建，时序安全。</p>
 *
 * <p><b>按类别统计</b>：{@link #newChildAllocator(String, long)} 的子分配器按 {@code category} 归组，
 * {@link #snapshot()} 给出每个类别的「当前用量 / 峰值 / 上限」，供监控 UI 细看。已 close 的子分配器
 * 靠 {@code root.getChildAllocators()} 的存活集合定期剪除（不依赖 close 钩子，天然无泄漏）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/7
 */
public final class NodeMemory {

    private static final Logger log = LoggerFactory.getLogger(NodeMemory.class);

    /**
     * 节点 <b>Arrow 共享根预算</b>（字节）系统属性；由 {@code ResourceConfig} 发布，缺省 2g。
     *
     * <p>注意：这是「JVM 直接内存 − 传输层预留」后的额度，<b>小于</b> {@code -XX:MaxDirectMemorySize}。
     * 差额留给不经 Arrow 记账的 Netty/gRPC/Flight 缓冲，保证传输层始终有额度可用（否则 Flight 线程会抛
     * JVM 级 direct OOM 打断结果链路，而不是让该 query 抛可捕获的 Arrow OOM）。</p>
     */
    public static final String PROP_DIRECT_BYTES = "dojao.node.direct.bytes";
    /** 背压高水位系统属性（(0,1]），缺省 0.85。 */
    public static final String PROP_HIGH_WATERMARK = "dojao.memory.highWatermark";
    /** 背压低水位系统属性（(0,1)），缺省 0.75。 */
    public static final String PROP_LOW_WATERMARK = "dojao.memory.lowWatermark";
    /** 单次背压最长等待毫秒系统属性，缺省 1000。 */
    public static final String PROP_THROTTLE_MAX_MS = "dojao.memory.throttleMaxMs";

    private static final long DEFAULT_BUDGET_BYTES = 2L * 1024 * 1024 * 1024;
    private static final double DEFAULT_HIGH_WATERMARK = 0.85;
    private static final double DEFAULT_LOW_WATERMARK = 0.75;
    private static final long DEFAULT_THROTTLE_MAX_MS = 1000L;

    private static final long SAMPLE_INTERVAL_MS = 1000L;

    /** Arrow 子分配器名字的自增序号（保证兄弟分配器名唯一）。 */
    private static final java.util.concurrent.atomic.AtomicLong ARROW_NAME_SEQ =
            new java.util.concurrent.atomic.AtomicLong();

    private static volatile NodeMemory instance;

    /** 进程级共享根分配器（上限 = 本节点堆外总量 − 传输层预留）。所有消费方的分配器都是它的子。 */
    private final RootAllocator root;
    private final long directBudget;
    private final double highWatermark;
    private final double lowWatermark;
    private final long throttleMaxMs;

    /** 按类别归组（category → 该类别下全部子分配器 + 峰值）。 */
    private final Map<String, CategoryStat> categories = new ConcurrentHashMap<>();

    private NodeMemory() {
        this.directBudget = readLongProperty(PROP_DIRECT_BYTES, DEFAULT_BUDGET_BYTES, 1L);
        this.root = new RootAllocator(directBudget);
        this.highWatermark = readDoubleProperty(PROP_HIGH_WATERMARK, DEFAULT_HIGH_WATERMARK);
        this.lowWatermark = readDoubleProperty(PROP_LOW_WATERMARK, DEFAULT_LOW_WATERMARK);
        this.throttleMaxMs = readLongProperty(PROP_THROTTLE_MAX_MS, DEFAULT_THROTTLE_MAX_MS, 0L);
        log.info("[内存] 节点共享分配器：堆外上限 {}，背压水位 高={} 低={}，单次最长等待 {}ms",
                formatBytes(directBudget), highWatermark, lowWatermark, throttleMaxMs);
        startSampler();
    }

    /** 进程级单例（惰性初始化）。 */
    public static NodeMemory shared() {
        NodeMemory local = instance;
        if (local == null) {
            synchronized (NodeMemory.class) {
                local = instance;
                if (local == null) {
                    local = new NodeMemory();
                    instance = local;
                }
            }
        }
        return local;
    }

    /** 重置单例（<b>仅供离线探针</b>在进程内重建不同配置的场景使用，生产勿调）。 */
    public static synchronized void resetForTest() {
        instance = null;
    }

    /**
     * 建一个消费方子分配器（有独立上限、可单独 close），内存账挂在<b>共享根</b>上 —— 全进程堆外用量受
     * {@link #directBudget} 硬约束。{@code childLimit} 会被封顶到节点堆外总量（子不能大于父）。
     *
     * @param category  统计类别（如 {@code op/hashjoin}、{@code connector/mysql}、{@code exchange/shuffle}）
     * @param childLimit 子分配器上限（字节）；{@code <= 0} = 用节点堆外总量
     */
    public BufferAllocator newChildAllocator(String category, long childLimit) {
        long limit = childLimit > 0 ? Math.min(childLimit, directBudget) : directBudget;
        // Arrow 兄弟分配器名须唯一（同名会被 Arrow 拒绝）—— 统计类别是稳定的，Arrow 名字按序自增，
        // 于是「多实例同类别」既能聚合统计，又不会撞名。
        BufferAllocator child = root.newChildAllocator(category + "#" + ARROW_NAME_SEQ.incrementAndGet(), 0, limit);
        CategoryStat stat = categories.computeIfAbsent(category, k -> new CategoryStat());
        stat.children.add(child);
        if (limit > stat.limitBytes) {
            stat.limitBytes = limit;
        }
        return child;
    }

    /** 算子子分配器便捷入口：类别自动归入 {@code op/<name>}。 */
    public BufferAllocator newOperatorAllocator(String name, long childLimit) {
        return newChildAllocator("op/" + name, childLimit);
    }

    /** 当前全进程堆外真实占用（字节）。 */
    public long allocatedBytes() {
        return root.getAllocatedMemory();
    }

    /** 本节点堆外上限（字节）。 */
    public long budgetBytes() {
        return directBudget;
    }

    /** 堆外用量占比（0~1）。 */
    public double directUsageRatio() {
        return directBudget <= 0 ? 0.0 : (double) root.getAllocatedMemory() / directBudget;
    }

    /**
     * 背压闸门 —— <b>只在「即将读取/累积更多数据」的地方调用</b>（扫描读页、溢写算子累积页），
     * 绝不放在溢写 / 释放 / 归并路径上（那些是「把内存还回来」的动作，卡住它们会永久无法解除背压）。
     *
     * <p>语义：堆外用量低于高水位 → 立即返回（零开销）；否则短暂等待，回落到低水位或超过
     * {@link #throttleMaxMs} 即放行。有界等待意味着即便出现跨查询循环等待也不会永久卡死
     * （超时放行后由共享根的硬上限兜底）。</p>
     */
    public void throttle() {
        if (directUsageRatio() < highWatermark) {
            return;
        }
        long deadline = System.nanoTime() + throttleMaxMs * 1_000_000L;
        while (directUsageRatio() >= lowWatermark && System.nanoTime() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** 按类别聚合的快照（用量降序）。监控 UI / 心跳用它细看各消费方占用。 */
    public List<AllocatorUsage> snapshot() {
        refresh();
        List<AllocatorUsage> out = new ArrayList<>(categories.size());
        for (Map.Entry<String, CategoryStat> e : categories.entrySet()) {
            CategoryStat s = e.getValue();
            out.add(new AllocatorUsage(e.getKey(), s.lastAllocated, s.peakBytes, s.limitBytes));
        }
        out.sort((a, b) -> Long.compare(b.allocatedBytes(), a.allocatedBytes()));
        return out;
    }

    // ==================== 内部 ====================

    /** 刷新各类别的当前用量与峰值；同时剪除已 close 的子分配器（避免长跑进程里列表无限增长）。 */
    private void refresh() {
        Set<BufferAllocator> live = Collections.newSetFromMap(new IdentityHashMap<>());
        live.addAll(root.getChildAllocators());
        for (CategoryStat stat : categories.values()) {
            stat.children.removeIf(c -> !live.contains(c));
            long sum = 0;
            for (BufferAllocator c : stat.children) {
                try {
                    sum += c.getAllocatedMemory();
                } catch (Throwable ignore) {
                    // 并发 close 时可能抛错，忽略该子项
                }
            }
            stat.lastAllocated = sum;
            if (sum > stat.peakBytes) {
                stat.peakBytes = sum;
            }
        }
    }

    private void startSampler() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(SAMPLE_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                try {
                    refresh();
                } catch (Throwable ignore) {
                    // 采样线程永不因异常退出
                }
            }
        }, "dojao-node-memory-sampler");
        t.setDaemon(true);
        t.start();
    }

    /** 一个统计类别：其下全部子分配器 + 汇总字段。 */
    private static final class CategoryStat {
        final List<BufferAllocator> children = new CopyOnWriteArrayList<>();
        volatile long limitBytes;
        volatile long peakBytes;
        volatile long lastAllocated;
    }

    private static long readLongProperty(String key, long def, long min) {
        String v = System.getProperty(key);
        if (v == null || v.isBlank()) {
            return def;
        }
        try {
            long parsed = Long.parseLong(v.trim());
            return parsed >= min ? parsed : def;
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static double readDoubleProperty(String key, double def) {
        String v = System.getProperty(key);
        if (v == null || v.isBlank()) {
            return def;
        }
        try {
            double parsed = Double.parseDouble(v.trim());
            return parsed > 0 && parsed <= 1 ? parsed : def;
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        if (bytes < 1024L * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        }
        return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
}
