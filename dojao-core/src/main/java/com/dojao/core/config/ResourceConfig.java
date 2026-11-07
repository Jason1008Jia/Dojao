package com.dojao.core.config;

import com.dojao.common.memory.NodeMemory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;

/**
 * Dojao 配置统一入口 —— <b>一个 {@code conf/dojao.conf} 描述整个集群</b>，三层加载：
 * <b>系统属性（{@code -Dkey=value}） &gt; 配置文件 &gt; 内置默认</b>。
 *
 * <p><b>键命名标准</b>（必须遵守）：
 * <ul>
 *   <li><b>系统级</b>一律 {@code system.} 开头，第二段是<b>作用域 / 模块</b>（{@code master} / {@code worker}
 *       / {@code ui} …），再逐级向下：{@code system.master.port}、{@code system.worker.parallelism}、
 *       {@code system.ui.port}；</li>
 *   <li><b>查询相关</b>一律 {@code query.} 开头（<b>不加</b> {@code system.} 前缀）：{@code query.slot.memory}、
 *       {@code query.spill.ratio}；</li>
 *   <li>{@code ai.*} 预留给后续 AI 相关配置。</li>
 * </ul>
 *
 * <p><b>角色与作用域</b>：{@code system.role = master | worker | both}。{@code both} 是<b>机器的角色</b>而非
 * 进程的角色 —— 启动入口据此在同机拉起 Master + Worker 两个子 JVM（分别盖 {@code -Dsystem.role=master}
 * / {@code -Dsystem.role=worker}）。键查找链由此扩展为
 * <b>{@code -D<key>} &gt; 文件 {@code <role>.<key>} &gt; 文件 {@code <key>} &gt; 内置默认</b>，
 * 例如 {@code master.system.master.host = 0.0.0.0} 与 {@code worker.system.master.host = <Master IP>}
 * 可共存于同一份 conf（仅在需要一份文件描述多种角色时使用；常规部署每台机器各写本机键，无需前缀）。</p>
 *
 * <p><b>全仓唯一入口</b>：{@code dojao-metastore} / {@code dojao-ui} 看不到本类，其
 * {@code System.getProperty} 调用点靠本类类初始化时的<b>回填</b>（{@link #backfillToSystemProperties()}）
 * 吃到配置值；因此启动入口必须第一行调用 {@link #init()}。</p>
 *
 * <p>配置文件定位：{@code -Ddojao.conf=路径} 优先，其次当前目录 {@code conf/dojao.conf}，
 * 再次 classpath 根 {@code /dojao.conf}；全部未找到时仅用内置默认 + 系统属性。
 * 文件在<b>类初始化时读一次</b>（{@code -D} 亦然）⇒ 改完必须重启。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/29
 */
public final class ResourceConfig {

    private static final Logger log = LoggerFactory.getLogger(ResourceConfig.class);

    // ==================== 配置键 ====================

    /** 配置文件路径系统属性。 */
    public static final String CONF_FILE_KEY = "dojao.conf";

    /** 安装根目录系统属性（{@code bin/dojao} 设置；相对路径配置项按它解析）。 */
    public static final String HOME_DIR_KEY = "dojao.home";

    // ---- 节点角色 / 部署拓扑（system.role 起，逐级向下展开）----

    /** 本节点角色（{@code system.role}）：{@code master} / {@code worker} / {@code both}。 */
    public static final String SYSTEM_ROLE = "system.role";
    /** Master Flight 绑定地址（Master 进程）/ 连接地址（Worker 进程）—— 靠角色前缀区分，不拆键。 */
    public static final String SYSTEM_MASTER_HOST = "system.master.host";
    /** Master Flight 交互端口，默认 {@code 6530}。 */
    public static final String SYSTEM_MASTER_PORT = "system.master.port";
    /** Worker Flight 绑定地址，默认 {@code 127.0.0.1}。 */
    public static final String SYSTEM_WORKER_HOST = "system.worker.host";
    /** Worker Flight 端口，默认 {@code 4009}（单机单 Worker；开发三 Worker 用 4007-4009）。 */
    public static final String SYSTEM_WORKER_PORT = "system.worker.port";
    /** Web 控制台端口，默认 {@code 2380}。 */
    public static final String SYSTEM_UI_PORT = "system.ui.port";
    /** 元数据 SQLite 文件路径（Master / Worker 须同一文件），默认 {@code data/dojao-metastore.db}。 */
    public static final String SYSTEM_METASTORE_SQLITE_PATH = "system.metastore.sqlite.path";

    // ---- 连接器（Master / Worker 同源配置）----

    public static final String SYSTEM_CONNECTOR_CATALOG = "system.connector.catalog";
    /**
     * 连接器<b>类型</b>（{@code system.connector.type}）—— 用于按 {@code ConnectorFactory} 现场装配
     * dev 兜底 catalog（{@code DojaoMaster/DojaoWorker}）。缺省 = {@code system.connector.catalog} 的值，
     * 于是既有 {@code -Dsystem.connector.catalog=mysql} 的写法行为不变（类型 = 名字）。
     */
    public static final String SYSTEM_CONNECTOR_TYPE = "system.connector.type";
    public static final String SYSTEM_CONNECTOR_URL = "system.connector.url";
    public static final String SYSTEM_CONNECTOR_USER = "system.connector.user";
    public static final String SYSTEM_CONNECTOR_PASSWORD = "system.connector.password";

    /** Master 子进程直接内存上限（{@code system.master.direct}）—— 启动器据此设 {@code -XX:MaxDirectMemorySize}。 */
    public static final String SYSTEM_MASTER_DIRECT = "system.master.direct";
    /** Worker 子进程直接内存上限（{@code system.worker.direct}）—— 同上。 */
    public static final String SYSTEM_WORKER_DIRECT = "system.worker.direct";

    /** Master 子进程堆上限（{@code system.master.heap}）—— 启动器据此设 {@code -Xmx}。 */
    public static final String SYSTEM_MASTER_HEAP = "system.master.heap";
    /** Worker 子进程堆上限（{@code system.worker.heap}）—— 同上。 */
    public static final String SYSTEM_WORKER_HEAP = "system.worker.heap";
    /**
     * Master 进程可用内存<b>总量</b>（{@code system.master.memory}，堆 + 堆外）—— 精简配置主键之一：
     * 与 {@link #QUERY_MEMORY_HEAP_RATIO} 一起反推 {@code -Xmx} 与 {@code -XX:MaxDirectMemorySize}。
     */
    public static final String SYSTEM_MASTER_MEMORY = "system.master.memory";

    public static final String SYSTEM_MASTER_PARALLELISM = "system.master.parallelism";
    public static final String SYSTEM_WORKER_PARALLELISM = "system.worker.parallelism";
    public static final String SYSTEM_WORKER_MEMORY = "system.worker.memory";
    public static final String QUERY_PARALLELISM = "query.parallelism";
    public static final String QUERY_SLOT_MEMORY = "query.slot.memory";
    public static final String QUERY_SPILL_RATIO = "query.spill.ratio";

    // ---- 精简内存模型（query.memory.*）：只暴露 4 个键，其余全部由它们反推 ----
    /**
     * 单个 query 在<b>每台 Worker</b> 上的内存上限（{@code query.memory.max}，堆 + 堆外）。
     *
     * <p>与 {@link #QUERY_MEMORY_HEAP_RATIO} 一起反推「每算子预算 → 卡槽阈值」：把 query 的堆外份额
     * 按「单 Worker 并发片段数」摊薄，再按「3 卡槽流水线 × 溢写比例」摊到单卡槽，得到
     * {@code query.slot.memory} 的派生值。不配 = 直接用本节点堆外全量（{@link #nodeDirectMemoryBytes()}）。</p>
     */
    public static final String QUERY_MEMORY_MAX = "query.memory.max";
    /** query 预算 / 节点内存里<b>堆内</b>占比（{@code query.memory.heapRatio}，(0,1)，默认 0.5）。 */
    public static final String QUERY_MEMORY_HEAP_RATIO = "query.memory.heapRatio";
    /** 节点堆外用量<b>高水位</b>（{@code query.memory.highWatermark}，默认 0.85）：达到即对读取端施加背压。 */
    public static final String QUERY_MEMORY_HIGH_WATERMARK = "query.memory.highWatermark";
    /** 节点堆外用量<b>低水位</b>（{@code query.memory.lowWatermark}，默认 0.75）：回落到此值解除背压。 */
    public static final String QUERY_MEMORY_LOW_WATERMARK = "query.memory.lowWatermark";
    /** 单次背压<b>最长</b>等待毫秒（{@code query.memory.throttleMaxMs}，默认 1000）—— 有界等待，绝不永久阻塞。 */
    public static final String QUERY_MEMORY_THROTTLE_MAX_MS = "query.memory.throttleMaxMs";
    /** 单 Worker 并发片段数上限的显式覆盖（{@code query.memory.maxFragments}）；默认 = 2 × {@code system.worker.parallelism}。 */
    public static final String QUERY_MEMORY_MAX_FRAGMENTS = "query.memory.maxFragments";
    /**
     * 节点堆外中<b>预留</b>给传输层（Netty / gRPC / Arrow Flight 收发缓冲）的比例
     * （{@code query.memory.transportReserveRatio}，(0,1)，默认 0.2）。
     *
     * <p>为什么必须预留：这些缓冲是 JVM <b>直接内存</b>的一部分，却<b>不经过 Arrow 分配器记账</b>。
     * 若 {@link NodeMemory} 的共享根上限直接取整个 {@code -XX:MaxDirectMemorySize}，Arrow 就可能在
     * 传输层正忙时把预算吃干 —— 此时再来的 Flight 消息会抛 <b>JVM 级</b>
     * {@code OutOfMemoryError: Cannot reserve ... direct buffer memory}（日志栈在 {@code ArrowMessage.frame}），
     * 直接打断 Worker→Master 的结果链路。预留后 Arrow 先撞自己的软上限（可捕获的 Arrow OOM，查询失败
     * 但节点/传输存活），传输层始终有额度可用。</p>
     */
    public static final String QUERY_MEMORY_TRANSPORT_RESERVE_RATIO = "query.memory.transportReserveRatio";
    /** 溢写目录（{@code query.spill.dir}）：逗号分隔可配多个目录，默认 {@code <DOJAO_HOME>/spill}。 */
    public static final String QUERY_SPILL_DIR = "query.spill.dir";
    /** 溢写盘使用率上限（{@code query.spill.disk.maxUsage}，(0,1]）：达到即不再往该盘写。 */
    public static final String QUERY_SPILL_DISK_MAX_USAGE = "query.spill.disk.maxUsage";
    /** 单个溢写算子的 Arrow 直接内存上限（{@code query.direct.limit}，字节；0/未配 = 由 slot.memory 派生）。 */
    public static final String QUERY_DIRECT_LIMIT = "query.direct.limit";
    public static final String QUERY_DEFAULT_PARALLELISM = "query.default.parallelism";
    public static final String QUERY_ADMISSION_RATIO = "query.admission.ratio";
    public static final String QUERY_PARALLELISM_BYTES_PER_SLOT = "query.parallelism.bytesPerSlot";
    public static final String QUERY_PARALLELISM_ROWS_PER_SLOT = "query.parallelism.rowsPerSlot";
    public static final String QUERY_MAX_PARALLELISM = "query.max.parallelism";
    /** Master 侧回退单机聚合的显式内存预算（{@code system.master.agg.memory}）。 */
    public static final String SYSTEM_MASTER_AGG_MEMORY = "system.master.agg.memory";
    /** 聚合算子 Phase 2 每批聚合的 key 数（{@code query.agg.perBatchKeys}）。 */
    public static final String QUERY_AGG_PER_BATCH_KEYS = "query.agg.perBatchKeys";
    /** 聚合算子 Phase 2 溢写归并每批处理的 key 数（{@code query.agg.spill.perBatchKeys}）。 */
    public static final String QUERY_AGG_SPILL_PER_BATCH_KEYS = "query.agg.spill.perBatchKeys";
    /**
     * collect 聚合 Phase 2 单次交付的<b>值总数</b>预算（{@code query.agg.collectDeliverValues}）。
     *
     * <p>collect 契约要求把每个 key 的全部值一次性交给外挂函数求值，单批 key 数不能独立于值数控制。
     * 溢写路径按该预算切 key 批次，交付峰值 ≈ 一个交付 batch + 一个溢写文件。</p>
     */
    public static final String QUERY_AGG_COLLECT_DELIVER_VALUES = "query.agg.collectDeliverValues";
    /** 窗口算子输出页行数（{@code query.window.perBatchKeys}）。 */
    public static final String QUERY_WINDOW_PER_BATCH_KEYS = "query.window.perBatchKeys";
    /** 窗口算子<b>溢写</b>每批行数 —— Phase 1 落盘文件切分粒度（{@code query.window.spill.perBatchRows}）。 */
    public static final String QUERY_WINDOW_SPILL_PER_BATCH_ROWS = "query.window.spill.perBatchRows";
    /** 窗口算子<b>拉取</b>每批 key 数 —— Phase 2 成页回传下游的 key 组数（{@code query.window.spill.fetchPerKeys}）。 */
    public static final String QUERY_WINDOW_SPILL_FETCH_PER_KEYS = "query.window.spill.fetchPerKeys";
    /** 无 ORDER BY 聚合窗口的整分区缓冲上限（字节，{@code query.window.partitionAggBufferBytes}），超限转磁盘溢写。 */
    public static final String QUERY_WINDOW_PARTITION_AGG_BUFFER_BYTES =
            "query.window.partitionAggBufferBytes";
    /** 顶层无 LIMIT 的 ORDER BY 全局排序超内存预算策略（{@code query.sort.topSpillPolicy}）：error / warn / spill。 */
    public static final String QUERY_SORT_TOP_SPILL_POLICY = "query.sort.topSpillPolicy";

    /**
     * Master 结果回传看门狗超时（毫秒）：从最后一次「接触」（数据页 / 完成 / 存活心跳）起，
     * 超过该值仍无任何回传 → 判链路故障 fail-fast。默认 10 分钟（见 {@link #QUERY_RESULT_WATCHDOG_MS_DEFAULT}）。
     */
    public static final String QUERY_RESULT_WATCHDOG_MS = "query.result.watchdogMs";    /** 本地 join 算法（{@code query.join.algorithm}）：auto / hash / sort_merge / broadcast。 */
    public static final String QUERY_JOIN_ALGORITHM = "query.join.algorithm";
    /** 小表档阈值（{@code query.join.smallRatio}）：s ≤ U × ratio → 广播候选。 */
    public static final String QUERY_JOIN_SMALL_RATIO = "query.join.smallRatio";
    /** 大表档阈值（{@code query.join.largeRatio}）：s ≤ U × ratio → 哈希可行；超出 → 排序归并。 */
    public static final String QUERY_JOIN_LARGE_RATIO = "query.join.largeRatio";
    /** 判定基准 U 里「最弱一台 worker 物理可用内存」的占比（{@code query.join.workerMemRatio}）。 */
    public static final String QUERY_JOIN_WORKER_MEM_RATIO = "query.join.workerMemRatio";
    /** 广播 join 开关（{@code query.join.broadcast.enabled}）。 */
    public static final String QUERY_JOIN_BROADCAST_ENABLED = "query.join.broadcast.enabled";
    /** 文件型压缩数据源的「解压膨胀系数」（{@code query.join.compressedExpandRatio}）。 */
    public static final String QUERY_JOIN_COMPRESSED_EXPAND_RATIO = "query.join.compressedExpandRatio";
    /** 二级 COUNT(DISTINCT) 改写开关（{@code query.distinct.twoLevel}，默认开）。 */
    public static final String QUERY_DISTINCT_TWO_LEVEL = "query.distinct.twoLevel";
    /** 二级 COUNT(DISTINCT) 的子桶数（{@code query.distinct.twoLevel.buckets}，0 = 自动 = 并行度）。 */
    public static final String QUERY_DISTINCT_TWO_LEVEL_BUCKETS = "query.distinct.twoLevel.buckets";
    /** 分区元数据扫描开关（{@code query.partition.metadataScan}，默认开）。 */
    public static final String QUERY_PARTITION_METADATA_SCAN = "query.partition.metadataScan";
    /** 多次引用 CTE 物化开关（{@code query.cte.materialize}，默认开）。 */
    public static final String QUERY_CTE_MATERIALIZE = "query.cte.materialize";
    /**
     * 物化 CTE 随切片下发的 wire 载荷总量上限（{@code query.cte.wire.maxBytes}）。
     *
     * <p>物化文件在 Master 侧重编码成 Arrow IPC 字节后随切片常驻 Master 堆直至派发完成；
     * 超过上限直接失败并提示调高本配置，而不是把整个 Master 堆交给一条查询。</p>
     */
    public static final String QUERY_CTE_WIRE_MAX_BYTES = "query.cte.wire.maxBytes";
    /**
     * 多维聚合展开开关（{@code query.groupingSets.expand}，默认开）：{@code ROLLUP/CUBE/GROUPING SETS}
     * 只扫一次表（展开算子 + 单次聚合）而非每维度组合各扫一次（UNION ALL 多腿）。关掉 = 回退旧多腿路径。
     */
    public static final String QUERY_GROUPING_SETS_EXPAND = "query.groupingSets.expand";

    // ==================== 内置默认 ====================

    /** 默认角色 {@code both}：单机一键部署 = 同一台机器上 Master + Worker 两个 JVM。 */
    public static final String DEFAULT_ROLE = "both";
    /** Master 默认绑定全部网卡（集群部署时 Worker 才连得进来）。 */
    public static final String DEFAULT_MASTER_HOST = "0.0.0.0";
    public static final int DEFAULT_MASTER_PORT = 6530;
    public static final String DEFAULT_WORKER_HOST = "127.0.0.1";
    /** Worker 默认端口 4009：全机唯一 Worker（分机部署）时的取值。 */
    public static final int DEFAULT_WORKER_PORT = 4009;
    public static final int DEFAULT_UI_PORT = 2380;
    public static final String DEFAULT_METASTORE_SQLITE_PATH = "data/dojao-metastore.db";
    public static final String DEFAULT_CONNECTOR_CATALOG = "mysql";
    public static final String DEFAULT_CONNECTOR_URL = "jdbc:mysql://localhost:3306/dga";
    public static final String DEFAULT_CONNECTOR_USER = "root";
    public static final String DEFAULT_CONNECTOR_PASSWORD = "123456";

    public static final int DEFAULT_MASTER_PARALLELISM = 4;
    /** 单个 Worker 默认任务池容量：测试环境给 8-10 即可。 */
    public static final int DEFAULT_WORKER_PARALLELISM = 8;
    /** Worker 进程内存总量默认 5g（堆 2.5g + 堆外 2.5g），与随包 conf 的 4C8G 预设一致。 */
    public static final long DEFAULT_WORKER_MEMORY_BYTES = 5L * 1024 * 1024 * 1024;
    /** 子进程直接内存默认 2g（与 {@code ClusterMain} / {@code DevClusterMain} 的兜底一致）。 */
    public static final long DEFAULT_NODE_DIRECT_MEMORY_BYTES = 2L * 1024 * 1024 * 1024;
    /** 子进程堆默认 2g。 */
    public static final long DEFAULT_NODE_HEAP_MEMORY_BYTES = 2L * 1024 * 1024 * 1024;
    /** Master 进程内存总量默认 2g（堆 1g + 堆外 1g）。 */
    public static final long DEFAULT_MASTER_MEMORY_BYTES = 2L * 1024 * 1024 * 1024;
    /** 内存模型默认堆内占比 0.5：一半堆内（分组索引 / 装箱对象），一半堆外（Arrow 列向量）。 */
    public static final double DEFAULT_MEMORY_HEAP_RATIO = 0.5;
    /** 节点堆外用量高水位默认 0.85：超过即对读取端施加背压。 */
    public static final double DEFAULT_MEMORY_HIGH_WATERMARK = 0.85;
    /** 节点堆外用量低水位默认 0.75：回落到此值解除背压。 */
    public static final double DEFAULT_MEMORY_LOW_WATERMARK = 0.75;
    /** 单次背压最长等待默认 1000ms（有界，避免跨查询循环等待死锁）。 */
    public static final long DEFAULT_MEMORY_THROTTLE_MAX_MS = 1000L;
    /** 传输层预留比例默认 0.2：堆外的 20% 留给 Netty/gRPC（不经 Arrow 记账），Arrow 只用其余部分。 */
    public static final double DEFAULT_MEMORY_TRANSPORT_RESERVE_RATIO = 0.2;
    /** 传输层预留的绝对上限 1g：大节点（如 32g 堆外）留 20% 过多，1g 已足够数百条 Flight 流。 */
    public static final long TRANSPORT_RESERVE_CAP_BYTES = 1024L * 1024 * 1024;
    /** 三卡槽流水线槽位数 —— 与 {@code AbstractSpillableOperator.SLOT_COUNT} 同源。 */
    public static final int SLOT_PIPELINE = 3;
    /** 卡槽预算下限 32M：派生值再小也不至于一页都装不下。 */
    public static final long MIN_SLOT_MEMORY_BYTES = 32L * 1024 * 1024;
    /**
     * 单算子分配器上限相对「三槽设计峰值」的余量系数。
     *
     * <p>设计峰值 = {@code SLOT_PIPELINE × slot × spillRatio}（各分区阈值已按分区数摊薄，见
     * {@code AbstractSpillableOperator}）。但真实峰值更高：① 溢写时 {@code buildSortedSpillRoot}
     * 会再拷一份待写数据（同一槽位瞬时 ×2）；② Arrow 变长列按 2 的幂扩容，realloc 保存新 buffer
     * 再释放旧 buffer（瞬时 ×~2）。故系数取 {@value}（&gt; 2），让「设计峰值的 2 倍 + 归并批」
     * 仍落在硬上限内 —— 否则多分区窗口/聚合会在单槽都没到阈值时先撞硬上限报 Arrow OOM。
     */
    public static final double DIRECT_LIMIT_HEADROOM = 2.5;
    /** 每 query 算子内存预算默认 2G，对齐旧 CommonConfig.OPERATOR_MEMORY_LIMIT_BYTES。 */

    public static final long DEFAULT_SLOT_MEMORY_BYTES = 2L * 1024 * 1024 * 1024;

    public static final double DEFAULT_SPILL_RATIO = 0.8;
    /** 默认溢写目录名（相对安装根目录）：{@code <DOJAO_HOME>/spill}。 */
    public static final String DEFAULT_SPILL_DIR_NAME = "spill";
    /** 默认溢写盘使用率上限 0.8：用满 80% 的盘不再承接溢写。 */
    public static final double DEFAULT_SPILL_DISK_MAX_USAGE = 0.8;
    /** 聚合 Phase 2 每批 key 数默认 50000（query.agg.perBatchKeys）。 */
    public static final int DEFAULT_AGG_PER_BATCH_KEYS = 50000;
    /** 聚合溢写归并每批 key 数默认 10000（query.agg.spill.perBatchKeys）。 */
    public static final int DEFAULT_AGG_SPILL_PER_BATCH_KEYS = 10000;
    /** collect 聚合 Phase 2 单次交付值总数默认 100 万（query.agg.collectDeliverValues）。 */
    public static final int DEFAULT_AGG_COLLECT_DELIVER_VALUES = 1_000_000;
    /** 窗口输出页行数默认 50000（query.window.perBatchKeys）。 */
    public static final int DEFAULT_WINDOW_PER_BATCH_KEYS = 50000;
    /** 窗口<b>溢写</b>每批行数默认 10000（query.window.spill.perBatchRows）。 */
    public static final int DEFAULT_WINDOW_SPILL_PER_BATCH_ROWS = 10000;
    /** 窗口<b>拉取</b>每批 key 数默认 10000（query.window.spill.fetchPerKeys）。 */
    public static final int DEFAULT_WINDOW_SPILL_FETCH_PER_KEYS = 10000;
    /** 无 ORDER BY 聚合窗口的整分区内存缓冲上限默认 64MB，超过转磁盘溢写。 */
    public static final long DEFAULT_WINDOW_PARTITION_AGG_BUFFER_BYTES = 64L * 1024 * 1024;
    /** 顶层无 LIMIT 的 ORDER BY 全局排序超预算默认策略 = {@code error}（全量收拢 Master 排序+溢写危险，宁可拒绝并提示加 LIMIT）。 */
    public static final String DEFAULT_TOP_ORDER_SORT_SPILL_POLICY = "error";
    /**
     * Master 结果回传看门狗默认超时 = 10 分钟（{@code query.result.watchdogMs}）。
     *
     * <p>刻意放宽：溢写算子的 Phase 2 归并、大表 join/聚合的 build 期都可能远超 1 分钟，旧默认 60s 会把
     * 「合法的长时间不吐页」误判成回传链路故障而 fail-fast（报错文案还指错方向）。真正的死锁/链路故障
     * 仍会被兜住，只是晚 10 分钟暴露；需要更快失败时把本键调小即可。</p>
     */
    public static final long DEFAULT_RESULT_WATCHDOG_MS = 10L * 60 * 1000;
    /** 本地 join 算法默认 {@code auto}：按两侧数据量自动选（build 侧装得下就 HASH，装不下退 SORT_MERGE 溢写）。 */
    public static final String DEFAULT_JOIN_ALGORITHM = "auto";
    /** 小表档默认 0.25：单侧体量 ≤ 单台预算的四分之一才考虑广播（复制到每台，代价 ×worker 数）。 */
    public static final double DEFAULT_JOIN_SMALL_RATIO = 0.25;
    /** 大表档默认 0.8：单侧体量 ≤ 单台预算的 80% 才认为哈希 build 侧装得下（留 20% 给估算误差）。 */
    public static final double DEFAULT_JOIN_LARGE_RATIO = 0.8;
    /** 判定基准里 worker 物理可用内存的默认占比 0.5（物理内存 ≠ 堆，只当上限用，不敢占满）。 */
    public static final double DEFAULT_JOIN_WORKER_MEM_RATIO = 0.5;
    /** 广播默认开启。 */
    public static final boolean DEFAULT_JOIN_BROADCAST_ENABLED = true;
    /** 文件型压缩数据源默认膨胀系数 1.0：按文件字节估内存，若数据源是 ORC/Parquet 等压缩格式可调高。 */
    public static final double DEFAULT_JOIN_COMPRESSED_EXPAND_RATIO = 1.0;
    /** 二级 COUNT(DISTINCT) 改写默认开启（单热点分组横向分摊）。 */
    public static final boolean DEFAULT_DISTINCT_TWO_LEVEL = true;
    /** 子桶数默认 0 = 自动（取查询并行度），clamp 到 [2, 256]。 */
    public static final int DEFAULT_DISTINCT_TWO_LEVEL_BUCKETS = 0;
    /** 分区元数据扫描默认开启（聚合输入全是分区列时不扫数据文件）。 */
    public static final boolean DEFAULT_PARTITION_METADATA_SCAN = true;
    /** 多次引用 CTE 物化默认开启（同一查询体只执行一次，结果落盘供各引用重放）。 */
    public static final boolean DEFAULT_CTE_MATERIALIZE = true;
    /** wire 载荷总量默认上限：512MB（单条 CTE；超出需显式调高）。 */
    public static final long DEFAULT_CTE_WIRE_MAX_BYTES = 512L * 1024 * 1024;
    /** 多维聚合展开默认开启（表只扫一次，各维度组合由放大后的行在聚合端分开）。 */
    public static final boolean DEFAULT_GROUPING_SETS_EXPAND = true;
    /** Master 侧回退单机聚合默认预算 256M（见 {@link #masterAggregationMemoryBytes()}）。 */
    public static final long DEFAULT_MASTER_AGG_MEMORY_BYTES = 256L * 1024 * 1024;
    /** 无元数据可探查时的兜底并行度（query.default.parallelism）。 */
    public static final int DEFAULT_QUERY_PARALLELISM = 4;
    /** 准入阈值：可用并行度 < 应需 × ratio 时阻塞等待（query.admission.ratio）。 */
    public static final double DEFAULT_ADMISSION_RATIO = 0.8;
    /** 每个 scan 分区承载的数据量（字节）：并行度 = ceil(byteSize / bytesPerSlot)（query.parallelism.bytesPerSlot）。
     *  默认 1G：对齐 query.slot.memory（每分区一个算子的内存预算），也避免小表被切成过多分区。 */
    public static final long DEFAULT_PARALLELISM_BYTES_PER_SLOT = (long) 1024 * 1024 * 1024;
    /** 每 scan 分区行数：bytes 未知时兜底（query.parallelism.rowsPerSlot）。 */
    public static final long DEFAULT_PARALLELISM_ROWS_PER_SLOT = 1_000_000L;
    /**
     * 每 query 并行度硬上限（{@code query.max.parallelism}）；{@code -1} = auto：
     * 未设置时用默认公式 = 集群容量 / 健康 worker 数（单个 query 最多用「一个 Worker 的平均份额」）。
     */
    public static final int DEFAULT_SYSTEM_QUERY_MAX_PARALLELISM = -1;

    /** {@code query.parallelism = auto} 的哨兵：沿用 {@code system.master.parallelism}。 */
    public static final int QUERY_PARALLELISM_AUTO = -1;

    /** 配置文件内容（类初始化读一次；找不到文件时为空，仅系统属性 + 默认生效）。 */
    private static final Properties FILE_PROPS = loadFileProps();

    /** 进程生效角色（类初始化解析一次，见 {@link #SYSTEM_ROLE}）。 */
    private static final Role ROLE = resolveRole();

    /** 类初始化前就已存在的系统属性（来自 {@code -D}）：回填时一律不覆盖，保证 {@code -D} > 文件。 */
    private static final java.util.Set<String> PRESET_KEYS =
            new java.util.HashSet<>(System.getProperties().stringPropertyNames());

    static {
        backfillToSystemProperties();
        publishMemoryProperties(false);
    }

    /**
     * 把内存派生值发布成系统属性，供<b>不依赖本类</b>的 {@link NodeMemory}（在 {@code dojao-common}）读取 ——
     * 于是「算子 + 连接器（插件）+ 传输」能共用同一本堆外账，而 {@code dojao-common} 无需反向依赖
     * {@code dojao-core}。
     *
     * <p>{@code ifAbsent=true}（类初始化时）：不覆盖已由 {@code -D} 显式设置的值；
     * {@code ifAbsent=false}（测试重启场景）：强制覆盖。</p>
     *
     * <p><b>注意发布的是 Arrow 预算（{@link #arrowDirectBudgetBytes()}），不是 JVM 直接内存全量</b>：
     * 差额（{@link #transportReserveRatio()}）留给不经 Arrow 记账的 Netty/gRPC 缓冲，避免传输层在
     * Arrow 吃满预算时抛 JVM 级 direct OOM、拖垮 Worker→Master 的结果链路。</p>
     */
    private static void publishMemoryProperties(boolean ifAbsent) {
        publish(NodeMemory.PROP_DIRECT_BYTES, Long.toString(arrowDirectBudgetBytes()), ifAbsent);
        publish(NodeMemory.PROP_HIGH_WATERMARK, Double.toString(memoryHighWatermark()), ifAbsent);
        publish(NodeMemory.PROP_LOW_WATERMARK, Double.toString(memoryLowWatermark()), ifAbsent);
        publish(NodeMemory.PROP_THROTTLE_MAX_MS, Long.toString(memoryThrottleMaxMs()), ifAbsent);
    }

    /**
     * 重新发布内存派生值为系统属性（<b>强制覆盖</b>）—— 供测试在改动内存配置后调用，
     * 使随后首次 {@link NodeMemory#shared()} 读到新值。生产无需调用（类初始化已发布一次）。
     */
    public static void publishMemoryProperties() {
        publishMemoryProperties(false);
    }

    private static void publish(String key, String value, boolean ifAbsent) {
        if (ifAbsent && System.getProperty(key) != null) {
            return;
        }
        System.setProperty(key, value);
    }

    private ResourceConfig() {
    }

    /**
     * 触发配置初始化：读 {@code conf/dojao.conf} → 按角色展平 → 回填进系统属性 → 打印生效摘要。
     *
     * <p>各启动入口（{@code ClusterMain} / {@code DojaoMaster} / {@code DojaoWorker}）应在<b>第一行</b>调用，
     * 保证其它模块里既有的 {@code System.getProperty(...)} 调用点在此之前就已吃到配置文件的值
     * （{@code dojao-metastore} / {@code dojao-ui} 看不到本类，只能走这条路）。</p>
     */
    public static void init() {
        log.info("[ResourceConfig] 角色={} 配置={} 文件键={} 项",
                ROLE.prefix(), confPathDescription(), FILE_PROPS.size());
    }

    // ==================== 角色 ====================

    /** 本节点角色（{@code system.role}）：决定角色前缀查找与启动拓扑。 */
    public enum Role {
        MASTER, WORKER, BOTH;

        /** 角色前缀 = 配置文件里 {@code <role>.<key>} 的 {@code <role>} 段。 */
        public String prefix() {
            return name().toLowerCase();
        }
    }

    /**
     * 解析生效角色：{@code -Dsystem.role} &gt; 文件 {@code system.role} &gt; {@link #DEFAULT_ROLE}（both）。
     * 显式写了但认不出来（如 {@code mater} 笔误）直接抛错 —— 角色错会让整台机器起错进程，不能静默兜底。
     */
    private static Role resolveRole() {
        String v = System.getProperty(SYSTEM_ROLE);
        if (v == null || v.isBlank()) {
            v = FILE_PROPS.getProperty(SYSTEM_ROLE);
        }
        if (v == null || v.isBlank()) {
            return Role.BOTH;
        }
        return switch (v.trim().toLowerCase()) {
            case "master" -> Role.MASTER;
            case "worker" -> Role.WORKER;
            case "both" -> Role.BOTH;
            default -> throw new IllegalStateException(
                    "system.role 取值非法: '" + v + "'（只能是 master / worker / both）");
        };
    }

    /**
     * 把配置文件的值回填进系统属性，使全仓既有 {@code System.getProperty} 调用点零改动即吃上配置。
     *
     * <p>两遍：先普通键，再<b>本角色</b>前缀键（{@code <role>.<key>} 更具体，覆盖前者）；已由 {@code -D}
     * 显式设置的键（{@link #PRESET_KEYS}）任何一遍都不覆盖。其它角色的前缀键一律忽略。</p>
     */
    private static void backfillToSystemProperties() {
        if (FILE_PROPS.isEmpty()) {
            return;
        }
        for (String name : FILE_PROPS.stringPropertyNames()) {
            if (!name.contains(".") || isRolePrefixed(name)) {
                continue;   // 角色前缀键留到第二遍
            }
            backfill(name, FILE_PROPS.getProperty(name));
        }
        String prefix = ROLE.prefix() + ".";
        for (String name : FILE_PROPS.stringPropertyNames()) {
            if (name.startsWith(prefix)) {
                backfill(name.substring(prefix.length()), FILE_PROPS.getProperty(name));
            }
        }
    }

    private static void backfill(String key, String value) {
        if (key == null || key.isBlank() || value == null || PRESET_KEYS.contains(key)) {
            return;
        }
        System.setProperty(key, value);
    }

    /** 首段是 master / worker / both 的键 = 角色前缀键（合规键里已无此类前缀，可安全判定）。 */
    private static boolean isRolePrefixed(String name) {
        int dot = name.indexOf('.');
        if (dot <= 0) {
            return false;
        }
        return switch (name.substring(0, dot)) {
            case "master", "worker", "both" -> true;
            default -> false;
        };
    }

    /** 生效配置文件的可读描述（用于启动日志；配置没生效时从日志一眼看出）。 */
    private static String confPathDescription() {
        String path = confFile();
        return path != null ? path
                : ResourceConfig.class.getResource("/dojao.conf") != null ? "classpath:/dojao.conf"
                : "(未找到，仅 -D + 内置默认)";
    }

    /**
     * 生效配置文件<b>绝对路径</b>（{@code -Ddojao.conf} 优先，其次当前目录 {@code conf/dojao.conf}）；
     * 都没有时返回 {@code null}。启动入口用它给子进程透传 {@code -Ddojao.conf=...}，
     * 避免子进程工作目录不同而读不到同一份配置。
     */
    public static String confFile() {
        String explicit = System.getProperty(CONF_FILE_KEY);
        if (explicit != null && !explicit.isBlank()) {
            return Path.of(explicit).toAbsolutePath().normalize().toString();
        }
        Path cwd = Path.of("conf", "dojao.conf");
        return Files.exists(cwd) ? cwd.toAbsolutePath().normalize().toString() : null;
    }

    // ==================== 访问器 ====================

    /** Master 查询并行分区数（{@code system.master.parallelism}，≥ 1）。 */
    public static int masterParallelism() {
        return Math.max(1, intValue(SYSTEM_MASTER_PARALLELISM, DEFAULT_MASTER_PARALLELISM));
    }

    /** 单个 Worker 任务池容量（{@code system.worker.parallelism}，≥ 1）。 */
    public static int workerParallelism() {
        return Math.max(1, intValue(SYSTEM_WORKER_PARALLELISM, DEFAULT_WORKER_PARALLELISM));
    }

    /** 单个 Worker 内存池总量（{@code system.worker.memory}，字节）。 */
    public static long workerMemoryBytes() {
        return Math.max(1L, sizeValue(SYSTEM_WORKER_MEMORY, DEFAULT_WORKER_MEMORY_BYTES));
    }

    // ==================== 精简内存模型（派生链） ====================
    //
    // 设计目标：用户只需声明「节点内存总量 + 每 query 上限 + 堆内占比」，其余全部反推 ——
    //   ① 节点级：nodeDirect = node总内存 × (1−ratio)（喂 -XX:MaxDirectMemorySize / 共享分配器）
    //             nodeHeap   = node总内存 × ratio    （喂 -Xmx）
    //   ② query  级：perQueryDirect = min(query.memory.max × (1−ratio), nodeDirect)
    //   ③ 片段  级：fragmentDirect = perQueryDirect / 单 Worker 并发片段数
    //   ④ 卡槽  级：slot.memory = fragmentDirect / (3 × spill.ratio)
    //
    // 兼容：任一旧键（system.*.heap / .direct / query.slot.memory / query.direct.limit）显式设置即
    //       生效，派生只在「未显式设置」时兜底 —— 存量部署零改动。

    /** 本节点（按 role 口径；{@code both} 取 worker）内存总量键。 */
    private static String nodeMemoryKey() {
        return ROLE == Role.MASTER ? SYSTEM_MASTER_MEMORY : SYSTEM_WORKER_MEMORY;
    }

    private static String nodeHeapKey() {
        return ROLE == Role.MASTER ? SYSTEM_MASTER_HEAP : SYSTEM_WORKER_HEAP;
    }

    private static String nodeDirectKey() {
        return ROLE == Role.MASTER ? SYSTEM_MASTER_DIRECT : SYSTEM_WORKER_DIRECT;
    }

    /** 是否显式声明了节点内存（总量 / 堆 / 堆外任一）—— 决定是否启用派生（否则保持旧默认）。 */
    private static boolean nodeMemoryConfigured() {
        return sizeValue(nodeMemoryKey(), 0L) > 0
                || sizeValue(nodeHeapKey(), 0L) > 0
                || sizeValue(nodeDirectKey(), 0L) > 0;
    }

    /** {@code query.memory.max} 是否显式配置。 */
    private static boolean queryMemoryMaxConfigured() {
        return sizeValue(QUERY_MEMORY_MAX, 0L) > 0;
    }

    /** 指定角色的进程内存总量（{@code system.master.memory} / {@code system.worker.memory}）。 */
    public static long nodeMemoryBytes(Role role) {
        String key = role == Role.MASTER ? SYSTEM_MASTER_MEMORY : SYSTEM_WORKER_MEMORY;
        long def = role == Role.MASTER ? DEFAULT_MASTER_MEMORY_BYTES : DEFAULT_WORKER_MEMORY_BYTES;
        return Math.max(1L, sizeValue(key, def));
    }

    /**
     * 指定角色的 {@code -Xmx}：显式 {@code system.<role>.heap} &gt; 由内存总量 × {@link #memoryHeapRatio()}
     * 派生 &gt; 旧默认 2g。
     */
    public static long heapMemoryBytes(Role role) {
        String heapKey = role == Role.MASTER ? SYSTEM_MASTER_HEAP : SYSTEM_WORKER_HEAP;
        long explicit = sizeValue(heapKey, 0L);
        if (explicit > 0) {
            return explicit;
        }
        String memKey = role == Role.MASTER ? SYSTEM_MASTER_MEMORY : SYSTEM_WORKER_MEMORY;
        long total = sizeValue(memKey, 0L);
        if (total > 0) {
            return Math.max(1L, (long) (total * memoryHeapRatio()));
        }
        return DEFAULT_NODE_HEAP_MEMORY_BYTES;
    }

    /**
     * 指定角色的 {@code -XX:MaxDirectMemorySize}：显式 {@code system.<role>.direct} &gt; 由内存总量 ×
     * {@code (1−ratio)} 派生 &gt; 旧默认 2g。
     */
    public static long directMemoryBytes(Role role) {
        String directKey = role == Role.MASTER ? SYSTEM_MASTER_DIRECT : SYSTEM_WORKER_DIRECT;
        long explicit = sizeValue(directKey, 0L);
        if (explicit > 0) {
            return explicit;
        }
        String memKey = role == Role.MASTER ? SYSTEM_MASTER_MEMORY : SYSTEM_WORKER_MEMORY;
        long total = sizeValue(memKey, 0L);
        if (total > 0) {
            return Math.max(1L, (long) (total * (1.0 - memoryHeapRatio())));
        }
        return DEFAULT_NODE_DIRECT_MEMORY_BYTES;
    }

    /** 内存模型堆内占比（{@code query.memory.heapRatio}，(0,1)）。 */
    public static double memoryHeapRatio() {
        double d = doubleValue(QUERY_MEMORY_HEAP_RATIO, DEFAULT_MEMORY_HEAP_RATIO);
        return d > 0 && d < 1 ? d : DEFAULT_MEMORY_HEAP_RATIO;
    }

    /** 节点堆外用量高水位（{@code query.memory.highWatermark}，(0,1]）。 */
    public static double memoryHighWatermark() {
        double d = doubleValue(QUERY_MEMORY_HIGH_WATERMARK, DEFAULT_MEMORY_HIGH_WATERMARK);
        return d > 0 && d <= 1 ? d : DEFAULT_MEMORY_HIGH_WATERMARK;
    }

    /** 节点堆外用量低水位（{@code query.memory.lowWatermark}，(0,1)）。 */
    public static double memoryLowWatermark() {
        double d = doubleValue(QUERY_MEMORY_LOW_WATERMARK, DEFAULT_MEMORY_LOW_WATERMARK);
        return d > 0 && d < 1 ? d : DEFAULT_MEMORY_LOW_WATERMARK;
    }

    /** 单次背压最长等待毫秒（{@code query.memory.throttleMaxMs}，≥ 0）。 */
    public static long memoryThrottleMaxMs() {
        return Math.max(0L, longValue(QUERY_MEMORY_THROTTLE_MAX_MS, DEFAULT_MEMORY_THROTTLE_MAX_MS));
    }

    /**
     * 单 Worker 并发片段数上限：显式 {@code query.memory.maxFragments} &gt; 默认 {@code 2 × worker.parallelism}。
     * 因子 2 = Worker 的两个任务池（消费者主池 + 叶子专线），见 {@code WorkerManager}。
     */
    public static int maxConcurrentFragments() {
        int explicit = intValue(QUERY_MEMORY_MAX_FRAGMENTS, 0);
        return explicit > 0 ? explicit : Math.max(1, 2 * workerParallelism());
    }

    /**
     * 单个片段可用的堆外份额 = min(query 堆外份额, 节点堆外) / 并发片段数。
     *
     * <p>「节点堆外」取 <b>Arrow 预算</b>（{@link #arrowDirectBudgetBytes()}，已扣传输层预留）——
     * 片段预算要摊的是 Arrow 实际能用的额度；用 JVM 全量会把每个片段放大，并发时过早撞 Arrow 上限。</p>
     */
    private static long fragmentDirectBytes() {
        long nodeDirect = arrowDirectBudgetBytes();
        long qmax = sizeValue(QUERY_MEMORY_MAX, 0L);
        long budget = qmax > 0
                ? Math.min((long) (qmax * (1.0 - memoryHeapRatio())), nodeDirect)
                : nodeDirect;
        return Math.max(1L, budget / Math.max(1, maxConcurrentFragments()));
    }

    /** 每 query 并行度（{@code query.parallelism}）；{@code auto} 时沿用 {@code system.master.parallelism}。 */
    public static int queryParallelism() {
        int v = intValue(QUERY_PARALLELISM, QUERY_PARALLELISM_AUTO);
        return v > 0 ? v : masterParallelism();
    }

    /**
     * 每 query 算子内存预算（{@code query.slot.memory}，字节）—— 单卡槽阈值 = 本值 × {@link #spillRatio()}。
     *
     * <p>取值：显式 {@code query.slot.memory} &gt; 派生 {@code fragmentDirect / (SLOT_PIPELINE × spillRatio)}
     * （仅在声明了节点内存或 {@code query.memory.max} 时启用，保证「一个 Worker 上全部算子并存也放得下」）
     * &gt; 旧默认 2G。</p>
     */
    public static long slotMemoryBytes() {
        long explicit = sizeValue(QUERY_SLOT_MEMORY, 0L);
        if (explicit > 0) {
            return explicit;
        }
        if (!nodeMemoryConfigured() && !queryMemoryMaxConfigured()) {
            return Math.max(1L, sizeValue(QUERY_SLOT_MEMORY, DEFAULT_SLOT_MEMORY_BYTES));
        }
        // 用 double 除（不可先 (long) 截断 3×0.8=2.4→2）：截断会把 slot 放大 20%，
        // 使三槽峰值 3×slot×ratio 反而超过 fragmentDirect，破坏「本片段全部算子并存也放得下」的保证。
        double divisor = Math.max(1.0, SLOT_PIPELINE * spillRatio());
        return Math.max(MIN_SLOT_MEMORY_BYTES, (long) (fragmentDirectBytes() / divisor));
    }

    /**
     * 本节点 JVM 的直接内存预算（{@code system.master.direct} / {@code system.worker.direct}，
     * 按 {@link #role()} 取，默认 2g）。
     *
     * <p>与启动器传给子进程的 {@code -XX:MaxDirectMemorySize} 同源（{@code ClusterMain} /
     * {@code DevClusterMain} 读的就是这两个键），故可当作「本进程堆外能用的总量」的准确口径。
     * 角色为 {@code both}（纯启动器，自己不跑算子）时取 worker 的值。</p>
     */
    public static long nodeDirectMemoryBytes() {
        return directMemoryBytes(ROLE);
    }

    /** 传输层（Netty/gRPC/Flight）预留比例（{@code query.memory.transportReserveRatio}，(0,1)，默认 0.2）。 */
    public static double transportReserveRatio() {
        double d = doubleValue(QUERY_MEMORY_TRANSPORT_RESERVE_RATIO, DEFAULT_MEMORY_TRANSPORT_RESERVE_RATIO);
        return d > 0 && d < 1 ? d : DEFAULT_MEMORY_TRANSPORT_RESERVE_RATIO;
    }

    /**
     * 指定角色进程里 <b>Arrow 共享根分配器</b>可用的堆外预算 = JVM 直接内存 − 传输层预留。
     *
     * <p>JVM 的 {@code -XX:MaxDirectMemorySize} 仍是全量（{@link #directMemoryBytes(Role)}）：Netty/gRPC
     * 的直接缓冲必须有额度可用。Arrow 侧的共享根（{@link NodeMemory}）只拿扣掉预留后的部分 ⇒ 算子内存
     * 失控时先抛可捕获的 Arrow OOM（该 query 失败、节点与传输存活），而不是在 Flight 线程里抛 JVM 级
     * direct OOM 打断结果链路。</p>
     */
    public static long arrowDirectBudgetBytes(Role role) {
        long direct = directMemoryBytes(role);
        long reserve = Math.min((long) (direct * transportReserveRatio()), TRANSPORT_RESERVE_CAP_BYTES);
        return Math.max(1L, direct - reserve);
    }

    /** 本节点（按 {@link #role()}）的 Arrow 共享根预算，见 {@link #arrowDirectBudgetBytes(Role)}。 */
    public static long arrowDirectBudgetBytes() {
        return arrowDirectBudgetBytes(ROLE);
    }

    /** 本节点 JVM 的堆上限（{@code -Xmx} 口径）：显式 {@code system.<role>.heap} &gt; 内存总量 × ratio 派生。 */
    public static long nodeHeapMemoryBytes() {
        return heapMemoryBytes(ROLE);
    }

    /**
     * 单个溢写算子的 Arrow 分配器上限（{@code query.direct.limit}，字节）。
     *
     * <p>为什么需要：Arrow 向量走<b>堆外</b>内存，而 {@link com.dojao.core.operator.AbstractSpillableOperator}
     * 的分配器此前是 {@code RootAllocator(Long.MAX_VALUE)}（无上限）—— 内存估算一旦失准（例如曾经漏算
     * 分组索引）就会无限增长，在 8GB 小机上被内核 OOM-killer 直接杀进程（<b>没有任何 Java 异常</b>，
     * 表现为「worker 服务器莫名挂掉」）。给分配器一个硬上限后，超限抛 Arrow OOM（算子可捕获）⇒
     * 查询失败而非节点死亡。</p>
     *
     * <p>未配置（0）时按 {@code max(2G, query.slot.memory x 3)} 派生：覆盖「三槽流水线 + 归并批」的
     * 峰值，又远小于无上限。</p>
     *
     * <p><b>再封顶到本节点 Arrow 预算</b>（{@link #arrowDirectBudgetBytes()}，= JVM 直接内存 −
     * 传输层预留）：派生公式里的 {@code slot.memory x 3} 会跟着 {@code query.slot.memory} 一起放大 ——
     * 小内存机上若 slot 按大机器配了 2G，派生值就是 6G，而 {@code -XX:MaxDirectMemorySize} 可能只有 2g：
     * 算子会先越过 JVM 堆外限额，被内核 OOM-killer 杀掉（无 Java 异常，表现为「worker 卡死 / 莫名挂了」）。
     * 封顶后最多撞 Arrow 的硬上限 → 抛 OOM，查询失败但节点存活；同时传输层预留始终不被算子占用。</p>
     */
    public static long queryDirectLimit() {
        long nodeDirect = arrowDirectBudgetBytes();
        long v = sizeValue(QUERY_DIRECT_LIMIT, 0L);
        if (v > 0) {
            return Math.min(v, nodeDirect);   // 显式配置优先（兼容旧 conf / 探针）
        }
        // 派生：单算子的子分配器上限。设计峰值 ≈ SLOT_PIPELINE × slot × spillRatio（各分区阈值已按
        // 分区数摊薄 ⇒ 与分区数无关），再乘余量系数覆盖「溢写拷贝 + 变长扩容」的瞬时翻倍（见
        // DIRECT_LIMIT_HEADROOM）；最后兜下限、封顶到节点堆外（子分配器不可能超过父）。
        long child = (long) (slotMemoryBytes() * SLOT_PIPELINE * spillRatio() * DIRECT_LIMIT_HEADROOM)
                + (16L * 1024 * 1024);
        return Math.min(Math.max(child, MIN_SLOT_MEMORY_BYTES), nodeDirect);
    }

    /** 溢写触发比例（{@code query.spill.ratio}，(0, 1]）。 */
    public static double spillRatio() {
        double d = doubleValue(QUERY_SPILL_RATIO, DEFAULT_SPILL_RATIO);
        return d > 0 && d <= 1 ? d : DEFAULT_SPILL_RATIO;
    }

    /**
     * 溢写盘使用率上限（{@code query.spill.disk.maxUsage}，(0, 1]）。
     *
     * <p>达到该使用率的盘不再承接新的溢写：{@code query.spill.dir} 配了多块盘时换下一块；
     * 所有盘都达到上限则 {@link com.dojao.core.operator.SpillDirs.SpillSpaceException} —— 查询
     * 失败并列出全部候选目录与使用率，而不是把节点磁盘写爆。</p>
     */
    public static double spillDiskMaxUsageRatio() {
        double d = doubleValue(QUERY_SPILL_DISK_MAX_USAGE, DEFAULT_SPILL_DISK_MAX_USAGE);
        return d > 0 && d <= 1 ? d : DEFAULT_SPILL_DISK_MAX_USAGE;
    }

    /**
     * 溢写目录列表（{@code query.spill.dir}，逗号分隔多目录；默认 {@code <DOJAO_HOME>/spill}）。
     *
     * <p><b>多目录的用法</b>：把溢写 I/O 摊到多块盘 —— {@code AbstractSpillableOperator} 每建一个
     * 算子实例就按 <b>round-robin</b> 取下一个目录建自己的子目录（子目录名唯一，各实例互不干扰，
     * 关闭时只删自己的）。</p>
     *
     * <p>解析规则：相对路径按 {@link #dojaoHome()} 解析；空项（如末尾逗号）忽略；未配置或全部非法时
     * 退回默认目录。保序去重。</p>
     */
    public static List<Path> spillDirs() {
        String raw = value(QUERY_SPILL_DIR, null);
        LinkedHashSet<Path> dirs = new LinkedHashSet<>();
        if (raw != null) {
            for (String part : raw.split(",")) {
                String s = part.trim();
                if (s.isEmpty()) {
                    continue;
                }
                Path p = Path.of(s);
                dirs.add((p.isAbsolute() ? p : dojaoHome().resolve(p)).normalize());
            }
        }
        if (dirs.isEmpty()) {
            dirs.add(dojaoHome().resolve(DEFAULT_SPILL_DIR_NAME).normalize());
        }
        return new ArrayList<>(dirs);
    }

    /**
     * 安装根目录（DOJAO_HOME）：{@code -Ddojao.home} 优先，其次由生效配置文件反推
     * （{@code <HOME>/conf/dojao.conf} ⇒ {@code <HOME>}），都没有时退回进程工作目录。
     *
     * <p>工作目录兜底是安全的：{@code bin/dojao} 启动前已 {@code cd $DOJAO_HOME}
     * （{@code conf/} 与 {@code data/} 本就按相对路径解析）。配置文件的父目录反推这条是给
     * <b>子 JVM</b> 用的 —— ClusterMain 只透传 {@code -Ddoja.conf}，子进程据此也能找到同一根目录。</p>
     */
    public static Path dojaoHome() {
        String home = System.getProperty(HOME_DIR_KEY);
        if (home != null && !home.isBlank()) {
            return Path.of(home).toAbsolutePath().normalize();
        }
        String conf = confFile();
        if (conf != null) {
            Path confDir = Path.of(conf).getParent();          // <HOME>/conf
            if (confDir != null && confDir.getParent() != null) {
                return confDir.getParent().toAbsolutePath().normalize();
            }
        }
        return Path.of(System.getProperty("user.dir", ".")).toAbsolutePath().normalize();
    }

    /** 无元数据可探查时的兜底并行度（{@code query.default.parallelism}，≥ 1）。 */
    public static int queryDefaultParallelism() {
        return Math.max(1, intValue(QUERY_DEFAULT_PARALLELISM, DEFAULT_QUERY_PARALLELISM));
    }

    /** 准入阈值（{@code query.admission.ratio}，(0, 1]）：可用 < 应需 × ratio 时阻塞等待。 */
    public static double queryAdmissionRatio() {
        double d = doubleValue(QUERY_ADMISSION_RATIO, DEFAULT_ADMISSION_RATIO);
        return d > 0 && d <= 1 ? d : DEFAULT_ADMISSION_RATIO;
    }

    /** 每 scan 分区承载字节数（{@code query.parallelism.bytesPerSlot}）。 */
    public static long parallelismBytesPerSlot() {
        return Math.max(1L, sizeValue(QUERY_PARALLELISM_BYTES_PER_SLOT, DEFAULT_PARALLELISM_BYTES_PER_SLOT));
    }

    /** 每 scan 分区行数（{@code query.parallelism.rowsPerSlot}）。 */
    public static long parallelismRowsPerSlot() {
        return Math.max(1L, sizeValue(QUERY_PARALLELISM_ROWS_PER_SLOT, DEFAULT_PARALLELISM_ROWS_PER_SLOT));
    }

    /** 聚合算子 Phase 2 每批聚合的 key 数（{@code query.agg.perBatchKeys}，≥ 1，默认 50000）。 */
    public static int aggPerBatchKeys() {
        return Math.max(1, intValue(QUERY_AGG_PER_BATCH_KEYS, DEFAULT_AGG_PER_BATCH_KEYS));
    }

    /** 聚合算子溢写归并每批 key 数（{@code query.agg.spill.perBatchKeys}，≥ 1，默认 10000）。 */
    public static int aggSpillPerBatchKeys() {
        return Math.max(1, intValue(QUERY_AGG_SPILL_PER_BATCH_KEYS, DEFAULT_AGG_SPILL_PER_BATCH_KEYS));
    }

    /** collect 聚合 Phase 2 单次交付值总数预算（{@code query.agg.collectDeliverValues}，≥ 1，默认 100 万）。 */
    public static int aggCollectDeliverValues() {
        return Math.max(1, intValue(QUERY_AGG_COLLECT_DELIVER_VALUES, DEFAULT_AGG_COLLECT_DELIVER_VALUES));
    }

    /** 窗口算子输出页行数（{@code query.window.perBatchKeys}，≥ 1，默认 50000）。 */
    public static int windowPerBatchKeys() {
        return Math.max(1, intValue(QUERY_WINDOW_PER_BATCH_KEYS, DEFAULT_WINDOW_PER_BATCH_KEYS));
    }

    /** 窗口算子<b>溢写</b>每批行数（{@code query.window.spill.perBatchRows}，≥ 1，默认 10000）。 */
    public static int windowSpillPerBatchRows() {
        return Math.max(1, intValue(QUERY_WINDOW_SPILL_PER_BATCH_ROWS, DEFAULT_WINDOW_SPILL_PER_BATCH_ROWS));
    }

    /** 窗口算子<b>拉取</b>每批 key 数（{@code query.window.spill.fetchPerKeys}，≥ 1，默认 10000）。 */
    public static int windowSpillFetchPerKeys() {
        return Math.max(1, intValue(QUERY_WINDOW_SPILL_FETCH_PER_KEYS, DEFAULT_WINDOW_SPILL_FETCH_PER_KEYS));
    }

    /**
     * 无 ORDER BY 聚合窗口的整分区内存缓冲上限（{@code query.window.partitionAggBufferBytes}，字节，默认 64MB）。
     *
     * <p>这类窗口（如 {@code count(*) OVER (PARTITION BY status)}）必须看到整个分区才能给出每一行的值，
     * 因此 `WindowFunctionOperator` 会按分区缓冲再回放；超过该上限即把缓冲页转写磁盘 Arrow 文件，
     * 避免大分区把 JVM 堆撑爆。</p>
     */
    public static long windowPartitionAggBufferBytes() {
        long v = longValue(QUERY_WINDOW_PARTITION_AGG_BUFFER_BYTES,
                DEFAULT_WINDOW_PARTITION_AGG_BUFFER_BYTES);
        return v > 0 ? v : DEFAULT_WINDOW_PARTITION_AGG_BUFFER_BYTES;
    }

    /**
     * 顶层<b>无 LIMIT</b> 的 ORDER BY 全局排序超内存预算策略（{@code query.sort.topSpillPolicy}）：
     * <ul>
     *   <li>{@code error}（默认）→ 累积超预算即抛错拒绝，避免把整表收拢 Master 排序 + 溢写拖垮节点；</li>
     *   <li>{@code warn} → 打印一次醒目警告后照常溢写；</li>
     *   <li>{@code spill} → 与其它算子一样静默溢写（旧行为）。</li>
     * </ul>
     * 只作用于 Planner 在顶层 ORDER BY 且无 LIMIT 时给 WindowSort 设置的策略；
     * 窗口分区排序 / 哈希聚合 / 带 LIMIT 的 ORDER BY 不受影响（恒 spill）。
     */
    public static String topOrderSortSpillPolicy() {
        String v = value(QUERY_SORT_TOP_SPILL_POLICY, DEFAULT_TOP_ORDER_SORT_SPILL_POLICY);
        String lower = v.trim().toLowerCase();
        return switch (lower) {
            case "warn", "spill" -> lower;
            default -> DEFAULT_TOP_ORDER_SORT_SPILL_POLICY;   // 非法值兜底 = 最安全的 error
        };
    }

    /**
     * join 算法（{@code query.join.algorithm}）：
     * <ul>
     *   <li>{@code auto}（默认）→ 由 {@code JoinDecider} 按两侧<b>估算体量</b>与<b>单台 worker 预算</b>
     *       自动选 hash / broadcast / sort_merge（见该类 javadoc 的判定表）；</li>
     *   <li>{@code hash} → 恒用分区哈希 join（旧行为；超预算即报错，便于对照验证）；</li>
     *   <li>{@code sort_merge} → 恒用排序归并 join（强制溢写路径）；</li>
     *   <li>{@code broadcast} → 恒用广播 join（小表整份复制到每台 worker；build 侧必须装得下才安全）。</li>
     * </ul>
     * 非法值兜底 {@code auto}。
     */
    /**
     * Master 结果回传看门狗超时（毫秒，{@code query.result.watchdogMs}，≥ 1000，默认 10 分钟）。
     *
     * <p>从最后一次「接触」——数据页 / 完成 / 存活心跳（{@code markAlive}）——起算，超过该值仍无回传
     * 才判回传链路故障 fail-fast。旧实现硬编码 {@code -DDojaoResultWatchdogMs} 默认 60s。</p>
     */
    public static long resultWatchdogMs() {
        return Math.max(1000L, longValue(QUERY_RESULT_WATCHDOG_MS, DEFAULT_RESULT_WATCHDOG_MS));
    }

    public static String joinAlgorithm() {
        String v = value(QUERY_JOIN_ALGORITHM, DEFAULT_JOIN_ALGORITHM);
        String lower = v.trim().toLowerCase();
        return switch (lower) {
            case "hash", "sort_merge", "broadcast" -> lower;
            default -> DEFAULT_JOIN_ALGORITHM;
        };
    }

    /**
     * 小表档阈值（{@code query.join.smallRatio}，默认 0.25）：单侧估算体量 ≤ {@code U × ratio}
     * 视为「小表」—— 够小到整份复制到每台 worker 还划算。
     */
    public static double joinSmallRatio() {
        double d = doubleValue(QUERY_JOIN_SMALL_RATIO, DEFAULT_JOIN_SMALL_RATIO);
        return d > 0 && d < 1 ? d : DEFAULT_JOIN_SMALL_RATIO;
    }

    /**
     * 大表档阈值（{@code query.join.largeRatio}，默认 0.8）：单侧估算体量 ≤ {@code U × ratio}
     * 视为「中表」—— 哈希 join 的 build 侧装得下；超出则只能走可溢写的排序归并。
     *
     * <p>刻意贴着 1.0 留 20% 余量：连接器统计是<b>估算值</b>（MySQL {@code TABLE_ROWS} 是采样值），
     * 真按 1.0 卡会把估算误差直接变成运行时失败。</p>
     */
    public static double joinLargeRatio() {
        double d = doubleValue(QUERY_JOIN_LARGE_RATIO, DEFAULT_JOIN_LARGE_RATIO);
        return d > 0 && d <= 1.0 ? d : DEFAULT_JOIN_LARGE_RATIO;
    }

    /**
     * 判定基准 {@code U} 里「最弱一台 worker 物理可用内存」的占比（{@code query.join.workerMemRatio}，默认 0.5）。
     *
     * <p>为什么不直接用配置的 {@code query.slot.memory}：配置值可能与现实脱节（机器只有 4G 内存却配了
     * 2G×8 槽位）。取两者较小值 → 配置值是主口径，物理现实是上限。而 {@code ResourceSummary} 报的是
     * <b>物理内存不是 JVM 堆</b>，所以只敢取一半，且只用来「封顶」不用来「放大」。</p>
     */
    public static double joinWorkerMemRatio() {
        double d = doubleValue(QUERY_JOIN_WORKER_MEM_RATIO, DEFAULT_JOIN_WORKER_MEM_RATIO);
        return d > 0 && d <= 1.0 ? d : DEFAULT_JOIN_WORKER_MEM_RATIO;
    }

    /** 广播 join 开关（{@code query.join.broadcast.enabled}，默认开）。关掉后 auto 只会选 hash / sort_merge。 */
    public static boolean joinBroadcastEnabled() {
        String v = value(QUERY_JOIN_BROADCAST_ENABLED, String.valueOf(DEFAULT_JOIN_BROADCAST_ENABLED));
        return !"false".equalsIgnoreCase(v.trim());
    }

    /**
     * 二级 COUNT(DISTINCT) 改写开关（{@code query.distinct.twoLevel}，默认开）。
     *
     * <p>开启后，满足严格判据的「有 GROUP BY + 恰好一个普通列 COUNT(DISTINCT) + 其余只有
     * count/sum/min/max + ≥2 台健康 Worker」查询会改写成内层 {@code GROUP BY k, hash(v)%K}
     * 二级聚合、外层按 {@code k} 合并子计数 —— 精确等价，把单热点分组摊到全部 Worker。</p>
     */
    public static boolean distinctTwoLevelEnabled() {
        String v = value(QUERY_DISTINCT_TWO_LEVEL, String.valueOf(DEFAULT_DISTINCT_TWO_LEVEL));
        return !"false".equalsIgnoreCase(v.trim());
    }

    /** 二级 COUNT(DISTINCT) 的子桶数（{@code query.distinct.twoLevel.buckets}）；0 = 自动（并行度）。 */
    public static int distinctTwoLevelBuckets() {
        return Math.max(0, intValue(QUERY_DISTINCT_TWO_LEVEL_BUCKETS, DEFAULT_DISTINCT_TWO_LEVEL_BUCKETS));
    }

    /**
     * 分区元数据扫描开关（{@code query.partition.metadataScan}，默认开）。
     *
     * <p>聚合输入全是分区列（{@code MIN/MAX/COUNT(DISTINCT) 分区列 + COUNT(*)}）时，
     * 不扫数据文件，只读 HMS 分区列举 + 文件 footer 精确行数。</p>
     */
    public static boolean partitionMetadataScanEnabled() {
        String v = value(QUERY_PARTITION_METADATA_SCAN, String.valueOf(DEFAULT_PARTITION_METADATA_SCAN));
        return !"false".equalsIgnoreCase(v.trim());
    }

    /**
     * 多次引用 CTE 物化开关（{@code query.cte.materialize}，默认开）。
     *
     * <p>同一个 CTE 查询体被 FROM 引用 ≥2 次（典型：漏斗 SQL 的 {@code UNION ALL} 各分支
     * {@code FROM f}）时，只执行一次查询体并落盘，各引用点重放文件 —— 否则每个引用都会
     * 独立规划/执行一遍源表扫描与聚合。关闭 = 逐字节回退旧的「每引用执行一次」行为。</p>
     */
    public static boolean cteMaterializeEnabled() {
        String v = value(QUERY_CTE_MATERIALIZE, String.valueOf(DEFAULT_CTE_MATERIALIZE));
        return !"false".equalsIgnoreCase(v.trim());
    }

    /**
     * 物化 CTE 随切片下发的 wire 载荷总量上限（{@code query.cte.wire.maxBytes}，默认 512MB）。
     *
     * <p>载荷是 Master 堆上的 {@code byte[]}，且要在全部引用点派发完成前常驻；没有上限时
     * 一条大 CTE 查询就可能吃穿 Master 堆。超限由 {@code MaterializedCteScanOperator.Store}
     * 响亮失败，并在消息里给出本配置键与当前值。</p>
     */
    public static long cteWireMaxBytes() {
        return longValue(QUERY_CTE_WIRE_MAX_BYTES, DEFAULT_CTE_WIRE_MAX_BYTES);
    }

    /**
     * 多维聚合展开开关（{@code query.groupingSets.expand}，默认开）。
     *
     * <p>{@code GROUP BY ROLLUP/CUBE/GROUPING SETS} 原先把每个维度组合当成一条独立 SELECT 腿，
     * N 个组合 = N 次扫表 + N 次 join。打开后改为<b>一趟扫描</b>：展开算子把每行复制 N 份
     * （未参与维度置 NULL，追加 {@code __grouping_id}），再按 {@code (维度…, gid)} 做一次聚合 ——
     * 结果与多腿 UNION ALL 逐行相同。关闭 = 逐字节回退旧多腿路径（排障 / 回归对照用）。</p>
     */
    public static boolean groupingSetsExpandEnabled() {
        String v = value(QUERY_GROUPING_SETS_EXPAND, String.valueOf(DEFAULT_GROUPING_SETS_EXPAND));
        return !"false".equalsIgnoreCase(v.trim());
    }

    /**
     * 文件型压缩数据源的「解压膨胀系数」（{@code query.join.compressedExpandRatio}，默认 1.0）。
     *
     * <p>ORC / Parquet 报出的体量是<b>压缩后</b>的文件长度，而 join 的 build 侧在内存里是<b>解压后</b>
     * 的 Arrow 列式向量 —— 直接按文件字节判会把本该走排序归并的表误判成「中表」。连接器可覆写
     * {@code DojaoConnector.joinSizeExpandRatio()} 声明自己的系数，此处是全局兜底默认值。</p>
     */
    public static double joinCompressedExpandRatio() {
        double d = doubleValue(QUERY_JOIN_COMPRESSED_EXPAND_RATIO, DEFAULT_JOIN_COMPRESSED_EXPAND_RATIO);
        return d >= 1.0 ? d : DEFAULT_JOIN_COMPRESSED_EXPAND_RATIO;
    }

    /**
     * 每 query 并行度硬上限（{@code query.max.parallelism}）；≤ 0（含未设置）返回 {@code -1} = auto，
     * 由 {@code ParallelismBudget.perQueryMax()} 按默认公式（集群容量 / worker 数）计算。
     */
    public static int systemQueryMaxParallelism() {
        int v = intValue(QUERY_MAX_PARALLELISM, DEFAULT_SYSTEM_QUERY_MAX_PARALLELISM);
        return v > 0 ? v : DEFAULT_SYSTEM_QUERY_MAX_PARALLELISM;
    }

    /**
     * Master 侧<b>回退单机聚合</b>的显式内存预算（{@code system.master.agg.memory}，字节，默认 256M）。
     *
     * <p>分布式多 join / 无法下推的聚合会回退到 Master 进程内跑 {@code HashAggregationOperator}。
     * Master 堆很小（如 1G），JVM {@code MaxDirectMemorySize} 默认 = {@code -Xmx}，聚合哈希表 +
     * 流式回传页 + gRPC 帧叠加会直接撞直接内存限额 OOM。此处用远小于 slot 的显式预算，让聚合
     * 提前溢写（磁盘）替代内存爆掉；Worker 侧算子内存不受影响（仍按 {@code query.slot.memory} 传）。</p>
     */
    public static long masterAggregationMemoryBytes() {
        return Math.max(1L, sizeValue(SYSTEM_MASTER_AGG_MEMORY, DEFAULT_MASTER_AGG_MEMORY_BYTES));
    }

    // ==================== 部署拓扑访问器 ====================

    /** 本节点角色（{@code system.role}）。 */
    public static Role role() {
        return ROLE;
    }

    /** Master Flight 绑定 / 连接地址（{@code system.master.host}）。 */
    public static String masterHost() {
        return value(SYSTEM_MASTER_HOST, DEFAULT_MASTER_HOST);
    }

    /** Master Flight 端口（{@code system.master.port}，默认 6530）。 */
    public static int masterPort() {
        return Math.max(1, intValue(SYSTEM_MASTER_PORT, DEFAULT_MASTER_PORT));
    }

    /** Worker Flight 绑定地址（{@code system.worker.host}）。 */
    public static String workerHost() {
        return value(SYSTEM_WORKER_HOST, DEFAULT_WORKER_HOST);
    }

    /** Worker Flight 端口（{@code system.worker.port}，默认 4009）。 */
    public static int workerPort() {
        return Math.max(1, intValue(SYSTEM_WORKER_PORT, DEFAULT_WORKER_PORT));
    }

    /** Web 控制台端口（{@code system.ui.port}，默认 2380）。 */
    public static int uiPort() {
        return Math.max(1, intValue(SYSTEM_UI_PORT, DEFAULT_UI_PORT));
    }

    /** 元数据 SQLite 文件路径（{@code system.metastore.sqlite.path}）。 */
    public static String metastoreSqlitePath() {
        return value(SYSTEM_METASTORE_SQLITE_PATH, DEFAULT_METASTORE_SQLITE_PATH);
    }

    public static String connectorCatalog() {
        return value(SYSTEM_CONNECTOR_CATALOG, DEFAULT_CONNECTOR_CATALOG);
    }

    /**
     * dev 兜底 catalog 的连接器<b>类型</b>（{@code system.connector.type}）。
     * 缺省回落到 {@link #connectorCatalog()} —— 历史配置里 catalog 名即类型名（{@code mysql}），
     * 于是「连接器已插件化、类型由工厂解析」之后，旧 {@code -Dsystem.connector.catalog=mysql} 写法依旧可用。
     */
    public static String connectorType() {
        return value(SYSTEM_CONNECTOR_TYPE, connectorCatalog());
    }

    public static String connectorUrl() {
        return value(SYSTEM_CONNECTOR_URL, DEFAULT_CONNECTOR_URL);
    }

    public static String connectorUser() {
        return value(SYSTEM_CONNECTOR_USER, DEFAULT_CONNECTOR_USER);
    }

    public static String connectorPassword() {
        return value(SYSTEM_CONNECTOR_PASSWORD, DEFAULT_CONNECTOR_PASSWORD);
    }

    // ==================== 读取 ====================

    /** 取值：系统属性 &gt; 配置文件 &gt; 默认。 */
    private static String value(String key, String def) {
        String sys = System.getProperty(key);
        if (sys != null && !sys.isBlank()) {
            return sys;
        }
        String file = FILE_PROPS.getProperty(key);
        return file != null && !file.isBlank() ? file : def;
    }

    private static int intValue(String key, int def) {
        String v = value(key, null);
        if (v == null || v.isBlank()) {
            return def;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static long longValue(String key, long def) {
        String v = value(key, null);
        if (v == null || v.isBlank()) {
            return def;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static double doubleValue(String key, double def) {
        String v = value(key, null);
        if (v == null || v.isBlank()) {
            return def;
        }
        try {
            return Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    /** 大小解析：支持 {@code 512}（字节）、{@code 512M / 512MB}、{@code 3G / 3GB}、{@code 64K / 64KB}。 */
    private static long sizeValue(String key, long def) {
        String raw = value(key, null);
        if (raw == null || raw.isBlank()) {
            return def;
        }
        String v = raw.trim();
        long mult = 1;
        if (v.endsWith("B") || v.endsWith("b")) {
            v = v.substring(0, v.length() - 1).trim();
        }
        if (!v.isEmpty()) {
            char unit = Character.toUpperCase(v.charAt(v.length() - 1));
            if (unit == 'K') {
                mult = 1024L;
                v = v.substring(0, v.length() - 1);
            } else if (unit == 'M') {
                mult = 1024L * 1024;
                v = v.substring(0, v.length() - 1);
            } else if (unit == 'G') {
                mult = 1024L * 1024 * 1024;
                v = v.substring(0, v.length() - 1);
            }
        }
        try {
            return (long) (Double.parseDouble(v.trim()) * mult);
        } catch (NumberFormatException e) {
            return def;
        }
    }

    /** 依次加载 -D 指定路径 / 当前目录 conf / classpath 根，先到先用（同一文件内前值优先）。 */
    private static Properties loadFileProps() {
        Properties p = new Properties();
        String explicit = System.getProperty(CONF_FILE_KEY);
        if (explicit != null && !explicit.isBlank()) {
            loadFile(p, Path.of(explicit));
        }
        loadFile(p, Path.of("conf", "dojao.conf"));
        loadFileFromClasspath(p, "/dojao.conf");
        return p;
    }

    private static void loadFile(Properties p, Path path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (InputStream in = Files.newInputStream(path)) {
            p.load(in);
        } catch (IOException ignored) {
            // 配置损坏不致命：其余层仍生效
        }
    }

    private static void loadFileFromClasspath(Properties p, String resource) {
        try (InputStream in = ResourceConfig.class.getResourceAsStream(resource)) {
            if (in != null) {
                p.load(in);
            }
        } catch (IOException ignored) {
        }
    }
}
