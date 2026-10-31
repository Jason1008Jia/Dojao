package com.dojao.server.function;

import com.dojao.core.rpc.udf.FunctionSignature;
import com.dojao.core.rpc.udf.UdfControl;
import com.dojao.core.rpc.udf.UdfMeta;
import com.dojao.core.rpc.udf.UdfPing;
import com.dojao.server.external.ExternalServiceManager;
import com.dojao.server.util.ExternalProcessRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import com.dojao.common.memory.NodeMemory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sidecar 进程池 —— <b>语言无关</b>的外挂函数运行时管理器（对应设计文档 §7.7 + DFP v2）。
 *
 * <p>它不认识任何具体语言：启动什么命令、带什么环境，全由 {@link Runtime} 描述（由各语言的
 * {@code FunctionBackend} 提供，如 Python 后端给 {@code ["python","-m","dojao_udf.sidecar"]}
 * + {@code PYTHONPATH}）。进程编排（spawn / 端口分配 / 探活 / 进程池 / 看门狗 / 登记表回收）
 * 与协议（Arrow Flight + {@code UdfControl}/{@code UdfMeta}）才是本类的职责。</p>
 *
 * <p><b>传输：Arrow Flight（DFP v2 定稿）</b>。每个 sidecar 一个 {@link FlightClient}：
 * <ul>
 *   <li>探活 + 版本协商 → {@code doAction("ping")}（载荷 = {@link UdfPing}，含 runtime/protocol）；</li>
 *   <li>冒烟校验 → {@code doAction("check_function")}（body = function_ref，载荷 = {@link UdfMeta}）；</li>
 *   <li>单批调用 → {@code doExchange(path="invoke")}：请求流 1 条（原生 Arrow batch +
 *       metadata={@link UdfControl}），响应流 1 条（结果 batch + metadata={@link UdfMeta}），
 *       <b>响应到达 = 该批完成</b>（§7.7 批次完成语义）。</li>
 * </ul></p>
 *
 * <p><b>新增一种语言</b>：写一个 {@code FunctionBackend}，构造 {@code SidecarPool} 时传入该语言的
 * {@link Runtime}（启动命令 + 环境）即可 —— 本类零改动。语言特有的函数引用解析 / 契约声明由
 * 该语言的 sidecar 实现（见 PROTOCOL.md）。</p>
 */
public final class SidecarPool implements ExternalServiceManager {

    /** 引擎实现的协议版本（与 udf.proto 头注释一致）；spawn 握手时校验 sidecar 版本兼容。 */
    public static final int PROTOCOL_VERSION = 2;

    /** 运行时描述 —— 语言无关：引擎只按 {@code command} 拉起进程、注入 {@code env}，不解释其内容。 */
    public record Runtime(String name, List<String> command, Map<String, String> env) {
    }

    /** 进程池配置。 */
    public record Config(int poolSize, Duration pingTimeout, Duration rpcTimeout) {
        public static Config defaults() {
            return of(1, Duration.ofSeconds(3), Duration.ofSeconds(30));
        }

        public static Config of(int poolSize, Duration pingTimeout, Duration rpcTimeout) {
            return new Config(poolSize, pingTimeout, rpcTimeout);
        }
    }

    /** 单批调用结果：响应 metadata（status/新状态）+ 解码后的首值（标量结果；聚合忽略）。 */
    public record InvokeResult(UdfMeta meta, Object value) {
    }

    /** 整批调用结果：响应 metadata + 结果 batch（已复制到独立 allocator root，调用方负责 close）。 */
    public record InvokeBatch(UdfMeta meta, VectorSchemaRoot resultRoot) {
    }

    /** 一个 sidecar 子进程 + 对应 Flight 客户端。 */
    public static final class SidecarProcess implements AutoCloseable {
        private final Process process;
        private final FlightClient client;
        private final BufferAllocator allocator;
        private final StringBuilder stderrBuf;
        private final Duration rpcTimeout;
        private final int id;
        private final ExternalProcessRegistry registry;   // 注册表（可 null）：close 时解除登记
        private final String registryUuid;                // 本进程在注册表中的 uuid

        private SidecarProcess(int id, Process process, FlightClient client,
                               BufferAllocator allocator, StringBuilder stderrBuf, Duration rpcTimeout,
                               ExternalProcessRegistry registry, String registryUuid) {
            this.id = id;
            this.process = process;
            this.client = client;
            this.allocator = allocator;
            this.stderrBuf = stderrBuf;
            this.rpcTimeout = rpcTimeout;
            this.registry = registry;
            this.registryUuid = registryUuid;
        }

        /** 探活 + 能力/版本上报：doAction("ping") 成功且进程存活 → 返回 {@link UdfPing}，否则 null。 */
        public UdfPing ping(Duration timeout) {
            try {
                Iterator<Result> results = client.doAction(
                        new Action("ping", new byte[0]),
                        CallOptions.timeout(timeout.toMillis(), TimeUnit.MILLISECONDS));
                if (!results.hasNext()) {
                    return null;
                }
                UdfPing p = UdfPing.parseFrom(results.next().getBody());
                return process.isAlive() ? p : null;
            } catch (Exception e) {
                return null;
            }
        }

        /**
         * CREATE 冒烟校验：doAction("check_function")（body = function_ref），通知 sidecar 导入
         * 函数 + 校验契约。UdfMeta.status=ERROR → 用户代码错误（装饰器缺阶段注解等），抛 USER_CODE_ERROR。
         *
         * @return UdfMeta —— 成功时 {@link UdfMeta#getSignature()} 携带结构化函数签名
         *         （kind + 输入列 + 输出 Arrow schema），供 {@code FunctionBackend} 判定契约并分派注册表。
         */
        public UdfMeta checkFunction(String functionRef) {
            byte[] body = functionRef.getBytes(StandardCharsets.UTF_8);
            UdfMeta meta;
            try {
                Iterator<Result> results = client.doAction(
                        new Action("check_function", body),
                        CallOptions.timeout(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS));
                if (!results.hasNext()) {
                    throw new SidecarException(SidecarException.Kind.CRASHED,
                            "sidecar check_function 未返回结果");
                }
                meta = UdfMeta.parseFrom(results.next().getBody());
            } catch (InvalidProtocolBufferException e) {
                throw new SidecarException(SidecarException.Kind.CRASHED,
                        "sidecar check_function 响应不是合法 UdfMeta（协议违例）", e);
            }
            if (meta.getStatus() == UdfMeta.Status.ERROR) {
                throw new SidecarException(SidecarException.Kind.USER_CODE_ERROR,
                        "CREATE 冒烟校验失败（函数代码有误）:\n" + meta.getMessage());
            }
            return meta;
        }

        /**
         * 单批调用（§7.7 批次完成语义：响应返回 = 该批完成）。
         *
         * <p>doExchange：请求流写 1 条（inputRoot 为 null → 空 root，无数据 op 用），
         * 响应流读 1 条；解码首值前调用，结果复制到调用方。</p>
         */
        public InvokeResult invoke(UdfControl control, VectorSchemaRoot inputRoot) {
            // Arrow 15：doExchange 返回 ExchangeReaderWriter（读写双子），writer 写请求流、reader 读响应流
            FlightClient.ExchangeReaderWriter rw = client.doExchange(
                    FlightDescriptor.path("invoke"),
                    CallOptions.timeout(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS));
            FlightClient.ClientStreamListener writer = rw.getWriter();
            VectorSchemaRoot reqRoot = inputRoot != null ? inputRoot : ArrowRowCodec.emptyRoot(allocator);
            writer.start(reqRoot);
            byte[] ctrl = control.toByteArray();
            // putNext 会接管 metaBuf 所有权（写入后由 Flight 负责释放），勿再 close —— 否则双释放 RefCnt 为负
            org.apache.arrow.memory.ArrowBuf metaBuf = allocator.buffer(ctrl.length);
            metaBuf.setBytes(0, ctrl);
            metaBuf.writerIndex(ctrl.length);
            writer.putNext(metaBuf);
            writer.completed();     // 请求方向结束 → sidecar 处理完即关响应流

            FlightStream resp = rw.getReader();
            try {
                if (!resp.next()) {
                    throw new SidecarException(SidecarException.Kind.CRASHED,
                            "sidecar 未返回响应");
                }
                UdfMeta meta;
                org.apache.arrow.memory.ArrowBuf respMetaBuf = resp.getLatestMetadata();
                if (respMetaBuf == null || respMetaBuf.readableBytes() == 0) {
                    // 无 metadata = 成功：UdfMeta(status=OK) 序列化为空字节（OK=0 是 protobuf 默认值，被省略）
                    meta = UdfMeta.getDefaultInstance();
                } else {
                    try {
                        meta = UdfMeta.parseFrom(metadataBytes(respMetaBuf));
                    } catch (InvalidProtocolBufferException e) {
                        // 响应 metadata 不是合法 UdfMeta → 协议违例，sidecar 视为已损坏
                        throw new SidecarException(SidecarException.Kind.CRASHED,
                                "sidecar 响应 metadata 不是合法 UdfMeta（协议违例）", e);
                    }
                }
                Object value = ArrowRowCodec.decodeResult(resp.getRoot());
                return new InvokeResult(meta, value);
            } finally {
                try {
                    rw.close();
                } catch (Exception ignored) {
                }
            }
        }

        /**
         * 整批调用（批量标量/聚合路径）—— doExchange 发整批输入，读整批结果。
         *
         * <p>与 {@link #invoke} 的差异：输入 root 由调用方持有（可能引用上游页向量），
         * 发送前深拷贝到独立 root（异步传输安全，避免页被调用方 close 后 use-after-free）；
         * 结果整批复制到独立 root 返回（响应流 close 前转移所有权），调用方负责 close。</p>
         *
         * <p>UdfMeta.status=ERROR 在此 fail-fast（抛 USER_CODE_ERROR），调用方无需再检查。</p>
         */
        public InvokeBatch invokeBatch(UdfControl control, VectorSchemaRoot inputRoot) {
            VectorSchemaRoot reqRoot = inputRoot != null ? copyRoot(inputRoot) : ArrowRowCodec.emptyRoot(allocator);
            FlightClient.ExchangeReaderWriter rw = client.doExchange(
                    FlightDescriptor.path("invoke"),
                    CallOptions.timeout(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS));
            try {
                FlightClient.ClientStreamListener writer = rw.getWriter();
                writer.start(reqRoot);
                byte[] ctrl = control.toByteArray();
                org.apache.arrow.memory.ArrowBuf metaBuf = allocator.buffer(ctrl.length);
                metaBuf.setBytes(0, ctrl);
                metaBuf.writerIndex(ctrl.length);
                writer.putNext(metaBuf);
                writer.completed();

                FlightStream resp = rw.getReader();
                if (!resp.next()) {
                    throw new SidecarException(SidecarException.Kind.CRASHED,
                            "sidecar 未返回响应");
                }
                UdfMeta meta = readMeta(resp);
                if (meta.getStatus() == UdfMeta.Status.ERROR) {
                    throw new SidecarException(SidecarException.Kind.USER_CODE_ERROR,
                            meta.getMessage());
                }
                // rw.close() 会释放响应流 root → 必须在此之前把结果复制到独立 root
                VectorSchemaRoot result = copyRoot(resp.getRoot());
                return new InvokeBatch(meta, result);
            } finally {
                try { reqRoot.close(); } catch (Exception ignored) { }
                try { rw.close(); } catch (Exception ignored) { }
            }
        }

        /** 深拷贝一个 root 到独立 allocator root（跨 root 用 copyValueSafe，规避 transfer 共享约束）。 */
        private VectorSchemaRoot copyRoot(VectorSchemaRoot src) {
            List<Field> fields = src.getSchema().getFields();
            List<FieldVector> vectors = new ArrayList<>();
            int n = src.getRowCount();
            for (int c = 0; c < src.getFieldVectors().size(); c++) {
                FieldVector sv = src.getVector(c);
                FieldVector dv = fields.get(c).createVector(allocator);
                dv.setInitialCapacity(n);
                dv.allocateNew();
                TransferPair tp = sv.makeTransferPair(dv);
                for (int r = 0; r < n; r++) tp.copyValueSafe(r, r);
                dv.setValueCount(n);
                vectors.add(dv);
            }
            return new VectorSchemaRoot(fields, vectors, n);
        }

        /** 读响应 metadata：空 = 默认 OK（OK=0 是 protobuf 默认值被省略）。 */
        private UdfMeta readMeta(FlightStream resp) {
            org.apache.arrow.memory.ArrowBuf respMetaBuf = resp.getLatestMetadata();
            if (respMetaBuf == null || respMetaBuf.readableBytes() == 0) {
                return UdfMeta.getDefaultInstance();
            }
            try {
                return UdfMeta.parseFrom(metadataBytes(respMetaBuf));
            } catch (InvalidProtocolBufferException e) {
                throw new SidecarException(SidecarException.Kind.CRASHED,
                        "sidecar 响应 metadata 不是合法 UdfMeta（协议违例）", e);
            }
        }

        private static byte[] metadataBytes(org.apache.arrow.memory.ArrowBuf buf) {
            // readableBytes() 返回 long，数组长度需 int（metadata 体积极小，直接截断）
            byte[] out = new byte[Math.toIntExact(buf.readableBytes())];
            buf.readBytes(out);
            return out;
        }

        public boolean isAlive() {
            return process.isAlive();
        }

        public int id() {
            return id;
        }

        public String stderrTail() {
            synchronized (stderrBuf) {
                return stderrBuf.toString();
            }
        }

        @Override
        public void close() {
            // 先解除登记再杀（kill 由注册表统一接管时避免重复；未接注册表则 registry==null no-op）
            if (registry != null && registryUuid != null) {
                registry.unregister(registryUuid);
            }
            try {
                process.destroyForcibly();
            } catch (Exception ignored) {
            }
            try {
                client.close();
            } catch (Exception ignored) {
            }
        }
    }

    private volatile Runtime runtime;
    private final Config config;
    private final ExternalProcessRegistry registry;   // 外部进程注册表（可 null：未接注册表时仅自清理）
    private final List<SidecarProcess> pool = new ArrayList<>();
    private final AtomicInteger roundRobin = new AtomicInteger();
    private final BufferAllocator allocator = NodeMemory.shared().newChildAllocator("sidecar-udf", 0);
    private volatile boolean ready;
    private volatile boolean closed;

    public SidecarPool(Runtime runtime, Config config) {
        this(runtime, config, null);
    }

    /**
     * @param registry 外部进程注册表：每个 sidecar spawn 成功后登记（uuid / port / queryId=null），
     *                 进程关闭时解除登记。可传 null（不接注册表，仅进程池自清理）。
     */
    public SidecarPool(Runtime runtime, Config config, ExternalProcessRegistry registry) {
        this.runtime = runtime;
        this.config = config;
        this.registry = registry;
    }

    /**
     * 更新运行时描述（如后端新增代码目录 → 环境变化）。已就绪则<b>重建进程池</b> —— 已起 sidecar
     * 的环境不含新变化，不重建则后续函数加载必失败。未就绪仅更新，下次 {@link #ensureReady()} 生效。
     */
    public synchronized void setRuntime(Runtime runtime) {
        this.runtime = runtime;
        if (ready) {
            respawn();
        }
    }

    public Runtime runtime() {
        return runtime;
    }

    public String runtimeName() {
        return runtime.name();
    }

    /** 关闭并重建进程池（用当前 runtime 重新 spawn）。 */
    private synchronized void respawn() {
        pool.forEach(SidecarProcess::close);
        pool.clear();
        ready = false;
        ensureReady();
    }

    /**
     * 启动进程池并逐个 PING 探活 + 协议版本协商。
     *
     * @throws SidecarException NO_RUNTIME_ENV：spawn 失败；STARTUP_FAILED：启动后无响应；
     *                          PROTOCOL_MISMATCH：sidecar 协议版本不兼容
     */
    public synchronized void ensureReady() {
        if (ready) {
            return;
        }
        List<String> failures = new ArrayList<>();
        boolean noRuntimeEnv = false;
        int n = config.poolSize();
        // 并发 spawn：每个 sidecar 相互独立（端口 / 进程 / FlightClient），运行时冷启动 ~1s，
        // 串行建池 poolSize=4 时 ~4s。并行拉起把整池启动压缩到 ~1s（每次集群启动 / 重建都受益）。
        ExecutorService spawner = Executors.newFixedThreadPool(n, r -> {
            Thread t = new Thread(r, "sidecar-spawn");
            t.setDaemon(true);
            return t;
        });
        List<Future<SidecarProcess>> futures = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final int id = i;
            futures.add(spawner.submit(() -> spawn(id)));
        }
        spawner.shutdown();
        for (Future<SidecarProcess> f : futures) {
            try {
                pool.add(f.get());   // spawn 异常被 Future 包裹为 ExecutionException，解包后语义与原串行版一致
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof SidecarException pse) {
                    failures.add(pse.getMessage());
                    if (pse.kind() == SidecarException.Kind.NO_RUNTIME_ENV) {
                        noRuntimeEnv = true;
                    }
                } else {
                    failures.add(String.valueOf(cause));
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                failures.add("spawn 被中断: " + ie);
            }
        }
        if (pool.isEmpty()) {
            // 只有运行时二进制真的找不到才报 NO_RUNTIME_ENV（否则会被误读成"本机无该运行时"）；
            // 进程起了但没握上手（ping 超时/即退）属 STARTUP_FAILED。
            throw new SidecarException(
                    noRuntimeEnv ? SidecarException.Kind.NO_RUNTIME_ENV
                                 : SidecarException.Kind.STARTUP_FAILED,
                    summarizeFailures(failures));
        }
        ready = true;
    }

    /**
     * 汇总 spawn 失败：整池因<同一原因>挂掉（如都缺同一个包）时只报一次 + 计数，
     * 避免 poolSize 份逐字重复的长堆栈把用户淹掉。
     */
    private static String summarizeFailures(List<String> failures) {
        if (failures.isEmpty()) {
            return "全部 sidecar 启动失败（无详情）";
        }
        List<String> distinct = new ArrayList<>(new LinkedHashSet<>(failures));
        if (distinct.size() == 1) {
            return "全部 " + failures.size() + " 个 sidecar 启动失败（同一原因）: " + distinct.get(0);
        }
        return "全部 " + failures.size() + " 个 sidecar 启动失败（去重后 " + distinct.size()
                + " 种原因）: " + String.join(" | ", distinct);
    }

    /** 轮询取一个存活 sidecar：从旋转起点向后扫第一个存活的（单进程崩溃自动跳过）。 */
    public SidecarProcess acquire() {
        ensureReady();
        int start = Math.floorMod(roundRobin.getAndIncrement(), pool.size());
        for (int step = 0; step < pool.size(); step++) {
            SidecarProcess p = pool.get((start + step) % pool.size());
            if (p.isAlive()) {
                return p;
            }
        }
        throw new SidecarException(
                SidecarException.Kind.CRASHED, "进程池全部不可用");
    }

    /**
     * CREATE 冒烟校验：分配一个存活 sidecar 通知其导入函数 + 校验契约完整性。
     * 校验失败（如 Python 装饰器缺阶段注解）→ {@link SidecarException.Kind#USER_CODE_ERROR}。
     */
    public void checkFunction(String functionRef) {
        ensureReady();
        acquire().checkFunction(functionRef);
    }

    /**
     * CREATE 冒烟校验 + 结构化函数签名（供 FunctionBackend 判定契约分派注册表）：
     * 成功返回 {@link FunctionSignature}，校验失败抛 USER_CODE_ERROR。
     */
    public FunctionSignature checkFunctionSignature(String functionRef) {
        ensureReady();
        return acquire().checkFunction(functionRef).getSignature();
    }

    /** 能力标记：至少一个 sidecar 存活且 PING 上报运行时就绪（供心跳上报）。 */
    public boolean runtimeAvailable() {
        if (pool.isEmpty()) {
            return false;
        }
        return pool.stream().anyMatch(p -> {
            UdfPing ping = p.ping(config.pingTimeout());
            return ping != null && ping.getRuntimeReady();
        });
    }

    /** 共享分配器（桥编解码 Arrow 用）。 */
    public BufferAllocator allocator() {
        return allocator;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;   // 幂等：BackendRegistry.closeAll() 与显式生命周期钩子可能都调
        }
        closed = true;
        ready = false;
        pool.forEach(SidecarProcess::close);
        pool.clear();
        allocator.close();
    }

    // ==================== ExternalServiceManager 生命周期契约 ====================

    @Override
    public String serviceType() {
        return runtime.name();
    }

    /** 启动进程池（幂等：已 ready 时 no-op）。 */
    @Override
    public void start() {
        ensureReady();
    }

    /** 能力探活：至少一个 sidecar 存活且运行时就绪（供心跳上报 / 规划前置规避）。 */
    @Override
    public boolean isAvailable() {
        return runtimeAvailable();
    }

    // kill() 继承接口默认实现 = close()：sidecar 无优雅停机概念（纯计算服务），
    // close 即强制杀（destroyForcibly），与 kill 语义一致。

    // ==================== 内部 ====================

    /** spawn 一个 sidecar：占端口 → 起进程 → PING 探活 + 版本协商。 */
    private SidecarProcess spawn(int id) {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        } catch (IOException e) {
            throw new SidecarException(SidecarException.Kind.STARTUP_FAILED,
                    "无法分配 sidecar 端口", e);
        }

        Runtime rt = runtime;
        List<String> cmd = new ArrayList<>(rt.command());
        cmd.add("--port");
        cmd.add(String.valueOf(port));
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        if (rt.env() != null && !rt.env().isEmpty()) {
            pb.environment().putAll(rt.env());
        }

        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            // 进程层感知：运行时二进制找不到 → NO_RUNTIME_ENV
            throw new SidecarException(SidecarException.Kind.NO_RUNTIME_ENV,
                    "spawn 失败（" + rt.name() + " 环境缺失?）: " + e.getMessage(), e);
        }

        // stderr 异步捕获（stderrTail）：崩溃诊断的关键。
        // 注意不可同步阻塞读 —— sidecar 存活期间 stdout 不会关闭，会死锁。
        StringBuilder stderrBuf = new StringBuilder();
        Thread drainer = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    synchronized (stderrBuf) {
                        if (stderrBuf.length() > 8000) {
                            stderrBuf.setLength(0); // 只保留尾部
                        }
                        stderrBuf.append(line).append('\n');
                    }
                }
            } catch (IOException ignored) {
            }
        }, "sidecar-stderr-" + id);
        drainer.setDaemon(true);
        drainer.start();

        // Arrow 15：无 builder(Location)，用 builder().location().allocator().build()
        FlightClient client = FlightClient.builder()
                .location(Location.forGrpcInsecure("127.0.0.1", port))
                .allocator(allocator)
                .build();
        // 进程登记进注册表（uuid / port / queryId=null=worker 级共享）：JVM 管不到的外部进程由注册表统一清理
        String registryUuid = registry == null ? null
                : registry.register(rt.name() + "-sidecar", process, "127.0.0.1", port, null);
        SidecarProcess sidecar = new SidecarProcess(id, process, client, allocator, stderrBuf,
                config.rpcTimeout(), registry, registryUuid);

        // 启动竞态兜底：运行时冷启动（import 依赖库等）需要 1~2s 才监听端口，此刻连接被拒 →
        // gRPC connection-refused 立即返回（不等 timeout）。在 pingTimeout 窗口内重试，
        // 避免把"仍在启动"的健康进程误判为"启动失败"（否则 close() 杀掉进程，exitValue 恒为 1）。
        long deadline = System.nanoTime() + config.pingTimeout().toNanos();
        UdfPing readyPing = null;
        while (System.nanoTime() < deadline) {
            long remainMs = Math.max(0, (deadline - System.nanoTime()) / 1_000_000);
            UdfPing p = sidecar.ping(Duration.ofMillis(Math.min(500, remainMs)));
            if (p != null && p.getRuntimeReady()) {
                readyPing = p;
                break;
            }
            if (!process.isAlive()) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (readyPing == null) {
            // 先读真实退出码再 close()（close 会 destroyForcibly，Windows 上强制杀进程退出码恒为 1，
            // 后读会把"ping 超时"误报成"进程崩溃 exit=1"）。
            int exit = process.isAlive() ? -1 : process.exitValue();
            String reason = process.isAlive()
                    ? "ping 超时 " + config.pingTimeout().toMillis() + "ms"
                    : "进程退出 exit=" + exit;
            sidecar.close();
            throw new SidecarException(SidecarException.Kind.STARTUP_FAILED,
                    "sidecar#" + id + " 启动后无响应（" + reason + "）, stderr=" + trim(sidecar.stderrTail()));
        }
        // 协议版本协商：sidecar 实现的版本必须与引擎一致（v2 起为强校验；未来可放宽为区间）。
        if (readyPing.getProtocolVersion() != PROTOCOL_VERSION) {
            sidecar.close();
            throw new SidecarException(SidecarException.Kind.PROTOCOL_MISMATCH,
                    "sidecar#" + id + " 协议版本不兼容：sidecar=" + readyPing.getProtocolVersion()
                            + "（runtime=" + readyPing.getRuntimeName() + "）引擎=" + PROTOCOL_VERSION
                            + "；请更新 sidecar SDK / 引擎");
        }
        return sidecar;
    }

    private static String trim(String s) {
        if (s == null || s.isBlank()) {
            return "(空)";
        }
        // ★保留尾部：Python 等运行时的 traceback 把「真正的错误」放在最后一行
        // （如 ModuleNotFoundError: No module named 'google.protobuf'），截头部会把关键信息切掉。
        return s.length() > 800 ? "…" + s.substring(s.length() - 800) : s;
    }
}
