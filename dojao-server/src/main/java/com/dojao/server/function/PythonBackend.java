package com.dojao.server.function;

import com.dojao.core.function.BackendRegistry;
import com.dojao.core.function.PathReaders;
import com.dojao.core.function.ResolvedCode;
import com.dojao.core.rpc.udf.FunctionSignature;
import com.dojao.spi.bean.FunctionDefinition;
import com.dojao.spi.function.AggregateFunction;
import com.dojao.spi.function.AggregateFunctionRegistry;
import com.dojao.spi.function.CollectAggregateFunction;
import com.dojao.spi.function.CollectAggregateRegistry;
import com.dojao.spi.function.FunctionBackend;
import com.dojao.spi.function.ScalarFunction;
import com.dojao.spi.function.TableFunction;
import com.dojao.spi.function.WindowFunction;
import com.dojao.server.util.ExternalProcessRegistry;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Python 函数后端 —— type = {@code "python"}，官方参考实现（DFP v2）。
 *
 * <p>通过 {@link SidecarPool}（语言无关的进程池）+ {@link PythonFunctionBridge}（SPI 代理）执行，
 * 并由本类提供 Python 特有的两件事：<b>启动命令 / 环境</b>（{@code python -m dojao_udf.sidecar}
 * + {@code PYTHONPATH}）与 <b>function_ref 语义</b>（{@code module.ClassName}）。其余语言的后端
 * 照此写自己的 {@code FunctionBackend} 即可 —— 引擎零改动。</p>
 *
 * <h3>注册时机</h3>
 * <p>server 启动时（每 worker 节点）构造 + {@link #install()}。</p>
 *
 * <h3>能力上报</h3>
 * <p>{@link #available()} 供心跳标记该节点 python 可用性，规划器据此前置规避无 python 环境的节点。</p>
 */
public final class PythonBackend implements FunctionBackend {

    private final SidecarPool pool;
    private final String pythonBin;
    private final String udfRuntime;
    private final List<String> codePaths = new ArrayList<>();   // 用户函数代码目录（spawn 前注入 PYTHONPATH）

    public PythonBackend(String pythonBin, String udfRuntime, SidecarPool.Config config) {
        this(pythonBin, udfRuntime, config, null);
    }

    /**
     * @param pythonBin  python 解释器（如 {@code python3}，或 {@code <env>/bin/python} 绝对路径）
     * @param udfRuntime dojao_udf SDK（sidecar 运行时）目录（含 {@code dojao_udf} 包，加入 PYTHONPATH）
     * @param registry   外部进程注册表（可 null）
     */
    public PythonBackend(String pythonBin, String udfRuntime, SidecarPool.Config config,
                         ExternalProcessRegistry registry) {
        this.pythonBin = pythonBin;
        this.udfRuntime = udfRuntime;
        this.pool = new SidecarPool(pythonRuntime(), config, registry);
    }

    /** 注册进 {@link BackendRegistry}（type="python" 即可被 CREATE FUNCTION 分发）。 */
    public void install() {
        BackendRegistry.register(this);
    }

    /** 语言无关的 sidecar 进程池（供生命周期钩子 close / 能力上报）。 */
    public SidecarPool pool() {
        return pool;
    }

    /** 该节点 python 环境是否可用（能力上报 / 规划前置规避）。 */
    public boolean available() {
        return pool.runtimeAvailable();
    }

    @Override
    public String type() {
        return "python";
    }

    @Override
    public void init() {
        pool.ensureReady();
    }

    @Override
    public void validate(FunctionDefinition def) {
        // 1. code_path → 注入 PYTHONPATH（必须在 spawn 之前）
        addCodePath(codeImportRoot(def));
        // 2. 冒烟校验：sidecar import 模块 + 解析契约 → 缺阶段注解等坏函数在此 fail-fast；
        //    环境缺包（python/pyarrow/protobuf）也在此首次暴露，并翻译成可操作错误给用户。
        try {
            pool.ensureReady();
            pool.checkFunction(def.className());
        } catch (SidecarException e) {
            throw describeStartupFailure(e);
        }
    }

    @Override
    public ScalarFunction loadScalar(FunctionDefinition def) {
        addCodePath(codeImportRoot(def));   // 幂等：确保 PYTHONPATH 已含代码目录
        // 契约感知：仅标量函数注册进 ScalarFunctionRegistry；聚合/collect 等非标量契约
        // 抛 IllegalArgumentException（FunctionManager.ensureLoadedFrom 捕获即跳过），
        // 防止 collect 聚合被误注册成标量（与 JavaBackend 契约试错语义对称）。
        if (signatureOf(def).getKind() != FunctionSignature.FunctionKind.SCALAR) {
            throw new IllegalArgumentException("非标量函数契约: " + def.name());
        }
        return new PythonFunctionBridge(pool, def.name(), def.className());
    }

    @Override
    public void loadAggregate(FunctionDefinition def) {
        addCodePath(codeImportRoot(def));   // 幂等：确保 PYTHONPATH 已含代码目录
        // sidecar check_function 回传结构化签名：COLLECT → 注册 CollectAggregateRegistry
        // （框架攒值，外部只写 evaluate）；AGGREGATE → 注册 AggregateFunctionRegistry（增量四阶段）；
        // 标量等非聚合契约 → IllegalArgumentException 跳过（同 loadScalar）。
        FunctionSignature sig = signatureOf(def);
        if (sig.getKind() == FunctionSignature.FunctionKind.COLLECT) {
            ArrowType out = outputTypeOf(sig.getOutputSchema().toByteArray());
            CollectAggregateFunction bridge = new PythonFunctionBridge(
                    pool, def.name(), def.className(), sig.getInputColumnsList(), out);
            CollectAggregateRegistry.registerGlobal(def.name(), bridge);
            return;
        }
        if (sig.getKind() == FunctionSignature.FunctionKind.AGGREGATE) {
            AggregateFunction<?> bridge = new PythonFunctionBridge(pool, def.name(), def.className());
            AggregateFunctionRegistry.registerGlobal(def.name(), bridge);
            return;
        }
        throw new IllegalArgumentException("非聚合函数契约: " + def.name());
    }

    @Override
    public WindowFunction loadWindow(FunctionDefinition def) {
        // 与 loadTable 同理：抛 IllegalArgumentException（= "非该契约 → 跳过"），
        // 对齐 FunctionManager.ensureLoadedFrom 的 catch 契约。loadWindow 当前未接入查询期
        // 加载循环，但保持一致可防未来接线后复发 Bug #1。
        throw new IllegalArgumentException("Python 窗口函数留待 v2（非窗口函数契约）");
    }

    @Override
    public TableFunction loadTable(FunctionDefinition def) {
        // 必须抛 IllegalArgumentException（= "非该契约 → 跳过"），对齐 FunctionManager.ensureLoadedFrom
        // 的 catch 契约（只捕 IllegalArgumentException）。抛别的会穿透 ensureLoaded()，
        // 导致"定义任意 Python 函数后所有查询都失败"（Bug #1）。
        throw new IllegalArgumentException("Python 表函数留待 v2（非表函数契约）");
    }

    @Override
    public void close() {
        pool.close();
    }

    // ==================== Python 运行时描述 ====================

    /** 构造 Python sidecar 的启动命令 + 环境（语言特有；引擎不解释）。 */
    private SidecarPool.Runtime pythonRuntime() {
        List<String> cmd = new ArrayList<>();
        cmd.add(pythonBin);
        cmd.add("-m");
        cmd.add("dojao_udf.sidecar");

        Map<String, String> env = new LinkedHashMap<>();
        List<String> parts = new ArrayList<>();
        if (udfRuntime != null && !udfRuntime.isBlank()) {
            parts.add(udfRuntime);
        }
        parts.addAll(codePaths);
        String inherited = System.getenv("PYTHONPATH");
        if (inherited != null && !inherited.isBlank()) {
            parts.add(inherited);
        }
        if (!parts.isEmpty()) {
            env.put("PYTHONPATH", String.join(File.pathSeparator, parts));
        }
        return new SidecarPool.Runtime("python", cmd, env);
    }

    /**
     * 追加用户函数代码目录到 sidecar 的 PYTHONPATH（幂等去重）。
     *
     * <p>CREATE FUNCTION 冒烟校验时由 {@link #validate} 注入。若进程池已启动，新增目录会经
     * {@link SidecarPool#setRuntime} <b>重建进程池</b> —— 已起 sidecar 的 PYTHONPATH 不包含新目录，
     * 不重建则后续函数 import 必失败（ModuleNotFoundError）。</p>
     */
    private synchronized void addCodePath(String dir) {
        String norm = Path.of(dir).toAbsolutePath().normalize().toString();
        if (codePaths.contains(norm)) {
            return;
        }
        codePaths.add(norm);
        pool.setRuntime(pythonRuntime());   // 未就绪仅更新；已就绪则重建进程池
    }

    /**
     * code_path → 需要注入 PYTHONPATH 的目录：{@code .py} 文件 → 父目录；
     * 目录 / {@code .zip} / {@code .whl}（Python 可直接从 zip 导入）→ 自身。
     * 经 {@link PathReaders} 统一解析（file/shared 直通，http/hdfs/s3 先下载到缓存）。
     */
    private static String codeImportRoot(FunctionDefinition def) {
        ResolvedCode rc = PathReaders.resolve(def.codePath());
        Path p = Path.of(rc.localPath());
        if (Files.isRegularFile(p) && rc.localPath().toLowerCase().endsWith(".py")) {
            return p.toAbsolutePath().getParent().toString();
        }
        return p.toAbsolutePath().normalize().toString();
    }

    // ==================== 函数签名 ====================

    private FunctionSignature signatureOf(FunctionDefinition def) {
        try {
            return pool.checkFunctionSignature(def.className());
        } catch (SidecarException e) {
            throw describeStartupFailure(e);
        }
    }

    /**
     * 把 sidecar <b>启动失败</b>（{@code NO_RUNTIME_ENV} / {@code STARTUP_FAILED}）翻译成对用户可操作的错误：
     * 显式点明「用的哪个解释器 + 缺依赖 + 怎么修」，而不是丢一段裸的 sidecar stderr。这样用户
     * CREATE / 首次使用 python 函数时就能第一时间知道是"缺包"，而不用去翻启动日志。
     *
     * <p>其它类别（{@code USER_CODE_ERROR} 用户代码异常 / {@code PROTOCOL_MISMATCH} …）原样返回 —— 它们
     * 已自带足够信息，不该被这里的"环境"文案覆盖。</p>
     */
    private SidecarException describeStartupFailure(SidecarException e) {
        if (e.kind() != SidecarException.Kind.NO_RUNTIME_ENV
                && e.kind() != SidecarException.Kind.STARTUP_FAILED) {
            return e;
        }
        String hint = "Python 运行时不可用（无法启动 sidecar）。请在解释器 '" + pythonBin
                + "' 上装齐依赖：" + pythonBin + " -m pip install -r " + udfRuntime
                + "/requirements.txt（需 pyarrow / numpy / protobuf）；"
                + "若本机有多套 Python / conda，用 -Dsystem.python.home=<环境根> 指到装了依赖的那一个。";
        return new SidecarException(e.kind(), hint + " 底层原因 → " + e.getMessage(), e);
    }

    private static final ArrowType DEFAULT_OUTPUT =
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

    /**
     * 输出 Arrow schema（IPC 序列化字节）→ 首个字段的 Arrow 类型。
     * 用 Arrow 承载类型 = 语言无关，引擎不做"语言类型名 → Arrow"的映射表。
     */
    private static ArrowType outputTypeOf(byte[] ipcSchema) {
        if (ipcSchema == null || ipcSchema.length == 0) {
            return DEFAULT_OUTPUT;
        }
        try (ReadChannel ch = new ReadChannel(new ByteArrayReadableSeekableByteChannel(ipcSchema))) {
            Schema schema = MessageSerializer.deserializeSchema(ch);
            if (schema.getFields().isEmpty()) {
                return DEFAULT_OUTPUT;
            }
            return schema.getFields().get(0).getType();
        } catch (IOException e) {
            throw new IllegalStateException("输出 Arrow schema 反序列化失败", e);
        }
    }
}
