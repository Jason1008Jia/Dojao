package com.dojao.server.util;

import com.dojao.core.config.ResourceConfig;
import com.dojao.server.external.ExternalServiceManager;
import com.dojao.server.function.PythonBackend;
import com.dojao.server.function.SidecarPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Python 后端引导 —— Master / Worker 独立进程统一装配入口（L1）。
 *
 * <p>与单进程版 {@code DojaoServer} 一致：启动期<b>只把 {@code PythonBackend} 注册进
 * BackendRegistry</b>（使 {@code CREATE FUNCTION type='python'} 与查询期加载可被分发），
 * <b>不 spawn sidecar、不探活</b>。本机是否装了 python / 依赖一律<b>不查</b> —— 首次 CREATE /
 * 执行 python 函数时才拉起 sidecar，失败则把<b>缺包等显式错误</b>抛给用户（见 {@link #install}）。</p>
 *
 * <p>参数（均可用 -D 覆盖，语义与 Python 概念对齐）：
 * <ul>
 *   <li>{@code system.python.home} —— <b>Python 环境路径</b>（安装前缀 / conda env 根）。
 *       服务器有多个 Python（{@code python3.11}、conda env…）时，这里指向<b>具体那一个环境</b>；
 *       解释器取 {@code <home>/bin/python}（Windows {@code <home>\python.exe}）。
 *       留空 → 用 <b>PATH 上的默认解释器</b>（Linux {@code python3} / Windows {@code python}）；
 *       也可直接给解释器文件路径（如 {@code /usr/bin/python3.11}）。</li>
 *   <li>{@code system.python.udf.runtime} —— <b>dojao_udf SDK（sidecar 运行时）所在路径</b>，
 *       默认 {@code <DOJAO_HOME>/python}（分发包自带，解压即用；含 {@code dojao_udf} 包的目录）。</li>
 *   <li>{@code system.python.pool} —— sidecar 进程数，默认 4。</li>
 * </ul>
 * <p>★ {@code system.python.udf.runtime} 是<b>引擎自带的 sidecar 运行时</b>路径，<b>不是</b>用户函数
 * 代码 —— 用户函数路径由每个 {@code CREATE FUNCTION} 的 {@code code_path} 各自声明。</p>
 *
 * <p>sidecar 进程数 ≥ 分区并行度：分布式聚合按"每分区一个 AggregateConsumer"切分，collect-evaluate
 * 的评估并行度 = 分区数；pool 小于分区数时多个消费者的评估会串行排队在少数 sidecar 上。</p>
 */
public final class PythonBackendBootstrap {

    /** Python 环境路径（安装前缀 / conda env 根；空 → PATH 默认解释器）。 */
    public static final String PROP_PYTHON_HOME = "system.python.home";
    /** dojao_udf SDK（sidecar 运行时）路径。 */
    public static final String PROP_UDF_RUNTIME = "system.python.udf.runtime";
    /** sidecar 进程数。 */
    public static final String PROP_POOL = "system.python.pool";

    /** 兜底 SDK 路径：分发包里没有 {@code python/}、也非开发树时用（开发树见 {@link #udfRuntime()}）。 */
    private static final String FALLBACK_UDF_RUNTIME = "python";

    private PythonBackendBootstrap() {
    }

    /** 解释器命令：由 {@code system.python.home} 推导（空 → PATH 默认；目录 → 其下 bin/python；文件 → 原样）。 */
    public static String pythonBin() {
        return resolveInterpreter(System.getProperty(PROP_PYTHON_HOME, ""));
    }

    /**
     * dojao_udf SDK（sidecar 运行时）路径 —— 非用户函数代码。
     *
     * <p>解析顺序：① 显式 {@code -Dsystem.python.udf.runtime}；② <b>{@code <DOJAO_HOME>/python}</b>
     * —— 分发包自带这份 SDK（见 {@code dojao-dist} 装配），解压即用、无需任何配置；
     * ③ 都没有时退回相对目录 {@code python}（交给进程工作目录解析）。</p>
     */
    public static String udfRuntime() {
        String explicit = System.getProperty(PROP_UDF_RUNTIME);
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        Path bundled = ResourceConfig.dojaoHome().resolve("python");
        if (Files.isDirectory(bundled)) {
            return bundled.toString();
        }
        return FALLBACK_UDF_RUNTIME;
    }

    /**
     * {@code python-home} → 解释器命令。
     * <ul>
     *   <li>空 → PATH 上的默认（Linux {@code python3} / Windows {@code python}）；</li>
     *   <li>目录（conda env / 安装前缀）→ {@code <home>/bin/python}（Windows {@code <home>\python.exe}），
     *       目录里找不到则该环境不可用，回退 PATH 默认；</li>
     *   <li>文件 → 视为解释器路径，原样使用。</li>
     * </ul>
     */
    static String resolveInterpreter(String pythonHome) {
        if (pythonHome == null || pythonHome.isBlank()) {
            return defaultInterpreter();
        }
        Path p = Path.of(pythonHome);
        if (Files.isDirectory(p)) {
            Path exe = isWindows() ? p.resolve("python.exe") : p.resolve("bin").resolve("python");
            return Files.exists(exe) ? exe.toString() : defaultInterpreter();
        }
        return pythonHome;
    }

    private static String defaultInterpreter() {
        return isWindows() ? "python" : "python3";
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }

    /**
     * 注册 Python 后端（<b>懒启动</b>，启动期零外部依赖检查）。
     *
     * @param role 进程角色标签（日志用，如 "Master" / "Worker"）
     * @return 后端进程池（<b>始终非 null</b>；关闭钩子对它调 close 即可，未启动时 no-op）。
     */
    public static ExternalServiceManager install(String role) {
        return install(role, null);
    }

    /**
     * 注册 Python 后端（<b>懒启动</b>，启动期零外部依赖检查）—— 接入外部进程注册表。
     *
     * <p>★ 启动期<b>只注册、不 spawn、不探活</b>：本机是否装了 python / pyarrow / protobuf 一律<b>不查</b>。
     * 等到用户真正 CREATE / 执行 python 函数那一刻，才由 {@link PythonBackend#validate} /
     * {@link PythonBackend#loadScalar} 触发 sidecar 拉起；失败即以<b>显式错误</b>抛给用户
     * （缺哪个包一目了然），而不是淹没在启动日志里、也不是退化成「不支持的函数类型: python」。</p>
     *
     * <p>收益：Master / Worker 启动不再被外部依赖拖累与拖慢，错误集中暴露在「用户的操作」上。
     * 其它语言后端（R / Julia / …）一律照此：启动只 register，环境校验推迟到首次使用。</p>
     *
     * @param role     进程角色标签（日志用）
     * @param registry 外部进程注册表：sidecar spawn 时登记（uuid / port / queryId=null），关闭时解除；可传 null
     * @return 后端进程池（<b>始终非 null</b>）
     */
    public static ExternalServiceManager install(String role, ExternalProcessRegistry registry) {
        PythonBackend backend = new PythonBackend(pythonBin(), udfRuntime(),
                SidecarPool.Config.of(Integer.getInteger(PROP_POOL, 4),
                        Duration.ofSeconds(15), Duration.ofSeconds(30)), registry);
        backend.install();   // 只注册：type='python' 成为已知类型；不 spawn、不探活
        System.err.println(">>> [" + role + "] Python 后端已注册（type=python；懒启动 —— 首次 CREATE/执行 python 函数时才拉起 sidecar）");
        return backend.pool();
    }
}
