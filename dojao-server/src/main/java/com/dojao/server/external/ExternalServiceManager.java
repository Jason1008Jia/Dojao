package com.dojao.server.external;

/**
 * 外部服务生命周期管理器 —— 语言无关的生命周期契约（进程拓扑改造设计里程碑，2026-08-30 定稿）。
 *
 * <p>Dojao 多语言 UDF 外挂进程（python sidecar / 未来的 R、Rust 等）由各自语言的 manager 管理。
 * <b>worker 不关心具体语言</b>，只持有本接口，启动/关闭/强杀统一走 {@link #start()} /
 * {@link #close()} / {@link #kill()}。具体实现（如 {@code SidecarPool}）负责 spawn 子进程、
 * 登记进 {@code ExternalProcessRegistry}、按语言特性清理。</p>
 *
 * <p><b>职责边界</b>（与本接口配套的设计）：</p>
 * <ul>
 *   <li><b>生命周期（本接口）</b>：语言无关。start / isAvailable / close / kill。</li>
 *   <li><b>语言调用</b>：留在具体类（如 {@code SidecarPool.invoke/invokeBatch}），
 *       只被该语言的后端绑定（如 {@code PythonBackend}）使用，worker 永远不接触。</li>
 *   <li><b>登记表</b>：{@code ExternalProcessRegistry}（com.dojao.server.util）统一登记每个外部进程
 *       （uuid / type / Process / host:port / queryId），任务结束 {@code killByQuery}、关机
 *       {@code killAll} —— 语言无关，本接口的具体实现把进程登记进去。</li>
 * </ul>
 *
 * <p><b>kill 责任归 worker</b>（用户确认）：worker 关闭前先检测外部服务，有则杀掉再关机；
 * master 只负责协调（发关闭指令 / 判 LOST）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/30
 */
public interface ExternalServiceManager extends AutoCloseable {

    /** 服务类型标识（如 "python" / "r" / "rust"）。 */
    String serviceType();

    /** 启动外部服务（spawn 进程 / 建立连接）。已启动则幂等 no-op。 */
    void start();

    /** 能力探活：当前能否服务（供心跳上报 / 规划前置规避）。 */
    boolean isAvailable();

    /** 优雅关闭：先优雅停机（如 destroy → 等 grace → destroyForcibly），再释放资源。幂等。 */
    @Override
    void close();

    /**
     * 强制关闭：跳过优雅等待直接强杀（进程无响应 / worker 被强杀前的最后手段）。幂等。
     * 默认实现委托 {@link #close()}（无优雅停机概念的语言直接继承默认即可）。
     */
    default void kill() {
        close();
    }
}
