package com.dojao.common;

/**
 * Master 可插拔模块接口。
 *
 * <p>实现此接口的模块会在 Master 启动时自动加载、关闭时自动卸载。
 * 新增模块只需实现此接口并注册到 MasterServer 即可，无需修改 Master 核心代码。</p>
 *
 * <h3>使用方式</h3>
 * <pre>{@code
 *   MasterServer master = new MasterServer(config);
 *   master.addModule(new WebUiModule(4009));
 *   master.start();  // WebUiModule 随 Master 一起启动
 * }</pre>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/08 15:30
 */
public interface MasterModule {

    /** 模块名称，用于日志和监控。 */
    String name();

    /** 启动模块。Master 在 Flight 服务器启动后调用。 */
    void start() throws Exception;

    /** 关闭模块。Master 在 Flight 服务器关闭前调用。 */
    void shutdown();
}
