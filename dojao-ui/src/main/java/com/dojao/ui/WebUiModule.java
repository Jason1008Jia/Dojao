package com.dojao.ui;

import com.dojao.common.MasterModule;
import com.dojao.metastore.MetaStore;
import com.dojao.ui.monitor.QueryMonitor;

/**
 * 将 Dojao Web UI 适配为 {@link MasterModule}，
 * 使其随 Master 一起启动和关闭。
 *
 * <h3>使用方式</h3>
 * <pre>{@code
 *   MasterServer master = new MasterServer(config);
 *   WebUiModule web = new WebUiModule(2380);
 *   web.bind(monitor, metaStore);   // 注入查询监控实现 + 引擎的元数据库（dojao-server 侧）
 *   master.addModule(web);
 *   master.start();
 *   // 浏览器访问 http://localhost:2380
 * }</pre>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/08 15:30
 */
public class WebUiModule implements MasterModule {

    private final int port;
    private QueryMonitor monitor;
    /** 引擎的元数据库（与 SQL 执行同源）—— UI 只读，不自建。 */
    private MetaStore metaStore;
    private DojaoUiApp app;

    /** SPI 自动发现时使用的默认构造器（端口 2380）。 */
    public WebUiModule() {
        this(2380);
    }

    public WebUiModule(int port) {
        this.port = port;
    }

    /**
     * 绑定查询监控实现（实现在 dojao-server，UI 模块不感知 server 类型）与引擎的元数据库。
     * 未绑定 monitor 时监控 / SQL 执行 REST 接口返回 503，仅保留 SQL 计划 DAG 预览。
     *
     * @param monitor   查询监控实现（可为 null = 独立预览模式）
     * @param metaStore 引擎正在使用的 MetaStore（catalog 注册表唯一权威；UI 只读它）
     */
    public void bind(QueryMonitor monitor, MetaStore metaStore) {
        this.monitor = monitor;
        this.metaStore = metaStore;
    }

    @Override
    public String name() {
        return "WebUI";
    }

    @Override
    public void start() {
        app = new DojaoUiApp(monitor, metaStore);
        app.start(port);
    }

    @Override
    public void shutdown() {
        if (app != null) {
            app.stop();
        }
    }
}
