package com.dojao.server;

import com.dojao.ui.WebUiModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dojao Server 启动入口。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/11
 */
public class DojaoServerLauncher {

    private static final Logger log = LoggerFactory.getLogger(DojaoServerLauncher.class);

    public static void main(String[] args) throws Exception {
        DojaoServer server = new DojaoServer();


        // Catalog 由 MetaStore 从外接存储恢复
        server.addModule(WebUiModule.class);
        server.start();





        log.info("Dojao Server 已启动，按 Ctrl+C 停止");
        Thread.currentThread().join();
    }
}
