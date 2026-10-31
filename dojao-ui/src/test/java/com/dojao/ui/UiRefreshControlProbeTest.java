package com.dojao.ui;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 探针（离线静态检查）：UI 顶栏「刷新当前页面」按钮 + 整页刷新拦截。
 *
 * <p>校验的是一组<b>装配关系</b>（这些静态资源不会在 JVM 里执行）：</p>
 * <ul>
 *   <li>{@code index.html}：刷新按钮确实在顶栏、且紧邻设置按钮，点击调 {@code refreshCurrentView()}；</li>
 *   <li>{@code dashboard.js}：刷新按当前 tab 分派（首页 / 任务管理 / 监控 / AI，DAG 单独刷新），
 *       且 {@code beforeunload} + F5 / Ctrl+R 拦截已绑定，越界时用 {@code mgModal} 提示；</li>
 *   <li>{@code app.js}：初始化确实调用 {@code bindRefreshGuard()}；</li>
 *   <li>{@code auth.js} / {@code utils.js}：主动登出 / 401 跳转前放行，避免误弹「请勿刷新」；</li>
 *   <li>{@code style.css}：刷新按钮与弹窗文案样式存在。</li>
 * </ul>
 *
 * <p><b>判别力</b>：删掉 {@code bindRefreshGuard();} 或把某个 tab 分支改坏（如
 * {@code currentTab === 'monitor' → 'monitorX'}），对应用例立即 FAIL。</p>
 */
public final class UiRefreshControlProbeTest {

    private static int fail = 0;

    public static void main(String[] args) throws Exception {
        String html = read("dojao-ui/src/main/resources/public/index.html");
        String dashboard = read("dojao-ui/src/main/resources/public/js/dashboard.js");
        String app = read("dojao-ui/src/main/resources/public/js/app.js");
        String auth = read("dojao-ui/src/main/resources/public/js/auth.js");
        String utils = read("dojao-ui/src/main/resources/public/js/utils.js");
        String simulation = read("dojao-ui/src/main/resources/public/js/simulation.js");
        String css = read("dojao-ui/src/main/resources/public/css/style.css");

        // ---- 顶栏按钮：刷新紧邻设置 ----
        int refreshBtn = html.indexOf("id=\"refresh-btn\"");
        int settingsBtn = html.indexOf("id=\"settings-btn\"");
        check("顶栏存在刷新按钮", refreshBtn >= 0);
        check("刷新按钮紧邻设置按钮（位于其左侧）", refreshBtn >= 0 && settingsBtn > refreshBtn);
        check("刷新按钮点击调 refreshCurrentView()", html.contains("onclick=\"refreshCurrentView()\""));

        // ---- 刷新分派：只刷新当前选中的功能区 ----
        check("refreshCurrentView 已定义", dashboard.contains("function refreshCurrentView()"));
        check("首页分支：catalog + 当前结果",
                dashboard.contains("currentTab === 'home'")
                        && dashboard.contains("loadCatalogTree()")
                        && dashboard.contains("pollSqlResult()"));
        check("任务分支：列表与集群卡片", dashboard.contains("currentTab === 'tasks'")
                && dashboard.contains("refreshAll()"));
        check("任务分支：DAG 视图单独刷新", dashboard.contains("pollDagOnce()"));
        check("监控分支：只重拉监控快照", dashboard.contains("currentTab === 'monitor'")
                && dashboard.contains("refreshMonitor()"));
        check("AI 分支：只重拉会话列表（不打断对话）",
                dashboard.contains("currentTab === 'ai'")
                        && dashboard.contains("refreshAiSessions(false)"));
        check("刷新过程有按钮旋转态", dashboard.contains("classList.add('spinning')")
                && dashboard.contains("classList.remove('spinning')"));

        // ---- 整页刷新拦截 ----
        check("bindRefreshGuard 已定义", dashboard.contains("function bindRefreshGuard()"));
        check("拦截 F5", dashboard.contains("key === 'f5'"));
        check("拦截 Ctrl+R / Cmd+R", dashboard.contains("key === 'r'"));
        check("键盘拦截先 preventDefault", dashboard.contains("e.preventDefault()"));
        check("绑定 beforeunload", dashboard.contains("addEventListener('beforeunload'"));
        check("给浏览器确认框设置提示文案", dashboard.contains("请勿刷新页面：刷新会导致未保存的数据丢失"));
        check("页面内提示弹窗（mgModal）", dashboard.contains("mgModal('请勿刷新整个页面'"));
        check("引导使用顶栏刷新按钮", dashboard.contains("只刷新当前选中的页面"));
        check("登出 / 401 可放行整页跳转",
                auth.contains("window.__dojaoAllowUnload = true")
                        && utils.contains("window.__dojaoAllowUnload = true"));

        // ---- 装配：初始化真的绑定 ----
        check("app.js 初始化调用 bindRefreshGuard()", hasLiveLine(app, "bindRefreshGuard();"));

        // ---- 轮询函数返回 Promise（刷新旋转态能等到本次请求收尾）----
        check("pollSqlResult 返回请求 Promise", dashboard.contains("return httpGet('/api/query/'"));
        check("pollDagOnce 返回请求 Promise", simulation.contains("return httpGet('/api/queries/'"));

        // ---- 样式 ----
        check("刷新按钮样式与设置按钮一致", css.contains(".theme-toggle, .settings-btn, .refresh-btn"));
        check("刷新按钮旋转动画", css.contains(".refresh-btn.spinning svg")
                && css.contains("@keyframes refresh-spin"));
        check("拦截弹窗文案样式", css.contains(".refresh-guard-tip"));

        System.out.println(fail == 0 ? "\n全部通过 ✅" : "\n失败 " + fail + " 项 ❌");
        if (fail > 0) {
            System.exit(1);
        }
    }

    private static String read(String path) {
        try {
            return Files.readString(Path.of(path), StandardCharsets.UTF_8);
        } catch (Exception e) {
            check("读取失败: " + path + "（" + e.getMessage() + "）", false);
            return "";
        }
    }

    /** 源码里存在未被注释掉的该行（防止「注释掉调用」也让探针误判为通过）。 */
    private static boolean hasLiveLine(String source, String snippet) {
        for (String line : source.split("\\R")) {
            String t = line.strip();
            if (t.startsWith("//") || t.startsWith("*") || t.startsWith("/*")) continue;
            if (t.contains(snippet)) return true;
        }
        return false;
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "  ✅ " : "  ❌ ") + name);
        if (!ok) {
            fail++;
        }
    }
}
