package com.dojao.ui;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 探针（离线静态检查）：首页「Query History（任务列表）」的状态筛选。
 *
 * <p>校验的是一组<b>装配关系</b>（静态资源不会在 JVM 里执行）：</p>
 * <ul>
 *   <li>{@code index.html}：筛选按钮组位于任务列表头部、且在标题左侧；六个状态取值齐全；</li>
 *   <li>{@code dashboard.js}：筛选状态有记忆键、有合法值白名单、切换时回到第 1 页；
 *       {@code renderJobTable} 在筛选结果上分页（而不是全量），空结果给出「查看全部」出口；</li>
 *   <li>{@code state.js} / {@code app.js}：全局状态 + 首屏恢复；</li>
 *   <li>{@code style.css}：头部行与筛选按钮（含选中态）样式存在。</li>
 * </ul>
 *
 * <p><b>判别力</b>：删掉 {@code jobList.filter(jobMatchesFilter)} 或把
 * {@code setJobStateFilter} 里的 {@code jobPage = 0;} 去掉，对应用例立即 FAIL。</p>
 */
public final class UiTaskFilterProbeTest {

    private static int pass = 0;
    private static int fail = 0;

    public static void main(String[] args) throws Exception {
        String html = read("dojao-ui/src/main/resources/public/index.html");
        String dashboard = read("dojao-ui/src/main/resources/public/js/dashboard.js");
        String state = read("dojao-ui/src/main/resources/public/js/state.js");
        String app = read("dojao-ui/src/main/resources/public/js/app.js");
        String css = read("dojao-ui/src/main/resources/public/css/style.css");

        // ---- 位置：任务列表头部、标题左侧 ----
        int head = html.indexOf("class=\"job-list-head\"");
        int filters = html.indexOf("id=\"job-filters\"");
        int title = html.indexOf("Query History（任务列表）", Math.max(head, 0));
        check("任务列表存在头部行", head >= 0);
        check("筛选按钮组在头部行内", filters > head);
        check("筛选按钮组位于标题左侧（同一行）", title > filters);
        check("筛选按钮组带无障碍分组语义", html.contains("role=\"group\"")
                && html.contains("aria-label=\"按任务状态筛选\""));

        // ---- 状态取值：全部 / 运行中 / 排队中 / 已完成 / 失败 / 已取消 ----
        String[] states = {"ALL", "RUNNING", "SUBMITTED", "FINISHED", "FAILED", "CANCELED"};
        for (String s : states) {
            check("存在状态筛选按钮 " + s,
                    html.contains("data-state=\"" + s + "\"")
                            && html.contains("setJobStateFilter('" + s + "')"));
        }
        check("「全部」默认选中", html.contains("class=\"job-filter active\" data-state=\"ALL\""));

        // ---- dashboard.js：筛选实现 ----
        check("定义合法筛选值白名单", dashboard.contains(
                "JOB_STATE_FILTERS = ['ALL', 'RUNNING', 'SUBMITTED', 'FINISHED', 'FAILED', 'CANCELED']"));
        check("筛选选择有 localStorage 记忆键", dashboard.contains(
                "JOB_STATE_FILTER_KEY = 'dojao.tasks.stateFilter'"));
        check("定义首屏恢复函数", dashboard.contains("function loadJobStateFilterPref()"));
        check("恢复时校验取值合法性", dashboard.contains("JOB_STATE_FILTERS.indexOf(s) >= 0"));
        check("定义切换函数", dashboard.contains("function setJobStateFilter(state)"));
        check("定义按钮选中态同步", dashboard.contains("function renderJobFilters()"));
        check("定义筛选谓词", dashboard.contains("function jobMatchesFilter(job)"));

        String setter = functionBody(dashboard, "function setJobStateFilter(state) {");
        check("切换筛选时校验取值", setter.contains("JOB_STATE_FILTERS.indexOf(state) < 0"));
        check("切换筛选时回到第 1 页", setter.contains("jobPage = 0;"));
        check("切换筛选时记忆选择", setter.contains("localStorage.setItem(JOB_STATE_FILTER_KEY, state)"));
        check("切换筛选后立即重绘", setter.contains("renderJobTable();"));

        String table = functionBody(dashboard, "function renderJobTable() {");
        check("任务表按筛选结果分页", table.contains("jobList.filter(jobMatchesFilter)"));
        check("分页切片使用筛选结果", table.contains("visibleJobs.slice("));
        check("筛选后无结果时给出「查看全部」出口",
                table.contains("当前筛选条件下没有任务")
                        && table.contains("setJobStateFilter(\\'ALL\\')"));
        check("每次重绘同步筛选按钮选中态", table.contains("renderJobFilters();"));
        check("分页条展示筛选后数量",
                table.contains("renderJobPagination(totalPages, visibleJobs.length)"));

        check("分页条显示「筛选后 / 全部」口径",
                dashboard.contains("（筛选后 · 全部 ' + jobList.length + ' 条）"));

        // ---- state.js / app.js：装配 ----
        check("全局筛选状态已声明", state.contains("var jobStateFilter = 'ALL';"));
        check("app.js 初始化恢复筛选偏好",
                hasLiveLine(app, "loadJobStateFilterPref();"));

        // ---- 样式 ----
        check("头部行样式", css.contains(".job-list-head")
                && css.contains("display: flex; align-items: center"));
        check("筛选按钮样式", css.contains(".job-filter {"));
        check("筛选按钮选中态样式", css.contains(".job-filter.active"));
        check("筛选空态「查看全部」按钮样式", css.contains(".job-filter-clear"));

        System.out.println("\n========== 结果：PASS=" + pass + " FAIL=" + fail + " ==========");
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

    /** 截取一个顶层函数体（从 header 到下一个行首 {@code }}）；找不到返回空串。 */
    private static String functionBody(String source, String header) {
        int start = source.indexOf(header);
        if (start < 0) return "";
        int end = source.indexOf("\n}", start);
        return end < 0 ? source.substring(start) : source.substring(start, end + 2);
    }

    private static void check(String label, boolean ok) {
        if (ok) {
            pass++;
            System.out.println("[PASS] " + label);
        } else {
            fail++;
            System.out.println("[FAIL] " + label);
        }
    }
}
