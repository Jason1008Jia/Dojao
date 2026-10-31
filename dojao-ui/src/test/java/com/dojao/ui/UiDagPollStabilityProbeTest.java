package com.dojao.ui;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 探针（离线静态检查）：运行中任务的 DAG / 详情页「一直闪、一直在抖」。
 *
 * <h3>病根</h3>
 * <ul>
 *   <li><b>详情页</b>：DAG 每 300ms 轮询一次，{@code refreshLiveOperator} / {@code renderTaskPage}
 *       / {@code refreshTaskDetail} 每次都对整块 DOM 做 {@code innerHTML} 重建 ⇒ 运行中任务的
 *       {@code .task-dot2.run} 的 CSS {@code pulse} 动画每 300ms 从头重播（闪）、
 *       {@code .task-row:hover} / {@code .metric-card:hover} 的过渡被打断（抖），
 *       滚动位置与文本选中也会丢；</li>
 *   <li><b>DAG</b>：节点 label 含实时数字，{@code measureText} 宽度每拍都变 ⇒ vis 节点包围盒变
 *       ⇒ 连线端点被反复重锚定；重建整图时旧 vis 实例不销毁；并发轮询的旧响应晚到会把
 *       结构键来回改 ⇒ 整图反复重建（闪）。</li>
 * </ul>
 *
 * <h3>校验的装配关系</h3>
 * <ul>
 *   <li>{@code detail.js}：结构签名（{@code data-op-sig} / {@code data-list-sig} /
 *       {@code data-task-sig}）不匹配才重建，否则原地改数值；</li>
 *   <li>{@code dag-renderer.js}：节点尺寸量化 + 只增不减缓存；结构键对 children 排序；
 *       重建前销毁旧网络；</li>
 *   <li>{@code simulation.js}：丢弃迟到 / 乱序 / 切换查询后的旧快照；空算子快照保留上一帧。</li>
 * </ul>
 *
 * <p><b>判别力</b>：把 {@code data-op-sig} 判定改回无条件重建、去掉 {@code stableNodeSize}、
 * 或去掉 {@code seq < dagAppliedSeq} 守卫，对应用例立即 FAIL。</p>
 */
public final class UiDagPollStabilityProbeTest {

    private static int pass = 0;
    private static int fail = 0;

    public static void main(String[] args) throws Exception {
        String detail = read("dojao-ui/src/main/resources/public/js/detail.js");
        String dag = read("dojao-ui/src/main/resources/public/js/dag-renderer.js");
        String sim = read("dojao-ui/src/main/resources/public/js/simulation.js");
        String css = read("dojao-ui/src/main/resources/public/css/style.css");

        // ==================== 详情页：增量渲染 ====================
        check("详情页提供「仅变化才写 DOM」助手", detail.contains("function setTextIfChanged(el, text)"));
        check("详情页提供指标卡原地刷新助手", detail.contains("function setMetric(scope, key, value, sub)"));
        check("算子详情指标卡带 data-metric 键",
                detail.contains("data-metric=\"' + key + '\""));

        String live = functionBody(detail, "function refreshLiveOperator() {");
        check("算子详情按结构签名判定是否重建", live.contains("body.getAttribute('data-op-sig') !== sig"));
        check("算子详情重建后记录结构签名", live.contains("body.setAttribute('data-op-sig', sig)"));
        check("算子详情结构未变时原地刷指标", live.contains("setMetric(body, 'rowsIn', fmtRows(op.rowsIn))"));
        check("算子详情不再无条件重建（innerHTML 在签名判定之内）",
                indexOf(live, "body.getAttribute('data-op-sig') !== sig") < indexOf(live, "body.innerHTML = failHtml"));

        String page = functionBody(detail, "function renderTaskPage(page) {");
        check("任务列表按任务集合签名判定是否重建", page.contains("container.getAttribute('data-list-sig') !== sig"));
        check("任务列表重建后记录任务集合签名", page.contains("container.setAttribute('data-list-sig', sig)"));
        check("任务列表逐行原地更新", page.contains("function updateTaskRow(row, t, listIdx)")
                || detail.contains("function updateTaskRow(row, t, listIdx)"));
        check("任务行仅在状态类名变化时改 className",
                detail.contains("if (dot.className !== cls) dot.className = cls;"));
        check("任务行仅在 data-idx 变化时重绑点击",
                detail.contains("if (row.getAttribute('data-idx') !== String(listIdx))"));
        check("分页条按签名判定是否重建", page.contains("pg.getAttribute('data-pg-sig') !== pgSig"));

        String showTask = functionBody(detail, "function showTaskDetail(listIdx) {");
        check("任务详情先显示再填充（不会先露出上一次任务的内容）",
                indexOf(showTask, "classList.add('show')") < indexOf(showTask, "refreshTaskDetail()"));

        String task = functionBody(detail, "function refreshTaskDetail() {");
        check("任务详情按任务签名判定是否重建", task.contains("body.getAttribute('data-task-sig') !== sig"));
        check("任务详情重建后记录任务签名", task.contains("body.setAttribute('data-task-sig', sig)"));
        check("任务详情结构未变时原地刷状态/行数/耗时",
                task.contains("body.querySelectorAll('.task-metric-value')")
                        && task.contains("setTextIfChanged(vals[4], fmtRows(t.rows))"));

        // 病根仍在：CSS 的脉冲动画会被 DOM 重建打断（探针把因果钉在代码里）
        check("运行中任务点确实有循环脉冲动画（重建即闪的根因）",
                css.contains(".task-dot2.run") && css.contains("animation: pulse"));

        // ==================== DAG：节点尺寸稳定 ====================
        check("DAG 节点尺寸量化缓存存在", dag.contains("var nodeSizeCache = {};")
                && dag.contains("var NODE_SIZE_QUANT = 20;"));
        check("节点宽度按步长向上取整",
                dag.contains("Math.ceil(rawW / NODE_SIZE_QUANT) * NODE_SIZE_QUANT"));
        check("节点尺寸只增不减（轮询不会缩小→再放大）",
                dag.contains("if (qw > c.w) c.w = qw;") && dag.contains("if (qh > c.h) c.h = qh;"));
        check("渲染器使用稳定尺寸而非每帧重新测量即用",
                dag.contains("var box = stableNodeSize(id, Math.max(minW, maxLineW + pad * 2)"));
        check("重建整图时清空尺寸缓存", dag.contains("nodeSizeCache = {};"));

        // ==================== DAG：结构键 / 重建 ====================
        String keyFn = functionBody(dag, "function dagStructureKey(ops) {");
        check("结构键对 children 做数值排序（边枚举顺序变化不触发重建）",
                keyFn.contains("slice().sort(function (a, b) { return a - b; })"));
        check("整图重建前销毁旧 vis 网络", dag.contains("if (network) { network.destroy(); network = null; }"));

        // ==================== 轮询：乱序 / 空快照 ====================
        check("DAG 轮询有请求序号", sim.contains("var dagPollSeq = 0;")
                && sim.contains("var dagAppliedSeq = 0;"));
        check("丢弃乱序到达的旧快照", sim.contains("seq < dagAppliedSeq) return;"));
        check("丢弃切换查询 / 重开 DAG 后上一会话的旧响应",
                sim.contains("qid !== currentJobId || session !== dagPollSession"));
        String poll = functionBody(sim, "function pollDagOnce() {");
        check("空算子快照保留上一帧而不是拆图",
                poll.contains("if (!liveOps.length && network)"));
        check("重建/刷新仍由结构键决定", poll.contains("key === window._dagKey"));

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

    /** 截取一个顶层函数体（从 header 到下一个行首 {@code }}）；找不到返回空串。 */
    private static String functionBody(String source, String header) {
        int start = source.indexOf(header);
        if (start < 0) return "";
        int end = source.indexOf("\n}", start);
        return end < 0 ? source.substring(start) : source.substring(start, end + 2);
    }

    private static int indexOf(String source, String needle) {
        int i = source.indexOf(needle);
        return i < 0 ? Integer.MAX_VALUE : i;
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
