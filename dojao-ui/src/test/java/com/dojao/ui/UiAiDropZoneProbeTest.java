package com.dojao.ui;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 探针（离线静态检查）：Dojao AI 对话区的文件拖拽落区。
 *
 * <p>校验一组<b>装配关系</b>（静态资源不在 JVM 里执行）：</p>
 * <ul>
 *   <li>{@code index.html}：提示层 {@code #ai-drop-overlay} 位于 {@code #ai-main} 内、
 *       默认 hidden、文案说明「松开添加」；输入区提示行提到可拖拽；</li>
 *   <li>{@code dashboard.js}：{@code bindAiDropZone()} 已定义并在 {@code bindAiChat()} 里调用；
 *       dragenter/over/dragleave/drop 全部按捕获阶段绑定；只认带 Files 的拖拽；
 *       dragover/drop 必须 preventDefault；enter/leave 用深度计数防闪烁；
 *       文件夹不递归（isDirectory 跳过并提示）；drop 复用 {@code aiHandleAttachmentFiles}
 *       （配额/大小口径与回形针一致）；推理中/解析中有守卫；</li>
 *   <li>{@code style.css}：{@code .ai-main} 为定位容器，提示层绝对定位覆盖且
 *       {@code pointer-events:none}（不拦截拖拽），并有深色主题样式。</li>
 * </ul>
 *
 * <p><b>判别力</b>：删掉 {@code bindAiDropZone();} 调用、把 drop 分支改成不复用
 * {@code aiHandleAttachmentFiles}、去掉 dragover 的 preventDefault、或把提示层移出
 * {@code #ai-main}，对应用例立即 FAIL。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/22
 */
public final class UiAiDropZoneProbeTest {

    private static int fail = 0;

    public static void main(String[] args) throws Exception {
        String html = read("dojao-ui/src/main/resources/public/index.html");
        String dashboard = read("dojao-ui/src/main/resources/public/js/dashboard.js");
        String css = read("dojao-ui/src/main/resources/public/css/style.css");

        // ---- index.html：提示层位置与默认态 ----
        int aiMain = html.indexOf("id=\"ai-main\"");
        int overlay = html.indexOf("id=\"ai-drop-overlay\"");
        int composer = html.indexOf("class=\"ai-composer\"");
        int dagView = html.indexOf("id=\"dag-view\"");
        check("AI 主区存在", aiMain >= 0);
        check("提示层位于 AI 主区内（接线位置正确）",
                overlay > aiMain && overlay > composer && dagView > overlay);
        check("提示层默认隐藏", overlay >= 0
                && html.substring(Math.max(0, overlay - 120), overlay + 120).contains("hidden"));
        check("提示层文案说明松开添加", html.contains("松开以添加参考文件"));
        check("提示层列出支持类型与配额", html.contains("单文件 ≤ 10MB，最多 5 个"));
        check("输入区提示行提到可拖拽", html.contains("直接把文件拖到这里"));

        // ---- dashboard.js：函数与接线 ----
        check("bindAiDropZone 已定义", dashboard.contains("function bindAiDropZone()"));
        check("bindAiChat 真正调用 bindAiDropZone()", hasLiveLine(dashboard, "bindAiDropZone();"));
        check("落区只挂在 ai-main", dashboard.contains("document.getElementById('ai-main')"));
        check("仅认带 Files 的拖拽", dashboard.contains("function aiDragHasFiles")
                && dashboard.contains("dt.types[i] === 'Files'"));
        check("dragenter 已绑定", dashboard.contains("addEventListener('dragenter'"));
        check("dragover 已绑定", dashboard.contains("addEventListener('dragover'"));
        check("dragleave 已绑定", dashboard.contains("addEventListener('dragleave'"));
        check("drop 已绑定", dashboard.contains("addEventListener('drop'"));
        check("四个拖拽监听均走捕获阶段", countOccurrences(dashboard, "}, true);") >= 4);

        // ---- dragover / drop 的默认行为 ----
        check("dragover preventDefault（否则 drop 不触发）",
                dashboard.contains("e.preventDefault();") && dashboard.contains("dropEffect = 'copy'"));
        check("drop preventDefault", dashboard.contains("e.preventDefault();"));
        check("drop 阻止冒泡（避免浏览器打开文件）", dashboard.contains("e.stopPropagation();"));

        // ---- 防闪烁 + 防卡住 ----
        check("enter/leave 深度计数", dashboard.contains("depth++") && dashboard.contains("depth--"));
        check("provide aiSetDropActive", dashboard.contains("function aiSetDropActive"));
        check("提示层用 hidden 切换", dashboard.contains("overlay.hidden = !on"));
        check("拖出窗口/取消时兜底清理",
                dashboard.contains("addEventListener('dragend', reset)")
                        && dashboard.contains("addEventListener('blur', reset)")
                        && dashboard.contains("addEventListener('drop', reset)"));

        // ---- 文件收集与复用上传链路 ----
        check("文件夹不递归并提示", dashboard.contains("it.webkitGetAsEntry")
                && dashboard.contains("entry.isDirectory")
                && dashboard.contains("暂不支持拖拽文件夹"));
        check("drop 复用 aiHandleAttachmentFiles（配额/大小同口径）",
                dashboard.contains("aiHandleAttachmentFiles(files)"));
        check("推理中不接受拖拽", dashboard.contains("请等待本轮推理结束后再添加附件"));
        check("解析中不接受重复拖拽", dashboard.contains("正在解析已拖入的文件，请稍候"));

        // ---- 样式 ----
        check("ai-main 为定位容器", css.contains(".ai-main {")
                && css.contains("position: relative; }"));
        check("提示层绝对定位覆盖整区", css.contains(".ai-drop-overlay {")
                && css.contains("position: absolute; inset: 0;"));
        check("提示层不拦截拖拽事件", css.contains("pointer-events: none;"));
        check("hidden 时真正不占位", css.contains(".ai-drop-overlay[hidden] { display: none; }"));
        check("虚线落区卡片样式", css.contains("border: 2px dashed var(--accent)"));
        check("深色主题适配", css.contains(":root[data-theme=\"dark\"] .ai-drop-overlay")
                && css.contains(":root[data-theme=\"dark\"] .ai-drop-box"));

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

    private static int countOccurrences(String source, String snippet) {
        int n = 0;
        int from = 0;
        while ((from = source.indexOf(snippet, from)) >= 0) {
            n++;
            from += snippet.length();
        }
        return n;
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
