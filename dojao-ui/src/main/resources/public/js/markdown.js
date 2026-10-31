/**
 * 轻量 Markdown → HTML 渲染器（无外部依赖，离线可用）。
 *
 * <p>用途：把 AI 流式回答的正文按 Markdown 渲染进对话区 —— 标题 / 列表 / 表格 / 代码块 /
 * 引用 / 行内样式。风格对齐仓库既有 vanilla-JS 工具（如 sql-highlight.js），不引 CDN。</p>
 *
 * <p><b>安全</b>：先对整段源文本做 HTML 实体转义（{@code & < >}），再做 Markdown 变换，
 * 因此模型输出里即使含 {@code <script>} 也只会变成可见文本，<b>不可能注入标签</b>。
 * 链接仅放行 http(s) / 相对 / 锚点，挡掉 {@code javascript:} 等伪协议。</p>
 *
 * <p>用法：{@code renderMarkdown('# 标题\n- 项')} → HTML 字符串；调用方用 innerHTML 写入。</p>
 */
(function () {
    'use strict';

    function esc(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    function escAttr(s) {
        return esc(s).replace(/"/g, '&quot;');
    }

    /** 行内样式：代码 / 链接 / 加粗 / 斜体 / 删除线。输入已整体转义。 */
    function inline(s) {
        // 先抽出行内代码（占位符），避免其内容被后续规则二次变换。
        var codes = [];
        s = s.replace(/`([^`]+)`/g, function (m, c) {
            codes.push(c);
            return '\u0000' + (codes.length - 1) + '\u0000';
        });
        // 链接：仅 http(s) / 相对路径 / 锚点。
        s = s.replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, function (m, t, u) {
            if (!/^(https?:\/\/|\/|#|\.)/i.test(u)) return m;
            return '<a href="' + escAttr(u) + '" target="_blank" rel="noopener noreferrer">' + t + '</a>';
        });
        s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
        s = s.replace(/__([^_]+)__/g, '<strong>$1</strong>');
        s = s.replace(/~~([^~]+)~~/g, '<del>$1</del>');
        s = s.replace(/(^|[^*])\*([^*\s][^*]*)\*/g, '$1<em>$2</em>');
        s = s.replace(/(^|[^\w])_([^_\s][^_]*)_(?![A-Za-z0-9_])/g, '$1<em>$2</em>');
        // 还原行内代码（内容已转义，直接包 <code>）。
        s = s.replace(/\u0000(\d+)\u0000/g, function (m, i) {
            return '<code>' + codes[+i] + '</code>';
        });
        return s;
    }

    function splitRow(row) {
        return row.trim().replace(/^\|/, '').replace(/\|$/, '').split('|').map(function (c) {
            return c.trim();
        });
    }

    function alignAttr(a) {
        return a ? ' style="text-align:' + a + '"' : '';
    }

    function parseTable(lines, start) {
        var header = splitRow(lines[start]);
        var align = splitRow(lines[start + 1]).map(function (c) {
            if (!/^:?-+:?$/.test(c)) return '';
            var l = c.charAt(0) === ':';
            var r = c.charAt(c.length - 1) === ':';
            return l && r ? 'center' : (r ? 'right' : (l ? 'left' : ''));
        });
        var rows = [];
        var i = start + 2;
        while (i < lines.length && lines[i].indexOf('|') >= 0 && !/^\s*$/.test(lines[i])) {
            rows.push(splitRow(lines[i]));
            i++;
        }
        var html = '<table class="md-table"><thead><tr>';
        header.forEach(function (c, ci) {
            html += '<th' + alignAttr(align[ci]) + '>' + inline(c) + '</th>';
        });
        html += '</tr></thead><tbody>';
        rows.forEach(function (r) {
            html += '<tr>';
            header.forEach(function (_, ci) {
                html += '<td' + alignAttr(align[ci]) + '>' + inline(r[ci] == null ? '' : r[ci]) + '</td>';
            });
            html += '</tr>';
        });
        html += '</tbody></table>';
        return { html: html, next: i };
    }

    /** 某行是否是块级起始（段落收集时的终止条件）。 */
    function isBlockStart(line) {
        return /^\s*(```|~~~)/.test(line)
            || /^#{1,6}\s+/.test(line)
            || /^\s*&gt;\s?/.test(line)
            || /^\s*[-*+]\s+/.test(line)
            || /^\s*\d+[.)]\s+/.test(line)
            || /^\s*([-*_])\s*\1\s*\1/.test(line);
    }

    function render(src) {
        if (src == null) return '';
        var text = esc(String(src).replace(/\r\n?/g, '\n'));
        var lines = text.split('\n');
        var html = [];
        var list = null;   // 'ul' | 'ol'
        var i = 0;
        var n = lines.length;

        function closeList() {
            if (list) { html.push('</' + list + '>'); list = null; }
        }

        while (i < n) {
            var line = lines[i];
            var fence = line.match(/^\s*(```|~~~)/);
            if (fence) {
                closeList();
                var lang = line.replace(/^\s*(```|~~~)/, '').trim();
                var buf = [];
                i++;
                while (i < n && !/^\s*(```|~~~)\s*$/.test(lines[i])) { buf.push(lines[i]); i++; }
                i++; // 跳过收尾围栏
                html.push('<pre class="md-pre"' + (lang ? ' data-lang="' + escAttr(lang) + '"' : '')
                        + '><code>' + buf.join('\n') + '</code></pre>');
                continue;
            }
            var h = line.match(/^(#{1,6})\s+(.*)$/);
            if (h) {
                closeList();
                var lv = h[1].length;
                html.push('<h' + lv + '>' + inline(h[2].trim()) + '</h' + lv + '>');
                i++;
                continue;
            }
            if (/^\s*([-*_])\s*\1\s*\1[\s\-*_]*$/.test(line)) {
                closeList();
                html.push('<hr>');
                i++;
                continue;
            }
            if (/^\s*&gt;\s?/.test(line)) {
                closeList();
                var q = [];
                while (i < n && /^\s*&gt;\s?/.test(lines[i])) {
                    q.push(lines[i].replace(/^\s*&gt;\s?/, ''));
                    i++;
                }
                html.push('<blockquote>' + inline(q.join('<br>')) + '</blockquote>');
                continue;
            }
            if (line.indexOf('|') >= 0 && i + 1 < n
                    && lines[i + 1].indexOf('|') >= 0 && /^\s*\|?\s*:?-+:?/.test(lines[i + 1])) {
                closeList();
                var t = parseTable(lines, i);
                html.push(t.html);
                i = t.next;
                continue;
            }
            var ul = line.match(/^\s*[-*+]\s+(.*)$/);
            var ol = line.match(/^\s*\d+[.)]\s+(.*)$/);
            if (ul || ol) {
                var type = ul ? 'ul' : 'ol';
                if (list !== type) { closeList(); html.push('<' + type + '>'); list = type; }
                html.push('<li>' + inline((ul ? ul[1] : ol[1]).trim()) + '</li>');
                i++;
                continue;
            }
            if (/^\s*$/.test(line)) { closeList(); i++; continue; }
            // 段落：连续收集到空行或下一个块级起始
            closeList();
            var para = [line];
            i++;
            while (i < n && !/^\s*$/.test(lines[i]) && !isBlockStart(lines[i])
                    && lines[i].indexOf('|') < 0) {
                para.push(lines[i]);
                i++;
            }
            html.push('<p>' + inline(para.join('<br>')) + '</p>');
        }
        closeList();
        return html.join('\n');
    }

    window.renderMarkdown = render;
})();
