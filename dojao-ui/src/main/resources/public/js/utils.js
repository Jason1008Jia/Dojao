// ==================== Utility Functions ====================

function esc(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

/** 中间省略：保留前后各 n 字符 */
function midStr(s, n) {
    s = String(s == null ? '' : s);
    if (s.length <= n * 2 + 1) return s;
    return s.slice(0, n) + '\u2026' + s.slice(s.length - n);
}

/** 单行压缩到 maxLen（含省略号） */
function oneLine(s, maxLen) {
    s = String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
    if (s.length <= maxLen) return s;
    return s.slice(0, Math.max(1, maxLen - 1)) + '\u2026';
}

// ==================== HTTP（监控 REST）====================

function httpJson(method, path, body) {
    var opts = {
        method: method,
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin'   // 带上会话 cookie
    };
    if (body) opts.body = JSON.stringify(body);
    return fetch(path, opts).then(function (r) {
        if (r.ok) return r.json().catch(function () { return {}; });
        return r.json().catch(function () { return {}; }).then(function (j) {
            var msg = (j && j.error) ? j.error : ('HTTP ' + r.status);
            // 会话失效 → 整页跳登录页。登录接口自己的 401 除外：那里要把
            // 「用户名或口令不正确」显示在表单上，而不是把页面刷走。
            if (r.status === 401 && path.indexOf('/api/auth/login') !== 0) {
                window.__dojaoAllowUnload = true;   // 会话失效的自动跳转：不要触发「请勿刷新」拦截
                window.location.href = '/login.html';
            }
            var e = new Error(msg);
            e.status = r.status;
            throw e;
        });
    });
}
var httpGet = function (p) { return httpJson('GET', p); };
var httpPost = function (p, b) { return httpJson('POST', p, b); };

// ==================== 格式化 ====================

function fmtRows(n) {
    n = Number(n) || 0;
    if (n >= 100000000) return (n / 100000000).toFixed(1) + '\u4ebf';
    if (n >= 10000) return (n / 10000).toFixed(1) + 'W';
    return n.toLocaleString();
}

function fmtRate(perSec) {
    var n = Number(perSec) || 0;
    if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M/s';
    if (n >= 1000) return (n / 1000).toFixed(1) + 'K/s';
    return n + '/s';
}

function fmtBytes(b) {
    b = Number(b) || 0;
    if (b >= 1073741824) return (b / 1073741824).toFixed(1) + ' GB';
    if (b >= 1048576) return (b / 1048576).toFixed(1) + ' MB';
    if (b >= 1024) return (b / 1024).toFixed(1) + ' KB';
    return b + ' B';
}

/** 字节速率：沿用 fmtBytes 的 KB/MB/GB 阶梯，后缀 /s。 */
function fmtBytesRate(perSec) {
    return fmtBytes(Number(perSec) || 0) + '/s';
}

function fmtSec(s) {
    s = Number(s) || 0;
    if (s < 60) return s.toFixed(0) + 's';
    var m = Math.floor(s / 60), sec = Math.floor(s % 60);
    return m + 'm' + sec + 's';
}

/**
 * 耗时（毫秒）→ 分级可读文本，梯度随量级切换单位：
 * <pre>
 *   &le;1000ms   → 847ms            （原样毫秒）
 * 1s ~ 60s      → 20.3s            （保留 1 位小数）
 * 1min ~ 1h     → 5min20s          （整数秒）
 * &ge;1h         → 1h20min30s       （h/min/s 三段，各段补零占位）
 * </pre>
 * 每级内部各段固定显示（如 5min0s / 1h0min30s），让实时计时器文本宽度稳定、不跳变。
 */
function fmtDuration(ms) {
    ms = Math.floor(Number(ms) || 0);
    if (ms < 0) ms = 0;
    if (ms < 1000) return ms + 'ms';
    var s = ms / 1000;
    if (s < 60) return s.toFixed(1) + 's';
    var m = Math.floor(s / 60), sec = Math.floor(s % 60);
    if (s < 3600) return m + 'min' + sec + 's';
    var h = Math.floor(m / 60), min = m % 60;
    return h + 'h' + min + 'min' + sec + 's';
}

function fmtGb(bytes) {
    return ((Number(bytes) || 0) / 1073741824).toFixed(0) + 'GB';
}

/** queryId 前缀 yyyyMMddHHmmss → 本地时间串（无则空）。 */
function qidTime(id) {
    var m = /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/.exec(id || '');
    if (!m) return '';
    return new Date(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]).toLocaleTimeString();
}

// ==================== 状态 -> 徽标/颜色 ====================

/** 查询/算子状态 → badge class（配色见 css）。 */
function stCls(state) {
    switch (state) {
        case 'RUNNING': return 'running';
        case 'SUBMITTED': return 'queued';
        case 'FINISHED': return 'completed';
        case 'FAILED': return 'failed';
        case 'CANCELED': return 'canceled';
        default: return 'failed';
    }
}

/** 算子状态 → DAG 节点 status（progressNodeRenderer 用）。 */
function stNode(state) {
    switch (state) {
        case 'FINISHED': return 'done';
        case 'FAILED': return 'failed';
        case 'RUNNING': return 'running';
        default: return 'pending';   // SUBMITTED / NOT_STARTED / 未知
    }
}

/** 算子类型 → 友好短名（DAG 节点第一行）。 */
function opShortName(type) {
    var t = String(type || '');
    var lt = t.toLowerCase();
    if (lt.indexOf('aggregate') >= 0 && lt.indexOf('window') >= 0) return 'WindowAgg';
    if (lt.indexOf('window') >= 0 && lt.indexOf('join') >= 0) return 'JoinWindow';
    if (lt.indexOf('join') >= 0 && lt.indexOf('aggregate') >= 0) return 'JoinAgg';
    if (lt.indexOf('chained') >= 0) return 'Chain';
    if (lt.indexOf('partition') >= 0 || lt.indexOf('shuffle') >= 0) return 'Shuffle';
    if (lt.indexOf('scan') >= 0) return 'Scan';
    if (lt.indexOf('join') >= 0) return 'Join';
    if (lt.indexOf('window') >= 0) return 'Window';
    if (lt.indexOf('aggregate') >= 0 || lt.indexOf('group') >= 0) return 'Agg';
    if (lt.indexOf('project') >= 0) return 'Project';
    if (lt.indexOf('filter') >= 0) return 'Filter';
    if (lt.indexOf('sort') >= 0) return 'Sort';
    if (lt.indexOf('limit') >= 0) return 'Limit';
    if (lt.indexOf('union') >= 0) return 'Union';
    if (lt.indexOf('write') >= 0 || lt.indexOf('insert') >= 0) return 'Write';
    if (lt.indexOf('overwrite') >= 0) return 'Overwrite';
    if (lt.indexOf('ctas') >= 0) return 'CTAS';
    if (lt.indexOf('cross') >= 0) return 'Cross';
    return t.length > 16 ? t.slice(0, 15) + '\u2026' : t;
}

// ==================== 右键菜单 ====================

/** 右键菜单单例（全站共用一个 DOM，避免每个节点各建一份）。 */
var ctxMenuEl = null;
var ctxMenuBound = false;

/**
 * 在鼠标位置弹出右键菜单。
 *
 * @param {Array<{label:string, onClick:Function, disabled:boolean=, danger:boolean=, sep:boolean=}>} items
 *        菜单项；{@code sep:true} 的项渲染成分隔线（label/onClick 忽略）
 * @param {number} x 视口坐标（clientX）
 * @param {number} y 视口坐标（clientY）
 */
function showContextMenu(items, x, y) {
    var el = ensureContextMenu();
    el.innerHTML = '';
    (items || []).forEach(function (it) {
        if (it.sep) {
            var hr = document.createElement('div');
            hr.className = 'ctx-sep';
            el.appendChild(hr);
            return;
        }
        var row = document.createElement('div');
        row.className = 'ctx-item' + (it.danger ? ' ctx-danger' : '')
            + (it.disabled ? ' ctx-disabled' : '');
        row.textContent = it.label;
        if (!it.disabled) {
            row.onclick = function (e) {
                e.stopPropagation();
                hideContextMenu();
                it.onClick();
            };
        }
        el.appendChild(row);
    });
    el.classList.remove('hidden');

    // 贴边翻转：默认在光标右下，越界则往左上收，保证整块菜单在视口内
    var mw = el.offsetWidth, mh = el.offsetHeight;
    var vw = document.documentElement.clientWidth, vh = document.documentElement.clientHeight;
    var left = (x + mw + 4 > vw) ? Math.max(4, x - mw) : x;
    var top = (y + mh + 4 > vh) ? Math.max(4, y - mh) : y;
    el.style.left = left + 'px';
    el.style.top = top + 'px';
}

/** 收起右键菜单（幂等）。 */
function hideContextMenu() {
    if (ctxMenuEl) ctxMenuEl.classList.add('hidden');
}

/** 懒建菜单 DOM 并一次性绑好全局收起监听（点别处 / Esc / 滚动 / 改窗口大小）。 */
function ensureContextMenu() {
    if (ctxMenuEl) return ctxMenuEl;
    ctxMenuEl = document.createElement('div');
    ctxMenuEl.className = 'ctx-menu hidden';
    document.body.appendChild(ctxMenuEl);
    if (!ctxMenuBound) {
        ctxMenuBound = true;
        // 捕获阶段：先把菜单收起来，再让点击落到目标元素上，避免菜单自己被点中
        document.addEventListener('mousedown', function (e) {
            if (ctxMenuEl && !ctxMenuEl.classList.contains('hidden')
                    && !ctxMenuEl.contains(e.target)) {
                hideContextMenu();
            }
        }, true);
        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') hideContextMenu();
        });
        window.addEventListener('resize', hideContextMenu);
        // capture=true：滚动的可能是内部容器（结果区 / 目录树），scroll 不冒泡到 window
        window.addEventListener('scroll', hideContextMenu, true);
        window.addEventListener('blur', hideContextMenu);
    }
    return ctxMenuEl;
}

// ==================== HTML 组件 ====================

/** Rounded rectangle canvas path */
function roundRectPath(ctx, x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + w - r, y);
    ctx.arcTo(x + w, y, x + w, y + r, r);
    ctx.lineTo(x + w, y + h - r);
    ctx.arcTo(x + w, y + h, x + w - r, y + h, r);
    ctx.lineTo(x + r, y + h);
    ctx.arcTo(x, y + h, x, y + h - r, r);
    ctx.lineTo(x, y + r);
    ctx.arcTo(x, y, x + r, y, r);
    ctx.closePath();
}

/** Build metric card HTML */
function card(label, value, sub, cls, rightValue) {
    var vHtml = rightValue
        ? '<div class="value" style="display:flex;justify-content:space-between;align-items:baseline"><span>' + value + '</span><span class="value" style="white-space:nowrap;margin-left:8px">' + rightValue + '</span></div>'
        : '<div class="value">' + value + '</div>';
    return '<div class="metric-card ' + (cls || '') + '"><div class="label">' + label + '</div>' + vHtml + (sub ? '<div class="sub">' + sub + '</div>' : '') + '</div>';
}

/** Build metric row HTML */
function metricRow(label, value, cls) {
    return '<div class="task-metric-row">'
        + '<span class="task-metric-label">' + label + '</span>'
        + '<span class="task-metric-value ' + (cls || '') + '" style="max-width:60%;text-align:right;word-break:break-all">' + value + '</span>'
        + '</div>';
}

// ==================== 复制 / 轻提示 / 下载 ====================

/**
 * 统一的「复制到剪贴板」：优先 Clipboard API（需要安全上下文，http://localhost 算），
 * 失败或不可用时退回 execCommand（见 {@link fallbackCopy}）。
 *
 * @param {string} text   要复制的文本
 * @param {Function=} onDone 回调，参数为是否成功 —— 调用方据此给按钮 / 浮层反馈
 */
function copyText(text, onDone) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text)
            .then(function () { if (onDone) onDone(true); },
                  function () { if (onDone) onDone(!!fallbackCopy(text)); });
    } else {
        if (onDone) onDone(!!fallbackCopy(text));
    }
}

/** 轻提示定时器（连续调用时只保留最后一次）。 */
var appToastTimer = null;

/**
 * 右下角轻提示：复制 / 导出这类「一瞬间就完事」的操作需要一个反馈，
 * 但没有地方挂按钮文案（右键菜单点完就没了），统一走这个浮层。
 */
function showToast(msg) {
    var el = document.getElementById('app-toast');
    if (!el) {
        el = document.createElement('div');
        el.id = 'app-toast';
        el.className = 'app-toast';
        document.body.appendChild(el);
    }
    el.textContent = msg;
    // 强制重排后再加 show：连续两次提示时也要重新播一次进入动画
    void el.offsetWidth;
    el.classList.add('show');
    if (appToastTimer) clearTimeout(appToastTimer);
    appToastTimer = setTimeout(function () {
        el.classList.remove('show');
        appToastTimer = null;
    }, 1800);
}

/** 触发浏览器下载（Blob → 临时 a[download] → 立刻回收 URL）。 */
function downloadFile(filename, blob) {
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    // 立刻 revoke 会让部分浏览器的下载中断，延后一拍回收
    setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
}
