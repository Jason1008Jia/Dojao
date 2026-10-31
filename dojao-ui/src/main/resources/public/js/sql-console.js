// ==================== 首页「查询控制台」多标签 ====================
//
// 首页只有一个物理编辑器（#query-input）+ 一块结果区（#sql-result），所有绑定（高亮 /
// 补全 / 光标记忆 / 结果右键 / 分隔条）都挂在这一份 DOM 上。要在零重写这些绑定的前提下
// 支持多个「查询控制台」标签，这里采用「单 DOM 工作台 + 多控制台模型」：
//
//   - 每个控制台只是一份模型 {id, title, scriptId, text, savedText}，不持有任何 DOM；
//   - 切标签 = 把模型 text 回填进同一个 textarea（重置撤销栈 + 重画高亮 + 清空结果区）；
//   - 「脏」= text !== savedText（savedText 是上次保存 / 打开时的基线）。
//
// 标签与正文存 localStorage（键 dojao.sql.consoles），F5 后原样恢复；旧版单编辑器草稿
// （dojao.sql.draft，见 dashboard.js）迁移为第 1 个标签，不丢草稿。

var SQL_CONSOLES_KEY = 'dojao.sql.consoles';
var LEGACY_DRAFT_KEY = 'dojao.sql.draft';

var consoles = [];              // 控制台模型数组（至少 1 个）
var activeConsoleId = null;     // 当前激活控制台 id
var consoleSeq = 0;             // id 生成序号（仅保证本地唯一）
var _consolePersistTimer = null;

/** 当前激活控制台（找不到则退回首项）。 */
function activeConsole() {
    for (var i = 0; i < consoles.length; i++) {
        if (consoles[i].id === activeConsoleId) return consoles[i];
    }
    return consoles[0] || null;
}

/** 按 id 查控制台。 */
function findConsole(id) {
    for (var i = 0; i < consoles.length; i++) {
        if (consoles[i].id === id) return consoles[i];
    }
    return null;
}

/** 控制台是否「脏」（有未保存修改）。 */
function consoleDirty(c) {
    return !!c && c.text !== c.savedText;
}

/** 新 id（时间戳 + 序号，本地唯一即可）。 */
function newConsoleId() {
    consoleSeq++;
    return 'c' + consoleSeq + '-' + Date.now().toString(36);
}

/** 造一个控制台模型；savedText 缺省 = text（即「刚打开，未改动」）。 */
function makeConsole(opts) {
    opts = opts || {};
    var text = opts.text || '';
    return {
        id: opts.id || newConsoleId(),
        title: opts.title || '查询',
        scriptId: opts.scriptId || null,
        text: text,
        savedText: (opts.savedText != null) ? opts.savedText : text
    };
}

/** 「查询 N」标题：取现有查询名里最大的 N + 1（手动新建用）。 */
function nextConsoleTitle() {
    var max = 0;
    consoles.forEach(function (c) {
        var m = /^\s*查询\s*(\d+)\s*$/.exec(c.title || '');
        if (m) max = Math.max(max, parseInt(m[1], 10) || 0);
    });
    return '查询 ' + (max + 1);
}

// ==================== 初始化 ====================

/** 首页初始化时调一次（在 bindSqlEditor 之前）：装载模型 + 回填编辑器 + 绑标签栏交互。 */
function initSqlConsoles() {
    loadConsolesFromStorage();
    if (!consoles.length) {
        var draft = readLegacyDraft();
        consoles.push(makeConsole({ title: '查询 1', text: draft, savedText: draft }));
        removeLegacyDraft();
    }
    if (!activeConsoleId || !findConsole(activeConsoleId)) activeConsoleId = consoles[0].id;
    applyActiveConsoleToEditor();
    renderConsoleTabs();
    bindConsoleTabsUi();
    persistConsoles();
}

// ==================== 模型 ⇄ 编辑器 ====================

/**
 * 把当前控制台正文回填进编辑器。
 *
 * <p>优先走 {@link window.sqlEditorController#reset} —— 它会顺带把撤销栈重置为「当前值」单快照
 * 并重画高亮，且<b>不派发 input 事件</b>（避免把「切换」误记成一次编辑 / 弄脏新控制台）。
 * 控制器尚未建立（初始化早期）时兜底直接赋值。</p>
 */
function applyActiveConsoleToEditor() {
    var c = activeConsole();
    var text = c ? (c.text || '') : '';
    if (window.sqlEditorController && window.sqlEditorController.reset) {
        window.sqlEditorController.reset(text);
    } else {
        var input = document.getElementById('query-input');
        if (input) input.value = text;
        if (typeof renderSqlHighlight === 'function') renderSqlHighlight();
    }
}

/** 把编辑器里最新内容同步回当前控制台模型（切走 / 新建前调用，防漏记）。 */
function syncEditorToActiveConsole() {
    var c = activeConsole();
    if (!c) return;
    var input = document.getElementById('query-input');
    if (input) c.text = input.value || '';
}

/** 切标签后的共用收尾：清结果区 + 光标记忆 + 补全弹层。 */
function afterConsoleSwitch() {
    if (typeof resetSqlResultUi === 'function') resetSqlResultUi();
    sqlCaretMemory = null;                            // 拖表名不从上个控制台的落点续写
    if (typeof acHide === 'function') acHide();
}

/** 编辑器内容变化回调（由 dashboard.js 的 persist 钩子调用）。 */
function onConsoleTextChanged(v) {
    var c = activeConsole();
    if (!c || c.text === v) return;
    var wasDirty = consoleDirty(c);
    c.text = v;
    if (wasDirty !== consoleDirty(c)) renderConsoleTabs();   // 脏态翻转才重画标签
    persistConsoles();
}

// ==================== 标签栏 ====================

function renderConsoleTabs() {
    var bar = document.getElementById('sql-console-tabs');
    if (!bar) return;
    bar.innerHTML = consoles.map(function (c) {
        var cls = 'sql-console-tab'
            + (c.id === activeConsoleId ? ' active' : '')
            + (consoleDirty(c) ? ' dirty' : '');
        return '<div class="' + cls + '" data-console-id="' + esc(c.id) + '"'
            + ' title="' + esc(c.title) + '">'
            + '<span class="sql-console-tab-title">' + esc(c.title) + '</span>'
            + '<button type="button" class="sql-console-tab-close"'
            + ' data-close-console="' + esc(c.id) + '" title="关闭" aria-label="关闭">&times;</button>'
            + '</div>';
    }).join('');
}

/** 绑定标签栏交互（一次性）：点标签切换 / 点 × 关闭 / ＋ 新建 / 打开本地文件。 */
function bindConsoleTabsUi() {
    var bar = document.getElementById('sql-console-tabs');
    if (bar) {
        bar.addEventListener('click', function (e) {
            var close = e.target.closest ? e.target.closest('[data-close-console]') : null;
            if (close) {
                e.stopPropagation();
                closeQueryConsole(close.getAttribute('data-close-console'));
                return;
            }
            var tab = e.target.closest ? e.target.closest('[data-console-id]') : null;
            if (tab) setActiveConsole(tab.getAttribute('data-console-id'));
        });
    }
    var newBtn = document.getElementById('sql-new-console-btn');
    if (newBtn) newBtn.addEventListener('click', function () { newQueryConsole({}); });

    var openBtn = document.getElementById('sql-open-file-btn');
    var fileInput = document.getElementById('sql-file-input');
    if (openBtn && fileInput) {
        openBtn.addEventListener('click', function () {
            fileInput.value = '';    // 允许重复选同一个文件
            fileInput.click();
        });
        fileInput.addEventListener('change', handleSqlFileChosen);
    }
}

function setActiveConsole(id) {
    if (id === activeConsoleId || !findConsole(id)) return;
    syncEditorToActiveConsole();
    activeConsoleId = id;
    applyActiveConsoleToEditor();
    afterConsoleSwitch();
    renderConsoleTabs();
    persistConsoles();
    var input = document.getElementById('query-input');
    if (input) input.focus();
}

/**
 * 新建一个查询控制台并激活。
 *
 * @param {{title?:string, scriptId?:string, text?:string, savedText?:string}} opts
 * @return 新控制台模型
 */
function newQueryConsole(opts) {
    opts = opts || {};
    syncEditorToActiveConsole();
    var c = makeConsole({
        title: opts.title || nextConsoleTitle(),
        scriptId: opts.scriptId || null,
        text: opts.text || '',
        savedText: (opts.savedText != null) ? opts.savedText : (opts.text || '')
    });
    consoles.push(c);
    activeConsoleId = c.id;
    applyActiveConsoleToEditor();
    afterConsoleSwitch();
    renderConsoleTabs();
    persistConsoles();
    var input = document.getElementById('query-input');
    if (input) input.focus();
    return c;
}

/** 关闭控制台：有未保存修改先弹三键 dialog（保存 / 不保存 / 取消）。 */
function closeQueryConsole(id) {
    var c = findConsole(id);
    if (!c) return;
    if (consoleDirty(c)) { showCloseConsoleDialog(c); return; }
    doCloseConsole(id);
}

function doCloseConsole(id) {
    var i = -1;
    for (var k = 0; k < consoles.length; k++) {
        if (consoles[k].id === id) { i = k; break; }
    }
    if (i < 0) return;
    var wasActive = (activeConsoleId === id);
    consoles.splice(i, 1);
    if (!consoles.length) {   // 关到 0 → 补一个空标签，保证始终有编辑器可用
        consoles.push(makeConsole({ title: '查询 1', text: '', savedText: '' }));
    }
    if (wasActive) {
        var j = Math.min(i, consoles.length - 1);   // 优先激活右边的邻居，否则左边的
        activeConsoleId = consoles[j].id;
        applyActiveConsoleToEditor();
        afterConsoleSwitch();
    }
    renderConsoleTabs();
    persistConsoles();
}

function showCloseConsoleDialog(c) {
    var overlay = mgModal('关闭控制台',
        '<div class="mg-note">「' + esc(c.title) + '」有未保存的修改，是否保存？</div>'
        + '<div class="mg-actions">'
        + '<button type="button" class="mg-btn" data-act="cancel">取消</button>'
        + '<button type="button" class="mg-btn" data-act="discard">不保存</button>'
        + '<button type="button" class="mg-btn primary" data-act="save">保存</button>'
        + '</div>');
    overlay.querySelector('[data-act="cancel"]').onclick = mgClose;
    overlay.querySelector('[data-act="discard"]').onclick = function () {
        mgClose();
        doCloseConsole(c.id);
    };
    overlay.querySelector('[data-act="save"]').onclick = function () {
        // 保存成功后再关；保存失败 / 取消命名则留在原地，不误关
        saveConsoleToScript(c, function () { doCloseConsole(c.id); });
    };
}

// ==================== 保存 / 打开文件 ====================

/**
 * 把某个控制台保存为命名脚本（当前登录用户私有，同名覆盖）。保存后对齐 title / savedText。
 *
 * @param {object} c       目标控制台（缺省 = 当前激活）
 * @param {Function} [onDone] 保存成功后的回调（关闭标签用）
 */
function saveConsoleToScript(c, onDone) {
    if (!c) c = activeConsole();
    if (!c) return;
    var content = c.text || '';
    if (!content.trim()) { showToast('控制台为空，没有可保存的 SQL'); return; }
    promptNameModal('保存 SQL 脚本', '为当前控制台的 SQL 起个名字（同名脚本将覆盖）。',
        c.scriptId ? c.title : '',
        function (name) {
            httpPost('/api/scripts', { name: name, content: content })
                .then(function (r) {
                    c.title = name;
                    c.savedText = content;
                    if (r && r.id) c.scriptId = r.id;
                    renderConsoleTabs();
                    persistConsoles();
                    showToast('已保存脚本：' + name);
                    if (typeof onDone === 'function') onDone();
                })
                .catch(function (err) {
                    showToast('保存失败：' + oneLine(err && err.message ? err.message : err, 80));
                });
        });
}

/** 「打开文件」→ FileReader 读入本地 .sql/.txt → 新开一个标签（不覆盖当前）。 */
function handleSqlFileChosen(e) {
    var file = (e && e.target && e.target.files) ? e.target.files[0] : null;
    if (!file) return;
    var reader = new FileReader();
    reader.onload = function () {
        var text = (typeof reader.result === 'string') ? reader.result : '';
        newQueryConsole({ title: file.name, text: text, savedText: text });
        showToast('已打开文件：' + file.name);
    };
    reader.onerror = function () { showToast('读取文件失败：' + file.name); };
    reader.readAsText(file, 'UTF-8');
}

// ==================== 持久化（localStorage） ====================

function loadConsolesFromStorage() {
    var raw = null;
    try { raw = window.localStorage.getItem(SQL_CONSOLES_KEY); } catch (e) { return; }
    if (!raw) return;
    var data = null;
    try { data = JSON.parse(raw); } catch (e) { data = null; }
    if (!data || !data.consoles || !data.consoles.length) return;
    consoles = data.consoles.map(function (c) {
        return makeConsole({
            id: c.id, title: c.title, scriptId: c.scriptId,
            text: c.text, savedText: c.savedText
        });
    });
    activeConsoleId = data.active || consoles[0].id;
    if (!findConsole(activeConsoleId)) activeConsoleId = consoles[0].id;
    consoleSeq = consoles.length;
}

/** 防抖落库：连续键入只写最后一次；页面卸载前强制冲刷（见 bindConsolePersistFlush）。 */
function persistConsoles() {
    if (_consolePersistTimer) clearTimeout(_consolePersistTimer);
    _consolePersistTimer = setTimeout(writeConsolesNow, 250);
}

function writeConsolesNow() {
    _consolePersistTimer = null;
    try {
        window.localStorage.setItem(SQL_CONSOLES_KEY, JSON.stringify({
            active: activeConsoleId,
            consoles: consoles
        }));
    } catch (e) { /* 隐私模式 / 配额满：静默退化成「不记忆」 */ }
}

// 卸载前把防抖尾巴冲刷掉，避免最后几个字符丢失。
window.addEventListener('pagehide', writeConsolesNow);
window.addEventListener('beforeunload', writeConsolesNow);

function readLegacyDraft() {
    try {
        var v = window.localStorage.getItem(LEGACY_DRAFT_KEY);
        return v || '';
    } catch (e) { return ''; }
}

function removeLegacyDraft() {
    try { window.localStorage.removeItem(LEGACY_DRAFT_KEY); } catch (e) { /* ignore */ }
}
