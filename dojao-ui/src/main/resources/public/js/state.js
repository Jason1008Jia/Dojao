// ==================== Shared global state ====================

/** 浅色主题配色镜像（与 css/style.css :root 变量同值，改色务必两处同步）。
 *  canvas（DAG 节点）与 JS 内联 style 不能用 CSS var()，统一走这里。 */
var UI_LIGHT = {
    page: '#f4f6fa', card: '#ffffff', raise: '#f8fafc', field: '#ffffff',
    line: '#e3e8ef', lineSoft: '#eef1f6', lineStrong: '#cdd5e0',
    text: '#1e2a3a', dim: '#55637a', faint: '#909db1',
    accent: '#2563eb', accentHover: '#1d4ed8', accentHi: '#1d4ed8',
    cyan: '#0e7490',
    ok: '#047857', warn: '#b45309', err: '#dc2626',
    // running：浅蓝底 + 中蓝进度条 + 深蓝字（进度条覆盖处仍可读）
    // 对应 css/style.css 的 --run-bg / --dag-run-overlay / --run-stroke / --dag-run-text
    runBg: '#e0ecff', runFill: '#93c5fd', runStroke: '#2563eb', runText: '#1e3a8a',
    doneFill: '#059669', doneStroke: '#047857',
    errFill: '#dc2626', neutral: '#64748b',
    warnFill: '#fde68a', warnStroke: '#d97706', warnText: '#78350f',
    failBg: '#fef2f2', failStroke: '#dc2626', failText: '#b91c1c',
    failBorder: 'rgba(220,38,38,0.28)'
};

/** 深色主题配色镜像（与 css/style.css 的 :root[data-theme="dark"] 同值，改色务必两处同步）。 */
var UI_DARK = {
    page: '#0b0f17', card: '#131926', raise: '#0f1420', field: '#1b2233',
    line: '#262f42', lineSoft: '#1d2534', lineStrong: '#38445c',
    text: '#e6ebf4', dim: '#a2adc0', faint: '#6b7688',
    accent: '#3b82f6', accentHover: '#60a5fa', accentHi: '#7dabff',
    cyan: '#22d3ee',
    ok: '#34d399', warn: '#fbbf24', err: '#f87171',
    // running：深蓝底 + 中蓝进度条 + 浅蓝字（覆盖处仍可读）
    runBg: '#1d2a44', runFill: '#3b82f6', runStroke: '#3b82f6', runText: '#dbeafe',
    doneFill: '#10b981', doneStroke: '#34d399',
    errFill: '#ef4444', neutral: '#64748b',
    warnFill: '#3a2e12', warnStroke: '#f59e0b', warnText: '#fde68a',
    failBg: '#3a1b1d', failStroke: '#ef4444', failText: '#fca5a5',
    failBorder: 'rgba(248,113,113,0.32)'
};

/** 当前生效的主题配色（切换主题时整体替换；所有读取点都是每次现取 UI.xxx，故重赋即生效）。 */
var UI = UI_LIGHT;

// ==================== 主题（浅色 / 深色）切换 ====================
//
// 机制：<html data-theme="light|dark"> 决定 CSS 变量分支；UI / CHART_BRAND 由本模块同步替换。
// 偏好存 localStorage('dojao.theme')，首屏在 <head> 内联脚本里先落一次 data-theme（防闪烁）。
// 约定：新增 canvas 上色一律走 UI.* 或 charts.js 的 CHART_BRAND，勿写死颜色。

var THEME_KEY = 'dojao.theme';

/** 当前主题（读 <html data-theme>，缺省 light）。 */
function currentTheme() {
    return document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
}

/** 把主题落到 <html> + 替换 UI / CHART_BRAND（不改 localStorage，不加副作用）。 */
function setThemePalette(dark) {
    document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light');
    UI = dark ? UI_DARK : UI_LIGHT;
    if (typeof applyChartTheme === 'function') applyChartTheme(dark);
}

/** 首屏初始化：按已存偏好应用主题（无存储 = 浅色）。由 app.js 调用一次。 */
function initTheme() {
    var stored = null;
    try { stored = localStorage.getItem(THEME_KEY); } catch (e) { /* 隐私模式等忽略 */ }
    var dark = stored === 'dark';
    setThemePalette(dark);
    syncThemeToggle(dark);
}

/** 用户点击切换：应用 + 落库 + 立即重绘依赖 canvas 的视图。 */
function toggleTheme() {
    var dark = currentTheme() !== 'dark';
    setThemePalette(dark);
    try { localStorage.setItem(THEME_KEY, dark ? 'dark' : 'light'); } catch (e) { /* ignore */ }
    syncThemeToggle(dark);
    redrawThemeDependent();
}

/** 同步顶栏切换按钮的提示文案（图标由 CSS 按 data-theme 互斥显示）。 */
function syncThemeToggle(dark) {
    var btn = document.getElementById('theme-toggle');
    if (!btn) return;
    var label = dark ? '切换到浅色模式' : '切换到深色模式';
    btn.setAttribute('title', label);
    btn.setAttribute('aria-label', label);
}

/**
 * 主题切换后立即重绘「颜色写死在 canvas / 内联 style」的视图：
 * 监控页迷你趋势线 + 折线图，执行 DAG 的节点/连线。详情页内联色靠下一拍轮询自愈，不在此重绘。
 */
function redrawThemeDependent() {
    if (typeof currentTab !== 'undefined' && currentTab === 'monitor') {
        if (typeof monitorData !== 'undefined' && monitorData && typeof renderMonitorCards === 'function') renderMonitorCards(monitorData);
        if (typeof monitorHistory !== 'undefined' && monitorHistory && typeof renderMonitorCharts === 'function') renderMonitorCharts(monitorHistory);
    }
    if (typeof applyDagTheme === 'function') applyDagTheme();
}

var network = null;            // vis.Network instance（当前 DAG）
var nodeProgress = {};         // { nodeId: { pct:0-100, status:'pending'|'running'|'done'|'failed' } }
var nodeLabelCache = {};       // nodeId -> 多行 label
var nodeDispPct = {};          // nodeId -> 缓动后的显示进度（轮询是跳变的，缓动后视觉上连续推进）
var dagAnimTimer = null;       // 进度缓动定时器（无节点在推进时自动停）
var DAG_SCALE = 1.0;           // 节点缩放（节点多时自动缩小）

// ---- Dashboard（监控）----
var jobList = [];              // GET /api/queries -> MonitorJob[]
var clusterData = null;        // GET /api/cluster -> MonitorCluster
var clusterTimer = null;       // 看板轮询定时器
var jobPerPage = 10;           // 任务列表每页条数（默认 10，可在「页面设置」调整，存 localStorage）
var jobPage = 0;               // 任务列表当前页（0-based；refreshAll 每 2s 重绘时保持不变）
var jobStateFilter = 'ALL';    // 任务列表状态筛选（ALL/RUNNING/SUBMITTED/FINISHED/FAILED/CANCELED，存 localStorage）

// ---- DAG 视图（当前打开的查询）----
var currentJobId = null;       // 正在 DAG 视图展示的 queryId
var liveSnapshot = null;       // 最近一次 MonitorQuery 快照
var liveOps = [];              // liveSnapshot.operators（按下标/idx 顺序）
var dagPollTimer = null;       // DAG 轮询定时器

// ---- SQL 页预览 ----
// 注意：下面这些是「当前显示中」那个控制台的镜像；真实状态存在每个控制台的
// preview 模型里（sql-console.js），轮询句柄存在 dashboard.js 的 sqlPreviewPolls。
var sqlQueryId = null;         // 当前结果预览的 queryId
var sqlPollTimer = null;       // 已废弃（轮询按控制台保存于 sqlPreviewPolls），保留以兼容旧引用
var sqlTickTimer = null;       // 「计算中」计时器（100ms 刷新耗时；仅当前显示的控制台需要）
var sqlStartMs = 0;            // 本次查询提交时刻（Date.now()）
var sqlElapsedMs = 0;          // 查询结束时的最终耗时
var sqlRunning = false;        // 是否处于「正在跑」状态（提交那一刻就置位，不等 POST 往返）
var sqlErrStreak = 0;          // 结果预览轮询连续失败次数
var lastSqlError = '';         // 最近一次查询报错（一行 cause 链，真因在链尾）
var lastSqlErrorDetail = '';   // 最近一次报错的完整多行堆栈（可折叠展开，「复制」「用 AI 修复」带上）
var lastSqlText = '';          // 最近一次真正提交执行的 SQL 原文（编辑器有选区时 = 选中的那段）
var sqlRanSelection = false;   // 本次提交是否只跑了选中片段（结果区提示用）
var aiInputGrow = null;        // AI 输入框自适应高度函数（切到 AI 页 / 预填内容后重算）
// 结果表格头排序（纯前端，只排「预览到的这些行」；每次新查询重置）
var sqlSortCol = -1;           // 当前排序列下标（-1 = 不排序，保持引擎返回顺序）
var sqlSortDir = 0;            // 0 取消 / 1 升序 / -1 降序
var sqlTransposed = false;     // 结果区「列转行」开关（在结果表上按 Tab 切换，纯前端展示）
var sqlLastResult = null;      // 最近一次 renderSqlTable 的结果快照（点表头就地重排，不等下一拍轮询）
