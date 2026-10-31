// ==================== 执行 DAG 实时轮询（真实快照）====================

function stopDagPoll() {
    if (dagPollTimer) { clearInterval(dagPollTimer); dagPollTimer = null; }
}

/** DAG 轮询序号：请求会并发在途（300ms 一拍），必须丢弃迟到 / 乱序的旧快照——
 *  旧响应晚到会把结构键来回改、整图反复重建，看起来就是一直闪。 */
var dagPollSeq = 0;
var dagAppliedSeq = 0;
var dagPollSession = 0;

/** 打开某个 queryId 的 DAG 视图并开始轮询。 */
function startDagPoll(queryId) {
    stopDagPoll();
    dagPollSession++;
    dagPollSeq = 0;
    dagAppliedSeq = 0;
    currentJobId = queryId;
    pollDagOnce();
    dagPollTimer = setInterval(pollDagOnce, 300);
}

/** 轮询一次：拉 MonitorQuery 快照 → 建/刷 DAG → 刷顶栏与详情。 */
function pollDagOnce() {
    if (!currentJobId) return;
    var qid = currentJobId;
    var session = dagPollSession;
    var seq = ++dagPollSeq;
    return httpGet('/api/queries/' + encodeURIComponent(qid))
        .then(function (snap) {
            // 已切到别的查询 / 重开过 DAG / 这是比已应用快照更旧的响应 → 丢弃
            if (qid !== currentJobId || session !== dagPollSession || seq < dagAppliedSeq) return;
            dagAppliedSeq = seq;
            liveSnapshot = snap;
            liveOps = (snap && snap.operators) || [];
            updateDagTopbar(snap);
            // 运行中偶发空算子快照（快照瞬态 / 查询刚被清理）→ 保留上一帧，别拆掉整图再重画
            if (!liveOps.length && network) {
                refreshLiveOperator();
                refreshTaskDetail();
                return;
            }
            // 结构变化（如分布式任务阶段在运行中才陆续展开）→ 整图重建；否则原地刷新状态
            var key = dagStructureKey(liveOps);
            if (network && key === window._dagKey) {
                refreshDag(snap);
            } else {
                renderDag(snap);
            }
            refreshLiveOperator();   // 若处于算子详情页则刷新
            refreshTaskDetail();     // 若处于任务详情页则刷新
        })
        .catch(function (err) {
            if (qid !== currentJobId || session !== dagPollSession) return;
            // 快照暂不可用（如查询刚被清理）→ 仅顶栏给提示，不打断轮询
            if (!network && (!liveOps || !liveOps.length)) {
                showDagEmpty('无法获取查询快照：' + oneLine(err && err.message ? err.message : err, 60));
            }
        });
}

/** 刷新 DAG 顶栏：状态徽标 + SQL 标题。 */
function updateDagTopbar(snap) {
    if (!snap) return;
    var badge = document.getElementById('dag-status-badge');
    if (badge) {
        badge.textContent = snap.state || '?';
        badge.className = 'status-badge ' + stCls(snap.state);
    }
    var title = document.getElementById('dag-query-text');
    if (title) {
        title.textContent = oneLine(snap.sql || '', 120);
        title.style.color = snap.state === 'FINISHED' ? UI.ok
            : snap.state === 'FAILED' ? UI.err : UI.text;
    }
    var tooltip = document.getElementById('sql-tooltip-body');
    // 仅在内容变化时改写：避免每 300ms 重建文本节点打断用户正在进行的选中
    if (tooltip && tooltip.textContent !== (snap.sql || '')) tooltip.textContent = snap.sql || '';
}

// ==================== SQL 提示框：hover 保持 + 一键复制 ====================

var sqlTooltipHideTimer = null;

/** 显示 SQL 提示框（hover 截断标题或框体本身都调用），并取消待执行的隐藏。 */
function showSqlTooltip() {
    if (sqlTooltipHideTimer) { clearTimeout(sqlTooltipHideTimer); sqlTooltipHideTimer = null; }
    var t = document.getElementById('sql-tooltip');
    if (t) t.style.display = 'block';
}

/**
 * 延迟隐藏：鼠标从截断标题移向框体时会短暂离开两者，延时留出移动路径；
 * 一旦移入框体（mouseenter）即取消。只有真正停在框体之外才消失。
 */
function scheduleHideSqlTooltip() {
    if (sqlTooltipHideTimer) clearTimeout(sqlTooltipHideTimer);
    sqlTooltipHideTimer = setTimeout(function () {
        sqlTooltipHideTimer = null;
        var t = document.getElementById('sql-tooltip');
        if (t) t.style.display = 'none';
    }, 260);
}

/** 立即隐藏提示框（切换视图等场景调用，避免残留）。 */
function hideSqlTooltip() {
    if (sqlTooltipHideTimer) { clearTimeout(sqlTooltipHideTimer); sqlTooltipHideTimer = null; }
    var t = document.getElementById('sql-tooltip');
    if (t) t.style.display = 'none';
}

/** 复制提示框内的完整 SQL；按钮短暂反馈结果。 */
function copySqlTooltip(ev) {
    if (ev) ev.stopPropagation();
    var body = document.getElementById('sql-tooltip-body');
    var btn = document.getElementById('sql-tooltip-copy');
    var text = body ? body.textContent : '';
    var feedback = function (okFlag) {
        if (!btn) return;
        btn.textContent = okFlag ? '已复制' : '复制失败';
        btn.classList.toggle('copied', okFlag);
        setTimeout(function () {
            btn.textContent = '复制全部';
            btn.classList.remove('copied');
        }, 1200);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
        // http://localhost 属安全上下文；非安全上下文（远端 IP 访问）走兜底
        navigator.clipboard.writeText(text)
            .then(function () { feedback(true); }, function () { feedback(fallbackCopy(text)); });
    } else {
        feedback(fallbackCopy(text));
    }
}

/** 剪贴板兜底（非安全上下文 / 旧浏览器）：临时 textarea + execCommand。 */
function fallbackCopy(text) {
    var ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.top = '-1000px';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    var okFlag = false;
    try { okFlag = document.execCommand('copy'); } catch (e) { okFlag = false; }
    document.body.removeChild(ta);
    return okFlag;
}
