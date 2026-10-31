// ==================== Detail Pages（真实快照驱动）====================

function liveOpForIdx(idx) {
    if (!liveOps || !liveOps.length) return null;
    for (var i = 0; i < liveOps.length; i++) {
        if (liveOps[i].idx === idx) return liveOps[i];
    }
    return null;
}

function stateTextColor(state) {
    return state === 'FINISHED' ? UI.ok
        : state === 'FAILED' ? UI.err
        : state === 'RUNNING' ? UI.accentHi
        : UI.dim;
}

/** 状态 → 实心 pill 背景（白字）。 */
function statePillBg(state) {
    return state === 'FINISHED' ? UI.doneFill
        : state === 'FAILED' ? UI.errFill
        : state === 'RUNNING' ? UI.runFill
        : UI.neutral;
}

// ==================== 算子详情 ====================

/** 双击 DAG 节点进入（node.idx）。 */
function openOperatorDetail(idx) {
    hideTaskDetail();
    window._detailIdx = idx;
    document.getElementById('dag').style.display = 'none';
    document.getElementById('detail-page').classList.add('show');
    document.getElementById('dag-topbar').style.display = 'none';
    refreshLiveOperator();
}

/** 只在文本真的变化时写 DOM（避免每拍无意义的节点替换）。 */
function setTextIfChanged(el, text) {
    if (el && el.textContent !== text) el.textContent = text;
}

/** 算子详情指标卡：带 data-metric 键，轮询时可原地刷新数值而不重建 DOM。 */
function opCard(key, label, value, sub, cls) {
    return '<div class="metric-card ' + (cls || '') + '" data-metric="' + key + '">'
        + '<div class="label">' + label + '</div>'
        + '<div class="value">' + value + '</div>'
        + (sub ? '<div class="sub">' + sub + '</div>' : '')
        + '</div>';
}

/** 原地写某张指标卡的数值 / 副标题。 */
function setMetric(scope, key, value, sub) {
    var card = scope.querySelector('[data-metric="' + key + '"]');
    if (!card) return;
    setTextIfChanged(card.querySelector('.value'), value);
    if (sub !== undefined) setTextIfChanged(card.querySelector('.sub'), sub);
}

/**
 * 轮询刷新算子详情（若处于详情页）。
 *
 * <p><b>增量刷新</b>：DAG 每 300ms 调一次本函数。旧实现对 {@code #detail-content} 整体
 * {@code innerHTML} 重建 —— 运行中任务行里的 {@code .task-dot2.run} 会因此每 300ms 重启一次
 * CSS {@code pulse} 动画（视觉上一直闪），鼠标悬停行 / 指标卡的过渡也会被反复打断（视觉上一直抖），
 * 滚动位置与文本选中同样会被清掉。现在按「结构签名」判定：算子 / 任务集合不变时只原地改数值。</p>
 */
function refreshLiveOperator() {
    var idx = window._detailIdx;
    if (idx === null || idx === undefined) return;
    var page = document.getElementById('detail-page');
    if (!page || !page.classList.contains('show')) return;

    var op = liveOpForIdx(idx);
    if (!op) return;

    var topbar = document.getElementById('detail-topbar');
    topbar.style.background = UI.card;
    topbar.style.borderBottom = '2px solid ' + stateTextColor(op.state);

    var tag = document.getElementById('detail-tag');
    setTextIfChanged(tag, op.state || '?');
    tag.style.background = statePillBg(op.state);

    setTextIfChanged(document.getElementById('detail-label'),
        opShortName(op.type) + '   ' + (op.operatorId || ''));
    setTextIfChanged(document.getElementById('detail-op'),
        op.type + (op.parallelism ? '  ·  parallelism ' + op.parallelism : ''));

    var body = document.getElementById('detail-content');
    var tasks = op.tasks || [];
    // 结构签名：算子 / 失败态 / 任务集合（taskId 序列）变化才重建骨架，否则原地刷新
    var sig = idx + '|' + (op.type || '') + '|' + (op.operatorId || '') + '|' + (op.parallelism || 0)
        + '|' + (op.state === 'FAILED' ? (op.failureMessage || '') : '')
        + '|' + tasks.length + '|' + tasks.map(function (t) { return t.taskId; }).join(',');

    if (body.getAttribute('data-op-sig') !== sig) {
        var failHtml = op.state === 'FAILED'
            ? '<div class="section"><div class="section-title">失败原因</div>'
            + '<div style="background:' + UI.failBg + ';border:1px solid ' + UI.failBorder + ';color:' + UI.failText + ';border-radius:8px;padding:12px 16px;font-family:monospace;font-size:12px;white-space:pre-wrap;word-break:break-all">'
            + esc(op.failureMessage || 'unknown') + '</div></div>' : '';

        body.innerHTML = failHtml
            + '<div class="section"><div class="section-title">数据吞吐</div><div class="metric-cards">'
            + opCard('rowsIn', 'Input Rows', fmtRows(op.rowsIn), '', 'io-in')
            + opCard('rowsOut', 'Output Rows', fmtRows(op.rowsOut), '根算子 = 查询结果行', 'io-out')
            + opCard('avgRate', 'Avg Rate', fmtRate(op.avgRowsPerSec), '累计平均', 'io-in')
            + opCard('instantRate', 'Instant Rate', fmtRate(op.instantRowsPerSec), '两快照差分', 'io-out')
            + opCard('bytesIn', 'Input Bytes', fmtBytes(op.inputBytes), '累计输入字节', 'io-in')
            + opCard('bytesOut', 'Output Bytes', fmtBytes(op.outputBytes), '累计输出字节', 'io-out')
            + opCard('bytesRate', 'Avg Rate', fmtBytesRate(op.avgBytesPerSec), '累计平均 · 字节', 'io-in')
            + opCard('bytesInstant', 'Instant Rate', fmtBytesRate(op.instantBytesPerSec), '两快照差分 · 字节', 'io-out')
            + '</div></div>'
            + '<div class="section"><div class="section-title">资源</div><div class="metric-cards">'
            + opCard('elapsed', 'Elapsed', fmtDuration(op.elapsedMs), '', '')
            + opCard('cpu', 'CPU', fmtDuration(op.cpuMs), '', '')
            + opCard('mem', 'Memory', fmtBytes(op.currentMemBytes), '峰值 ' + fmtBytes(op.peakMemBytes), '')
            + opCard('spilled', 'Spilled', op.spilledBytes > 0 ? fmtBytes(op.spilledBytes) : '\u2014', '溢写总量', '')
            + '</div></div>'
            + '<div class="section"><div class="section-title">分布式任务（' + tasks.length + '）</div>'
            + '<div class="task-list2" id="task-list-container"></div>'
            + '<div id="task-pagination" style="display:flex;justify-content:center;align-items:center;gap:6px;margin-top:10px;flex-wrap:wrap;font-size:13px"></div>'
            + '</div>';
        body.setAttribute('data-op-sig', sig);
    }

    // 原地刷新指标（结构未变时唯一变化的只有数字）
    setMetric(body, 'rowsIn', fmtRows(op.rowsIn));
    setMetric(body, 'rowsOut', fmtRows(op.rowsOut));
    setMetric(body, 'avgRate', fmtRate(op.avgRowsPerSec));
    setMetric(body, 'instantRate', fmtRate(op.instantRowsPerSec));
    setMetric(body, 'bytesIn', fmtBytes(op.inputBytes));
    setMetric(body, 'bytesOut', fmtBytes(op.outputBytes));
    setMetric(body, 'bytesRate', fmtBytesRate(op.avgBytesPerSec));
    setMetric(body, 'bytesInstant', fmtBytesRate(op.instantBytesPerSec));
    setMetric(body, 'elapsed', fmtDuration(op.elapsedMs));
    setMetric(body, 'cpu', fmtDuration(op.cpuMs));
    setMetric(body, 'mem', fmtBytes(op.currentMemBytes), '峰值 ' + fmtBytes(op.peakMemBytes));
    setMetric(body, 'spilled', op.spilledBytes > 0 ? fmtBytes(op.spilledBytes) : '\u2014');

    // 保持原先分页位置
    var keepPage = window._taskPage || 0;
    window._allTasks = tasks;
    renderTaskPage(keepPage);
}

function hideNodeDetail() {
    window._detailIdx = null;
    document.getElementById('task-detail-overlay').classList.remove('show');
    document.getElementById('detail-page').classList.remove('show');
    document.getElementById('dag').style.display = '';
    document.getElementById('dag-topbar').style.display = '';
}

// ==================== 任务分页列表 ====================

function taskStateBadge(st) {
    return '<span class="status-badge ' + stCls(st) + '">' + esc(st || '?') + '</span>';
}

/**
 * 原地刷新一行任务：只改状态点 / 状态文本 / 行数耗时。
 *
 * <p>轮询每 300ms 调一次；保留 DOM 节点，运行中点的心跳动画（pulse）才不会每拍从头重播，
 * 鼠标 hover 的过渡也不会被反复打断。</p>
 */
function updateTaskRow(row, t, listIdx) {
    if (!row) return;
    var dot = row.querySelector('.task-dot2');
    if (dot) {
        var cls = 'task-dot2 ' + (t.state === 'RUNNING' ? 'run' : 'done');
        if (dot.className !== cls) dot.className = cls;
    }
    var st = row.querySelector('.task-status');
    if (st) {
        setTextIfChanged(st, t.state || '?');
        var color = stateTextColor(t.state);
        if (st.style.color !== color) st.style.color = color;
    }
    var metrics = row.querySelector('.task-metrics');
    if (metrics) {
        setTextIfChanged(metrics, fmtRows(t.rows) + ' \u884c \u00b7 ' + fmtDuration(t.elapsedMs));
    }
    if (row.getAttribute('data-idx') !== String(listIdx)) {
        row.setAttribute('data-idx', String(listIdx));
        row.setAttribute('onclick', 'showTaskDetail(' + listIdx + ')');
    }
}

/**
 * 任务分页列表（增量渲染）。
 *
 * <p>当前页的 taskId 序列不变时只原地改数值，<b>不重建 DOM</b> —— 旧实现每 300ms 整体
 * {@code innerHTML} 重建，会让运行中任务的 pulse 动画重启、hover 过渡重置，看起来一直闪 / 抖。</p>
 */
function renderTaskPage(page) {
    var tasks = window._allTasks || [];
    var perPage = 12;
    var totalPages = Math.ceil(tasks.length / perPage) || 1;
    page = Math.max(0, Math.min(page, totalPages - 1));
    window._taskPage = page;

    var start = page * perPage;
    var pageTasks = tasks.slice(start, start + perPage);
    var container = document.getElementById('task-list-container');
    if (container) {
        if (pageTasks.length === 0) {
            if (container.getAttribute('data-list-sig') !== 'empty') {
                container.innerHTML = '<div style="padding:12px;color:' + UI.faint + ';font-size:13px">单机算子：无分布式任务</div>';
                container.setAttribute('data-list-sig', 'empty');
            }
        } else {
            var sig = pageTasks.map(function (t) { return t.taskId; }).join('|');
            if (container.getAttribute('data-list-sig') !== sig) {
                container.innerHTML = pageTasks.map(function (t, k) {
                    var listIdx = start + k;
                    var running = t.state === 'RUNNING';
                    var kind = t.kind ? ('<span style="color:' + UI.accentHi + ';font-weight:600">' + esc(t.kind) + '</span>') : '';
                    return '<div class="task-row" data-idx="' + listIdx + '" onclick="showTaskDetail(' + listIdx + ')">'
                        + '<span class="task-dot2 ' + (running ? 'run' : 'done') + '"></span>'
                        + '<span class="task-name" title="' + esc(t.taskId) + '">' + midStr(t.taskId, 14) + '</span>'
                        + kind
                        + '<span style="font-size:11px;color:' + UI.faint + ';max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + esc(t.workerId) + '">@' + midStr(t.workerId, 8) + '</span>'
                        + '<span class="task-status" style="color:' + stateTextColor(t.state) + '">' + esc(t.state) + '</span>'
                        + '<span class="task-metrics" style="font-size:11px;color:' + UI.faint + '">' + fmtRows(t.rows) + ' 行 · ' + fmtDuration(t.elapsedMs) + '</span>'
                        + '</div>';
                }).join('');
                container.setAttribute('data-list-sig', sig);
            }
            for (var k = 0; k < pageTasks.length; k++) {
                updateTaskRow(container.children[k], pageTasks[k], start + k);
            }
        }
    }
    var pg = document.getElementById('task-pagination');
    if (pg) {
        var pgSig = totalPages + ':' + page;
        if (totalPages > 1) {
            if (pg.getAttribute('data-pg-sig') !== pgSig) {
                var html = '';
                for (var p = 0; p < totalPages; p++) {
                    var isCur = p === page;
                    html += '<span onclick="renderTaskPage(' + p + ')" style="cursor:pointer;padding:4px 10px;border-radius:4px;'
                        + (isCur ? 'background:' + UI.accent + ';color:#fff;font-weight:600' : 'color:' + UI.dim + ';border:1px solid ' + UI.line)
                        + '">' + (p + 1) + '</span>';
                }
                pg.innerHTML = html;
                pg.setAttribute('data-pg-sig', pgSig);
            }
        } else if (pg.getAttribute('data-pg-sig') !== 'none') {
            pg.innerHTML = '';
            pg.setAttribute('data-pg-sig', 'none');
        }
    }
}

// ==================== 任务详情 ====================

function showTaskDetail(listIdx) {
    var tasks = window._allTasks || [];
    if (listIdx < 0 || listIdx >= tasks.length) return;
    window._taskIdx = listIdx;
    // 先显示再填充：refreshTaskDetail 在 overlay 未显示时会直接返回，
    // 顺序反了会先露出上一次任务的旧内容，下一拍才被替换（点任务时闪一下）。
    document.getElementById('task-detail-overlay').classList.add('show');
    refreshTaskDetail();
}

function hideTaskDetail() {
    window._taskIdx = null;
    document.getElementById('task-detail-overlay').classList.remove('show');
}

function refreshTaskDetail() {
    var listIdx = window._taskIdx;
    if (listIdx === null || listIdx === undefined) return;
    var tasks = window._allTasks || [];
    var overlay = document.getElementById('task-detail-overlay');
    if (!overlay || !overlay.classList.contains('show')) return;
    var t = tasks[listIdx];
    if (!t) return;

    setTextIfChanged(document.getElementById('task-detail-label'), midStr(t.taskId, 20));
    setTextIfChanged(document.getElementById('task-detail-sub'), (t.kind || 'task')
        + (t.producer ? ' · producer' : ' · consumer') + '   @' + (t.workerId || ''));

    var tag = document.getElementById('task-detail-tag');
    setTextIfChanged(tag, t.state || '?');
    tag.style.background = statePillBg(t.state);

    var body = document.getElementById('task-detail-body');
    // 同一任务且失败信息未变 → 只刷新会变的三个数值，不重建 DOM（避免 300ms 一拍的闪动）
    var sig = t.taskId + '|' + (t.failure || '');
    if (body.getAttribute('data-task-sig') !== sig) {
        body.innerHTML =
            '<div class="section"><div class="section-title">任务</div>'
            + '<div style="background:' + UI.raise + ';border:1px solid ' + UI.lineSoft + ';border-radius:8px;padding:12px 16px;font-family:monospace;font-size:12px;white-space:pre-wrap;word-break:break-all;color:' + UI.text + '">'
            + esc(t.taskId) + '</div></div>'
            + (t.failure ? '<div class="section"><div class="section-title">失败原因</div>'
                + '<div style="background:' + UI.failBg + ';border:1px solid ' + UI.failBorder + ';color:' + UI.failText + ';border-radius:8px;padding:12px 16px;font-family:monospace;font-size:12px;white-space:pre-wrap;word-break:break-all">'
                + esc(t.failure) + '</div></div>' : '')
            + '<div class="section"><div class="section-title">详情</div><div>'
            + metricRow('状态', esc(t.state || ''))
            + metricRow('类型', esc(t.kind || ''))
            + metricRow('角色', t.producer ? 'producer（生成方）' : 'consumer（消费方）')
            + metricRow('Worker', esc(t.workerId || ''))
            + metricRow('输出行数', fmtRows(t.rows))
            + metricRow('已用耗时', fmtDuration(t.elapsedMs))
            + '</div></div>';
        body.setAttribute('data-task-sig', sig);
    }

    var vals = body.querySelectorAll('.task-metric-value');
    if (vals.length >= 6) {
        setTextIfChanged(vals[0], t.state || '');
        setTextIfChanged(vals[4], fmtRows(t.rows));
        setTextIfChanged(vals[5], fmtDuration(t.elapsedMs));
    }
}
