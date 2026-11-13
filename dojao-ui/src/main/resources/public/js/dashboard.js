// ==================== Dashboard（监控 + SQL 控制台）====================

// ---------------- 顶栏横幅 / 轮询 ----------------

function setBanner(msg) {
    var el = document.getElementById('monitor-banner');
    if (!el) return;
    if (msg) {
        el.innerHTML = '<svg class="ui-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor"'
            + ' stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
            + '<path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3'
            + 'L13.71 3.86a2 2 0 0 0-3.42 0z"/><path d="M12 9.5v3.5"/><path d="M12 17h.01"/></svg>'
            + esc(msg);
        el.classList.remove('hidden');
    } else {
        el.classList.add('hidden');
    }
}

function startDashboardPoll() {
    stopDashboardPoll();
    refreshAll();
    clusterTimer = setInterval(refreshAll, 2000);
}

function stopDashboardPoll() {
    if (clusterTimer) { clearInterval(clusterTimer); clusterTimer = null; }
}

function refreshAll() {
    return Promise.allSettled([
        httpGet('/api/queries').then(function (d) {
            jobList = (d && d.queries) || [];
            renderJobTable();
        }),
        httpGet('/api/cluster').then(function (d) {
            clusterData = d;
            renderClusterCards();
        })
    ]).then(function (results) {
        var banner = null;
        results.forEach(function (r) {
            if (r.status === 'fulfilled') return;
            if (r.reason && r.reason.status === 503) {
                banner = banner || '控制台未绑定查询监控（独立预览模式），监控与 SQL 执行接口不可用。';
            } else if (!banner) {
                banner = oneLine((r.reason && r.reason.message) || r.reason, 80);
            }
        });
        setBanner(banner);
    });
}

// ---------------- 集群概览卡片 ----------------

function renderClusterCards() {
    var cluster = clusterData;
    if (!cluster) return;
    var active = (cluster.activeQueries) || [];
    var recent = (cluster.recentQueries) || [];
    var all = jobList && jobList.length ? jobList : active.concat(recent);

    var running = all.filter(function (j) { return j.state === 'RUNNING'; }).length;
    var queued = all.filter(function (j) { return j.state === 'SUBMITTED'; }).length;
    var failed = all.filter(function (j) { return j.state === 'FAILED'; }).length;

    var workers = cluster.workers || [];
    var healthy = workers.filter(function (w) { return w.healthy; });
    var totalCores = 0, usedCores = 0, totalMem = 0, usedMem = 0, runningTasks = 0;
    healthy.forEach(function (w) {
        if (w.cpuCores) totalCores += w.cpuCores;
        if (w.cpuUsagePct && w.cpuCores) usedCores += Math.round(w.cpuCores * w.cpuUsagePct / 100);
        if (w.totalMemoryBytes) totalMem += w.totalMemoryBytes;
        if (w.usedMemoryBytes) usedMem += w.usedMemoryBytes;
        runningTasks += w.runningQueries || 0;
    });

    setVal('stat-running-val', running);
    setVal('stat-queued-val', queued);
    setVal('stat-blocked-val', failed);
    setVal('stat-workers-val', healthy.length);
    setVal('stat-workers-sub', 'of ' + workers.length + ' total · ' + runningTasks + ' 运行任务');
    setVal('stat-cpu-val', usedCores + ' / ' + totalCores);
    setVal('stat-cpu-sub', totalCores ? Math.round(usedCores / totalCores * 100) + '% utilized' : '');
    setVal('stat-memory-val', fmtGb(usedMem) + ' / ' + fmtGb(totalMem));
    setVal('stat-memory-sub', totalMem ? Math.round(usedMem / totalMem * 100) + '% utilized' : '');

    var cpuPct = totalCores ? usedCores / totalCores * 100 : 0;
    var memPct = totalMem ? usedMem / totalMem * 100 : 0;
    ['stat-cpu', 'stat-memory'].forEach(function (id) {
        var card = document.getElementById(id);
        if (card) card.classList.remove('warn', 'critical');
    });
    ['stat-cpu', 'stat-memory'].forEach(function (id) {
        var card = document.getElementById(id);
        if (!card) return;
        var pct = id === 'stat-cpu' ? cpuPct : memPct;
        if (pct > 95) card.classList.add('critical');
        else if (pct > 80) card.classList.add('warn');
    });
}

function setVal(id, v) {
    var el = document.getElementById(id);
    if (el) el.textContent = v;
}

// ---------------- 任务列表 ----------------

function badgeHtml(state) {
    return '<span class="status-badge ' + stCls(state) + '">' + esc(state || '?') + '</span>';
}

/** 任务列表每页条数的 localStorage 键。 */
var JOB_PER_PAGE_KEY = 'dojao.tasks.perPage';

/** 首屏从 localStorage 恢复「每页条数」（缺省 10），由 app.js 调用一次。 */
function loadJobPerPagePref() {
    var n = 10;
    try {
        var s = localStorage.getItem(JOB_PER_PAGE_KEY);
        if (s) n = parseInt(s, 10);
    } catch (e) { /* 隐私模式等忽略 */ }
    if (!(n > 0)) n = 10;
    jobPerPage = n;
}

/** 任务列表状态筛选的 localStorage 键 + 合法取值（顺序与 index.html 的按钮一致）。 */
var JOB_STATE_FILTER_KEY = 'dojao.tasks.stateFilter';
var JOB_STATE_FILTERS = ['ALL', 'RUNNING', 'SUBMITTED', 'FINISHED', 'FAILED', 'CANCELED'];

/** 首屏从 localStorage 恢复状态筛选（缺省 ALL），由 app.js 调用一次。 */
function loadJobStateFilterPref() {
    var f = 'ALL';
    try {
        var s = localStorage.getItem(JOB_STATE_FILTER_KEY);
        if (s && JOB_STATE_FILTERS.indexOf(s) >= 0) f = s;
    } catch (e) { /* 隐私模式等忽略 */ }
    jobStateFilter = f;
}

/** 切换状态筛选：回到第 1 页、记忆选择并重绘。 */
function setJobStateFilter(state) {
    if (JOB_STATE_FILTERS.indexOf(state) < 0) state = 'ALL';
    jobStateFilter = state;
    jobPage = 0;
    try { localStorage.setItem(JOB_STATE_FILTER_KEY, state); } catch (e) { /* ignore */ }
    renderJobTable();
}

/** 刷新筛选按钮的选中态（任务表每 2s 重绘时同步，避免与筛选状态脱节）。 */
function renderJobFilters() {
    var el = document.getElementById('job-filters');
    if (!el) return;
    var btns = el.querySelectorAll('.job-filter');
    for (var i = 0; i < btns.length; i++) {
        btns[i].classList.toggle('active', btns[i].getAttribute('data-state') === jobStateFilter);
    }
}

/** 当前筛选下任务是否可见（ALL = 不过滤）。 */
function jobMatchesFilter(job) {
    return jobStateFilter === 'ALL' || job.state === jobStateFilter;
}

/** 设置每页条数：落 localStorage + 回到第 1 页 + 重绘（「页面设置」与分页条共用）。 */
function setJobPerPage(n) {
    n = parseInt(n, 10);
    if (!(n > 0)) n = 10;
    jobPerPage = n;
    jobPage = 0;
    try { localStorage.setItem(JOB_PER_PAGE_KEY, String(n)); } catch (e) { /* ignore */ }
    renderJobTable();
}

/** 任务列表翻页。 */
function gotoJobPage(p) {
    jobPage = p;
    renderJobTable();
}

/** 分页条：上一页 / 页码 / 下一页 + 「共 N 条」+ 「每页条数」。 */
function renderJobPagination(totalPages, visibleCount) {
    var el = document.getElementById('job-pagination');
    if (!el) return;
    var total = (typeof visibleCount === 'number') ? visibleCount : jobList.length;
    var info = '共 ' + total + ' 条';
    if (jobStateFilter !== 'ALL') info += '（筛选后 · 全部 ' + jobList.length + ' 条）';
    var html = '<span class="job-page-info">' + info + ' · 第 '
        + (jobPage + 1) + '/' + totalPages + ' 页</span>';

    function btn(target, label, disabled, active) {
        if (disabled) return '<button class="job-page-btn" disabled>' + label + '</button>';
        if (active) return '<button class="job-page-btn active">' + label + '</button>';
        return '<button class="job-page-btn" onclick="gotoJobPage(' + target + ')">' + label + '</button>';
    }

    html += '<div class="job-page-btns">';
    html += btn(jobPage - 1, '\u2039 \u4e0a\u4e00\u9875', jobPage <= 0, false);
    // 每页最多 100 条上限（后端 RECENT_CAPACITY=100）→ 页码最多十来页，全量渲染即可
    for (var p = 0; p < totalPages; p++) {
        html += btn(p, String(p + 1), false, p === jobPage);
    }
    html += btn(jobPage + 1, '\u4e0b\u4e00\u9875 \u203a', jobPage >= totalPages - 1, false);
    html += '<select class="job-page-size" title="\u6bcf\u9875\u6761\u6570"'
        + ' onchange="setJobPerPage(this.value)">';
    [10, 20, 50, 100].forEach(function (n) {
        html += '<option value="' + n + '"' + (n === jobPerPage ? ' selected' : '') + '>'
            + n + ' \u6761/\u9875</option>';
    });
    html += '</select>';
    html += '</div>';
    el.innerHTML = html;
}

function renderJobTable() {
    var container = document.getElementById('job-table-container');
    if (!container) return;
    renderJobFilters();
    if (!jobList.length) {
        container.innerHTML = '<div class="empty-state"><div class="icon">'
            + '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.3"'
            + ' stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
            + '<circle cx="10.8" cy="10.8" r="7.3"/><path d="m20.2 20.2-4.6-4.6"/></svg>'
            + '</div>'
            + '<div>No queries yet. 提交 SQL 后这里会出现任务，点击查看执行 DAG。</div></div>';
        return;
    }
    // 状态筛选：只在当前筛选结果上分页（jobList 每 2s 整体刷新一次）
    var visibleJobs = jobList.filter(jobMatchesFilter);
    if (!visibleJobs.length) {
        container.innerHTML = '<div class="empty-state"><div class="icon">'
            + '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.3"'
            + ' stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
            + '<circle cx="10.8" cy="10.8" r="7.3"/><path d="m20.2 20.2-4.6-4.6"/></svg>'
            + '</div>'
            + '<div>当前筛选条件下没有任务。</div>'
            + '<button type="button" class="job-filter-clear"'
            + ' onclick="setJobStateFilter(\'ALL\')">查看全部任务</button></div>';
        return;
    }
    // 分页切片：每 2s 轮询会整体重绘，故这里只按当前页取一段；列表缩短时把页码夹回有效范围
    var perPage = jobPerPage > 0 ? jobPerPage : 10;
    var totalPages = Math.max(1, Math.ceil(visibleJobs.length / perPage));
    if (jobPage > totalPages - 1) jobPage = totalPages - 1;
    if (jobPage < 0) jobPage = 0;
    var start = jobPage * perPage;
    var pageJobs = visibleJobs.slice(start, start + perPage);

    var html = '<table class="job-table"><thead><tr>'
        + '<th>Query ID</th><th>SQL</th><th>Status</th><th>Wall</th><th>Rows Out</th><th>Submitted</th><th></th>'
        + '</tr></thead><tbody>';

    pageJobs.forEach(function (job) {
        var id = esc(job.queryId);
        var sqlFull = esc(job.sql);
        var sqlPrev = esc(oneLine(job.sql, 60));
        var wall = (job.wallMs && job.wallMs > 0) ? fmtDuration(job.wallMs) : '\u2014';
        // 只有「排队中 / 运行中」才给终止入口：已结束的任务取消没有意义，
        // 且 Master 侧 cancel 是无条件置 CANCELED，对已完成查询点了会把状态改坏。
        var cancellable = (job.state === 'RUNNING' || job.state === 'SUBMITTED');
        var stopBtn = cancellable
            ? '<button class="stop-btn" title="\u5f3a\u5236\u7ec8\u6b62\u8be5\u67e5\u8be2"'
                + ' onclick="event.stopPropagation(); cancelJob(\'' + job.queryId + '\')">\u505c\u6b62</button>'
            : '';
        html += '<tr onclick="switchToDagView(\'' + job.queryId + '\')">'
            + '<td><span class="job-id" title="' + id + '">' + midStr(job.queryId, 12) + '</span></td>'
            + '<td><span class="job-sql" title="' + sqlFull + '">' + sqlPrev + '</span></td>'
            + '<td>' + badgeHtml(job.state) + '</td>'
            + '<td style="font-size:12px;color:' + UI.dim + '">' + wall + '</td>'
            + '<td style="font-size:12px;color:' + UI.dim + '">' + fmtRows(job.totalRowsOut) + '</td>'
            + '<td style="font-size:12px;color:' + UI.faint + '">' + qidTime(job.queryId) + '</td>'
            + '<td class="job-actions">'
            + '<button class="view-btn" onclick="event.stopPropagation(); switchToDagView(\'' + job.queryId + '\')">\u67e5\u770b\u8be6\u60c5</button>'
            + stopBtn + '</td>'
            + '</tr>';
    });
    html += '</tbody></table>';
    html += '<div class="job-pagination" id="job-pagination"></div>';
    container.innerHTML = html;
    renderJobPagination(totalPages, visibleJobs.length);
}

// ---------------- SQL 页：异步执行 + 结果预览 ----------------

/** 控制台一次展示 / 拉取的行数上限（后端 UiQueryMonitor 预览上限同为 500）。 */
var SQL_PREVIEW_LIMIT = 500;
/** 结果预览轮询连续失败多少次后放弃（单次网络抖动不清场）。 */
var SQL_ERR_STREAK_MAX = 5;
/** 本次提交是否由前端自动补了 LIMIT（用于结果区提示）。 */
var sqlAutoLimited = false;
/** 本次提交按顶层分号切出的语句条数（>1 = 批量，引擎串行执行；用于结果区提示）。 */
var sqlBatchCount = 0;

/**
 * 按顶层分号切分后的「有效语句条数」：引号 / 注释里的分号不算，纯注释 / 空白段不算。
 *
 * <p>只用于前端判断是否批量提交（批量时不能自动补 LIMIT、结果区标注「批量 N 条」）；
 * 真正的切分与串行执行由引擎侧（UiQueryMonitor + SqlSplitter）完成。</p>
 */
function sqlStatementCount(text) {
    var s = String(text || '');
    var n = s.length, i = 0, count = 0, hasCode = false;
    while (i < n) {
        var c = s.charAt(i);
        if (c === '\'' || c === '"' || c === '`') {
            hasCode = true;
            var q = c;
            i++;
            while (i < n) {
                var d = s.charAt(i);
                i++;
                if (d === q) {
                    if (i < n && s.charAt(i) === q) i++; else break;
                }
            }
            continue;
        }
        if (c === '-' && s.charAt(i + 1) === '-') {
            while (i < n && s.charAt(i) !== '\n') i++;
            continue;
        }
        if (c === '/' && s.charAt(i + 1) === '*') {
            i += 2;
            while (i < n && !(s.charAt(i) === '*' && s.charAt(i + 1) === '/')) i++;
            i += 2;
            continue;
        }
        if (c === ';') {
            if (hasCode) count++;
            hasCode = false;
            i++;
            continue;
        }
        if (!/\s/.test(c)) hasCode = true;
        i++;
    }
    if (hasCode) count++;
    return count;
}

/**
 * 用户没写 LIMIT 时给 SELECT 自动补一个 —— 控制台只展示前 N 行，别把整表拉出来。
 * 只处理 SELECT / WITH 开头的语句（DDL / INSERT / EXPLAIN 不动）。
 * 注意：引擎 parser 不接受收尾分号，这里顺手剥掉。
 *
 * @return {{sql: string, autoLimit: boolean}} sql = 真正发给后端的语句
 */
function withAutoLimit(raw) {
    var sql = String(raw || '').trim();
    // 批量提交整段交给引擎按分号切分：不能追加 LIMIT —— 它会粘到最后一条语句上，
    // 若最后一条是 DDL / INSERT 会直接语法错，且前面的语句也用不上这个 LIMIT。
    if (sqlStatementCount(sql) > 1) return { sql: sql, autoLimit: false };
    sql = sql.replace(/;\s*$/, '');
    if (!/^\s*(select|with)\b/i.test(sql)) return { sql: sql, autoLimit: false };
    if (/\blimit\b/i.test(sql)) return { sql: sql, autoLimit: false };
    return { sql: sql + ' limit ' + SQL_PREVIEW_LIMIT, autoLimit: true };
}

/**
 * 本次要执行的 SQL：编辑器里<b>有选中就只跑选中那段</b>（同一框里常并排放着多条 SQL，
 * 选中即指定本次执行范围），没有选中才跑整框。顺带记录「是否跑了选区」供结果区提示。
 */
function editorSqlToRun(input) {
    var full = (input && input.value) || '';
    var sel = '';
    if (input && input.selectionStart != null && input.selectionEnd > input.selectionStart) {
        sel = full.slice(input.selectionStart, input.selectionEnd);
    }
    sqlRanSelection = !!sel.trim();
    return (sqlRanSelection ? sel : full).trim();
}

/** ${name} 参数占位（name 为合法标识符；不支持表达式 / 引号）。 */
var SQL_PARAM_RE = /\$\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}/g;

/** 收集 SQL 中出现的去重参数名（按首次出现顺序）。 */
function findSqlParams(sql) {
    var out = [], seen = {};
    var m;
    SQL_PARAM_RE.lastIndex = 0;
    while ((m = SQL_PARAM_RE.exec(sql)) !== null) {
        if (!seen[m[1]]) { seen[m[1]] = 1; out.push(m[1]); }
    }
    return out;
}

/** 用填好的值替换所有 ${name} 占位（按字面替换，不额外加引号 —— 值由用户按 SQL 语义给出）。 */
function applySqlParams(sql, values) {
    return sql.replace(SQL_PARAM_RE, function (whole, name) {
        return Object.prototype.hasOwnProperty.call(values, name) ? values[name] : whole;
    });
}

function submitQuery() {
    var input = document.getElementById('query-input');
    var raw = editorSqlToRun(input);
    var errEl = document.getElementById('query-error');
    if (!raw) { errEl.textContent = '请输入 SQL。'; return; }
    errEl.textContent = '';
    // 含 ${name} 占位：先弹窗收集取值，替换后再提交（用户可选一段带占位的 SQL 单独跑）
    var params = findSqlParams(raw);
    if (params.length) {
        openSqlParamsDialog(params, function (values) {
            doSubmitQuery(applySqlParams(raw, values));
        });
        return;
    }
    doSubmitQuery(raw);
}

/** 真正提交执行（参数已替换）。 */
function doSubmitQuery(raw) {
    var errEl = document.getElementById('query-error');
    lastSqlText = raw;   // 「复制」「用 AI 修复」用本次真正跑的那段，而不是整框内容

    // 编辑器默认 catalog：两段式 schema.table 补全成 catalog.schema.table 后再交给引擎
    // （右侧下拉框选择当前 catalog；已写全三段式的 SQL 原样通过）。
    var sent = withAutoLimit(completeCatalogQualifiers(raw));
    sqlAutoLimited = sent.autoLimit;
    sqlBatchCount = sqlStatementCount(raw);   // >1 = 引擎侧按分号切分后串行执行
    var btn = document.getElementById('submit-btn');
    if (btn) btn.disabled = true;
    // 记住「是哪个控制台提交的」：POST 在途时用户可能切走，结果仍要能回到发起它的控制台
    var ownerId = (typeof activeConsoleId !== 'undefined') ? activeConsoleId : null;
    // 提交那一刻就进入「运行中」：POST 到 Master 也要一个往返，这段空窗里若画面完全静止，
    // 用户会怀疑「到底点上了没」。圆圈立刻转起来 = 明确的「已经在跑了」。
    beginSqlSubmit();
    scrollResultIntoView();
    httpPost('/api/query', { sql: sent.sql })
        .then(function (r) {
            // 控制台已被关闭：查询仍在服务端跑，但结果已无处展示
            var owner = (typeof findConsole === 'function') ? findConsole(ownerId) : null;
            if (!owner) { refreshAll(); return; }
            if (r.state === 'FAILED' || r.state === 'REJECTED') {
                var p = consolePreview(owner);
                p.submitPending = false;
                p.running = false;
                p.elapsedMs = Date.now() - p.startMs;
                p.lastError = '提交失败：' + (r.message || '未知错误');
                if (ownerId === activeConsoleId) {
                    if (errEl) errEl.textContent = p.lastError;
                    renderActivePreview();
                }
                maybeStopSqlTickTimer();
            } else {
                // 注意：不清空输入框 —— 提交后 SQL 要留在编辑器里，便于改参数重跑
                startSqlPreview(r.queryId, ownerId);
            }
            refreshAll();
        })
        .catch(function (err) {
            var owner = (typeof findConsole === 'function') ? findConsole(ownerId) : null;
            if (!owner) return;
            var p = consolePreview(owner);
            p.submitPending = false;
            p.running = false;
            p.elapsedMs = Date.now() - p.startMs;
            p.lastError = '提交失败：' + oneLine(err && err.message ? err.message : err, 120);
            if (ownerId === activeConsoleId) {
                if (errEl) errEl.textContent = p.lastError;
                if (err && err.status === 503) {
                    setBanner('控制台未绑定查询监控（独立预览模式），SQL 执行不可用。');
                }
                renderActivePreview();
            }
            maybeStopSqlTickTimer();
        })
        .finally(function () { if (btn) btn.disabled = false; });
}

function cancelSqlQuery() {
    if (!sqlQueryId) return;
    httpPost('/api/query/' + encodeURIComponent(sqlQueryId) + '/cancel').catch(function () { });
    // 状态由该控制台的 pollConsoleResult 更新
}

/** 任务列表：手动终止一个任务（仅 RUNNING/SUBMITTED 显示入口）。 */
function cancelJob(queryId) {
    if (!queryId) return;
    if (!window.confirm('\u786e\u5b9a\u7ec8\u6b62\u67e5\u8be2 ' + queryId + ' \uff1f')) return;
    // 取消是「尽力而为」的：任务可能刚好自己跑完/被别处杀掉，失败不当错误报出，
    // 一律刷新列表以服务端状态为准。
    httpPost('/api/query/' + encodeURIComponent(queryId) + '/cancel')
        .then(refreshAll)
        .catch(refreshAll);
}

/**
 * 编辑器底部的高度调节把手：上下拖动改变编辑器高度（结果区随之让位），双击恢复默认 50vh。
 * 有结果时才显示（没有结果时这条「交界」没有意义）。
 */
function bindSqlSplitter() {
    var splitter = document.getElementById('sql-splitter');
    var input = document.getElementById('query-input');
    if (!splitter || !input) return;

    var startY = 0, startH = 0;

    function onMove(e) {
        var h = startH + (e.clientY - startY);
        var max = Math.max(200, window.innerHeight * 0.85);
        input.style.height = Math.min(max, Math.max(120, h)) + 'px';
    }

    function onUp() {
        splitter.classList.remove('dragging');
        document.body.classList.remove('resizing');
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
    }

    splitter.addEventListener('mousedown', function (e) {
        e.preventDefault();
        startY = e.clientY;
        startH = input.getBoundingClientRect().height;
        splitter.classList.add('dragging');
        document.body.classList.add('resizing');
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
    });
    splitter.addEventListener('dblclick', function () { input.style.height = ''; });
}

/**
 * AI 输入框随内容长高：初始高度由 CSS 的 min-height 给出（两行多），打字超过后按
 * scrollHeight 撑开，到 max-height（220px）为止再出内部滚动条。
 */
function bindAiInputAutoGrow() {
    var input = document.getElementById('ai-input');
    if (!input) return;
    var grow = function () {
        // 视图隐藏（display:none）时 scrollHeight 恒为 0，直接写高会把输入框压没，
        // 故读不到高度就交给 CSS 的初始 height。切到 AI 页/预填内容时再调一次。
        if (!input.scrollHeight) return;
        input.style.height = 'auto';
        input.style.height = Math.min(input.scrollHeight, 220) + 'px';
    };
    input.addEventListener('input', grow);
    aiInputGrow = grow;   // 供 switchTab('ai') / askAiToFix 预填后调用
    grow();
}

/** 让结果区滚进可视范围（编辑器占了半屏，结果在下方）。 */
function scrollResultIntoView() {
    var box = document.getElementById('sql-result');
    if (box && box.scrollIntoView) box.scrollIntoView({ block: 'nearest' });
}

// ---------------- 结果预览：提交 → 「计算中」计时 → 结果表格 ----------------
//
// 状态归属：结果预览属于「发起查询的那个控制台」。
//   - 每个控制台持有自己的 preview 模型（见 sql-console.js 的 makeConsolePreview）与自己的
//     轮询定时器（sqlPreviewPolls[consoleId]）；
//   - 全局 sqlQueryId / sqlLastResult / sqlSortCol … 只是「当前显示中」那个控制台的镜像，
//     供导出 / 复制 / 排序 / 报错卡片这些只管界面的功能读取；
//   - 切标签 = capture 旧控制台的界面态 → 切换 → restore 新控制台的预览，**不停任何轮询**。
//
// 旧实现把状态全放全局、切标签整块 reset：切走就丢了 queryId 与轮询，服务端却还在跑，
// 跑完结果再也回不到页面（静默丢结果）。

/** consoleId → 结果预览轮询定时器（每个控制台独立；切标签不停）。 */
var sqlPreviewPolls = {};

/** 取某控制台的 preview 模型（没有就懒建）。 */
function consolePreview(c) {
    if (!c) return null;
    if (!c.preview && typeof makeConsolePreview === 'function') c.preview = makeConsolePreview();
    return c.preview || null;
}

/** 当前激活控制台的 preview。 */
function activePreview() {
    return (typeof activeConsole === 'function') ? consolePreview(activeConsole()) : null;
}

/** 停某个控制台的结果轮询（不动其它控制台，也不改 preview 数据）。 */
function stopPreviewPoll(consoleId) {
    if (!consoleId) return;
    var t = sqlPreviewPolls[consoleId];
    if (t) { clearInterval(t); delete sqlPreviewPolls[consoleId]; }
}

/** 是否还有控制台在跑（决定 100ms 计时器要不要留着）。 */
function anyPreviewRunning() {
    var any = false;
    (typeof consoles !== 'undefined' ? consoles : []).forEach(function (c) {
        if (c && c.preview && c.preview.running) any = true;
    });
    return any;
}

function ensureSqlTickTimer() {
    if (sqlTickTimer) return;
    sqlTickTimer = setInterval(tickSqlElapsed, 100);
}

function maybeStopSqlTickTimer() {
    if (!anyPreviewRunning() && sqlTickTimer) {
        clearInterval(sqlTickTimer);
        sqlTickTimer = null;
    }
}

/**
 * 点「运行」的瞬间：把结果区切成「运行中」——圆圈转起来、计时跑起来。
 *
 * <p>这一步发生在 POST 之前，所以后端还没返回 queryId。轮询要等拿到 queryId 才起
 * （见 {@link startSqlPreview}），这里只负责模型与视觉。</p>
 */
function beginSqlSubmit() {
    var c = activeConsole();
    var p = consolePreview(c);
    if (!p) return;
    if (c) stopPreviewPoll(c.id);     // 同一控制台重跑：旧查询的轮询作废（服务端仍在跑）
    p.queryId = null;
    p.submitPending = true;
    p.running = true;
    p.startMs = Date.now();           // 计时从「点击」那刻起，而不是从拿到 queryId 起
    p.elapsedMs = 0;
    p.errStreak = 0;
    // 新查询 = 新结果集：清掉上一条的排序状态，否则会「莫名其妙按上一列排着」
    p.sortCol = -1;
    p.sortDir = 0;
    p.lastResult = null;
    p.lastError = '';
    p.lastErrorDetail = '';
    sqlChartReset();                  // 结果换新：收起并销毁上一张图表
    renderActivePreview();
    ensureSqlTickTimer();
}

/**
 * 拿到 queryId 后接上结果轮询（绑定到发起它的控制台，即使用户已切走）。
 *
 * <p>{@link beginSqlSubmit} 已经摆好了「运行中」的样子（含计时），此处不重置，
 * 只补上轮询 —— 否则计时会从「拿到 queryId」重新开始，肉眼看到时间往回跳。</p>
 *
 * @param {string} queryId
 * @param {string} [consoleId] 发起查询的控制台（缺省 = 当前激活）
 */
function startSqlPreview(queryId, consoleId) {
    if (!consoleId && typeof activeConsoleId !== 'undefined') consoleId = activeConsoleId;
    var c = (typeof findConsole === 'function') ? findConsole(consoleId) : null;
    if (!c) return;
    var p = consolePreview(c);
    if (!p) return;
    stopPreviewPoll(consoleId);
    p.queryId = queryId;
    p.submitPending = false;
    p.running = true;
    p.errStreak = 0;
    if (!p.startMs) p.startMs = Date.now();
    ensureSqlTickTimer();
    if (consoleId === activeConsoleId) renderActivePreview();
    // 先立刻打一拍（拿到首帧不等 1s），再挂该控制台自己的轮询
    pollConsoleResult(consoleId);
    sqlPreviewPolls[consoleId] = setInterval(function () { pollConsoleResult(consoleId); }, 1000);
}

/** 计算中 / 结束 的视觉切换（转圈 + 计时 + 取消按钮）。 */
function setSqlRunning(running) {
    var spin = document.getElementById('sql-spinner');
    if (spin) spin.classList.toggle('on', !!running);
    var cancelBtn = document.getElementById('sql-cancel-btn');
    if (cancelBtn) cancelBtn.style.display = running ? '' : 'none';
}

/** 100ms 刷新一次「计算中」计时（毫秒 → min/s/h 分级，见 fmtDuration）。 */
function tickSqlElapsed() {
    var p = activePreview();
    var el = document.getElementById('sql-elapsed');
    if (!el) return;
    if (!p || !p.running) { maybeStopSqlTickTimer(); return; }   // 提交在途 / 运行中都照常计时
    el.textContent = '\u23f1 ' + fmtDuration(Date.now() - p.startMs);
    el.classList.add('running');
}

/** 结果区底部提示：列数 / 行数 / 估算表大小 / 执行形态。 */
function resultNoteText(p, r) {
    var cols = r.columns || [];
    var rows = r.rows || [];
    var parts = ['\u5217 ' + cols.length, '\u884c ' + fmtRows(r.totalRows)];
    // 表数据量（连接器元数据估算，非精确 count）：单表查询时告诉用户「这张表大概多大」
    if (r.sourceRows > 0) {
        parts.push('\u8868\u6570\u636e\u91cf \u2248 ' + fmtRows(r.sourceRows) + ' \u884c\uff08\u4f30\uff09');
    }
    if (p && p.ranSelection) {
        parts.push('\u4ec5\u6267\u884c\u9009\u4e2d\u7247\u6bb5');
    }
    if (p && p.autoLimited) {
        parts.push('\u5df2\u81ea\u52a8\u8865 limit ' + SQL_PREVIEW_LIMIT);
    }
    if (p && p.batchCount > 1) {
        parts.push('\u6279\u91cf ' + p.batchCount + ' \u6761 \u00b7 \u4e32\u884c\u6267\u884c');
    }
    if (r.truncated) {
        parts.push('\u4ec5\u9884\u89c8\u524d ' + rows.length + ' \u884c\uff08\u5df2\u622a\u65ad\uff09');
    }
    return parts.join(' \u00b7 ');
}

/**
 * 把当前激活控制台的 preview 整块画到结果区（切换回该控制台 / 轮询更新共用）。
 *
 * <p>只读模型、不碰任何轮询句柄 —— 轮询属于控制台，与「现在显示谁」解耦。</p>
 */
function renderActivePreview() {
    var p = activePreview();
    var hasUi = !!(p && (p.queryId || p.submitPending || p.running || p.lastResult || p.lastError));
    if (!hasUi) { clearSqlResultDom(); return; }

    // 全局镜像：导出 / 复制 / 排序 / 报错卡片读这些
    sqlQueryId = p.queryId;
    sqlRunning = !!p.running;
    sqlStartMs = p.startMs;
    sqlElapsedMs = p.elapsedMs;
    sqlErrStreak = p.errStreak;
    sqlSortCol = p.sortCol;
    sqlSortDir = p.sortDir;
    sqlTransposed = !!p.transposed;
    sqlLastResult = p.lastResult;
    lastSqlText = p.lastSqlText;
    sqlRanSelection = p.ranSelection;
    sqlAutoLimited = p.autoLimited;
    sqlBatchCount = p.batchCount;
    lastSqlError = p.lastError;
    lastSqlErrorDetail = p.lastErrorDetail;

    var box = document.getElementById('sql-result');
    if (box) box.style.display = 'block';
    var splitter = document.getElementById('sql-splitter');
    if (splitter) splitter.style.display = 'block';   // 有结果才给高度调节把手

    var idEl = document.getElementById('sql-result-id');
    if (idEl) idEl.textContent = p.queryId ? midStr(p.queryId, 16) : '';

    var state = p.lastResult ? (p.lastResult.state || '?')
        : (p.submitPending ? 'SUBMITTED' : (p.running ? 'RUNNING' : '?'));
    var badge = document.getElementById('sql-result-badge');
    if (badge) { badge.textContent = state; badge.className = 'status-badge ' + stCls(state); }

    var el = document.getElementById('sql-elapsed');
    if (el) {
        if (p.running) {
            el.textContent = '\u23f1 ' + fmtDuration(Date.now() - p.startMs);
            el.classList.add('running');
        } else if (p.elapsedMs > 0) {
            el.textContent = '\u23f1 ' + fmtDuration(p.elapsedMs);
            el.classList.remove('running');
        } else {
            el.textContent = '';
            el.classList.remove('running');
        }
    }
    setSqlRunning(!!p.running);
    if (p.running) ensureSqlTickTimer();

    var note = document.getElementById('sql-result-note');
    if (note) {
        if (!p.lastResult && p.lastError) {
            // 提交失败时编辑器上方已显示完整原因，这里不重复长文案
            var errEl = document.getElementById('query-error');
            var shownAbove = !!(errEl && errEl.textContent === p.lastError);
            note.textContent = shownAbove ? '提交失败' : p.lastError;
            note.style.color = UI.err;
        } else if (!p.lastResult) {
            note.textContent = p.submitPending ? '提交中\u2026' : (p.running ? '\u6b63\u5728\u8ba1\u7b97\u2026' : '');
            note.style.color = '';
        } else {
            // 报错不在这里重复：失败由中部「查询出错」卡片统一呈现（含复制 / AI 修复）
            note.textContent = resultNoteText(p, p.lastResult);
            note.style.color = '';
        }
    }

    if (p.lastResult) {
        renderSqlTable(p.lastResult);
    } else {
        renderSqlTable({ state: p.running || p.submitPending ? 'RUNNING' : '?', columns: [], rows: [], totalRows: 0 });
    }
}

/** 清空结果区 DOM 与全局镜像（不动任何控制台的 preview / 轮询）。 */
function clearSqlResultDom() {
    sqlQueryId = null;
    sqlRunning = false;
    sqlSortCol = -1;
    sqlSortDir = 0;
    sqlTransposed = false;
    sqlLastResult = null;
    sqlElapsedMs = 0;
    sqlErrStreak = 0;
    lastSqlText = '';
    sqlRanSelection = false;
    sqlAutoLimited = false;
    sqlBatchCount = 0;

    var box = document.getElementById('sql-result');
    if (box) box.style.display = 'none';
    var splitter = document.getElementById('sql-splitter');
    if (splitter) splitter.style.display = 'none';
    var idEl = document.getElementById('sql-result-id');
    if (idEl) idEl.textContent = '';
    var note = document.getElementById('sql-result-note');
    if (note) { note.textContent = ''; note.style.color = ''; }
    var el = document.getElementById('sql-elapsed');
    if (el) { el.textContent = ''; el.classList.remove('running'); }
    var headEl = document.getElementById('sql-result-head');
    if (headEl) headEl.innerHTML = '';
    var bodyEl = document.getElementById('sql-result-body');
    if (bodyEl) bodyEl.innerHTML = '';
    var errEl = document.getElementById('query-error');
    if (errEl) errEl.textContent = '';
    hideSqlError();
    setSqlRunning(false);
    syncResultTools();
}

/**
 * 切走前把「只存在于界面」的状态回写到当前控制台（目前只有表头排序）。
 * queryId / running / lastResult / 报错由各自的轮询线程直接维护，不在这里覆盖。
 */
function captureSqlResultUi() {
    var p = activePreview();
    if (!p) return;
    p.sortCol = sqlSortCol;
    p.sortDir = sqlSortDir;
    p.transposed = sqlTransposed;
}

/** 切回某控制台：恢复它的结果区（顶部临时错误不跨控制台，图表按新结果重建）。 */
function restoreSqlResultUi() {
    var errEl = document.getElementById('query-error');
    if (errEl) errEl.textContent = '';
    sqlChartReset();
    renderActivePreview();
}

/**
 * 重置当前控制台的结果预览（显式清场；切标签<b>不再</b>调用本函数）。
 * 该控制台在服务端的查询不会被取消，只是不再展示 —— 需要取消请点「取消」。
 */
function resetSqlResultUi() {
    var c = activeConsole();
    if (c) {
        stopPreviewPoll(c.id);
        if (typeof makeConsolePreview === 'function') c.preview = makeConsolePreview();
    }
    sqlChartReset();
    clearSqlResultDom();
    maybeStopSqlTickTimer();
}

/**
 * 轮询某个控制台的查询结果。控制台被关闭、或该控制台又提交了新查询时，
 * 迟到的响应一律丢弃（用 preview 对象身份 + queryId 双重校验）。
 */
function pollConsoleResult(consoleId) {
    var c = (typeof findConsole === 'function') ? findConsole(consoleId) : null;
    var p = c ? consolePreview(c) : null;
    if (!p || !p.queryId) return Promise.resolve();
    var queryId = p.queryId;
    return httpGet('/api/query/' + encodeURIComponent(queryId) + '/rows?offset=0&limit=' + SQL_PREVIEW_LIMIT)
        .then(function (r) {
            var cur = (typeof findConsole === 'function') ? findConsole(consoleId) : null;
            if (!cur || cur.preview !== p || p.queryId !== queryId) return;   // 控制台关闭 / 查询被替换
            p.errStreak = 0;
            var done = (r.state === 'FINISHED' || r.state === 'FAILED' || r.state === 'CANCELED');
            p.lastResult = r;
            p.running = !done;
            p.submitPending = false;
            if (done) {
                p.elapsedMs = Date.now() - p.startMs;
                stopPreviewPoll(consoleId);
                maybeStopSqlTickTimer();
            }
            if (consoleId === activeConsoleId) renderActivePreview();
        })
        .catch(function (err) {
            var cur = (typeof findConsole === 'function') ? findConsole(consoleId) : null;
            if (!cur || cur.preview !== p || p.queryId !== queryId) return;
            // 不再静默吞错：把原因显示出来，否则「查了没反应」无从排查。
            // 单次抖动不清场，连续失败 SQL_ERR_STREAK_MAX 次才停表。
            p.errStreak++;
            var msg = oneLine(err && err.message ? err.message : err, 140);
            p.lastError = '结果预览获取失败：' + msg;
            if (err && err.status === 503) {
                if (consoleId === activeConsoleId) {
                    setBanner('控制台未绑定查询监控（独立预览模式），SQL 执行不可用。');
                }
                p.errStreak = SQL_ERR_STREAK_MAX;
            }
            if (p.errStreak >= SQL_ERR_STREAK_MAX) {
                p.running = false;
                p.elapsedMs = Date.now() - p.startMs;
                stopPreviewPoll(consoleId);
                maybeStopSqlTickTimer();
            }
            if (consoleId === activeConsoleId) renderActivePreview();
        });
}

/** 刷新当前控制台的结果（顶栏刷新按钮 / 兼容旧调用）。 */
function pollSqlResult() {
    return pollConsoleResult(activeConsoleId);
}

// ---------------- 查询出错卡片：报错居中 + 复制 / AI 修复 ----------------

/**
 * 展示出错卡片（结果表格让位）。
 *
 * @param message 一行 cause 链（{@code A <- B <- C}，真因在链尾）—— 直接贴在卡片主区
 * @param detail  完整多行堆栈（可空）—— 折进「查看完整堆栈」面板，默认收起，供人 / AI 定位到代码行
 */
function showSqlError(message, detail) {
    var msg = message || '未知错误';
    lastSqlError = msg;
    lastSqlErrorDetail = detail || '';
    var msgEl = document.getElementById('sql-error-msg');
    var card = document.getElementById('sql-error-card');
    var wrap = document.getElementById('sql-result-wrap');
    var detailBox = document.getElementById('sql-error-detail');
    var detailPre = document.getElementById('sql-error-detail-pre');
    if (msgEl) msgEl.textContent = msg;
    if (detailBox) {
        if (detail) {
            if (detailPre) detailPre.textContent = detail;
            detailBox.style.display = '';
            detailBox.open = false;   // 默认折叠：主区只给一行链，堆栈按需展开
        } else {
            if (detailPre) detailPre.textContent = '';
            detailBox.style.display = 'none';
            detailBox.open = false;
        }
    }
    if (card) card.style.display = '';
    if (wrap) wrap.style.display = 'none';
}

function hideSqlError() {
    lastSqlError = '';
    lastSqlErrorDetail = '';
    var card = document.getElementById('sql-error-card');
    var wrap = document.getElementById('sql-result-wrap');
    var detailBox = document.getElementById('sql-error-detail');
    if (card) card.style.display = 'none';
    if (wrap) wrap.style.display = '';
    if (detailBox) { detailBox.open = false; detailBox.style.display = 'none'; }
}

/**
 * 出错卡片「复制 / 用 AI 修复」用的 SQL：优先用<b>本次真正执行的那段</b>
 * （有选区时是选中片段，不是整框），拿不到才退回编辑器全文。都不带 UI 自动补的 limit。
 */
function currentSqlText() {
    if (lastSqlText) return lastSqlText;
    var input = document.getElementById('query-input');
    return input && input.value ? input.value.trim() : '';
}

/** 复制「SQL + 报错」到剪贴板：方便贴给同事或贴进 AI 对话。反馈打在按钮上（首页无顶栏横幅）。 */
function copySqlError() {
    var text = 'SQL:\n' + currentSqlText() + '\n\n错误:\n' + (lastSqlError || '');
    if (lastSqlErrorDetail) text += '\n\n完整堆栈:\n' + lastSqlErrorDetail;
    var btn = document.getElementById('err-copy-btn');
    var feedback = function (okFlag) {
        if (!btn) return;
        var old = btn.innerHTML;
        btn.innerHTML = okFlag ? '\u2713 \u5df2\u590d\u5236' : '\u590d\u5236\u5931\u8d25';
        setTimeout(function () { btn.innerHTML = old; }, 1500);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
        // http://localhost 属安全上下文；非安全上下文（远端 IP 访问）走兜底 fallbackCopy（simulation.js）
        navigator.clipboard.writeText(text)
            .then(function () { feedback(true); }, function () { feedback(!!fallbackCopy(text)); });
    } else {
        feedback(!!fallbackCopy(text));
    }
}

/** 「用 AI 修复」：切到 AI 页，预填 SQL + 报错并<b>自动发起</b>一次修复提问。 */
function askAiToFix() {
    var prompt = '\u4ee5\u4e0b SQL \u6267\u884c\u62a5\u9519\uff0c\u8bf7\u5e2e\u6211\u4fee\u590d\uff1a\n\n'
            + currentSqlText() + '\n\n\u62a5\u9519\u4fe1\u606f\uff1a\n' + (lastSqlError || '')
            + (lastSqlErrorDetail ? '\n\n\u5b8c\u6574\u5806\u6808\uff1a\n' + lastSqlErrorDetail : '');
    var input = document.getElementById('ai-input');
    if (input) input.value = prompt;
    switchTab('ai');                       // 切页时已重算一次高度（见 switchTab）
    if (aiInputGrow) aiInputGrow();        // 预填的是多行文本，切完再撑一次（防差一帧）
    if (input) input.focus();
    sendAiMessage(prompt);                 // 一键修复：直接发问
}

/**
 * 一次发送的流式渲染状态。
 * 每轮对话 = 一个 .ai-turn：
 *   - .ai-user：本轮用户提问（纯文本气泡）；
 *   - .ai-trace（&lt;details&gt;，默认<b>收起</b>）：推理链（思考 + 工具调用/结果），
 *     摘要只写「做了哪些重要操作」，展开后正文限高滚动 —— 再长也不占满整屏；
 *   - .ai-answer：最终回答，按 Markdown 渲染。
 * 回答文本经 renderMarkdown（内部先转义）写入，其余全用 textContent ⇒ 无 XSS。
 */
var aiStream = null;
/** 对话区是否「吸附在底部」：用户手动上滚则不再自动滚，保证能回看历史。 */
var aiHeroStick = true;
/** 当前会话 id（null = 尚未落库的「新对话」，首次发送时创建）。 */
var aiSessionId = null;
/** 会话清单 [{id,title,updatedAt}]（左侧栏用）。 */
var aiSessions = [];
/** 是否有推理在跑（跑动期间禁止再次发送 / 切换会话）。 */
var aiBusy = false;
/** 当前挂起的 DDL 确认框（同一时刻只允许一个）。 */
var aiPendingConfirm = null;
/** 当前流式请求的 AbortController（点「停止」时 abort 掉 fetch）。 */
var aiAbort = null;
/** 已实例化的 ECharts 实例 [{chart, el}]（切会话 / 新对话时统一 dispose，避免泄漏）。 */
var aiCharts = [];
/** 尚未实例化的图表 [{el, spec}]（容器隐藏时量不到尺寸，等可见后再 init）。 */
var aiPendingCharts = [];
/** 技能清单 [{id,name,description,tools[]}]（composer 下拉用）。 */
var aiSkills = [];
/** 当前选中的技能 id（'' = 不使用技能）。 */
var aiSkillId = '';
/** 可绑定的工具名目录（技能管理表单用；取自 /api/ai/skills 响应）。 */
var aiAvailableTools = [];
/** 本轮待发送的附件（服务端 /api/ai/files/parse 解析后的结果）。 */
var aiAttachments = [];
/** 附件正在上传解析（期间禁止再次选文件 / 发送，避免顺序错乱）。 */
var aiAttachBusy = false;
/** 附件配额（与服务端 FileParser 的常量一致）。 */
var AI_ATTACH_MAX_FILES = 5;
var AI_ATTACH_MAX_BYTES = 10 * 1024 * 1024;
/** 记住「当前正在聊的会话 id」的 localStorage 键：刷新页面后据此回到原会话。 */
var AI_SESSION_KEY = 'dojao.ai.session';

/** 读取上次所在会话 id（无 / 不可用 → null）。 */
function aiStoredSessionId() {
    try { return window.localStorage ? window.localStorage.getItem(AI_SESSION_KEY) : null; }
    catch (e) { return null; }
}

/** 记住当前会话 id（id 为空则清除）。刷新后 refreshAiSessions 会据此恢复原会话。 */
function aiStoreSession(id) {
    try {
        if (!window.localStorage) return;
        if (id) window.localStorage.setItem(AI_SESSION_KEY, id);
        else window.localStorage.removeItem(AI_SESSION_KEY);
    } catch (e) { /* localStorage 不可用则忽略：仅退化为「回到最近会话」 */ }
}

/** 轮询「答案还没生成完」的末轮所用的定时器 id（null = 无）。 */
var aiPendPollTimer = null;
/** 上一次渲染的轮次指纹（轮询时只在内容有变化时重绘，避免无谓刷新图表/滚动位置）。 */
var aiLastTurnsSig = '';
/** 丢弃当前轮询（切会话 / 新对话 / 重新发送时调用）。 */
function aiStopPendingPoll() {
    if (aiPendPollTimer) { clearTimeout(aiPendPollTimer); aiPendPollTimer = null; }
}

/** 轮次内容指纹：回答长度 + 推理事件数 + 末条思考长度 + pending 状态。 */
function aiTurnsSignature(turns) {
    var out = [];
    for (var i = 0; i < (turns || []).length; i++) {
        var t = turns[i] || {};
        var tr = t.trace || [];
        var lastEv = tr.length ? tr[tr.length - 1] : null;
        out.push((t.turnId || '') + ':' + ((t.assistant || '').length)
            + ':' + tr.length + ':' + (lastEv && lastEv.t ? String(lastEv.t).length : 0)
            + ':' + (t.pending ? '1' : '0'));
    }
    return out.join('|');
}

/**
 * 有界轮询：若某会话末轮仍是 pending（刷新 / 断线时答案还在服务端生成），
 * 每隔 3 秒重取一次会话：
 *   - 内容有变化（思考 / 工具调用 / 半成品回答增量已落库）→ 重绘，实时看到当前进度；
 *   - pending 消失（服务端 finalizeTurn 已落库）→ 重绘最终答案与完整推理链；
 *   - 长时间无变化（模型卡住 / 服务端重启）→ 达到上限后放弃，用户可手动重开会话。
 *
 * @param sessionId 目标会话（一旦与当前 aiSessionId 不符即停止，避免串会话）
 * @param attempt   已尝试次数（上限 80 次 × 3s ≈ 4 分钟「无进展」）
 */
function aiPollPendingAnswer(sessionId, attempt) {
    aiStopPendingPoll();
    if (!sessionId || sessionId !== aiSessionId || aiBusy) return;
    if (attempt >= 80) {
        // ~4 分钟无任何进展：多半是服务端重启 / 生成线程已中断。把提示改成「可能已中断」，
        // 让用户知道不是页面没刷新，而是这一轮确实停住了。
        var notes = document.querySelectorAll('#ai-reply .ai-pending-note');
        if (notes.length) {
            var note = notes[notes.length - 1];
            note.textContent = '\u672c\u8f6e\u751f\u6210\u53ef\u80fd\u5df2\u4e2d\u65ad\uff08\u957f\u65f6\u95f4\u65e0\u8fdb\u5c55\uff09\uff0c\u53ef\u91cd\u65b0\u63d0\u95ee\u6216\u7a0d\u540e\u91cd\u65b0\u6253\u5f00\u672c\u4f1a\u8bdd\u67e5\u770b\u3002';
            note.classList.add('ai-pending-stale');
        }
        return;
    }
    aiPendPollTimer = setTimeout(function () {
        if (sessionId !== aiSessionId || aiBusy) return;
        fetch('/api/ai/sessions/' + encodeURIComponent(sessionId), { credentials: 'same-origin' })
            .then(function (r) { return r.ok ? r.json() : null; })
            .then(function (s) {
                if (!s || sessionId !== aiSessionId) return;
                var turns = s.turns || [];
                var last = turns.length ? turns[turns.length - 1] : null;
                if (!last) { aiStopPendingPoll(); return; }
                var sig = aiTurnsSignature(turns);
                if (sig !== aiLastTurnsSig) {
                    // 进度有变化（或刚好完成）→ 整体重绘；renderAiSession 内部会继续/停止轮询。
                    // preserveScroll=true：保持用户当前阅读位置，不被每 3 秒的刷新拽回底部。
                    renderAiSession(s, true);
                    return;
                }
                if (!last.pending) { aiStopPendingPoll(); return; }
                aiPollPendingAnswer(sessionId, attempt + 1);
            })
            .catch(function () { aiPollPendingAnswer(sessionId, attempt + 1); });
    }, 3000);
}

/** 生成流 id（DDL 确认时前后端据此关联）。 */
function newStreamId() {
    if (window.crypto && typeof crypto.randomUUID === 'function') return crypto.randomUUID();
    return 's' + Date.now() + '-' + Math.random().toString(16).slice(2);
}

// ==================== AI 附件（参考文件）====================

/** 附件 → 请求体形状（与服务端 FileParser.ParsedFile 一致）。 */
function aiAttachmentPayload(a) {
    return { name: a.name, format: a.format, chars: a.chars,
        truncated: !!a.truncated, text: a.text };
}

/** 渲染待发送附件条（无附件时整条隐藏）。 */
function aiRenderAttachments() {
    var bar = document.getElementById('ai-attach-bar');
    if (!bar) return;
    bar.textContent = '';
    if (!aiAttachments.length) { bar.hidden = true; return; }
    bar.hidden = false;
    aiAttachments.forEach(function (a, idx) {
        var chip = document.createElement('span');
        chip.className = 'ai-attach-chip';
        var name = document.createElement('span');
        name.className = 'ai-attach-name';
        name.textContent = a.name || '附件';
        name.title = (a.format ? a.format + ' · ' : '') + (a.chars || 0) + ' 字'
            + (a.truncated ? '（内容过长已截断）' : '');
        var meta = document.createElement('span');
        meta.className = 'ai-attach-meta';
        meta.textContent = aiAttachSizeLabel(a);
        var del = document.createElement('button');
        del.type = 'button';
        del.className = 'ai-attach-del';
        del.textContent = '\u00d7';
        del.title = '移除该附件';
        del.addEventListener('click', function () { aiRemoveAttachment(idx); });
        chip.appendChild(name);
        chip.appendChild(meta);
        chip.appendChild(del);
        bar.appendChild(chip);
    });
}

/** 附件字数标签：1234 → 1.2K 字。 */
function aiAttachSizeLabel(a) {
    var n = a && a.chars ? a.chars : 0;
    return n >= 1000 ? (Math.round(n / 100) / 10) + 'K 字' : n + ' 字';
}

function aiRemoveAttachment(idx) {
    if (idx < 0 || idx >= aiAttachments.length) return;
    aiAttachments.splice(idx, 1);
    aiRenderAttachments();
}

function aiClearAttachments() {
    aiAttachments = [];
    aiRenderAttachments();
}

/** 解析中态：回形针置灰 + 旋转样式（避免并发选择导致顺序错乱）。 */
function aiSetAttachBusy(busy) {
    aiAttachBusy = !!busy;
    var btn = document.getElementById('ai-attach');
    if (!btn) return;
    btn.disabled = !!busy;
    btn.classList.toggle('busy', !!busy);
    btn.title = busy ? '正在解析文件…' : '添加参考文件（txt / md / csv / excel / word 等）';
}

/**
 * 用户选好文件：逐个上传解析（单个失败只提示并跳过，不影响其它文件）。
 * 配额与服务端一致：单条消息 ≤ 5 个、单文件 ≤ 10MB。
 */
function aiHandleAttachmentFiles(files) {
    var list = [];
    for (var i = 0; i < (files || []).length; i++) list.push(files[i]);
    if (!list.length) return;
    if (aiBusy) { showToast('请等待本轮推理结束后再添加附件'); return; }
    var room = AI_ATTACH_MAX_FILES - aiAttachments.length;
    if (room <= 0) { showToast('单条消息最多 ' + AI_ATTACH_MAX_FILES + ' 个附件'); return; }
    if (list.length > room) {
        showToast('最多再添加 ' + room + ' 个附件（上限 ' + AI_ATTACH_MAX_FILES + ' 个）');
        list = list.slice(0, room);
    }
    aiSetAttachBusy(true);
    var chain = Promise.resolve();
    list.forEach(function (f) {
        chain = chain.then(function () { return aiUploadAttachment(f); });
    });
    chain.then(function () { aiSetAttachBusy(false); aiRenderAttachments(); });
}

/** 上传单个文件到 /api/ai/files/parse，成功后追加到待发送附件。 */
function aiUploadAttachment(file) {
    if (!file) return Promise.resolve();
    if (file.size > AI_ATTACH_MAX_BYTES) {
        showToast('「' + file.name + '」超过单文件上限 '
            + (AI_ATTACH_MAX_BYTES / 1024 / 1024) + 'MB');
        return Promise.resolve();
    }
    var form = new FormData();
    // 字段名任意：服务端取第一个上传文件。
    form.append('file', file, file.name);
    return fetch('/api/ai/files/parse', {
        method: 'POST',
        credentials: 'same-origin',
        body: form
    }).then(function (resp) {
        return resp.json().catch(function () { return {}; }).then(function (j) {
            if (!resp.ok) throw new Error((j && j.error) ? j.error : ('HTTP ' + resp.status));
            return j;
        });
    }).then(function (j) {
        if (!j || !j.text) throw new Error('文件内容为空');
        aiAttachments.push({
            name: j.name || file.name,
            format: j.format || '',
            chars: j.chars || 0,
            truncated: !!j.truncated,
            text: j.text
        });
        aiRenderAttachments();
    }).catch(function (e) {
        showToast('「' + file.name + '」解析失败：' + (e && e.message ? e.message : e));
    });
}

// ==================== AI 拖拽添加附件 ====================

/** 拖拽事件里是否真的带文件（排除拖动选中文字 / 页面 DOM 元素）。 */
function aiDragHasFiles(e) {
    var dt = e && e.dataTransfer;
    if (!dt || !dt.types) return false;
    for (var i = 0; i < dt.types.length; i++) {
        if (dt.types[i] === 'Files') return true;
    }
    return false;
}

/** 显示 / 隐藏拖拽落区提示层。 */
function aiSetDropActive(on) {
    var overlay = document.getElementById('ai-drop-overlay');
    if (!overlay) return;
    overlay.hidden = !on;
}

/**
 * 从 DataTransfer 取文件。文件夹不递归展开（直接跳过并提示）——
 * 浏览器只在 webkitGetAsEntry 上暴露 isDirectory，取不到 entry 时按普通文件处理。
 */
function aiCollectDroppedFiles(dt) {
    var files = [];
    var dirs = 0;
    var items = (dt && dt.items) ? dt.items : null;
    if (items && items.length) {
        for (var i = 0; i < items.length; i++) {
            var it = items[i];
            if (!it || it.kind !== 'file') continue;
            var entry = null;
            try { entry = it.webkitGetAsEntry ? it.webkitGetAsEntry() : null; } catch (err) { entry = null; }
            if (entry && entry.isDirectory) { dirs++; continue; }
            var f = it.getAsFile ? it.getAsFile() : null;
            if (f) files.push(f);
        }
    } else if (dt && dt.files) {
        for (var j = 0; j < dt.files.length; j++) files.push(dt.files[j]);
    }
    if (dirs > 0) showToast('暂不支持拖拽文件夹，请拖入具体文件');
    return files;
}

/**
 * 绑定 AI 对话主区文件拖拽。
 *
 * <p>只挂在 #ai-main（拖到会话列表 / 其它页面不接管）；文件最终走
 * aiHandleAttachmentFiles，因此配额（≤5 个）、大小（≤10MB）、顺序解析
 * 与回形针按钮完全一致。</p>
 *
 * <p>用 dragenter/dragleave 深度计数抵消子元素间移动造成的 enter/leave 抖动，
 * 否则提示层会闪。捕获阶段监听：文本输入框不会把文件拖拽事件当文本处理。</p>
 */
function bindAiDropZone() {
    var view = document.getElementById('ai-main');
    if (!view) return;
    var depth = 0;
    var reset = function () { depth = 0; aiSetDropActive(false); };

    view.addEventListener('dragenter', function (e) {
        if (!aiDragHasFiles(e)) return;
        e.preventDefault();
        depth++;
        aiSetDropActive(true);
    }, true);

    view.addEventListener('dragover', function (e) {
        if (!aiDragHasFiles(e)) return;
        // 必须 preventDefault：否则浏览器按「打开文件」处理，drop 永远不触发
        e.preventDefault();
        if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
        aiSetDropActive(true);
    }, true);

    // dragleave 不再判 types（部分浏览器此时已清空 types）；深度只在 enter 时增加，故安全。
    view.addEventListener('dragleave', function () {
        if (depth > 0) depth--;
        if (depth === 0) aiSetDropActive(false);
    }, true);

    view.addEventListener('drop', function (e) {
        if (!aiDragHasFiles(e)) return;
        e.preventDefault();
        e.stopPropagation();
        reset();
        if (aiBusy) { showToast('请等待本轮推理结束后再添加附件'); return; }
        if (aiAttachBusy) { showToast('正在解析已拖入的文件，请稍候'); return; }
        var files = aiCollectDroppedFiles(e.dataTransfer);
        if (files.length) aiHandleAttachmentFiles(files);
    }, true);

    // 拖出窗口 / Esc 取消时浏览器不保证补发 dragleave：兜底清理，避免提示层卡住。
    window.addEventListener('dragend', reset);
    window.addEventListener('drop', reset);
    window.addEventListener('blur', reset);
}

/**
 * 向 /api/ai/chat/stream 发起 ReAct 推理，NDJSON 事件流式渲染进 #ai-reply。
 * 会话历史由服务端按 sessionId 读取并拼进上下文（最近 N 轮）；本轮完成后服务端落库。
 * 未配置模型 → 后端 503，错误文案显示在对话区。
 */
function sendAiMessage(text) {
    var message = (text == null ? '' : String(text)).trim();
    var attachments = aiAttachments.slice();
    // 允许「只带附件、不写提问」：附件本身就是本轮输入。
    if ((!message && !attachments.length) || aiBusy || aiAttachBusy) return;
    var replyEl = document.getElementById('ai-reply');
    if (!replyEl) return;

    aiStopPendingPoll();
    ensureAiSession().then(function (sessionId) {
        if (!sessionId) return;
        aiBusy = true;
        aiSetSendBusy(true);
        aiHeroStick = true;
        aiPendingConfirm = null;
        aiAbort = (typeof AbortController === 'function') ? new AbortController() : null;
        // 立即落本轮用户气泡（提问 + 附件标签）+ 占位推理链/回答容器
        aiStream = aiCreateTurnState(replyEl, message, aiCurrentSkillName(), attachments);
        aiStream.streamId = newStreamId();
        // 附件已随本轮发出：清空待发送区（发送失败时用户可重新添加）。
        aiClearAttachments();

        fetch('/api/ai/chat/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            signal: aiAbort ? aiAbort.signal : undefined,
            body: JSON.stringify({ sessionId: sessionId, streamId: aiStream.streamId,
                message: message, skillId: aiSkillId || null,
                attachments: attachments.map(aiAttachmentPayload) })
        }).then(function (resp) {
            if (!resp.ok) {
                return resp.json().catch(function () { return {}; }).then(function (j) {
                    throw new Error((j && j.error) ? j.error : ('HTTP ' + resp.status));
                });
            }
            if (!resp.body || !resp.body.getReader) {
                throw new Error('当前浏览器不支持流式响应');
            }
            var reader = resp.body.getReader();
            var decoder = new TextDecoder('utf-8');
            var buf = '';
            var pump = function () {
                return reader.read().then(function (res) {
                    if (res.done) {
                        if (buf) { handleAiLine(replyEl, buf); buf = ''; }
                        return;
                    }
                    buf += decoder.decode(res.value, { stream: true });
                    var idx;
                    while ((idx = buf.indexOf('\n')) >= 0) {
                        var line = buf.slice(0, idx);
                        buf = buf.slice(idx + 1);
                        if (line.trim()) handleAiLine(replyEl, line);
                    }
                    return pump();
                });
            };
            return pump();
        }).catch(function (err) {
            // 用户主动停止（abort）→ 走「已停止」收尾，不报错。
            if (err && err.name === 'AbortError') {
                aiFinishStopped(replyEl);
                return;
            }
            aiBusy = false;
            aiSetSendBusy(false);
            aiRenderError(replyEl, err && err.message ? err.message : String(err));
        });
    });
}

/** 切换发送键形态：空闲 = 圆形小飞机；推理中 = 红色方框（停止）。
 *  图标由 CSS 按 .busy 切换，这里只改 class / 无障碍文案，不动子元素。 */
function aiSetSendBusy(busy) {
    var btn = document.getElementById('ai-send');
    if (!btn) return;
    btn.classList.toggle('busy', !!busy);
    btn.title = busy ? '停止本次推理' : '发送';
    btn.setAttribute('aria-label', busy ? '停止本次推理' : '发送');
}

/** 用户点「停止」：abort 掉 fetch + 通知后端中断推理线程，并当场收尾。 */
function stopAiMessage() {
    if (!aiBusy) return;
    var sid = aiStream && aiStream.streamId;
    if (aiAbort) { try { aiAbort.abort(); } catch (e) { /* ignore */ } }
    if (sid) {
        fetch('/api/ai/stop', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify({ streamId: sid })
        }).catch(function () { /* 停止请求失败不影响前端收尾 */ });
    }
    aiFinishStopped(document.getElementById('ai-reply'));
}

/** 「已停止」收尾（幂等：仅在 busy 时生效，故 abort 的 catch 再调一次也无副作用）。 */
function aiFinishStopped(replyEl) {
    if (!aiBusy) return;
    aiBusy = false;
    aiAbort = null;
    aiPendingConfirm = null;
    aiSetSendBusy(false);
    if (aiStream && aiStream.turnEl) {
        aiStream.done = true;
        var note = document.createElement('div');
        note.className = 'ai-stopped';
        note.textContent = '\u5df2\u505c\u6b62\u672c\u6b21\u63a8\u7406\u3002';
        aiStream.turnEl.appendChild(note);
        aiUpdateTraceSummary();
    }
    aiScrollToBottom();
    refreshAiSessions(false);
}

function handleAiLine(replyEl, line) {
    var ev;
    try { ev = JSON.parse(line); } catch (e) { return; }
    renderAiEvent(replyEl, ev);
}

function renderAiEvent(replyEl, ev) {
    if (!ev || !ev.type) return;
    if (ev.type === 'text') {
        aiAppendText(replyEl, ev.text || '');
    } else if (ev.type === 'thinking') {
        aiAppendThinking(replyEl, ev.text || '');
    } else if (ev.type === 'tool_call') {
        aiAppendToolCall(replyEl, ev.name, ev.args);
    } else if (ev.type === 'tool_result') {
        aiAppendToolResult(replyEl, ev.name, ev.result);
    } else if (ev.type === 'ddl_confirm') {
        aiRenderConfirm(replyEl, ev);
    } else if (ev.type === 'chart') {
        aiRenderChart(replyEl, ev);
    } else if (ev.type === 'file') {
        aiRenderFile(replyEl, ev);
    } else if (ev.type === 'error') {
        aiRenderError(replyEl, ev.message || '\u672a\u77e5\u9519\u8bef');
    } else if (ev.type === 'done') {
        aiFinishTurn();
    }
}

/**
 * 建一个 .ai-turn 骨架（用户气泡 + 折叠推理链 + 回答容器），返回流式状态对象。
 * 流式首帧与历史回填共用。
 */
function aiCreateTurnState(replyEl, userText, skillName, attachments) {
    var turn = document.createElement('div');
    turn.className = 'ai-turn';
    var attachList = (attachments && attachments.length) ? attachments : [];
    if (userText || attachList.length) {
        var u = document.createElement('div');
        u.className = 'ai-user';
        if (skillName) {
            var badge = document.createElement('span');
            badge.className = 'ai-user-skill';
            badge.textContent = skillName;
            badge.title = '本轮使用的技能：' + skillName;
            u.appendChild(badge);
        }
        if (userText) {
            var text = document.createElement('span');
            text.className = 'ai-user-text';
            text.textContent = userText;
            u.appendChild(text);
        }
        if (attachList.length) {
            // 本轮携带的参考文件：只显示元信息（正文不落 DOM），标题悬浮可看字数 / 截断。
            var attBox = document.createElement('span');
            attBox.className = 'ai-user-attachments';
            for (var i = 0; i < attachList.length; i++) {
                var a = attachList[i] || {};
                var chip = document.createElement('span');
                chip.className = 'ai-user-attach';
                chip.textContent = a.name || '附件';
                chip.title = (a.format ? a.format + ' · ' : '') + (a.chars || 0) + ' 字'
                    + (a.truncated ? '（内容过长已截断）' : '');
                attBox.appendChild(chip);
            }
            u.appendChild(attBox);
        }
        turn.appendChild(u);
    }
    var trace = document.createElement('details');
    trace.className = 'ai-trace';
    trace.style.display = 'none';           // 无推理链（纯问答）时不占位
    var sum = document.createElement('summary');
    var body = document.createElement('div');
    body.className = 'ai-trace-body';
    trace.appendChild(sum);
    trace.appendChild(body);
    turn.appendChild(trace);                // 推理链永远在回答之上
    var ans = document.createElement('div');
    ans.className = 'ai-answer';
    ans.style.display = 'none';
    turn.appendChild(ans);
    replyEl.appendChild(turn);
    aiUpdateLanding();
    aiScrollToBottom();
    return {
        ops: [], done: false,
        turnEl: turn, traceEl: trace, traceSummaryEl: sum, traceBodyEl: body,
        answerEl: ans, answerRaw: '', thinkPre: null
    };
}

/** 取当前轮状态；无则新建一个空轮（兜底：出错等早于首事件的场景）。 */
function aiEnsureTurn(replyEl) {
    if (!aiStream) aiStream = aiCreateTurnState(replyEl, '');
    return aiStream;
}

function aiShowTrace() {
    if (aiStream.traceEl.style.display === 'none') {
        aiStream.traceEl.style.display = '';
    }
}

function aiEnsureAnswer(replyEl) {
    aiEnsureTurn(replyEl);
    if (aiStream.answerEl && aiStream.answerEl.style.display === 'none') {
        aiStream.answerEl.style.display = '';
    }
    return aiStream.answerEl;
}

function aiAppendText(replyEl, text) {
    if (!text) return;
    aiEnsureAnswer(replyEl);
    aiStream.answerRaw += text;
    aiScheduleAnswerRender();
}

/** 回答按 Markdown 增量重渲染（requestAnimationFrame 节流，避免每个字符都重排）。 */
function aiScheduleAnswerRender() {
    if (!aiStream || aiStream.renderScheduled) return;
    aiStream.renderScheduled = true;
    var run = function () {
        if (!aiStream) return;
        aiStream.renderScheduled = false;
        if (aiStream.answerEl) {
            if (typeof renderMarkdown === 'function') {
                aiStream.answerEl.innerHTML = renderMarkdown(aiStream.answerRaw);
            } else {
                aiStream.answerEl.textContent = aiStream.answerRaw;
            }
        }
        aiScrollToBottom();
    };
    if (window.requestAnimationFrame) window.requestAnimationFrame(run); else window.setTimeout(run, 16);
}

function aiAppendThinking(replyEl, text) {
    if (!text) return;
    aiEnsureTurn(replyEl);
    aiShowTrace();
    if (!aiStream.thinkPre) {
        var wrap = document.createElement('div');
        wrap.className = 'ai-thinking';
        var pre = document.createElement('pre');
        wrap.appendChild(pre);
        aiStream.traceBodyEl.appendChild(wrap);
        aiStream.thinkPre = pre;
    }
    aiStream.thinkPre.textContent += text;
    aiScrollTrace();
}

function aiAppendToolCall(replyEl, name, args) {
    aiEnsureTurn(replyEl);
    aiShowTrace();
    aiStream.thinkPre = null;
    aiStream.ops.push(name || 'tool');
    aiAppendTraceBlock('ai-tool-call', '\u8c03\u7528\u5de5\u5177 ', name, args);
    aiUpdateTraceSummary();
}

function aiAppendToolResult(replyEl, name, result) {
    aiEnsureTurn(replyEl);
    aiShowTrace();
    aiStream.thinkPre = null;
    aiAppendTraceBlock('ai-tool-result', '\u8fd4\u56de\u7ed3\u679c ', name, result);
}

function aiAppendTraceBlock(cls, label, name, body) {
    var div = document.createElement('div');
    div.className = cls;
    var head = document.createElement('div');
    head.className = cls + '-head';
    head.textContent = label + (name == null ? '' : name);
    var pre = document.createElement('pre');
    pre.textContent = body == null ? '' : String(body);
    div.appendChild(head);
    div.appendChild(pre);
    aiStream.traceBodyEl.appendChild(div);
    aiScrollTrace();
}

/** 摘要只写「做了哪些重要操作」（工具名链），让用户在收起态一眼看到进展。 */
function aiUpdateTraceSummary() {
    if (!aiStream || !aiStream.traceSummaryEl) return;
    var ops = aiStream.ops || [];
    var txt;
    if (ops.length === 0) {
        txt = aiStream.done ? '\u63a8\u7406\u8fc7\u7a0b' : '\u601d\u8003\u4e2d\u2026';
    } else {
        var shown = ops.slice(0, 6).join(' \u2192 ');
        if (ops.length > 6) shown += ' \u2192 \u2026';
        txt = (aiStream.done ? '\u63a8\u7406\u8fc7\u7a0b' : '\u63a8\u7406\u4e2d')
                + '\uff1a' + shown + '\uff08' + ops.length + ' \u6b65\uff09';
    }
    aiStream.traceSummaryEl.textContent = txt;
}

function aiFinishTurn() {
    aiBusy = false;
    aiAbort = null;
    aiPendingConfirm = null;
    aiSetSendBusy(false);
    if (aiStream) {
        aiStream.done = true;
        aiUpdateTraceSummary();
        aiScrollToBottom();
    }
    // 后端在发 done 前已把本轮落库 ⇒ 现在刷新列表即可拿到新标题 / 新会话。
    refreshAiSessions(false);
}

/** 对话区吸底滚动：仅当用户在底部附近时才跟随（上滚回看时不被拽走）。 */
function aiScrollToBottom() {
    var box = document.getElementById('ai-hero');
    if (box && aiHeroStick) box.scrollTop = box.scrollHeight;
}

/** 推理链正文滚动：展开态下把最新一行滚进视野。 */
function aiScrollTrace() {
    var b = aiStream && aiStream.traceBodyEl;
    if (b && aiStream.traceEl.open) b.scrollTop = b.scrollHeight;
}

function aiRenderError(replyEl, msg) {
    aiBusy = false;
    aiAbort = null;
    aiSetSendBusy(false);
    aiEnsureTurn(replyEl);
    var div = document.createElement('div');
    div.className = 'ai-reply-error';
    div.textContent = '\u51fa\u9519\uff1a' + msg;
    aiStream.turnEl.appendChild(div);
    aiScrollToBottom();
}

/** 空态（尚无对话）：标题 + 输入框整组垂直居中（ChatGPT 起手位）。有对话则恢复吸底布局。 */
function aiUpdateLanding() {
    var view = document.getElementById('ai-view');
    var reply = document.getElementById('ai-reply');
    if (!view || !reply) return;
    view.classList.toggle('ai-landing', reply.children.length === 0);
}

/**
 * AI 代码区「复制全部」按钮（Markdown 代码块 .md-code / DDL 确认卡 .ai-confirm 共用）。
 * 流式回答每帧重绘 innerHTML 会重建按钮，故用事件委托，按钮本身不绑事件。
 */
function bindAiCodeCopy() {
    document.addEventListener('click', function (e) {
        var btn = e.target && e.target.closest ? e.target.closest('.md-code-copy') : null;
        if (!btn) return;
        var box = btn.closest('.md-code') || btn.closest('.ai-confirm');
        var pre = box ? box.querySelector('pre') : null;
        if (!pre) return;
        var codeEl = pre.querySelector('code') || pre;
        var text = codeEl.textContent || '';
        var done = function (ok) {
            btn.textContent = ok ? '\u5df2\u590d\u5236' : '\u590d\u5236\u5931\u8d25';
            btn.classList.toggle('copied', !!ok);
            setTimeout(function () {
                btn.textContent = '\u590d\u5236';
                btn.classList.remove('copied');
            }, 1500);
        };
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text)
                .then(function () { done(true); }, function () { done(!!fallbackCopy(text)); });
        } else {
            done(!!fallbackCopy(text));
        }
    });
}

/** 绑定 AI 页交互：发送按钮 + Enter 发送 / Ctrl+Enter 换行 + 新对话 + 对话区滚动吸附。 */
function bindAiChat() {
    bindAiCodeCopy();
    var sendBtn = document.getElementById('ai-send');
    var input = document.getElementById('ai-input');
    if (sendBtn) {
        sendBtn.addEventListener('click', function () {
            // 推理中 → 本按钮是「停止」，点了即强制终止；空闲 → 正常发送。
            if (aiBusy) { stopAiMessage(); return; }
            if (input) {
                var v = input.value;
                input.value = '';
                if (aiInputGrow) aiInputGrow();
                sendAiMessage(v);
            }
        });
    }
    if (input) {
        input.addEventListener('keydown', function (e) {
            // Enter 直接发送；Ctrl/Shift/Alt/⌘ + Enter 一律留给默认行为（换行）。
            // isComposing：中文输入法组字中的回车是「选词确认」，绝不能当发送。
            if (e.key === 'Enter' && !e.ctrlKey && !e.metaKey && !e.shiftKey && !e.altKey && !e.isComposing) {
                e.preventDefault();
                if (aiBusy) return;      // 推理中按 Enter 不误触「停止」（停止请点红色按钮）
                if (sendBtn) sendBtn.click();
            }
        });
    }
    // 附件：回形针按钮 → 隐藏 file input；选完立刻清空 input.value，同一个文件可重复选择。
    var attachBtn = document.getElementById('ai-attach');
    var fileInput = document.getElementById('ai-file-input');
    if (attachBtn && fileInput) {
        attachBtn.addEventListener('click', function () {
            if (aiBusy || aiAttachBusy) return;
            fileInput.value = '';
            fileInput.click();
        });
        fileInput.addEventListener('change', function () {
            // FileList 是活引用：先拷成数组再清空 input（否则清空后拿不到文件）。
            var picked = [];
            for (var i = 0; i < fileInput.files.length; i++) picked.push(fileInput.files[i]);
            fileInput.value = '';
            aiHandleAttachmentFiles(picked);
        });
    }
    // 拖拽：把文件直接拖进对话主区，等同于「点回形针选文件」。
    bindAiDropZone();
    var newBtn = document.getElementById('ai-newchat');
    if (newBtn) newBtn.addEventListener('click', newAiChat);
    // 技能下拉：选中项决定本轮推理的提示词与工具集。
    var skillSel = document.getElementById('ai-skill-select');
    if (skillSel) {
        skillSel.addEventListener('change', function () {
            aiSkillId = skillSel.value || '';
        });
    }
    var skillManageBtn = document.getElementById('ai-skill-manage');
    if (skillManageBtn && typeof openSkillManager === 'function') {
        skillManageBtn.addEventListener('click', function () { openSkillManager(); });
    }
    // 用户手动上滚离底 ⇒ 停止自动跟随，允许回看历史；回到底部附近再恢复吸附。
    var hero = document.getElementById('ai-hero');
    if (hero) {
        hero.addEventListener('scroll', function () {
            aiHeroStick = (hero.scrollHeight - hero.scrollTop - hero.clientHeight) < 40;
        });
    }
    // 窗口尺寸变化：已实例化的 ECharts 重新自适应（挂起未 init 的顺便补 init）。
    window.addEventListener('resize', aiFlushCharts);
    // 主题切换：charts.js 的 applyChartTheme 被 state.js 调用，拦一层顺带重建 AI 图表。
    if (typeof window.applyChartTheme === 'function' && !window.__aiChartThemeHooked) {
        var _origApplyChartTheme = window.applyChartTheme;
        window.applyChartTheme = function (dark) {
            _origApplyChartTheme(dark);
            aiReThemeCharts();
        };
        window.__aiChartThemeHooked = true;
    }
    // 首屏拉会话列表，并自动打开最近一次会话（有则续聊）。
    refreshAiSessions(true);
    refreshAiSkills();
}

// ==================== AI 长对话（会话）====================

/** 确保有会话：无则新建一个空会话（落库）；返回 sessionId（失败返回 null）。 */
function ensureAiSession() {
    if (aiSessionId) return Promise.resolve(aiSessionId);
    return fetch('/api/ai/sessions', { method: 'POST', credentials: 'same-origin' })
        .then(function (r) {
            if (r.ok) return r.json();
            return r.json().catch(function () { return {}; }).then(function (j) {
                throw new Error((j && j.error) ? j.error : ('HTTP ' + r.status));
            });
        })
        .then(function (s) {
            aiSessionId = s.id;
            aiStoreSession(aiSessionId);
            return aiSessionId;
        })
        .catch(function (e) {
            aiRenderError(document.getElementById('ai-reply'), e.message || String(e));
            return null;
        });
}

/** 拉取会话清单；autoOpen=true 且当前无会话时，优先恢复「上次所在的会话」（刷新前在聊的那个），
 *  该会话已不存在（被删 / 换浏览器）则回落到最近一次会话。 */
function refreshAiSessions(autoOpen) {
    return fetch('/api/ai/sessions', { credentials: 'same-origin' })
        .then(function (r) { return r.ok ? r.json() : { sessions: [] }; })
        .then(function (j) {
            aiSessions = (j && j.sessions) || [];
            renderAiSessionList();
            if (autoOpen && !aiSessionId && aiSessions.length) {
                var last = aiStoredSessionId();
                var stillThere = last && aiSessions.some(function (s) { return s.id === last; });
                openAiSession(stillThere ? last : aiSessions[0].id);
            }
        })
        .catch(function () { /* 拉取失败静默：不影响当前视图 */ });
}

// ==================== AI 技能（Skill）====================

/** 拉取技能清单并填充 composer 下拉（保持当前选择；选择已失效则回落到「不使用技能」）。 */
function refreshAiSkills() {
    return fetch('/api/ai/skills', { credentials: 'same-origin' })
        .then(function (r) { return r.ok ? r.json() : { skills: [], availableTools: [] }; })
        .then(function (j) {
            aiSkills = (j && j.skills) || [];
            aiAvailableTools = (j && j.availableTools) || [];
            renderAiSkillSelect();
        })
        .catch(function () { /* 拉取失败静默 */ });
}

/** 填充技能下拉：首项「（不使用技能）」，其余为技能名（value = id）。 */
function renderAiSkillSelect() {
    var sel = document.getElementById('ai-skill-select');
    if (!sel) return;
    sel.textContent = '';
    var none = document.createElement('option');
    none.value = '';
    none.textContent = '（不使用技能）';
    sel.appendChild(none);
    aiSkills.forEach(function (s) {
        var opt = document.createElement('option');
        opt.value = s.id;
        opt.textContent = s.name || '未命名技能';
        if (s.description) opt.title = s.description;
        sel.appendChild(opt);
    });
    // 若当前选择已不存在（被删），回落并清空全局选择。
    var stillThere = aiSkills.some(function (s) { return s.id === aiSkillId; });
    if (aiSkillId && !stillThere) aiSkillId = '';
    sel.value = aiSkillId || '';
}

/** 当前选中技能的名称（未选 / 已失效 → null）。 */
function aiCurrentSkillName() {
    if (!aiSkillId) return null;
    for (var i = 0; i < aiSkills.length; i++) {
        if (aiSkills[i].id === aiSkillId) return aiSkills[i].name || null;
    }
    return null;
}

/** 渲染左侧会话列表。 */
function renderAiSessionList() {
    var list = document.getElementById('ai-session-list');
    if (!list) return;
    list.textContent = '';
    if (!aiSessions.length) {
        var empty = document.createElement('div');
        empty.className = 'ai-session-empty';
        empty.textContent = '\u6682\u65e0\u5386\u53f2\u5bf9\u8bdd';
        list.appendChild(empty);
        return;
    }
    aiSessions.forEach(function (s) {
        var item = document.createElement('div');
        item.className = 'ai-session-item' + (s.id === aiSessionId ? ' active' : '');
        var title = document.createElement('span');
        title.className = 'ai-session-title';
        title.textContent = s.title || '\u65b0\u5bf9\u8bdd';
        var del = document.createElement('button');
        del.type = 'button';
        del.className = 'ai-session-del';
        del.title = '\u5220\u9664\u5bf9\u8bdd';
        del.setAttribute('aria-label', '\u5220\u9664\u5bf9\u8bdd');
        del.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"'
                + ' stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
                + '<path d="M3 6h18M8 6V4h8v2M6 6l1 14h10l1-14M10 10v7M14 10v7"/></svg>';
        del.addEventListener('click', function (e) {
            e.stopPropagation();
            deleteAiSession(s.id);
        });
        item.appendChild(title);
        item.appendChild(del);
        item.addEventListener('click', function () { openAiSession(s.id); });
        list.appendChild(item);
    });
}

/** 打开某个会话：拉全文并重绘对话区。 */
function openAiSession(id) {
    if (aiBusy || !id) return;
    aiStopPendingPoll();
    fetch('/api/ai/sessions/' + encodeURIComponent(id), { credentials: 'same-origin' })
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (s) { if (s) renderAiSession(s); })
        .catch(function () {});
}

/**
 * 用会话全文重绘对话区（清空 → 逐轮回填）。
 *
 * @param preserveScroll true = 轮询增量刷新：保持用户当前滚动位置（用户正贴底则继续跟随）
 */
function renderAiSession(s, preserveScroll) {
    var replyEl = document.getElementById('ai-reply');
    if (!replyEl) return;
    var hero = document.getElementById('ai-hero');
    var keepTop = (preserveScroll && hero) ? hero.scrollTop : 0;
    var wasStick = aiHeroStick;
    aiSessionId = s.id;
    aiStoreSession(aiSessionId);
    aiStream = null;
    aiPendingConfirm = null;
    aiDisposeCharts();
    replyEl.textContent = '';
    var turns = (s && s.turns) || [];
    turns.forEach(function (t) { aiRenderHistoryTurn(replyEl, t); });
    renderAiSessionList();
    aiUpdateLanding();
    aiLastTurnsSig = aiTurnsSignature(turns);
    if (preserveScroll) {
        aiHeroStick = wasStick;
        if (wasStick) aiScrollToBottom();
        else if (hero) hero.scrollTop = keepTop;
    } else {
        aiHeroStick = true;
        aiScrollToBottom();
    }
    // 末轮还在「生成中」（刷新 / 断线中断）→ 轮询并把新增的思考 / 工具过程增量重绘；
    // 否则停掉旧轮询。
    var last = turns.length ? turns[turns.length - 1] : null;
    if (last && last.pending) aiPollPendingAnswer(s.id, 0);
    else aiStopPendingPoll();
}

/**
 * 回填一轮历史：用户气泡 + 收起态推理链（思考 / 工具调用 / 结果，可完整回溯）+ Markdown 回答。
 * 生成中的轮次（pending）会显示半成品回答与实时进度提示，并由轮询继续增量刷新。
 */
function aiRenderHistoryTurn(replyEl, t) {
    var st = aiCreateTurnState(replyEl, t.user || '', t.skill || null, t.attachments || []);
    var pending = !!t.pending;
    var ops = (t.ops && t.ops.length) ? t.ops.slice() : [];
    var trace = (t.trace && t.trace.length) ? t.trace : [];
    st.done = !pending;
    aiStream = st;                     // 让推理链渲染助手（aiAppendThinking 等）复用该轮状态
    if (trace.length) {
        // ★ 完整回放推理过程：思考文本 / 工具调用与参数 / 工具结果 —— 刷新页面后也不再丢失。
        for (var i = 0; i < trace.length; i++) {
            var ev = trace[i];
            if (!ev) continue;
            if (ev.k === 'think') aiAppendThinking(replyEl, ev.t || '');
            else if (ev.k === 'call') aiAppendToolCall(replyEl, ev.name, ev.args);
            else if (ev.k === 'result') aiAppendToolResult(replyEl, ev.name, ev.t);
            else if (ev.k === 'note') aiAppendTraceNote(ev.t || '');
        }
    } else if (ops.length) {
        // 旧会话（只有工具名链，没有完整 trace）：保持原样显示。
        st.traceEl.style.display = '';
        st.ops = ops;
    }
    if (st.ops.length || trace.length) {
        aiUpdateTraceSummary();
        // 生成中的轮次默认展开推理链，让刷新回来的用户直接看到实时思考进度。
        if (pending) st.traceEl.open = true;
    } else {
        st.traceEl.parentNode.removeChild(st.traceEl);
    }
    // 回答：生成中的轮次可能已有「半成品回答」（服务端增量落库），照常渲染。
    var a = t.assistant || '';
    if (a) {
        st.answerEl.style.display = '';
        if (typeof renderMarkdown === 'function') {
            st.answerEl.innerHTML = renderMarkdown(a);
        } else {
            st.answerEl.textContent = a;
        }
    }
    // 回填本轮图表（落库时保存的 spec），重开会话也能看到图。
    var charts = (t.charts && t.charts.length) ? t.charts : [];
    for (var c = 0; c < charts.length; c++) {
        aiAppendChartCard(st, charts[c]);
    }
    // 回填本轮导出文件（元数据落库；文件本身在服务端按 TTL 存活，过期后下载给出提示）。
    var files = (t.files && t.files.length) ? t.files : [];
    for (var f = 0; f < files.length; f++) {
        aiAppendFileCard(st, files[f]);
    }
    if (pending) {
        // 答案还没生成完：提示进度实时同步（服务端把思考/工具过程增量写回了本轮）。
        if (!a) {
            st.answerEl.style.display = '';
            st.answerEl.className = 'ai-answer ai-answer-pending';
            st.answerEl.textContent = '\u6b63\u5728\u601d\u8003\u2026';
        }
        var note = document.createElement('div');
        note.className = 'ai-pending-note';
        note.textContent = '\u6b63\u5728\u751f\u6210\u56de\u7b54\u2026\uff08\u8fdb\u5ea6\u5b9e\u65f6\u540c\u6b65\uff0c\u5b8c\u6210\u540e\u81ea\u52a8\u66f4\u65b0\uff09';
        st.turnEl.appendChild(note);
    } else if (!a) {
        st.answerEl.parentNode.removeChild(st.answerEl);
    }
    aiStream = null;
}

/** 推理链里的提示行（如「记录过长，后续步骤未保存」）。 */
function aiAppendTraceNote(text) {
    if (!aiStream || !aiStream.traceBodyEl) return;
    var div = document.createElement('div');
    div.className = 'ai-trace-note';
    div.textContent = text;
    aiStream.traceBodyEl.appendChild(div);
    aiShowTrace();
}

/** 新对话：清空当前视图，下次发送时创建新会话。 */
function newAiChat() {
    if (aiBusy) return;
    aiStopPendingPoll();
    aiSessionId = null;
    aiStoreSession(null);
    aiStream = null;
    aiPendingConfirm = null;
    aiClearAttachments();
    aiDisposeCharts();
    var replyEl = document.getElementById('ai-reply');
    if (replyEl) replyEl.textContent = '';
    renderAiSessionList();
    aiUpdateLanding();
    var input = document.getElementById('ai-input');
    if (input) input.focus();
}

/** 删除一条会话；删的是当前会话则回到「新对话」。 */
function deleteAiSession(id) {
    if (!id) return;
    if (!window.confirm('\u5220\u9664\u8fd9\u6761\u5bf9\u8bdd\uff1f\u6b64\u64cd\u4f5c\u4e0d\u53ef\u64a4\u9500\u3002')) return;
    fetch('/api/ai/sessions/' + encodeURIComponent(id), { method: 'DELETE', credentials: 'same-origin' })
        .then(function (r) { return r.ok ? { ok: true } : r.json().catch(function () { return {}; }); })
        .then(function () {
            if (id === aiSessionId) newAiChat();
            refreshAiSessions(false);
        })
        .catch(function () {});
}

/** 渲染 DDL 确认框（模型要执行 DDL 时暂停，等用户点「确认 / 取消」）。 */
function aiRenderConfirm(replyEl, ev) {
    aiEnsureTurn(replyEl);
    aiShowTrace();
    if (aiPendingConfirm && aiPendingConfirm.parentNode) {
        aiPendingConfirm.parentNode.removeChild(aiPendingConfirm);
    }
    var box = document.createElement('div');
    box.className = 'ai-confirm';
    var head = document.createElement('div');
    head.className = 'ai-confirm-head';
    var headText = document.createElement('span');
    headText.textContent = '\u9700\u8981\u786e\u8ba4\uff1a\u5373\u5c06\u6267\u884c DDL';
    var copyBtn = document.createElement('button');
    copyBtn.type = 'button';
    copyBtn.className = 'md-code-copy';
    copyBtn.title = '\u590d\u5236\u5168\u90e8 SQL';
    copyBtn.setAttribute('aria-label', '\u590d\u5236\u5168\u90e8 SQL');
    copyBtn.textContent = '\u590d\u5236';
    head.appendChild(headText);
    head.appendChild(copyBtn);
    var pre = document.createElement('pre');
    pre.className = 'ai-confirm-sql';
    pre.textContent = ev.sql || '';
    var actions = document.createElement('div');
    actions.className = 'ai-confirm-actions';
    var okBtn = document.createElement('button');
    okBtn.type = 'button';
    okBtn.className = 'ai-confirm-ok';
    okBtn.textContent = '\u786e\u8ba4\u6267\u884c';
    var noBtn = document.createElement('button');
    noBtn.type = 'button';
    noBtn.className = 'ai-confirm-cancel';
    noBtn.textContent = '\u53d6\u6d88';
    actions.appendChild(okBtn);
    actions.appendChild(noBtn);
    box.appendChild(head);
    box.appendChild(pre);
    box.appendChild(actions);
    aiStream.turnEl.appendChild(box);
    aiPendingConfirm = box;
    var streamId = ev.streamId || (aiStream && aiStream.streamId);
    var decide = function (approve) {
        okBtn.disabled = true;
        noBtn.disabled = true;
        box.classList.add(approve ? 'ai-confirm-approved' : 'ai-confirm-cancelled');
        headText.textContent = approve ? '\u5df2\u786e\u8ba4\u6267\u884c' : '\u5df2\u53d6\u6d88\u6267\u884c';
        aiPendingConfirm = null;
        fetch('/api/ai/confirm', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify({ streamId: streamId, approve: approve })
        }).catch(function () {});
        aiScrollToBottom();
    };
    okBtn.addEventListener('click', function () { decide(true); });
    noBtn.addEventListener('click', function () { decide(false); });
    aiScrollToBottom();
}

// ==================== AI 图表（ECharts）====================

/** 收到 chart 事件：把图表卡片挂到本轮（回答下方）。 */
function aiRenderChart(replyEl, ev) {
    var spec = ev && ev.spec;
    if (typeof spec === 'string') {
        try { spec = JSON.parse(spec); } catch (e) { return; }
    }
    if (!spec || !spec.type) return;
    aiEnsureTurn(replyEl);
    aiAppendChartCard(aiStream, spec);
}

/** 在当前轮里追加一张图表卡片（ECharts 渲染）。spec 为对象。 */
function aiAppendChartCard(turnState, spec) {
    if (!turnState || !turnState.turnEl) { return; }
    var card = document.createElement('div');
    card.className = 'ai-chart';
    if (spec.title) {
        var h = document.createElement('div');
        h.className = 'ai-chart-title';
        h.textContent = spec.title;
        card.appendChild(h);
    }
    var isDataset = spec.type === 'dataset';
    var box = null;
    if (isDataset) {
        card.appendChild(aiDatasetTable(spec));
    } else {
        box = document.createElement('div');
        box.className = 'ai-chart-box';
        card.appendChild(box);
    }
    if (spec.truncated) {
        var shown = isDataset ? (spec.rows || []).length : (spec.categories || []).length;
        var note = document.createElement('div');
        note.className = 'ai-chart-note';
        note.textContent = '数据较大，仅展示前 ' + shown + ' 行'
                + (spec.totalRows ? '（实际 ' + spec.totalRows + ' 行）' : '');
        card.appendChild(note);
    }
    turnState.turnEl.appendChild(card);
    if (isDataset) {
        aiScrollToBottom();
        return;
    }
    // ECharts 未加载 / 容器隐藏（宽度 0）时先挂起，等切到 AI 页再 init。
    if (typeof echarts === 'undefined') {
        var miss = document.createElement('div');
        miss.className = 'ai-chart-note';
        miss.textContent = '图表库未加载，无法显示图形。';
        card.appendChild(miss);
        return;
    }
    if (box.clientWidth > 0) {
        aiInitChart(box, spec);
    } else {
        aiPendingCharts.push({ el: box, spec: spec });
    }
    aiScrollToBottom();
}

/** dataset 类型：把结果集渲染成可横向滚动的表格卡片（纯 textContent，无 XSS 面）。 */
function aiDatasetTable(spec) {
    var wrap = document.createElement('div');
    wrap.className = 'ai-dataset-wrap';
    var table = document.createElement('table');
    table.className = 'ai-dataset-table';
    var cols = spec.columns || [];
    var rows = spec.rows || [];
    var thead = document.createElement('thead');
    var hr = document.createElement('tr');
    for (var c = 0; c < cols.length; c++) {
        var th = document.createElement('th');
        th.textContent = cols[c];
        hr.appendChild(th);
    }
    thead.appendChild(hr);
    table.appendChild(thead);
    var tbody = document.createElement('tbody');
    for (var r = 0; r < rows.length; r++) {
        var tr = document.createElement('tr');
        var row = rows[r] || [];
        for (var k = 0; k < cols.length; k++) {
            var td = document.createElement('td');
            td.textContent = (row[k] === null || row[k] === undefined) ? '' : row[k];
            tr.appendChild(td);
        }
        tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    wrap.appendChild(table);
    return wrap;
}

// ==================== AI 导出文件（下载卡片）====================

/** 收到 file 事件：把下载卡片挂到本轮（回答下方）。 */
function aiRenderFile(replyEl, ev) {
    var file = ev && ev.file;
    if (typeof file === 'string') {
        try { file = JSON.parse(file); } catch (e) { return; }
    }
    if (!file || !file.url) return;
    aiEnsureTurn(replyEl);
    aiAppendFileCard(aiStream, file);
}

/** 文件字节数格式化（展示用）。 */
function aiFormatBytes(n) {
    if (!n || n < 0) return '';
    if (n < 1024) return n + ' B';
    var kb = n / 1024;
    if (kb < 1024) return Math.round(kb) + ' KB';
    return (kb / 1024).toFixed(1) + ' MB';
}

/** 导出格式的展示名。 */
function aiFileFormatLabel(format) {
    switch (format) {
        case 'md': return 'Markdown';
        case 'txt': return '纯文本';
        case 'html': return 'HTML 网页';
        case 'csv': return 'CSV 表格';
        case 'excel': return 'Excel 工作簿';
        default: return format || '文件';
    }
}

/**
 * 在当前轮里追加一张「文件下载」卡片。file = {id,name,format,size,url}（服务端 export_file 产出）。
 * 全部用 textContent / 属性赋值，避免模型给的文件名注入 HTML。
 */
function aiAppendFileCard(turnState, file) {
    if (!turnState || !turnState.turnEl || !file || !file.url) { return; }
    var card = document.createElement('div');
    card.className = 'ai-file';

    var badge = document.createElement('div');
    badge.className = 'ai-file-badge';
    badge.textContent = String(file.format || 'file').toUpperCase();
    card.appendChild(badge);

    var main = document.createElement('div');
    main.className = 'ai-file-main';
    var name = document.createElement('div');
    name.className = 'ai-file-name';
    name.textContent = file.name || 'dojao-export';
    main.appendChild(name);
    var meta = document.createElement('div');
    meta.className = 'ai-file-meta';
    meta.textContent = aiFileFormatLabel(file.format)
            + (file.size ? ' · ' + aiFormatBytes(file.size) : '');
    main.appendChild(meta);
    card.appendChild(main);

    var link = document.createElement('a');
    link.className = 'ai-file-download';
    link.href = file.url;
    link.setAttribute('download', file.name || '');
    link.textContent = '下载';
    card.appendChild(link);

    turnState.turnEl.appendChild(card);
    aiScrollToBottom();
}

/** 真正 init 一个 ECharts 实例并挂上 window resize 自适应。 */
function aiInitChart(box, spec) {
    try {
        var chart = echarts.init(box, aiChartThemeName());
        chart.setOption(aiChartOption(spec));
        aiCharts.push({ chart: chart, el: box, spec: spec });
        if (window.requestAnimationFrame) window.requestAnimationFrame(function () { try { chart.resize(); } catch (e) {} });
    } catch (e) {
        // 单个图渲染失败不影响其它内容
    }
}

/** 主题切换后重建图表（ECharts 主题只能在 init 时指定，故 dispose + 用新主题重画）。 */
function aiReThemeCharts() {
    if (!aiCharts.length) return;
    var specs = aiCharts.map(function (c) { return { el: c.el, spec: c.spec }; });
    for (var i = 0; i < aiCharts.length; i++) {
        try { aiCharts[i].chart.dispose(); } catch (e) {}
    }
    aiCharts = [];
    specs.forEach(function (s) { aiInitChart(s.el, s.spec); });
}

/** 当前主题名（ECharts 内建 'dark' 主题；浅色用默认）。 */
function aiChartThemeName() {
    return (document.documentElement.getAttribute('data-theme') === 'dark') ? 'dark' : null;
}

/** 把后端 spec 映射成 ECharts option。全部数值/文本渲染在 canvas 上，无 XSS 面。 */
function aiChartOption(spec) {
    var dark = aiChartThemeName() === 'dark';
    var ink = dark ? '#E5E9F0' : '#1E293B';
    var dim = dark ? '#94A3B8' : '#64748B';
    var axisLine = dark ? '#2E3849' : '#E5E7EB';
    var splitLine = dark ? '#232B39' : '#F1F5F9';

    var option = {
        backgroundColor: 'transparent',
        textStyle: { color: ink },
        // ★ 用 richText 模式 tooltip：不把数据内容拼进 HTML，杜绝注入。
        tooltip: { renderMode: 'richText', trigger: 'item' },
        legend: { textStyle: { color: dim }, bottom: 0 }
    };
    var cats = spec.categories || [];
    var series = spec.series || [];
    var type = spec.type;                    // 顶层默认图形（缺省；每条序列可用 ser.type 覆盖 ⇒ 混合图）

    // dataset 由 aiAppendChartCard 直接渲染成表格，不会走到这里；兜底返回空 option。
    if (type === 'dataset') {
        return option;
    }

    if (type === 'pie' || type === 'funnel') {
        var vals = (series[0] && series[0].data) || [];
        var data = [];
        for (var i = 0; i < cats.length; i++) {
            data.push({ name: cats[i], value: vals[i] });
        }
        if (!data.length) data = vals.slice();
        option.tooltip.trigger = 'item';
        if (type === 'funnel') {
            option.legend = { textStyle: { color: dim }, bottom: 0 };
            option.series = [{
                type: 'funnel',
                left: '12%',
                right: '12%',
                top: 36,
                bottom: 36,
                sort: 'descending',
                gap: 2,
                data: data,
                label: { color: ink, formatter: '{b}: {c}' },
                emphasis: { label: { fontWeight: 'bold' } }
            }];
            return option;
        }
        option.series = [{
            type: 'pie',
            radius: ['38%', '66%'],
            center: ['50%', '46%'],
            data: data,
            label: { color: ink },
            emphasis: { itemStyle: { shadowBlur: 8, shadowColor: 'rgba(0,0,0,0.25)' } }
        }];
        return option;
    }

    // 关系图：nodes + links（后端已按 category=源、target=目标、values[0]=权重 生成）。
    if (type === 'graph') {
        option.tooltip.trigger = 'item';
        option.legend = { show: false };
        option.series = [{
            type: 'graph',
            layout: 'force',
            roam: true,
            draggable: true,
            data: spec.nodes || [],
            links: spec.links || [],
            label: { show: true, color: ink, position: 'right' },
            lineStyle: { color: dim, width: 1, curveness: 0.12, opacity: 0.7 },
            force: { repulsion: 140, edgeLength: 90, gravity: 0.08 },
            emphasis: { focus: 'adjacency', lineStyle: { width: 2.5 } },
            symbolSize: 12
        }];
        return option;
    }

    // 矩形树图：扁平结果直接映射为 {name, value}。
    if (type === 'treemap') {
        var tv = (series[0] && series[0].data) || [];
        option.tooltip.trigger = 'item';
        option.legend = { show: false };
        option.series = [{
            type: 'treemap',
            left: 4,
            right: 4,
            top: 4,
            bottom: 4,
            roam: false,
            nodeClick: false,
            breadcrumb: { show: false },
            label: { show: true, color: '#fff', fontSize: 12 },
            upperLabel: { show: false },
            data: cats.map(function (c, i) {
                return { name: c, value: tv[i] };
            }),
            emphasis: { itemStyle: { shadowBlur: 8, shadowColor: 'rgba(0,0,0,0.25)' } }
        }];
        return option;
    }

    // 树图：扁平结果包一层根节点（根名用标题，缺省 root）。
    if (type === 'tree') {
        var yv = (series[0] && series[0].data) || [];
        option.tooltip.trigger = 'item';
        option.legend = { show: false };
        option.series = [{
            type: 'tree',
            data: [{
                name: spec.title || 'root',
                children: cats.map(function (c, i) {
                    return { name: c, value: yv[i] };
                })
            }],
            top: '6%',
            left: '8%',
            bottom: '6%',
            right: '22%',
            symbolSize: 8,
            orient: 'LR',
            initialTreeDepth: 2,
            label: { position: 'left', verticalAlign: 'middle', align: 'right', color: ink },
            leaves: { label: { position: 'right', align: 'left', color: dim } },
            lineStyle: { color: dim, width: 1.2, curveness: 0.4 },
            emphasis: { focus: 'descendant' }
        }];
        return option;
    }

    // 热力图：二维分类网格，series[0].data = [[xIdx, yIdx, value], ...]。
    if (type === 'heatmap') {
        var pairs = (series[0] && series[0].data) || [];
        var ycats = spec.yCategories || [];
        var min = Infinity, max = -Infinity;
        for (var hi = 0; hi < pairs.length; hi++) {
            var hv = pairs[hi] && pairs[hi][2];
            if (typeof hv === 'number' && isFinite(hv)) {
                if (hv < min) min = hv;
                if (hv > max) max = hv;
            }
        }
        if (!isFinite(min) || !isFinite(max)) { min = 0; max = 1; }
        if (min === max) { max = min + 1; }
        option.tooltip.trigger = 'item';
        option.legend = { show: false };
        option.grid = { left: 64, right: 24, top: 16, bottom: 72, containLabel: true };
        option.xAxis = {
            type: 'category',
            data: cats,
            splitArea: { show: true },
            axisLine: { lineStyle: { color: axisLine } },
            axisLabel: { color: dim }
        };
        option.yAxis = {
            type: 'category',
            data: ycats,
            splitArea: { show: true },
            axisLine: { lineStyle: { color: axisLine } },
            axisLabel: { color: dim }
        };
        option.visualMap = {
            min: min,
            max: max,
            calculable: true,
            orient: 'horizontal',
            left: 'center',
            bottom: 0,
            textStyle: { color: dim },
            inRange: {
                color: dark ? ['#1E3A5F', '#3B82F6', '#93C5FD'] : ['#DBEAFE', '#3B82F6', '#1E3A8A']
            }
        };
        option.series = [{
            type: 'heatmap',
            data: pairs,
            label: { show: true, color: ink, fontSize: 11 },
            emphasis: { itemStyle: { shadowBlur: 6, shadowColor: 'rgba(0,0,0,0.3)' } }
        }];
        return option;
    }

    // 散点图：category 全为数值时用数值 X 轴，并把数据转成 [x, y] 点。
    var numericX = type === 'scatter' && cats.length > 0 && cats.every(function (c) {
        return c !== null && c !== '' && isFinite(Number(c));
    });
    option.tooltip.trigger = numericX ? 'item' : 'axis';
    option.xAxis = numericX
            ? {
                type: 'value',
                axisLine: { lineStyle: { color: axisLine } },
                splitLine: { lineStyle: { color: splitLine } },
                axisLabel: { color: dim }
            }
            : {
                type: 'category',
                data: cats,
                axisLine: { lineStyle: { color: axisLine } },
                axisLabel: { color: dim }
            };
    var yMain = {
        type: 'value',
        axisLine: { show: false },
        splitLine: { lineStyle: { color: splitLine } },
        axisLabel: { color: dim }
    };
    // 混合图：存在副轴序列时用双 Y 轴（主 / 副各一根），让量纲不同的序列各自用合适的刻度。
    var dual = !!spec.dualAxis;
    if (dual) {
        option.yAxis = [yMain, {
            type: 'value',
            axisLine: { show: false },
            splitLine: { show: false },
            axisLabel: { color: dim }
        }];
    } else {
        option.yAxis = yMain;
    }
    option.series = series.map(function (ser) {
        var st = (ser && ser.type) || type;  // 每条序列各自的图形；缺省回退顶层 type
        var base = (st === 'area' ? 'line' : st);
        var raw = ser.data || [];
        if (numericX && st === 'scatter') {
            raw = raw.map(function (v, i) {
                var xv = Number(cats[i]);
                var yv = (v === null || v === '' || !isFinite(Number(v))) ? null : Number(v);
                return [xv, yv];
            });
        }
        var s = {
            name: ser.name,
            type: base,
            data: raw,
            smooth: (st === 'line' || st === 'area'),
            symbolSize: (st === 'scatter' ? 9 : 5),
            emphasis: { focus: 'series' }
        };
        if (st === 'area') {
            s.areaStyle = { opacity: 0.22 };
        }
        if (dual) {
            s.yAxisIndex = (ser && ser.axis === 'y2') ? 1 : 0;
        }
        return s;
    });
    return option;
}

/** 切到 AI 页 / 窗口尺寸变化时：给尺寸就绪的挂起图表补 init，并把已存在图表 resize。 */
function aiFlushCharts() {
    if (aiPendingCharts.length) {
        var rest = [];
        for (var i = 0; i < aiPendingCharts.length; i++) {
            var p = aiPendingCharts[i];
            if (p.el && p.el.clientWidth > 0) {
                aiInitChart(p.el, p.spec);
            } else {
                rest.push(p);
            }
        }
        aiPendingCharts = rest;
    }
    for (var j = 0; j < aiCharts.length; j++) {
        try { aiCharts[j].chart.resize(); } catch (e) {}
    }
}

/** 释放所有图表实例（切会话 / 新对话时调用）。 */
function aiDisposeCharts() {
    for (var i = 0; i < aiCharts.length; i++) {
        try { aiCharts[i].chart.dispose(); } catch (e) {}
    }
    aiCharts = [];
    aiPendingCharts = [];
}

/** 单元格是否「空」：null / undefined / 空串都算（结果表把 null 也渲染成空串）。 */
function sqlCellEmpty(v) {
    return v === undefined || v === null || v === '';
}

/** 单元格值 → 数值（非数值 → null）。用于「能比数就比数」。 */
function sqlCellNumber(v) {
    if (typeof v === 'number') return isFinite(v) ? v : null;
    if (typeof v !== 'string') return null;
    var s = v.trim();
    if (s === '') return null;
    var n = Number(s);
    return isFinite(n) ? n : null;
}

/**
 * 比较两个<b>非空</b>单元格值（升序）。两侧都是数值 → 按数值比（否则 "100" < "9"）；
 * 否则按本地化字符串比（numeric:true 让 "a2" < "a10"，「第 2 组」排在「第 10 组」前）。
 * 空值由调用方先剔除 —— 空值排序规则与方向有关，不能塞进纯函数。
 */
function sqlValueCompare(x, y) {
    var nx = sqlCellNumber(x), ny = sqlCellNumber(y);
    if (nx !== null && ny !== null) return nx < ny ? -1 : (nx > ny ? 1 : 0);
    return String(x).localeCompare(String(y), 'zh', { numeric: true, sensitivity: 'base' });
}

/**
 * 点表头：同一列 升 → 降 → 取消 循环；换列则从升序开始。
 * 只重排<b>已预览到的这些行</b>（客户端行为，受 SQL_PREVIEW_LIMIT 限制，不等价于 ORDER BY）。
 * 就地用缓存快照重渲染，不必等下一拍轮询（轮询 1s 一次，否则点完像没反应）。
 */
function sortSqlResult(i) {
    if (sqlSortCol === i) {
        sqlSortDir = (sqlSortDir === 1) ? -1 : (sqlSortDir === -1 ? 0 : 1);
    } else {
        sqlSortCol = i;
        sqlSortDir = 1;
    }
    if (sqlSortDir === 0) sqlSortCol = -1;   // 取消：回到引擎原始顺序
    if (sqlLastResult) renderSqlTable(sqlLastResult);
}

/** 表头展示名：'*' 是规划期未展开的占位（零行结果拿不到 Arrow schema 时会出现）→ 叫 colN。 */
function sqlColumnName(cols, i) {
    var raw = (i < cols.length) ? cols[i] : '';
    return (raw && raw !== '*') ? raw : ('col' + (i + 1));
}

/**
 * 当前结果区的「所见即所得」视图：列名 + 按当前排序排好的行。
 *
 * <p>渲染表格、导出、复制全部数据三处共用同一份 —— 保证「导出的 / 复制出去的」
 * 就是屏幕上看到的顺序（表头点过排序后也一样）。</p>
 *
 * @return {{cols: string[], rows: Array<Array>, width: number}}
 */
function sqlResultView() {
    var r = sqlLastResult;
    var cols = (r && r.columns) || [];
    var rows = (r && r.rows) || [];
    // 表头跟着数据走：以「列头 + 各行实际长度」的较大值为准，避免列被静默截掉
    var width = cols.length;
    rows.forEach(function (row) { if (row && row.length > width) width = row.length; });
    var names = [];
    for (var i = 0; i < width; i++) names.push(sqlColumnName(cols, i));

    // 排序作用于**副本**：原顺序留着，取消排序时逐字回到引擎给的顺序
    var view = rows;
    if (sqlSortCol >= 0 && sqlSortDir !== 0) {
        var col = sqlSortCol, sign = sqlSortDir;
        view = rows.slice().sort(function (a, b) {
            var x = a ? a[col] : null, y = b ? b[col] : null;
            var xe = sqlCellEmpty(x), ye = sqlCellEmpty(y);
            // 空值恒沉底（与升降无关）：否则降序时空值会被翻到最上面挡视线
            if (xe || ye) return (xe && ye) ? 0 : (xe ? 1 : -1);
            return sign * sqlValueCompare(x, y);
        });
    }
    var v = { cols: names, rows: view, width: width };
    return sqlTransposed ? transposeSqlView(v) : v;
}

/**
 * 列转行：把「列」变成「行」。
 *
 * <p>原视图 {@code 列名 | 第1行值 | 第2行值 …}；转置后每行 = 原一列：
 * {@code 字段 | 第1行…}。行号列仍由渲染层统一添加。</p>
 */
function transposeSqlView(v) {
    var cols = ['字段'];
    for (var r = 0; r < v.rows.length; r++) cols.push('第' + (r + 1) + '行');
    var rows = [];
    for (var c = 0; c < v.width; c++) {
        var out = [v.cols[c]];
        for (var i = 0; i < v.rows.length; i++) {
            var src = v.rows[i];
            out.push(src && src[c] != null ? src[c] : '');
        }
        rows.push(out);
    }
    return { cols: cols, rows: rows, width: cols.length };
}

/** 切换结果区「列转行」视图（纯前端，不重跑 SQL；状态跟随控制台）。 */
function toggleSqlTranspose() {
    if (!sqlLastResult || !(sqlLastResult.rows || []).length) {
        showToast('没有可转置的结果');
        return;
    }
    sqlTransposed = !sqlTransposed;
    var p = activePreview();
    if (p) p.transposed = sqlTransposed;
    renderSqlTable(sqlLastResult);
    showToast(sqlTransposed ? '已列转行（再次按 Tab 恢复）' : '已恢复行列视图');
}

/** 有数据才让导出按钮可用（没结果可导）。 */
function syncResultTools() {
    var has = !!(sqlLastResult && sqlLastResult.rows && sqlLastResult.rows.length);
    ['export-csv-btn', 'export-excel-btn', 'sql-chart-btn'].forEach(function (id) {
        var b = document.getElementById(id);
        if (b) b.disabled = !has;
    });
}

function renderSqlTable(r) {
    var headEl = document.getElementById('sql-result-head');
    var bodyEl = document.getElementById('sql-result-body');
    if (!headEl || !bodyEl) return;
    var cols = r.columns || [];
    var running = (r.state === 'RUNNING' || r.state === 'SUBMITTED');

    if (r.state === 'FAILED') {
        // 执行失败：不摆残缺表格，改出居中报错卡片（复制 / AI 修复）
        showSqlError(r.error, r.errorDetail);
        sqlLastResult = null;      // 上一条成功查询的快照作废，导出按钮随之禁用
        syncResultTools();
        return;
    }
    sqlLastResult = r;   // 留一份快照：点表头时据此就地重排（不依赖下一拍轮询）
    hideSqlError();
    // 还没拿到列头（多半是执行中）：显示「计算中」，而不是「（无列）」
    if (!cols.length) {
        headEl.innerHTML = '';
        bodyEl.innerHTML = '<tr><td style="color:' + UI.faint + '">'
            + (running ? '\u6b63\u5728\u8ba1\u7b97\u2026' : '\uff08\u65e0\u5217\uff09')
            + '</td></tr>';
        syncResultTools();
        return;
    }

    var view = sqlResultView();

    // 每个表头都可点排序：图标恒在（未排 = 淡显，升/降 = 亮显 + 降序翻转 180°）。
    // data-name 存「纯列名」：右键「复制列名」直接取它，不用去刨末端的排序箭头。
    // 行号列：显示区专用（不参与排序 / 导出 / 复制）—— 无 data-col，故右键不弹列名菜单。
    var ths = '<th class="row-num-th" title="\u884c\u53f7">#</th>';
    for (var i = 0; i < view.width; i++) {
        var active = (sqlSortCol === i && sqlSortDir !== 0);
        var cls = 'th-sort' + (active ? (sqlSortDir > 0 ? ' asc' : ' desc') : '');
        ths += '<th class="' + cls + '" data-col="' + i + '" data-name="' + esc(view.cols[i]) + '"'
            + ' onclick="sortSqlResult(' + i + ')"'
            + ' title="\u70b9\u51fb\u6392\u5e8f\uff1a\u5347\u5e8f \u2192 \u964d\u5e8f \u2192 \u53d6\u6d88\uff1b\u53f3\u952e\u590d\u5236\u5217\u540d">'
            + esc(view.cols[i]) + '<span class="sort-ico">\u25b2</span></th>';
    }
    headEl.innerHTML = '<tr>' + ths + '</tr>';

    if (!view.rows.length) {
        bodyEl.innerHTML = '<tr><td style="color:' + UI.faint + '" colspan="' + (view.width + 1) + '">'
            + (running ? '\u6b63\u5728\u8ba1\u7b97\u2026' : '\uff08\u7ed3\u679c\u96c6\u4e3a\u7a7a\uff09')
            + '</td></tr>';
        syncResultTools();
        return;
    }

    bodyEl.innerHTML = view.rows.map(function (row, idx) {
        // 行号按「显示顺序」给（排序后即为新位置 1..N）；无 res-cell / data-col，不参与导出与右键复制
        var cells = ['<td class="row-num" title="\u884c\u53f7">' + (idx + 1) + '</td>'];
        for (var i = 0; i < view.width; i++) {
            var v = (row && row[i] !== undefined && row[i] !== null) ? row[i] : '';
            // res-cell 标记「真数据格」：右键菜单据此区分数据区与「（结果集为空）」占位行。
            // 单元格内容包一层 .cell-clip：超长文本（如 JSON 字串）按固定宽度截断 + 省略号，
            // 不再把整列撑到几千像素、把右侧字段挤到看不见；hover 时 title 仍显示完整内容。
            // 注意：完整文本保持在 DOM 里（只是视觉截断），右键「复制当前数据」取 textContent 不受影响。
            cells.push('<td class="res-cell" data-col="' + i + '" title="' + esc(v) + '">'
                + '<span class="cell-clip">' + esc(v) + '</span></td>');
        }
        return '<tr>' + cells.join('') + '</tr>';
    }).join('');
    syncResultTools();
}

// ---------------- 结果手动出图（ECharts）----------------

/**
 * 结果区「出图」状态：把当前预览到的结果（所见即所得，含表头排序）渲染成 ECharts 图表。
 *
 * <p>数据在前端本地取（结果已在浏览器里），<b>不重跑 SQL</b>；spec→option 的映射与主题口径
 * 复用 AI 对话出图的 {@link aiChartOption}，两个入口视觉一致。</p>
 */
var sqlChartState = { visible: false, type: 'bar', categoryIndex: 0, valueIndexes: [], title: '' };
var sqlChartInstance = null;

/** 「出图」按钮：展开 / 收起图表配置条与画布（展开时按列类型智能选默认）。 */
function toggleSqlChartBar() {
    if (!sqlLastResult || !(sqlLastResult.rows || []).length) {
        showToast('没有可出图的结果');
        return;
    }
    sqlChartState.visible = !sqlChartState.visible;
    var bar = document.getElementById('sql-chart-bar');
    var panel = document.getElementById('sql-chart-panel');
    if (bar) bar.style.display = sqlChartState.visible ? '' : 'none';
    if (panel) panel.style.display = sqlChartState.visible ? '' : 'none';
    var btn = document.getElementById('sql-chart-btn');
    if (btn) btn.classList.toggle('active', sqlChartState.visible);
    if (sqlChartState.visible) {
        refreshSqlChartDefaults();
    } else {
        disposeSqlChart();
    }
}

/** 「收起」按钮：收掉配置条与画布（未展开则只清实例，无副作用）。 */
function closeSqlChart() {
    if (sqlChartState.visible) toggleSqlChartBar();
    else disposeSqlChart();
}

/** 清场（新查询提交 / 切控制台时调用）：收起面板 + 销毁实例 + 复位状态。 */
function sqlChartReset() {
    sqlChartState.visible = false;
    sqlChartState.title = '';
    var bar = document.getElementById('sql-chart-bar');
    if (bar) bar.style.display = 'none';
    var panel = document.getElementById('sql-chart-panel');
    if (panel) panel.style.display = 'none';
    var btn = document.getElementById('sql-chart-btn');
    if (btn) btn.classList.remove('active');
    disposeSqlChart();
}

/** 销毁 ECharts 实例（容器将隐藏 / 结果将更新时调用）。 */
function disposeSqlChart() {
    if (sqlChartInstance) {
        try { sqlChartInstance.dispose(); } catch (e) { /* ignore */ }
        sqlChartInstance = null;
    }
    var box = document.getElementById('sql-chart-box');
    if (box) box.innerHTML = '';
}

/** 结果集某一列是否「数值列」：抽样（前 50 行）非空单元格全能转成数字。 */
function sqlColumnIsNumeric(rows, idx) {
    var seen = 0;
    for (var i = 0; i < rows.length && i < 50; i++) {
        var v = rows[i] ? rows[i][idx] : null;
        if (sqlCellEmpty(v)) continue;
        seen++;
        if (sqlCellNumber(v) === null) return false;
    }
    return seen > 0;
}

/** 按列类型智能重选：分类 = 首个非数值列，数值 = 全部数值列（都没有则取末列）。 */
function refreshSqlChartDefaults() {
    var view = sqlResultView();
    var cols = view.cols, rows = view.rows;
    var numIdx = [], firstNonNum = -1;
    for (var i = 0; i < cols.length; i++) {
        if (sqlColumnIsNumeric(rows, i)) numIdx.push(i);
        else if (firstNonNum < 0) firstNonNum = i;
    }
    sqlChartState.categoryIndex = (firstNonNum >= 0) ? firstNonNum : 0;
    if (numIdx.length) {
        sqlChartState.valueIndexes = numIdx;
    } else {
        var fallback = [];
        for (var j = 0; j < cols.length; j++) {
            if (j !== sqlChartState.categoryIndex) fallback.push(j);
        }
        sqlChartState.valueIndexes = fallback.length ? [fallback[fallback.length - 1]] : [];
    }
    // 饼图只画单序列：多选无意义，默认收成第一个数值列。
    if (sqlChartState.type === 'pie' && sqlChartState.valueIndexes.length > 1) {
        sqlChartState.valueIndexes = [sqlChartState.valueIndexes[0]];
    }
    rebuildSqlChartControls();
    renderSqlChart();
}

/** 依据当前结果的列重建配置控件（类型下拉 / 分类下拉 / 数值勾选）。 */
function rebuildSqlChartControls() {
    var view = sqlResultView();
    var cols = view.cols;
    var typeSel = document.getElementById('sql-chart-type');
    if (typeSel) typeSel.value = sqlChartState.type;
    var titleInput = document.getElementById('sql-chart-title');
    if (titleInput) titleInput.value = sqlChartState.title || '';
    var catSel = document.getElementById('sql-chart-category');
    if (catSel) {
        catSel.innerHTML = cols.map(function (name, i) {
            return '<option value="' + i + '">' + esc(name) + '</option>';
        }).join('');
        catSel.value = String(sqlChartState.categoryIndex);
    }
    var box = document.getElementById('sql-chart-values');
    if (box) {
        box.innerHTML = cols.map(function (name, i) {
            var checked = sqlChartState.valueIndexes.indexOf(i) >= 0 ? ' checked' : '';
            return '<label class="scb-check"><input type="checkbox" value="' + i + '"' + checked
                + ' onchange="sqlChartValueToggled(' + i + ', this.checked)">'
                + '<span>' + esc(name) + '</span></label>';
        }).join('');
    }
}

/** 勾选某个数值列 → 更新状态并重绘。 */
function sqlChartValueToggled(i, checked) {
    var pos = sqlChartState.valueIndexes.indexOf(i);
    if (checked && pos < 0) sqlChartState.valueIndexes.push(i);
    else if (!checked && pos >= 0) sqlChartState.valueIndexes.splice(pos, 1);
    renderSqlChart();
}

/** 从控件回读状态（类型 / 分类 / 标题；数值勾选由 onchange 直接维护）。 */
function readSqlChartControls() {
    var typeSel = document.getElementById('sql-chart-type');
    if (typeSel && typeSel.value) sqlChartState.type = typeSel.value;
    var catSel = document.getElementById('sql-chart-category');
    if (catSel) {
        var ci = parseInt(catSel.value, 10);
        if (!isNaN(ci)) sqlChartState.categoryIndex = ci;
    }
    var titleInput = document.getElementById('sql-chart-title');
    if (titleInput) sqlChartState.title = titleInput.value;
}

/** 用当前状态 + 结果快照构建图表 spec（与后端 buildChartSpec 同形）。 */
function buildSqlChartSpec() {
    var view = sqlResultView();
    var cols = view.cols, rows = view.rows;
    var catIdx = sqlChartState.categoryIndex;
    var valIdx = sqlChartState.valueIndexes.slice().sort(function (a, b) { return a - b; });
    if (!valIdx.length) return null;
    var categories = rows.map(function (row) {
        var v = row ? row[catIdx] : null;
        return (v === undefined || v === null) ? '' : String(v);
    });
    var series = valIdx.map(function (vi) {
        return {
            name: cols[vi],
            data: rows.map(function (row) {
                var v = row ? row[vi] : null;
                if (sqlCellEmpty(v)) return null;
                var n = sqlCellNumber(v);
                return n === null ? v : n;   // 非数值列退化成文本（散点 / 类目轴）
            })
        };
    });
    var spec = { type: sqlChartState.type, categories: categories, series: series, totalRows: rows.length };
    if (sqlChartState.title) spec.title = sqlChartState.title;
    return spec;
}

/** 重绘结果图表（控件变化 / 首次展开时调用）。 */
function renderSqlChart() {
    if (!sqlChartState.visible) return;
    readSqlChartControls();
    var box = document.getElementById('sql-chart-box');
    if (!box) return;
    if (!sqlChartState.valueIndexes.length) { showSqlChartNote(box, '请至少勾选一个数值列。'); return; }
    if (typeof echarts === 'undefined') { showSqlChartNote(box, '图表库未加载，无法显示图形。'); return; }
    var spec = buildSqlChartSpec();
    if (!spec) return;
    try {
        if (sqlChartInstance && sqlChartInstance.getDom() === box) {
            sqlChartInstance.setOption(aiChartOption(spec), true);   // 就地重设：不重建实例、不闪
        } else {
            disposeSqlChart();
            sqlChartInstance = echarts.init(box, aiChartThemeName());
            sqlChartInstance.setOption(aiChartOption(spec));
        }
        sqlChartInstance.resize();
    } catch (e) {
        showSqlChartNote(box, '图表渲染失败：' + (e && e.message ? e.message : e));
    }
}

/** 把说明文案写进画布容器（纯 textContent，无 XSS）。 */
function showSqlChartNote(box, msg) {
    disposeSqlChart();
    box.innerHTML = '';
    var d = document.createElement('div');
    d.className = 'scb-note';
    d.textContent = msg;
    box.appendChild(d);
}

/** 窗口尺寸变化：结果图表自适应（未实例化则忽略）。 */
function resizeSqlChart() {
    if (sqlChartInstance) { try { sqlChartInstance.resize(); } catch (e) { /* ignore */ } }
}

/** 主题切换：ECharts 主题只能在 init 时指定 ⇒ 销毁实例后用新主题重画。 */
function sqlReThemeChart() {
    if (sqlChartState.visible && sqlChartInstance) {
        disposeSqlChart();
        renderSqlChart();
    }
}

// ---------------- 结果导出：CSV / Excel ----------------

/** 导出文件名：dojao-<queryId 短>-<时间戳>.<ext>。 */
function exportFileName(ext) {
    var ts = new Date();
    function p(n) { return (n < 10 ? '0' : '') + n; }
    var stamp = '' + ts.getFullYear() + p(ts.getMonth() + 1) + p(ts.getDate())
        + '-' + p(ts.getHours()) + p(ts.getMinutes()) + p(ts.getSeconds());
    var qid = sqlQueryId ? midStr(sqlQueryId, 6).replace(/[^\w.-]/g, '') : 'result';
    return 'dojao-' + qid + '-' + stamp + '.' + ext;
}

/** 单元格 → CSV 字段（含逗号 / 引号 / 换行就加引号，内部引号翻倍，RFC 4180）。 */
function csvField(v) {
    var s = (v === undefined || v === null) ? '' : String(v);
    return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

/** 导出 CSV：含表头，UTF-8 + BOM（没有 BOM 时 Excel 双击打开中文列名全是乱码）。 */
function exportSqlCsv() {
    var view = sqlResultView();
    if (!view.width || !view.rows.length) { showToast('没有可导出的结果'); return; }
    var lines = [view.cols.map(csvField).join(',')];
    view.rows.forEach(function (row) {
        var cells = [];
        for (var i = 0; i < view.width; i++) cells.push(csvField(row ? row[i] : ''));
        lines.push(cells.join(','));
    });
    var blob = new Blob(['\ufeff' + lines.join('\r\n')], { type: 'text/csv;charset=utf-8' });
    downloadFile(exportFileName('csv'), blob);
    showToast('已导出 CSV（' + view.rows.length + ' 行，含表头）');
}

/** XML 文本转义（Excel 工作簿就是 XML，& < > 必须转义，否则文件打不开）。 */
function xmlEsc(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&apos;');
}

/**
 * 单元格 → Excel 数字；不能「安全地」当数字就返回 null（按文本写进去）。
 *
 * <p>后端把每个格子都当字符串给过来，无脑 {@code Number()} 会把订单号这类长数字
 * 变成科学计数法（精度丢失）、把「0012」的前导零吃掉。只有同时满足
 * 「纯数字形态 + 无前导零 + 原样往返 + 不超过 15 位有效数字」才当数字。</p>
 */
function excelNumberOrNull(v) {
    if (typeof v === 'number') return isFinite(v) ? v : null;
    if (typeof v !== 'string') return null;
    var s = v.trim();
    if (s === '' || !/^-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$/.test(s)) return null;
    if (/^-?0\d/.test(s)) return null;                            // 前导零：编号，不是数值
    var n = Number(s);
    if (!isFinite(n)) return null;
    if (String(n) !== s) return null;                             // 往返不一致（1.50 / 1e3）→ 保原样
    if (Number.isInteger(n) && Math.abs(n) > 9007199254740991) return null;   // 超 2^53：精度必丢
    return n;
}

/**
 * 导出 Excel 工作簿。
 *
 * <p>刻意不引第三方库（前端零外网依赖，真 xlsx 还要现场拼 ZIP）：用 Excel 2003 的
 * SpreadsheetML（单个 XML 文件，后缀 .xls），Excel / WPS 双击即开，且能带<b>数字类型</b>
 * —— 这点 CSV 做不到（CSV 全按文本走，数字列在 Excel 里没法直接求和）。</p>
 */
function exportSqlExcel() {
    var view = sqlResultView();
    if (!view.width || !view.rows.length) { showToast('没有可导出的结果'); return; }
    var out = [];
    out.push('<?xml version="1.0" encoding="UTF-8"?>');
    out.push('<?mso-application progid="Excel.Sheet"?>');
    out.push('<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"'
        + ' xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">');
    out.push('<Styles><Style ss:ID="hdr"><Font ss:Bold="1"/>'
        + '<Interior ss:Color="#E0ECFF" ss:Pattern="Solid"/></Style></Styles>');
    out.push('<Worksheet ss:Name="Result"><Table>');
    out.push('<Row>' + view.cols.map(function (c) {
        return '<Cell ss:StyleID="hdr"><Data ss:Type="String">' + xmlEsc(c) + '</Data></Cell>';
    }).join('') + '</Row>');
    view.rows.forEach(function (row) {
        var cells = [];
        for (var i = 0; i < view.width; i++) {
            var raw = row ? row[i] : '';
            var num = excelNumberOrNull(raw);
            if (num !== null) {
                cells.push('<Cell><Data ss:Type="Number">' + num + '</Data></Cell>');
            } else {
                var s = (raw === undefined || raw === null) ? '' : String(raw);
                cells.push('<Cell><Data ss:Type="String">' + xmlEsc(s) + '</Data></Cell>');
            }
        }
        out.push('<Row>' + cells.join('') + '</Row>');
    });
    out.push('</Table></Worksheet></Workbook>');
    var blob = new Blob(['\ufeff' + out.join('')], { type: 'application/vnd.ms-excel;charset=utf-8' });
    downloadFile(exportFileName('xls'), blob);
    showToast('已导出 Excel（' + view.rows.length + ' 行，含表头）');
}

// ---------------- 结果区右键菜单：复制列名 / 复制数据 ----------------

/** 复制全部数据（制表符分隔：粘进 Excel / 飞书 / 钉钉表格直接就是一张表）。 */
function copyAllResultData() {
    var view = sqlResultView();
    if (!view.width || !view.rows.length) { showToast('没有可复制的数据'); return; }
    var lines = [view.cols.join('\t')];
    view.rows.forEach(function (row) {
        var cells = [];
        for (var i = 0; i < view.width; i++) {
            var v = (row && row[i] !== undefined && row[i] !== null) ? row[i] : '';
            // TSV 没法转义制表符 / 换行：替换成空格，保证行列数不错位
            cells.push(String(v).replace(/[\t\r\n]+/g, ' '));
        }
        lines.push(cells.join('\t'));
    });
    copyText(lines.join('\n'), function (okFlag) {
        showToast(okFlag
            ? '已复制 ' + view.rows.length + ' 行（含表头，可直接粘进 Excel）'
            : '复制失败');
    });
}

/**
 * 给结果表挂右键菜单（事件委托，绑一次即可 —— 表格 innerHTML 每秒都在重建）：
 * 表头 → 复制列名；数据格 → 复制当前数据 / 复制全部数据。
 */
function bindSqlResultContextMenu() {
    var wrap = document.getElementById('sql-result-wrap');
    if (!wrap) return;

    // 结果区「列转行」：在表格任意位置点过鼠标后按 Tab 切换（再按一次恢复）。
    // 表格每秒重绘、单元格不可聚焦，故用「最后点击落在结果区」的标记 + document 级 keydown。
    var pointerInside = false;
    wrap.addEventListener('mousedown', function () { pointerInside = true; });
    document.addEventListener('mousedown', function (e) {
        if (!wrap.contains(e.target)) pointerInside = false;
    }, true);
    document.addEventListener('keydown', function (e) {
        if (e.key !== 'Tab' || e.ctrlKey || e.metaKey || e.altKey || e.shiftKey) return;
        if (!pointerInside) return;
        e.preventDefault();            // 不把焦点移走
        toggleSqlTranspose();
    });

    wrap.addEventListener('contextmenu', function (e) {
        var t = e.target;
        if (!t || !t.closest) return;
        // 只对「真数据列表头」弹列名菜单：行号列表头无 data-col，右键不该出现空列名
        var th = t.closest('th[data-col]');
        if (th) {
            e.preventDefault();
            var name = th.getAttribute('data-name') || '';
            showContextMenu([{
                label: '复制列名',
                onClick: function () {
                    copyText(name, function (ok) {
                        showToast(ok ? '已复制列名：' + name : '复制失败');
                    });
                }
            }], e.clientX, e.clientY);
            return;
        }
        var td = t.closest('td.res-cell');
        if (!td) return;
        e.preventDefault();
        var val = td.textContent;
        showContextMenu([
            {
                label: '复制当前数据',
                onClick: function () {
                    copyText(val, function (ok) { showToast(ok ? '已复制该单元格' : '复制失败'); });
                }
            },
            { label: '复制全部数据', onClick: copyAllResultData }
        ], e.clientX, e.clientY);
    });
}

// ---------------- 顶层导航（首页 / 任务管理 / 监控 / AI）----------------

/** tab 名 → 顶层视图元素 id。 */
var TAB_VIEWS = { home: 'home-view', tasks: 'tasks-view', monitor: 'monitor-view', ai: 'ai-view' };
/** tab 名 → header 徽章文案。 */
var TAB_LABELS = { home: '首页', tasks: '任务管理', monitor: '监控', ai: 'AI' };
/** 当前顶层 tab。 */
var currentTab = 'home';

/** 高亮顶部 tab（不动视图）。 */
function highlightTab(name) {
    var tabs = document.querySelectorAll('#top-nav .nav-tab');
    for (var i = 0; i < tabs.length; i++) {
        tabs[i].classList.toggle('active', tabs[i].getAttribute('data-view') === name);
    }
}

/** 隐藏全部顶层视图与 DAG 视图。 */
function hideAllViews() {
    Object.keys(TAB_VIEWS).forEach(function (k) {
        var el = document.getElementById(TAB_VIEWS[k]);
        if (el) el.classList.add('hidden');
    });
    var dag = document.getElementById('dag-view');
    if (dag) dag.classList.remove('show');
}

/** 清掉 DAG 视图的临时状态（切 tab / 重开 DAG 前调用）。 */
function clearDagState() {
    currentJobId = null;
    liveSnapshot = null;
    liveOps = [];
    if (network) { network.destroy(); network = null; }
    stopDagPoll();
    stopDagAnim();
    nodeDispPct = {};
    hideNodeDetail();
    hideSqlTooltip();
}

/** 切换顶层 tab：首页 / 任务管理 / 监控 / AI。 */
function switchTab(name) {
    if (!TAB_VIEWS[name]) name = 'home';
    currentTab = name;
    clearDagState();
    hideAllViews();
    var view = document.getElementById(TAB_VIEWS[name]);
    if (view) view.classList.remove('hidden');
    highlightTab(name);
    var badge = document.getElementById('header-badge');
    if (badge) badge.textContent = TAB_LABELS[name];
    // 任务管理页轮询任务列表 / 集群卡片；监控页轮询资源快照 + 历史序列。切走即停。
    if (name === 'tasks') startDashboardPoll(); else stopDashboardPoll();
    if (name === 'monitor') startMonitorPoll(); else stopMonitorPoll();
    // 回到首页：按当前控制台的 preview 即时重画（后台轮询期间可能已经跑完）
    if (name === 'home') renderActivePreview();
    // AI 输入框在隐藏态量不到高度（scrollHeight=0），显示后按实际内容重算一次；
    // 同时把对话区滚到底，切回来直接看到最新回答。
    if (name === 'ai') {
        if (aiInputGrow) aiInputGrow();
        aiHeroStick = true;
        aiUpdateLanding();
        aiScrollToBottom();
        aiFlushCharts();          // 隐藏态渲染的图表此时才量得到尺寸，补 init + resize
        refreshAiSessions(false);
    }
}

/** 从任务列表下钻到执行 DAG（归属「任务管理」tab）。 */
function switchToDagView(queryId) {
    stopDashboardPoll();
    // 注意：不动 SQL 结果区的轮询 —— 它按控制台各自跑（见 startSqlPreview）。
    // 之前在这里停表，「点任务列表进 DAG 看进度」就会永久掐死结果区：回到首页只剩切走前
    // 那一帧（GROUP BY 这类查询首块要到收尾才发，定格的就是「正在计算…」），而后台其实
    // 早已跑完 —— 表现成「后台执行完毕，结果出不来」。
    clearDagState();
    hideAllViews();

    currentJobId = queryId;
    document.getElementById('dag-view').classList.add('show');
    highlightTab('tasks');
    var badge = document.getElementById('header-badge');
    if (badge) badge.textContent = '执行 DAG';

    var title = document.getElementById('dag-query-text');
    if (title) { title.textContent = '加载中\u2026'; title.style.color = UI.text; }
    var sbadge = document.getElementById('dag-status-badge');
    if (sbadge) { sbadge.textContent = '\u2026'; sbadge.className = 'status-badge queued'; }

    startDagPoll(queryId);
}

/** 兼容旧调用：从 DAG 返回任务列表。 */
function switchToDashboard() {
    switchTab('tasks');
}

// ---------------- 顶栏「刷新当前页面」+ 整页刷新拦截 ----------------

/** 允许整页跳转（登出 / 401 跳登录页）：此时不再弹「请勿刷新」确认框。 */
var dojaoAllowUnload = false;

/**
 * 刷新按钮：只刷新当前选中的功能区，其他页面保持不动。
 * <ul>
 *   <li>首页：重拉 catalog 树 + 当前查询结果（不动编辑器 / 控制台标签）；</li>
 *   <li>任务管理：重拉任务列表与集群卡片；若正在看 DAG，则只重拉该 DAG 快照；</li>
 *   <li>监控：重拉资源快照与趋势序列；</li>
 *   <li>AI：只重拉左侧会话列表，不打断当前对话。</li>
 * </ul>
 */
function refreshCurrentView() {
    var btn = document.getElementById('refresh-btn');
    if (btn) { btn.disabled = true; btn.classList.add('spinning'); }
    var p;
    if (currentTab === 'home') {
        p = Promise.allSettled([
            loadCatalogTree(),
            sqlQueryId ? pollSqlResult() : Promise.resolve()
        ]);
    } else if (currentTab === 'tasks') {
        var dag = document.getElementById('dag-view');
        p = (dag && dag.classList.contains('show')) ? Promise.resolve(pollDagOnce()) : refreshAll();
    } else if (currentTab === 'monitor') {
        p = refreshMonitor();
    } else if (currentTab === 'ai') {
        p = refreshAiSessions(false);
    } else {
        p = Promise.resolve();
    }
    var started = Date.now();
    Promise.resolve(p).catch(function () {
        /* 各页已有自己的错误提示，这里不重复弹错 */
    }).then(function () {
        var wait = Math.max(0, 320 - (Date.now() - started));
        setTimeout(function () {
            if (btn) { btn.disabled = false; btn.classList.remove('spinning'); }
            showToast('已刷新当前页面');
        }, wait);
    });
}

/** 弹窗提醒：整页刷新会丢数据，引导使用顶栏刷新按钮。 */
function showRefreshGuard() {
    if (typeof mgModal !== 'function') return;
    var overlay = mgModal('请勿刷新整个页面',
        '<div class="refresh-guard-tip">刷新浏览器会重新加载整个控制台，'
        + '未保存的 SQL 草稿、当前查询结果、正在进行的 AI 对话以及页面状态都可能丢失。</div>'
        + '<div class="refresh-guard-tip"><b>请点击右上角的「刷新」按钮</b>：'
        + '它只刷新当前选中的页面，其他页面的数据与状态保持不动。</div>'
        + '<div class="mg-actions"><button type="button" class="mg-btn primary" '
        + 'id="refresh-guard-ok">我知道了</button></div>');
    var ok = overlay.querySelector('#refresh-guard-ok');
    if (ok) ok.onclick = mgClose;
}

/** 绑定整页刷新拦截（F5 / Ctrl+R / Cmd+R 与浏览器工具栏刷新）。 */
function bindRefreshGuard() {
    // capture 阶段抢在浏览器默认行为之前：拦下键盘直接刷新，改为页面内提示。
    document.addEventListener('keydown', function (e) {
        var key = (e.key || '').toLowerCase();
        var isRefresh = key === 'f5'
            || ((e.ctrlKey || e.metaKey) && !e.altKey && key === 'r');
        if (!isRefresh) return;
        e.preventDefault();
        e.stopPropagation();
        showRefreshGuard();
    }, true);

    // 工具栏刷新 / 关闭标签页只能走原生 beforeunload 确认框（现代浏览器不允许自定义文案）。
    window.addEventListener('beforeunload', function (e) {
        if (dojaoAllowUnload || window.__dojaoAllowUnload) return;
        e.preventDefault();
        e.returnValue = '请勿刷新页面：刷新会导致未保存的数据丢失。请使用右上角的刷新按钮只刷新当前页面。';
        return e.returnValue;
    });
}

// ---------------- SQL 编辑器：草稿记忆 + Ctrl+Z 撤销 ----------------

var SQL_UNDO_MAX = 200;                  // 撤销栈深度（步）
var SQL_UNDO_MERGE_MS = 400;             // 连续输入合并窗口：窗口内的键入算一步

/** 编辑器当前状态的快照（值 + 光标/选区，撤销时一并还原）。 */
function snapshotSqlEditor(input) {
    return {
        v: input.value || '',
        s: input.selectionStart,
        e: input.selectionEnd
    };
}

/**
 * 绑定 SQL 编辑器的两个「内容不丢」能力（首页初始化时调一次）：
 *
 * <ol>
 *   <li><b>草稿记忆</b>：输入即写 localStorage，浏览器 F5 / 重开标签页后原样恢复 ——
 *       写了一半的 SQL 不再被刷新清空；</li>
 *   <li><b>Ctrl+Z 撤销 / Ctrl+Shift+Z（或 Ctrl+Y）重做</b>：自建快照栈。为什么不靠浏览器原生撤销？
 *       原生撤销栈会被「程序化改 value」静默清空（拖拽目录树引用插入、草稿恢复都是），届时不仅
 *       新插的内容撤不掉，<b>之前的输入历史也一并消失</b> —— 表现成「Ctrl+Z 没反应」。自建栈把这些
 *       程序化改动也记成可撤销的一步。</li>
 * </ol>
 */
function bindSqlEditor() {
    var input = document.getElementById('query-input');
    if (!input) return;

    // 草稿的装载 / 持久化已改由「多查询控制台」模型统一接管（见 js/sql-console.js：
    // initSqlConsoles 会把当前控制台正文回填进编辑器，并迁移旧 dojao.sql.draft）。
    // 这里不再单独读写 localStorage，只维护撤销栈。

    var stack = [snapshotSqlEditor(input)];
    var idx = 0;
    var lastAt = 0;

    // 内容变化 → 交给控制台模型（更新 text / 脏态 / 落库）；模型未就绪时静默退化成「不记忆」
    function persist(v) {
        if (typeof onConsoleTextChanged === 'function') onConsoleTextChanged(v);
    }

    /** 内容变化：记一步（SQL_UNDO_MERGE_MS 内的连续键入合并成一步，避免逐字符撤销）。 */
    function onChanged() {
        var cur = snapshotSqlEditor(input);
        if (idx >= 0 && stack[idx].v === cur.v) return;      // 值没变（如纯光标移动）
        var merge = idx >= 0 && (Date.now() - lastAt) < SQL_UNDO_MERGE_MS;
        stack = stack.slice(0, idx + 1);                      // 新编辑 → 丢弃「重做」尾巴
        if (merge) {
            stack[idx] = cur;
        } else {
            stack.push(cur);
            if (stack.length > SQL_UNDO_MAX) stack.shift();
            idx = stack.length - 1;
        }
        lastAt = Date.now();
        persist(cur.v);
    }

    /** 跳到栈中第 i 个快照（撤销 / 重做共用）。 */
    function applyAt(i) {
        idx = i;
        var s = stack[i];
        input.value = s.v;
        try { input.setSelectionRange(s.s, s.e); } catch (e) { /* 隐藏元素上可能不支持 */ }
        persist(s.v);
        lastAt = 0;              // 撤销后紧接着输入 → 另起一步，不与撤销目标合并
        input.focus();
        // 程序化改 value 不触发 input 事件 → 高亮垫层会停在撤销前的文本上，须手动重画
        renderSqlHighlight();
    }

    input.addEventListener('input', onChanged);
    input.addEventListener('keydown', function (e) {
        if (!(e.ctrlKey || e.metaKey) || e.altKey) return;
        var k = (e.key || '').toLowerCase();
        if (k === 'z' && !e.shiftKey) {
            e.preventDefault();
            if (idx > 0) applyAt(idx - 1);
        } else if (k === 'y' || (k === 'z' && e.shiftKey)) {
            e.preventDefault();
            if (idx < stack.length - 1) applyAt(idx + 1);
        }
    });

    // 右键：剪切 / 复制 / 粘贴 / 全选 + 保存当前控制台为命名脚本（用户私有，见 manage.js「SQL 脚本管理」）
    input.addEventListener('contextmenu', function (e) {
        e.preventDefault();
        var hasSel = input.selectionStart !== input.selectionEnd;
        showContextMenu([
            { label: '剪切', disabled: !hasSel, onClick: function () { cutSqlSelection(input); } },
            { label: '复制', disabled: !hasSel, onClick: function () { copySqlSelection(input); } },
            { label: '粘贴', onClick: function () { pasteIntoSqlEditor(input); } },
            { label: '全选', onClick: function () { input.focus(); input.select(); } },
            { sep: true },
            { label: '保存当前 SQL 文件', onClick: saveSqlScript }
        ], e.clientX, e.clientY);
    });

    // Ctrl+S / Cmd+S：保存当前控制台到 SQLite（从未保存 → 弹命名框；已绑定脚本 → 直接同名覆盖）。
    // 只在首页拦截，其它页面保留浏览器默认行为。
    document.addEventListener('keydown', function (e) {
        if (!(e.ctrlKey || e.metaKey) || e.altKey || e.shiftKey) return;
        if ((e.key || '').toLowerCase() !== 's') return;
        if (typeof currentTab !== 'undefined' && currentTab !== 'home') return;
        var overlay = document.getElementById('mg-overlay');
        if (overlay && overlay.classList.contains('show')) return;   // 弹窗里按 Ctrl+S 不叠弹窗
        e.preventDefault();
        saveSqlScript();
    });

    // 暴露给「多控制台」切标签用。reset(text) 程序化回填正文并把撤销栈重置为「当前值」单快照，
    // 且**不派发 input 事件**——否则切标签会被 onChanged 记成一次编辑，把新控制台误标成脏。
    window.sqlEditorController = {
        reset: function (text) {
            input.value = (text == null ? '' : text);
            stack = [snapshotSqlEditor(input)];
            idx = 0;
            lastAt = 0;
            renderSqlHighlight();
        },
        value: function () { return input.value || ''; },
        focus: function () { input.focus(); }
    };

    // 结果手动出图：窗口尺寸变化时自适应；主题切换时用新主题重画（ECharts 主题只能在 init 指定）。
    window.addEventListener('resize', resizeSqlChart);
    if (typeof window.applyChartTheme === 'function' && !window.__sqlChartThemeHooked) {
        var _origSqlApplyChartTheme = window.applyChartTheme;
        window.applyChartTheme = function (dark) {
            _origSqlApplyChartTheme(dark);
            sqlReThemeChart();
        };
        window.__sqlChartThemeHooked = true;
    }
}

/**
 * 保存当前控制台内容为命名脚本（当前用户私有）。名字经 promptNameModal 收集，
 * 同名脚本由后端覆盖。保存后可在「SQL 脚本管理」里加载 / 删除。
 * 具体落库 + 控制台 title/savedText 对齐交给 sql-console.js 的 saveConsoleToScript。
 */
function saveSqlScript() {
    saveConsoleToScript(activeConsole());
}

/** 编辑器「复制」：有选区复制选区，无选区复制全文。 */
function copySqlSelection(input) {
    var sel = input.value.slice(input.selectionStart, input.selectionEnd);
    copyText(sel || input.value, function (ok) {
        showToast(ok ? (sel ? '已复制选中 SQL' : '已复制全部 SQL') : '复制失败');
    });
}

/** 编辑器「剪切」：先写剪贴板，成功后再删选区（剪贴板失败时不动内容）。 */
function cutSqlSelection(input) {
    var sel = input.value.slice(input.selectionStart, input.selectionEnd);
    if (!sel) return;
    copyText(sel, function (ok) {
        if (!ok) { showToast('剪切失败：无法写入剪贴板'); return; }
        replaceSqlSelection(input, '');
        showToast('已剪切选中 SQL');
    });
}

/** 编辑器「粘贴」：读取系统剪贴板（非安全上下文 / 未授权时提示改用 Ctrl+V）。 */
function pasteIntoSqlEditor(input) {
    if (!(navigator.clipboard && navigator.clipboard.readText)) {
        showToast('当前浏览器不支持右键粘贴，请用 Ctrl+V');
        return;
    }
    input.focus();
    navigator.clipboard.readText().then(function (text) {
        if (text) {
            replaceSqlSelection(input, text);
            showToast('已粘贴');
        }
    }, function () {
        showToast('浏览器未授权读取剪贴板，请用 Ctrl+V');
    });
}

/** 用 text 替换编辑器当前选区（无选区即插入光标处），派发 input 事件让撤销栈 / 高亮 / 脏态生效。 */
function replaceSqlSelection(input, text) {
    var s = input.selectionStart, e = input.selectionEnd;
    input.value = input.value.slice(0, s) + text + input.value.slice(e);
    var pos = s + text.length;
    input.setSelectionRange(pos, pos);
    input.dispatchEvent(new Event('input', { bubbles: true }));
}

/** 从「SQL 脚本管理」加载脚本 → <b>新开一个查询控制台</b>（不覆盖当前编辑器），并切回首页。 */
function loadSqlScriptIntoEditor(id) {
    httpGet('/api/scripts/' + encodeURIComponent(id)).then(function (s) {
        if (!s) return;
        mgClose();
        newQueryConsole({
            title: s.name || '脚本',
            scriptId: id,
            text: s.content || '',
            savedText: s.content || ''
        });
        if (typeof switchTab === 'function') switchTab('home');
        showToast('已加载脚本：' + (s.name || ''));
    }).catch(function (err) {
        showToast('加载失败：' + oneLine(err && err.message ? err.message : err, 80));
    });
}
