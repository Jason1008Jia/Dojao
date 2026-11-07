// ==================== 监控页：集群概览 / 节点资源 / 资源趋势 ====================
//
// 数据源两个（都在 2s 轮询里并行拉）：
//   GET /api/cluster          —— 当前快照：Master + 各 Worker 节点卡 + 活动/最近查询
//   GET /api/cluster/history  —— 时间序列：概览卡迷你趋势线 + 八张折线图
//
// 设计取舍：
//   - 轮询只在「监控 tab 可见」时跑（switchTab 里 start/stop），切走即停，不占后台资源；
//   - 图表全部手写 canvas（charts.js），零依赖；
//   - 时间序列启动后才积累，没有历史回填 —— 刚打开时折线只有一两个点，属正常；
//   - 节点配色只有一份来源（monitorColorOf），卡片、迷你线、折线图三处必须一致。

/** 历史折线保留的采样点数（2s × 180 ≈ 6 分钟）。 */
var MONITOR_HISTORY_POINTS = 180;

var monitorTimer = null;         // 监控页轮询定时器
var monitorData = null;          // 最近一次 /api/cluster
var monitorHistory = null;       // 最近一次 /api/cluster/history（{series:{nodeId:[sample]}}）

function startMonitorPoll() {
    stopMonitorPoll();
    bindMonitorResize();
    refreshMonitor();
    monitorTimer = setInterval(refreshMonitor, 2000);
}

function stopMonitorPoll() {
    if (monitorTimer) { clearInterval(monitorTimer); monitorTimer = null; }
}

/** 拉一次快照 + 历史，然后整体重渲染。两个请求互不阻塞（allSettled）。 */
function refreshMonitor() {
    return Promise.allSettled([
        httpGet('/api/cluster').then(function (d) { monitorData = d; }),
        httpGet('/api/cluster/history?max=' + MONITOR_HISTORY_POINTS).then(function (d) {
            monitorHistory = d;
        })
    ]).then(function (results) {
        var err = null;
        results.forEach(function (r) {
            if (r.status === 'rejected' && !err) err = r.reason;
        });
        setMonitorBanner(err);
        if (monitorData) {
            renderMonitorCards(monitorData);
            renderNodeList(monitorData);
        }
        if (monitorHistory) renderMonitorCharts(monitorHistory);
    });
}

function setMonitorBanner(err) {
    var el = document.getElementById('monitor-error');
    if (!el) return;
    if (!err) { el.style.display = 'none'; el.textContent = ''; return; }
    el.style.display = '';
    el.textContent = err.status === 503
        ? '控制台未绑定查询监控（独立预览模式）—— 监控数据不可用。'
        : '监控数据拉取失败：' + oneLine(err.message || String(err), 120);
}

// ---------------- 通用：节点集合 / 顺序 / 配色 / 状态 ----------------

/** Master + 全部 Worker（统一成节点列表；master 为 null 时跳过）。 */
function allNodes(cluster) {
    var out = [];
    if (!cluster) return out;
    if (cluster.master) out.push(cluster.master);
    if (cluster.workers) cluster.workers.forEach(function (w) { out.push(w); });
    return out;
}

/**
 * 节点顺序（master 恒排第一，其余按 id 升序）与配色。
 * 卡片、迷你趋势线、折线图三处必须共用同一份映射，否则同一个 Worker
 * 在卡片里是蓝色、在折线图里是青色，看图对不上号。
 * 取并集：快照里的节点 + 历史里还有点的节点（刚下线的节点仍有曲线可看）。
 */
function monitorNodeIds() {
    var ids = [];
    allNodes(monitorData).forEach(function (n) {
        if (n && n.workerId && ids.indexOf(n.workerId) < 0) ids.push(n.workerId);
    });
    var series = (monitorHistory && monitorHistory.series) || {};
    Object.keys(series).forEach(function (id) { if (ids.indexOf(id) < 0) ids.push(id); });
    ids.sort(function (a, b) {
        if (a === 'master') return -1;
        if (b === 'master') return 1;
        return a < b ? -1 : a > b ? 1 : 0;
    });
    return ids;
}

/**
 * 节点配色：**master 深绿、worker 亮绿**（品牌双绿）。
 * 与 monitorNodeIds() 一样是全页唯一来源 —— 节点卡与折线图必须同色。
 */
function monitorColorOf() {
    var map = {};
    monitorNodeIds().forEach(function (id) {
        map[id] = (id === 'master') ? CHART_BRAND.master : CHART_BRAND.worker;
    });
    return map;
}

/**
 * workerId → 展示名映射：worker 一律显示<b>所在机器的 hostname</b>（master 仍显示 master）。
 *
 * <p>同机多 Worker（开发时常在一台机器起多个进程）hostname 会重名，此时追加端口区分；
 * 端口也拿不到就退回短 ID。快照里已下线、历史序列里仍有点的节点 → 短 ID 兜底。
 * 任务管理页（{@code clusterData}）与监控页（{@code monitorData}）共用同一份映射来源。</p>
 */
function nodeLabelMap() {
    var nodes = allNodes(monitorData);
    if (!nodes.length) nodes = allNodes(clusterData);
    var hostCount = {};
    nodes.forEach(function (n) {
        if (n && n.role !== 'master' && n.host) hostCount[n.host] = (hostCount[n.host] || 0) + 1;
    });
    var map = {};
    nodes.forEach(function (n) {
        if (!n || !n.workerId) return;
        if (n.role === 'master') { map[n.workerId] = 'master'; return; }
        if (!n.host) return;
        map[n.workerId] = hostCount[n.host] > 1
            ? n.host + (n.port > 0 ? ':' + n.port : ' #' + shortNodeId(n.workerId))   // 同机多 Worker：端口消歧
            : n.host;
    });
    return map;
}

/** 单节点展示名（节点卡 / 折线图图例 / 执行 DAG 任务行共用）。 */
function nodeLabelOf(id) {
    if (!id) return '-';
    if (id === 'master') return 'master';
    return nodeLabelMap()[id] || shortNodeId(id);
}

/** 兜底短名：快照里找不到该 worker（已下线）时用 workerId 前 8 位。 */
function shortNodeId(id) {
    if (!id) return '-';
    if (id === 'master') return 'master';
    return id.length > 10 ? id.slice(0, 8) : id;
}

/**
 * 把历史里「逐节点序列」按时间对齐成「一条集群序列」。
 *
 * <p>各节点的点由同一次心跳写入，但节点可能是中途注册的，长度会差几拍 ——
 * 因此**按尾部对齐**（最新点永远在末尾），短序列前面的空位跳过。</p>
 *
 * @param {string} field 采样字段名
 * @param {string} agg   'sum' | 'wavg'（按 CPU 核数加权平均）
 * @returns {Array<{t:number, v:number}>}
 */
function clusterSeries(field, agg) {
    var series = (monitorHistory && monitorHistory.series) || {};
    var ids = monitorNodeIds();
    var maxLen = 0;
    ids.forEach(function (id) {
        var n = (series[id] || []).length;
        if (n > maxLen) maxLen = n;
    });
    var weights = {};
    allNodes(monitorData).forEach(function (n) { weights[n.workerId] = n.cpuCores || 1; });

    var out = [];
    for (var i = 0; i < maxLen; i++) {
        var acc = 0, wsum = 0, t = 0, any = false;
        ids.forEach(function (id) {
            var pts = series[id] || [];
            var p = pts[pts.length - maxLen + i];
            if (!p) return;
            if (p.t > t) t = p.t;
            var v = p[field];
            if (!isFinite(v) || v < 0) return;
            if (agg === 'wavg') {
                var w = weights[id] || 1;
                acc += v * w;
                wsum += w;
            } else {
                acc += v;
            }
            any = true;
        });
        if (!any) continue;
        out.push({ t: t, v: agg === 'wavg' ? (wsum ? acc / wsum : 0) : acc });
    }
    return out;
}

/** 字节 → {num, unit}：复用 fmtBytes 的阶梯，只把「数值 / 单位」拆开给大字+小字排版。 */
function byteParts(b) {
    var s = fmtBytes(b).split(' ');
    return { num: s[0], unit: s[1] || 'B' };
}

/**
 * 节点状态点：离线灰 / 有任一资源 ≥80% 黄 / 其余绿。
 * @returns {'online'|'warn'|'offline'}
 */
function nodeStatus(n) {
    if (!n.healthy) return 'offline';
    var pcts = [n.cpuUsagePct, memPctOf(n), heapPctOf(n), diskPctOf(n)];
    for (var i = 0; i < pcts.length; i++) {
        if (Number(pcts[i]) >= 80) return 'warn';
    }
    return 'online';
}

function memPctOf(n) {
    return n.totalMemoryBytes ? n.usedMemoryBytes / n.totalMemoryBytes * 100 : 0;
}

function heapPctOf(n) {
    return n.heapMaxBytes > 0 ? n.heapUsedBytes / n.heapMaxBytes * 100 : 0;
}

function diskPctOf(n) {
    return n.totalDiskBytes ? (n.totalDiskBytes - n.freeDiskBytes) / n.totalDiskBytes * 100 : 0;
}

function directPctOf(n) {
    return n.directBudgetBytes > 0 ? n.directUsedBytes / n.directBudgetBytes * 100 : 0;
}

// ---------------- 概览卡片：大数字 + 小标签 + 迷你趋势线 ----------------

function renderMonitorCards(cluster) {
    var nodes = allNodes(cluster);
    var healthy = nodes.filter(function (n) { return n.healthy; });

    var cores = 0, cpuSum = 0, cpuW = 0;
    var totalMem = 0, usedMem = 0, heapUsed = 0;
    var threads = 0, slots = 0, tasks = 0;
    healthy.forEach(function (n) {
        cores += n.cpuCores || 0;
        if (n.cpuUsagePct >= 0) {
            var w = n.cpuCores || 1;
            cpuSum += n.cpuUsagePct * w;
            cpuW += w;
        }
        totalMem += n.totalMemoryBytes || 0;
        usedMem += n.usedMemoryBytes || 0;
        heapUsed += n.heapUsedBytes || 0;
        threads += n.threadCount || 0;
        slots += n.maxConcurrency || 0;
        tasks += n.runningQueries || 0;
    });
    var cpuPct = cpuW ? cpuSum / cpuW : 0;

    setText('mon-cpu', cpuW ? String(Math.round(cpuPct)) : '\u2014');
    var mem = byteParts(usedMem);
    setText('mon-mem', mem.num);
    setText('mon-mem-unit', mem.unit);
    var heap = byteParts(heapUsed);
    setText('mon-heap', heap.num);
    setText('mon-heap-unit', heap.unit);
    setText('mon-threads', fmtRows(threads));

    setText('mon-nodes', String(healthy.length));
    setText('mon-nodes-total', '/ ' + nodes.length);
    var offline = nodes.length - healthy.length;
    var warn = healthy.filter(function (n) { return nodeStatus(n) === 'warn'; }).length;
    setText('mon-nodes-sub', offline ? offline + ' 个离线'
            : (warn ? warn + ' 个负载偏高' : '全部在线'));

    setText('mon-cores', String(cores));
    setText('mon-cores-sub', uniform(nodes, 'cpuCores', ' 核'));

    setText('mon-slots', String(slots));
    setText('mon-slots-sub', uniform(nodes, 'maxConcurrency', ' 槽位'));

    setText('mon-tasks', String(tasks));
    var active = (cluster && cluster.activeQueries) ? cluster.activeQueries.length : 0;
    setText('mon-tasks-sub', active ? '活动查询 ' + active + ' 条' : '暂无运行中的查询');

    // 4 条集群级迷你趋势线（与折线图同一套派生口径；线色 = 品牌主绿）
    var ink = CHART_BRAND.master;
    sparkline(document.getElementById('mon-spark-cpu'), vals(clusterSeries('cpuPct', 'wavg')), ink);
    sparkline(document.getElementById('mon-spark-mem'), vals(clusterSeries('usedMemoryBytes', 'sum')), ink);
    sparkline(document.getElementById('mon-spark-heap'), vals(clusterSeries('heapUsedBytes', 'sum')), ink);
    sparkline(document.getElementById('mon-spark-thread'), vals(clusterSeries('threadCount', 'sum')), ink);
}

function vals(points) {
    return (points || []).map(function (p) { return p.v; });
}

/** 「单节点 N 核」这类说明行（各节点配置不一致时如实说明）。 */
function uniform(nodes, field, suffix) {
    if (!nodes.length) return '\u2014';
    var first = nodes[0][field] || 0;
    var same = nodes.every(function (x) { return (x[field] || 0) === first; });
    return same ? '单节点 ' + first + suffix : '各节点配置不一';
}

// ---------------- 节点卡片：一节点一卡，一个指标一行 ----------------

function renderNodeList(cluster) {
    var container = document.getElementById('mon-node-list');
    if (!container) return;
    var nodes = allNodes(cluster);
    if (!nodes.length) {
        container.innerHTML = '<div class="mon-empty">暂无节点（集群可能尚未启动 Worker）。</div>';
        return;
    }
    var labels = nodeLabelMap();
    container.innerHTML = nodes.map(function (n) {
        var isMaster = n.role === 'master';
        var label = labels[n.workerId] || shortNodeId(n.workerId);
        var memPct = memPctOf(n), heapPct = heapPctOf(n), diskPct = diskPctOf(n), directPct = directPctOf(n);
        var stateTxt = n.healthy ? esc(n.state) : 'UNHEALTHY';
        return '<div class="node-card">'
            + '<div class="node-head">'
            +   '<span class="node-dot ' + nodeStatus(n) + '"></span>'
            +   '<span class="node-name" title="' + esc(n.workerId) + '">' + esc(label) + '</span>'
            +   '<span class="node-role">' + (isMaster ? 'MASTER' : 'WORKER') + '</span>'
            + '</div>'
            + '<div class="node-meta">' + esc(n.host || '-') + (n.port ? ':' + n.port : '')
            +   ' · ' + stateTxt + '</div>'
            + '<div class="node-metrics">'
            +   metricRow('CPU', n.cpuUsagePct >= 0 ? Math.round(n.cpuUsagePct) + '%' : '\u2014',
                        n.cpuUsagePct >= 0 ? n.cpuUsagePct : 0)
            +   metricRow('系统内存', fmtBytes(n.usedMemoryBytes) + ' / ' + fmtBytes(n.totalMemoryBytes), memPct)
            +   (n.processRssBytes > 0
                    ? metricRow('进程内存(RSS)', fmtBytes(n.processRssBytes), 0)
                    : '')
            +   metricRow('堆', fmtBytes(n.heapUsedBytes) + ' / '
                        + (n.heapMaxBytes > 0 ? fmtBytes(n.heapMaxBytes) : '\u221e'), heapPct)
            +   metricRow('直接内存(受管)', fmtBytes(n.directUsedBytes) + ' / '
                        + (n.directBudgetBytes > 0 ? fmtBytes(n.directBudgetBytes) : '\u221e'), directPct)
            +   metricRow('磁盘', fmtBytes(n.totalDiskBytes - n.freeDiskBytes) + ' / '
                        + fmtBytes(n.totalDiskBytes), diskPct)
            + '</div>'
            + renderAllocators(n.allocators)
            + '<div class="node-foot">'
            +   '<span>线程 <b>' + fmtRows(n.threadCount) + '</b></span>'
            +   '<span>槽位 <b>' + (n.maxConcurrency || 0) + '</b></span>'
            +   '<span>任务 <b>' + (n.runningQueries || 0) + '</b></span>'
            + '</div>'
            + '</div>';
    }).join('');
}

/** 直接内存按类别明细（用量 / 峰值 / 上限），无数据时给一行占位。 */
function renderAllocators(allocators) {
    var list = (allocators || []).filter(function (a) { return (a.allocatedBytes || 0) > 0 || (a.peakBytes || 0) > 0; });
    if (!list.length) {
        return '<div class="node-alloc"><div class="node-alloc-empty">无直接内存分配</div></div>';
    }
    return '<div class="node-alloc">'
        + '<div class="node-alloc-title">直接内存明细</div>'
        + list.map(function (a) {
            var pct = a.limitBytes > 0 ? a.allocatedBytes / a.limitBytes * 100 : 0;
            return '<div class="alloc-row">'
                + '<div class="alloc-top">'
                +   '<span class="alloc-name" title="' + esc(a.category) + '">' + esc(a.category) + '</span>'
                +   '<span class="alloc-val">' + fmtBytes(a.allocatedBytes)
                +     (a.peakBytes > a.allocatedBytes ? ' <em>峰值 ' + fmtBytes(a.peakBytes) + '</em>' : '')
                +   '</span>'
                + '</div>'
                + usageBar(pct)
                + '</div>';
        }).join('')
        + '</div>';
}

/** 一个指标一行：标签 + 数值（同一行），下面一条细进度条（>80% 才变色）。 */
function metricRow(label, value, pct) {
    return '<div class="metric">'
        + '<div class="metric-top"><span class="metric-label">' + label + '</span>'
        + '<span class="metric-value">' + value + '</span></div>'
        + usageBar(pct)
        + '</div>';
}

// ---------------- 八张折线图 ----------------

function renderMonitorCharts(history) {
    var series = (history && history.series) || {};
    var colorOf = monitorColorOf();
    var labels = nodeLabelMap();      // 图例显示 worker 所在机器 hostname
    var order = monitorNodeIds();
    var ids = Object.keys(series);
    ids.sort(function (a, b) {
        var ia = order.indexOf(a), ib = order.indexOf(b);
        if (ia >= 0 && ib >= 0) return ia - ib;
        if (ia >= 0) return -1;
        if (ib >= 0) return 1;
        return a < b ? -1 : a > b ? 1 : 0;
    });

    var cpu = [], net = [], heap = [], thread = [], mem = [], direct = [], diskRead = [], diskWrite = [];
    ids.forEach(function (id, i) {
        var pts = series[id] || [];
        var color = colorOf[id] || chartColor(i);
        var label = labels[id] || shortNodeId(id);
        cpu.push({ name: label, color: color, points: pts.map(function (p) {
            return { t: p.t, v: p.cpuPct >= 0 ? p.cpuPct : 0 };
        }) });
        net.push({ name: label, color: color, points: pts.map(function (p) {
            return { t: p.t, v: (p.netRxBytesPerSec || 0) + (p.netTxBytesPerSec || 0) };
        }) });
        heap.push({ name: label, color: color, points: pts.map(function (p) {
            return { t: p.t, v: p.heapUsedBytes };
        }) });
        thread.push({ name: label, color: color, points: pts.map(function (p) {
            return { t: p.t, v: p.threadCount };
        }) });
        mem.push({ name: label, color: color, points: pts.map(function (p) {
            return { t: p.t, v: p.usedMemoryBytes };
        }) });
        direct.push({ name: label, color: color, points: pts.map(function (p) {
            return { t: p.t, v: p.directUsedBytes || 0 };
        }) });
        diskRead.push({ name: label, color: color, points: pts.map(function (p) {
            return { t: p.t, v: p.diskReadBytesPerSec || 0 };
        }) });
        diskWrite.push({ name: label, color: color, points: pts.map(function (p) {
            return { t: p.t, v: p.diskWriteBytesPerSec || 0 };
        }) });
    });

    lineChart(document.getElementById('mon-chart-cpu'), cpu,
            { unit: 'pct', max: 100, min: 0, emptyText: 'CPU 采样积累中\u2026' });
    lineChart(document.getElementById('mon-chart-net'), net,
            { unit: 'bytesPerSec', emptyText: '网络采样积累中\u2026' });
    lineChart(document.getElementById('mon-chart-heap'), heap,
            { unit: 'bytes', emptyText: '堆采样积累中\u2026' });
    lineChart(document.getElementById('mon-chart-thread'), thread,
            { unit: 'count', yTicks: 3, emptyText: '线程采样积累中\u2026' });
    lineChart(document.getElementById('mon-chart-mem'), mem,
            { unit: 'bytes', emptyText: '内存采样积累中\u2026' });
    lineChart(document.getElementById('mon-chart-direct'), direct,
            { unit: 'bytes', emptyText: '直接内存采样积累中\u2026' });
    lineChart(document.getElementById('mon-chart-diskread'), diskRead,
            { unit: 'bytes', emptyText: '磁盘读采样积累中\u2026' });
    lineChart(document.getElementById('mon-chart-diskwrite'), diskWrite,
            { unit: 'bytes', emptyText: '磁盘写采样积累中\u2026' });
}

/** window resize：canvas 有固定逻辑尺寸（dpr 缩放不会自动跟随宽度）→ 重绘。 */
var monitorResizeBound = false;
function bindMonitorResize() {
    if (monitorResizeBound) return;
    monitorResizeBound = true;
    window.addEventListener('resize', function () {
        if (currentTab !== 'monitor') return;
        if (monitorData) renderMonitorCards(monitorData);
        if (monitorHistory) renderMonitorCharts(monitorHistory);
    });
}

function setText(id, v) {
    var el = document.getElementById(id);
    if (el) el.textContent = v;
}
