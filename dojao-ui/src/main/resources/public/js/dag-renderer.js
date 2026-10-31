// ==================== DAG Rendering（真实算子快照驱动）====================

/** DAG 末端 Output 终端节点 id：展示查询最终结果回流（镜像根算子状态）。 */
var OUTPUT_NODE_ID = 'out';

/**
 * 节点尺寸缓存：宽度/高度按 20px 量化且只增不减。
 *
 * <p>label 里含实时数字（in/out/avg/elapsed/mem），轮询每 300ms 会让 {@code measureText}
 * 的宽度变几像素 ⇒ vis 的节点包围盒跟着变 ⇒ 连线端点被反复重新锚定，视觉上就是整张图
 * 一直在抖。量化后只有跨档（很少）才改尺寸，稳态下节点框完全不动。</p>
 */
var nodeSizeCache = {};
var NODE_SIZE_QUANT = 20;

function stableNodeSize(id, rawW, rawH) {
    var qw = Math.ceil(rawW / NODE_SIZE_QUANT) * NODE_SIZE_QUANT;
    var qh = Math.ceil(rawH / NODE_SIZE_QUANT) * NODE_SIZE_QUANT;
    var c = nodeSizeCache[id];
    if (!c) {
        c = { w: qw, h: qh };
        nodeSizeCache[id] = c;
        return c;
    }
    if (qw > c.w) c.w = qw;
    if (qh > c.h) c.h = qh;
    return c;
}

/** Custom node renderer（深色）：pending 灰 / running 深底+绿色进度条推进 / done 实心绿 /
 *  warn 黄（部分 task 失败、算子未判死）/ failed 整红。 */
function progressNodeRenderer(rendererParams) {
    const ctx = rendererParams.ctx;
    const id = rendererParams.id;
    var x = rendererParams.x;
    var y = rendererParams.y;

    var info = nodeProgress[id] || { pct: 0, status: 'pending' };
    var status = info.status;
    // 显示用进度：优先缓动中的 nodeDispPct（轮询一跳一跳，缓动后视觉上是连续推进）
    var pct = (nodeDispPct[id] != null) ? nodeDispPct[id] : (info.pct || 0);
    var label = nodeLabelCache[id] || ('' + id);
    var lines = label.split('\n');

    var s = DAG_SCALE;
    var fontSize = Math.round(12 * s < 10 ? 10 : 12 * s);
    var minW = Math.round(220 * s < 150 ? 150 : 220 * s);
    var lineH = Math.round(15 * s);
    var pad = Math.round(10 * s);

    ctx.font = fontSize + 'px monospace';
    var maxLineW = 0;
    for (var li = 0; li < lines.length; li++) {
        var m = ctx.measureText(lines[li]);
        if (m.width > maxLineW) maxLineW = m.width;
    }
    var box = stableNodeSize(id, Math.max(minW, maxLineW + pad * 2), pad * 2 + lineH * lines.length);
    var w = box.w, h = box.h;
    var halfW = w / 2, halfH = h / 2;
    var left = x - halfW, top = y - halfH;
    var radius = 4;

    // 语义优先分派：failed=整红 / warn=黄(部分 task 失败) / done=实心绿 /
    // running=浅蓝底 + 蓝进度条（overlay 按 pct 从左侧向前铺满） / pending=白底浅灰细边。
    // 边框一律 1px 细描边（见下方 ctx.lineWidth）——语义靠颜色区分，不靠加粗；
    // pending 也从 UI.card 改配 lineStrong：初始态轮廓必须看得见，否则节点像一团投影。
    var base, stroke, overlay, textColor;
    if (status === 'failed') {
        base = UI.errFill; stroke = UI.err; overlay = null;
        textColor = '#fff';
    } else if (status === 'warn') {
        base = UI.warnFill; stroke = UI.warnStroke; overlay = null;
        textColor = UI.warnText;
    } else if (status === 'running') {
        base = UI.runBg; stroke = UI.runStroke; overlay = UI.runFill;
        textColor = UI.runText;
    } else if (status === 'done') {
        base = UI.doneFill; stroke = UI.doneStroke; overlay = null;
        textColor = '#fff';
    } else { // pending / SUBMITTED / 未知
        base = UI.card; stroke = UI.lineStrong; overlay = null;
        textColor = UI.dim;
    }

    // 卡片底 + 柔和投影：给画布增加层次（画完立即 restore，避免文字发虚）。
    // pending 投影刻意压到很浅（blur 4 / offsetY 1）→ 细边框不会被投影糊成"粗边"。
    ctx.save();
    ctx.shadowColor = status === 'running' ? 'rgba(37,99,235,0.28)'
        : status === 'done' ? 'rgba(5,150,105,0.22)'
        : status === 'failed' ? 'rgba(220,38,38,0.25)'
        : status === 'warn' ? 'rgba(217,119,6,0.22)'
        : 'rgba(15,23,42,0.10)';
    ctx.shadowBlur = (status === 'pending') ? 4 : 10;
    ctx.shadowOffsetY = (status === 'pending') ? 1 : 0;
    roundRectPath(ctx, left, top, w, h, radius);
    ctx.fillStyle = base;
    ctx.fill();
    ctx.restore();

    // 进度条：占满节点高度、从左侧向前推进。
    //   ① 自身不描边 —— 一旦描边就会读成"进度条也有个边框"；轮廓只由节点自己的细边框界定。
    //   ② 内缩 1.5px，明确落在节点边框内侧，不与边框叠成双层线。
    //   ③ 只给前缘（推进方向 = 右）一层很浅的柔影（blur 4 / offsetX 2）→ 观感上一直在往前推，
    //      但不会糊成一条粗边（用户反馈「阴影不要那么大」）。
    //   ④ 阴影裁剪在节点圆角矩形内，不溢出到画布上。
    if (overlay) {
        var fillW = Math.max(0, w * Math.min(100, pct) / 100);
        var ins = 1.5;
        var bw = Math.min(w - ins * 2, fillW - ins);
        var bh = h - ins * 2;
        if (bw > 0 && bh > 0) {
            var innerR = Math.max(0, Math.min(radius - ins, Math.min(bw, bh) / 2));
            ctx.save();
            roundRectPath(ctx, left, top, w, h, radius);
            ctx.clip();
            roundRectPath(ctx, left + ins, top + ins, bw, bh, innerR);
            ctx.shadowColor = 'rgba(37, 99, 235, 0.22)';
            ctx.shadowBlur = 4;
            ctx.shadowOffsetX = 2;
            ctx.fillStyle = overlay;
            ctx.fill();
            ctx.restore();
        }
    }

    // 节点边框：统一 1px 细线，最后画 → 轮廓完整压在进度条两端之上
    roundRectPath(ctx, left, top, w, h, radius);
    ctx.strokeStyle = stroke;
    ctx.lineWidth = 1;
    ctx.stroke();

    ctx.fillStyle = textColor;
    ctx.font = fontSize + 'px monospace';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';

    for (var i = 0; i < lines.length; i++) {
        ctx.fillText(lines[i], x, top + pad + lineH * i + lineH / 2);
    }

    return { nodeDimensions: { width: w, height: h } };
}

/** Topological left-to-right layout */
function manualLayout(nodeList, edgeList) {
    var adj = {}, inDeg = {};
    nodeList.forEach(function (n) { adj[n.id] = []; inDeg[n.id] = 0; });
    edgeList.forEach(function (e) {
        if (adj[e.from]) adj[e.from].push(e.to);
        if (inDeg[e.to] !== undefined) inDeg[e.to]++;
    });

    const rank = {}, queue = [];
    Object.keys(inDeg).forEach(function (id) {
        if (inDeg[id] === 0) { queue.push(id); rank[id] = 0; }
    });
    while (queue.length > 0) {
        var u = queue.shift();
        (adj[u] || []).forEach(function (v) {
            rank[v] = Math.max(rank[v] || 0, (rank[u] || 0) + 1);
            inDeg[v]--;
            if (inDeg[v] === 0) queue.push(v);
        });
    }

    var byRank = {};
    Object.keys(rank).forEach(function (id) {
        var r = rank[id];
        if (!byRank[r]) byRank[r] = [];
        byRank[r].push(id);
    });

    var maxPerRank = 0;
    Object.keys(byRank).forEach(function (r) {
        if (byRank[r].length > maxPerRank) maxPerRank = byRank[r].length;
    });
    const totalRanks = Object.keys(byRank).length;
    const H_GAP = Math.max(250, 380 - totalRanks * 14);
    const V_GAP = Math.max(110, 170 - maxPerRank * 10);
    const pos = {};
    Object.keys(byRank).sort(function (a, b) { return a - b; }).forEach(function (r) {
        const ids = byRank[r];
        ids.forEach(function (id, i) {
            pos[id] = {
                x: r * H_GAP,
                y: i * V_GAP - (ids.length - 1) * V_GAP / 2
            };
        });
    });
    return pos;
}

/** DAG 结构签名：idx + children 的组合（任务阶段展开会让节点数在运行中变化，据此决定重建）。
 *  children 先按数值排序：服务端只是「同一组边」的枚举顺序变化时，不能触发整图重建。 */
function dagStructureKey(ops) {
    if (!ops || !ops.length) return '';
    var parts = ops.map(function (o) {
        var ch = (o.children || []).slice().sort(function (a, b) { return a - b; });
        return o.idx + ':' + ch.join(',');
    });
    parts.sort();
    return parts.join('|');
}

/** 判断是否为真正的 Scan 阶段节点，并取该来源的探查总量（每任务同源 expectedRows，>0 才有）；
 *  非 Scan/shuffle 阶段的节点（rowsOut ≠ 扫描行）不按行数算进度 → 返回 -1。 */
function scanStageExpected(op) {
    var tasks = op && op.tasks;
    var total = (tasks && tasks.length) || 0;
    if (!total) return -1;
    var t = String((op && op.type) || '').toLowerCase();
    if (t.indexOf('scan') < 0 && t.indexOf('shuffle') < 0) return -1;
    var expected = -1;
    for (var i = 0; i < total; i++) {
        var ex = Number((tasks[i] && tasks[i].expectedRows) || 0);
        if (ex <= 0) return -1;
        if (ex > expected) expected = ex;
    }
    return expected;
}

/** 进度副文本：Scan 叶子按「实时扫行 / 总量」显示（任务整程 RUNNING，任务数无意义）；其余按完成任务数。 */
function taskProgressText(op) {
    var tasks = op.tasks;
    var total = (tasks && tasks.length) || 0;
    if (!total) return '';
    var expected = scanStageExpected(op);
    if (expected > 0) {
        var live = Number(op.rowsOut) || 0;
        return fmtRows(live) + ' / ' + fmtRows(expected) + ' rows';
    }
    var done = 0;
    for (var j = 0; j < total; j++) {
        if (tasks[j].state === 'FINISHED') done++;
    }
    return done + '/' + total + ' tasks';
}

/** 实时进度百分比：
 *  ★ Scan 叶子（任务带探查总量 expectedRows>0）→ 实时扫行 op.rowsOut / 总量估计：
 *    扫描数据不回 Master、任务到 EOF 才 FINISHED，"完成任务占比"整程恒 0 —— 只有行数渐变真实。
 *    运行中封顶 99（避免提前看似满格），state 翻 FINISHED 才由 done 全绿。
 *  ★ 其余算子（终端计算 / 无总量）→ 仍按已完成任务数占比。 */
function opProgressPct(op) {
    if (!op) return 0;
    if (op.state === 'FINISHED') return 100;
    var expected = scanStageExpected(op);
    if (expected > 0) {
        var live = Number(op.rowsOut) || 0;
        return Math.min(99, Math.round(live * 100 / expected));
    }
    var tasks = op.tasks;
    var total = (tasks && tasks.length) || 0;
    var done = 0;
    for (var j = 0; j < total; j++) {
        if (tasks[j].state === 'FINISHED') done++;
    }
    return total ? Math.round(done * 100 / total) : 0;
}

/** 算子任务里是否有 FAILED / LOST（算子整体未判死前的「部分任务失败」信号 → 节点转黄）。 */
function anyTaskFailed(op) {
    var tasks = op && op.tasks;
    if (!tasks || !tasks.length) return false;
    for (var i = 0; i < tasks.length; i++) {
        var s = tasks[i].state;
        if (s === 'FAILED' || s === 'LOST') return true;
    }
    return false;
}

/**
 * 算子节点视觉元信息：status ∈ failed/warn/done/running/pending。
 * 规则：算子 FAILED → 红；算子未判死但已有 task FAILED/LOST → 黄（部分失败）；
 * 全部完成（FINISHED）→ 绿；RUNNING → 绿进度条按任务完成度推进；其余 → 灰。
 */
function nodeInfoFor(op) {
    var st = String((op && op.state) || '');
    var pct = opProgressPct(op);
    if (st === 'FAILED') return { pct: 100, status: 'failed' };
    if (anyTaskFailed(op)) return { pct: pct, status: 'warn' };
    if (st === 'FINISHED') return { pct: 100, status: 'done' };
    if (st === 'RUNNING') return { pct: pct, status: 'running' };
    return { pct: pct, status: 'pending' };
}

/** DAG 末端（sink）算子：未被任何其它算子 children 引用的节点 = 最终结果回流给客户端的那一个。 */
function rootOpOf(ops) {
    if (!ops || !ops.length) return null;
    var isChild = {};
    ops.forEach(function (o) { (o.children || []).forEach(function (c) { isChild[c] = true; }); });
    var sink = null;
    ops.forEach(function (o) { if (!isChild[o.idx] && (sink === null || o.idx < sink.idx)) sink = o; });
    return sink || ops[0];
}

/** Output 终端节点视觉：查询级状态优先（FAILED→红 / FINISHED→绿），运行中进度反射根算子。 */
function outputVisual(snap, rootOp) {
    var st = String((snap && snap.state) || '');
    var ri = rootOp ? nodeInfoFor(rootOp) : { pct: 0, status: 'pending' };
    if (st === 'FAILED') return { pct: 100, status: 'failed' };
    if (st === 'FINISHED') return { pct: 100, status: 'done' };
    if (ri.status === 'done' || ri.status === 'failed' || ri.status === 'warn') return ri;
    if (st === 'RUNNING') return { pct: ri.pct, status: 'running' };
    return { pct: 0, status: 'pending' };
}

/** Output 节点多行 label。 */
function outputLabel(snap) {
    var st = String((snap && snap.state) || '');
    var rows = Number((snap && snap.totalRowsOut) || 0);
    return 'Output   ' + st + '\n' + ' out ' + fmtRows(rows);
}

/**
 * DAG 节点标题：Scan 叶子带基表名（tasks[0].source），阶段框按任务类别取主语义。
 * 例如 Scan dga.users / Scan dga.products ⇒ Join ⇒ Agg。
 */
function opTitle(op) {
    var t = String(op.type || '');
    var lt = t.toLowerCase();
    var t0 = op.tasks && op.tasks.length ? op.tasks[0] : null;
    if (t0 && t0.producer && (lt.indexOf('scan') >= 0 || lt.indexOf('shuffle') >= 0)) {
        var src = (t0.source || '').trim();
        return 'Scan' + (src ? ' ' + src : '');
    }
    if (lt.indexOf('window') >= 0 && lt.indexOf('join') >= 0) return 'Join·Window';
    if (lt.indexOf('aggregate') >= 0 && lt.indexOf('join') >= 0) return 'Join·Agg';
    if (lt.indexOf('shuffle') >= 0 && lt.indexOf('join') >= 0) return 'Join';
    if (lt.indexOf('shuffle') >= 0 && lt.indexOf('aggregate') >= 0) return 'Agg';
    if (lt.indexOf('aggregate') >= 0) return 'Agg';
    if (lt.indexOf('window') >= 0) return 'Window';
    if (lt.indexOf('join') >= 0) return 'Join';
    if (lt.indexOf('scan') >= 0) return 'Scan';
    return opShortName(op.type);
}

/** 构建算子 label（多行紧凑文本）。 */
function opLabel(op) {
    var st = op.state || 'UNKNOWN';
    var l1 = opTitle(op) + '   ' + st;
    if (st === 'FAILED') {
        return [l1, ' ' + oneLine(op.failureMessage || '执行失败', 30)].join('\n');
    }
    if (st === 'RUNNING' || st === 'FINISHED') {
        var lines = [l1];
        var prog = taskProgressText(op);
        if (prog) lines.push(' ' + prog);
        lines.push('in ' + fmtRows(op.rowsIn) + '  out ' + fmtRows(op.rowsOut)
            + '  avg ' + fmtRate(op.avgRowsPerSec));
        var mem = fmtBytes(op.currentMemBytes);
        var misc = fmtDuration(op.elapsedMs) + '  cpu ' + fmtDuration(op.cpuMs) + '  p=' + op.parallelism;
        if (Number(op.spilledBytes) > 0) misc += '  spill ' + fmtBytes(op.spilledBytes);
        if (mem !== '0 B') misc += '  ' + mem;
        lines.push(misc);
        return lines.join('\n');
    }
    return [l1, ' p=' + op.parallelism].join('\n');
}

function opNodeId(idx) { return 'n' + idx; }

/** 根据快照构建 DAG 需要的 {nodes,edges}；edges 恒 child->parent（数据流左→右）。 */
function dagTopo(snap) {
    var ops = snap.operators || [];
    var idxToId = {};
    ops.forEach(function (o) { idxToId[o.idx] = opNodeId(o.idx); });
    var nodes = ops.map(function (o) { return { id: idxToId[o.idx], idx: o.idx }; });
    var edges = [], seen = {};
    ops.forEach(function (o) {
        var to = idxToId[o.idx];
        (o.children || []).forEach(function (c) {
            var from = idxToId[c];
            if (!from) return;
            var key = from + '>' + to;
            if (seen[key]) return;
            seen[key] = true;
            edges.push({ from: from, to: to });
        });
    });
    return { nodes: nodes, edges: edges };
}

/** 停掉进度缓动定时器（切走 DAG / 无节点在推进时调用）。 */
function stopDagAnim() {
    if (dagAnimTimer) {
        clearInterval(dagAnimTimer);
        dagAnimTimer = null;
    }
}

/**
 * 启动进度缓动：把 nodeDispPct 逐帧逼近 nodeProgress 的目标值。
 * <p>轮询每 300ms 才更新一次目标，直接画会「一格一格跳」；这里按 ~25%/40ms 缓动补齐，
 * 视觉上就是<b>连续推进</b>。节点首次出现（nodeDispPct 无值）直接取目标值，避免从 0 空涨；
 * 目标回退（如重置）也直接吸附，不退着走。全部到位后自动停表，下一次 refreshDag 再按需启动。</p>
 */
function startDagAnim() {
    var ease = function () {
        var moving = false;
        Object.keys(nodeProgress).forEach(function (id) {
            var target = Number((nodeProgress[id] || {}).pct) || 0;
            var cur = nodeDispPct[id];
            if (cur == null || target <= cur) {
                if (cur !== target) { nodeDispPct[id] = target; moving = true; }
                return;
            }
            var next = cur + (target - cur) * 0.25;
            if (target - next < 0.4) next = target; else moving = true;
            nodeDispPct[id] = next;
        });
        if (network) network.redraw();
        if (!moving) stopDagAnim();
    };
    stopDagAnim();
    if (network) dagAnimTimer = setInterval(ease, 40);
}

/** 无算子时的占位提示（嵌套进 #dag）。 */
function showDagEmpty(msg) {
    var el = document.getElementById('dag');
    if (el) {
        el.innerHTML = '<div class="dag-empty">' + esc(msg) + '</div>';
    }
}

/** 当前 vis 网络的边数据集（主题切换时更新连线色；节点是 ctxRenderer，重画即取新色）。 */
var dagEdgeSet = null;

/**
 * 主题切换后重着 DAG：节点走 ctxRenderer 每次绘制现取 UI.*，重画即可；
 * 边的颜色在建图时固化进数据集，需显式更新一次。
 */
function applyDagTheme() {
    if (!network) return;
    if (dagEdgeSet) {
        dagEdgeSet.getIds().forEach(function (id) {
            dagEdgeSet.update({ id: id, color: { color: UI.lineStrong, opacity: 0.9 } });
        });
    }
    network.redraw();
}

/** 首次渲染：由 MonitorQuery 快照创建 vis 网络。 */
function renderDag(snap) {
    var container = document.getElementById('dag');
    var ops = (snap && snap.operators) || [];
    if (!ops.length) {
        stopDagAnim();
        nodeDispPct = {};
        if (network) { network.destroy(); network = null; }
        dagEdgeSet = null;
        window._dagKey = null;
        showDagEmpty('算子尚未展开（查询刚提交 / 规划中）…');
        return;
    }
    // 重建前销毁旧网络：只清 DOM 会把旧 vis 实例（画布已脱离文档）留在内存里，
    // 其重绘循环/事件仍在跑，和多张残留画布叠加时表现为闪。
    if (network) { network.destroy(); network = null; }
    window._dagKey = dagStructureKey(ops);
    container.innerHTML = '';

    var topo = dagTopo(snap);
    var rawNodes = topo.nodes.slice();
    var rawEdges = topo.edges.slice();

    // 末端 Output 终端节点：从 DAG sink（未被引用的根算子，通常 idx0）汇出 → 展示最终结果回流。
    // 视觉状态 = outputVisual(查询级 + 根算子进度)；纳入布局后自然排在根算子右侧最末列。
    var rootOp = rootOpOf(ops);
    var outInfo = null;
    if (rootOp) {
        outInfo = outputVisual(snap, rootOp);
        rawNodes.push({ id: OUTPUT_NODE_ID, idx: rootOp.idx, output: true });
        rawEdges.push({ from: opNodeId(rootOp.idx), to: OUTPUT_NODE_ID });
    }

    nodeProgress = {};
    nodeLabelCache = {};
    nodeDispPct = {};          // 重建图：缓动值清空，新节点的进度直接取目标值（不从 0 涨）
    nodeSizeCache = {};        // 新节点重新量化尺寸（旧尺寸只用于旧图）
    ops.forEach(function (op) {
        nodeProgress[opNodeId(op.idx)] = nodeInfoFor(op);
        nodeLabelCache[opNodeId(op.idx)] = opLabel(op);
    });
    if (outInfo) {
        nodeProgress[OUTPUT_NODE_ID] = outInfo;
        nodeLabelCache[OUTPUT_NODE_ID] = outputLabel(snap);
    }

    var totalOps = ops.length;
    DAG_SCALE = Math.max(0.4, 1.0 - (totalOps - 3) * 0.08);

    var pos = manualLayout(rawNodes, rawEdges);

    var visNodes = rawNodes.map(function (n) {
        return { id: n.id, group: 'progressNode', x: pos[n.id] ? pos[n.id].x : 0, y: pos[n.id] ? pos[n.id].y : 0 };
    });
    var visEdges = rawEdges.map(function (e) {
        return {
            from: e.from, to: e.to, arrows: 'to',
            color: { color: UI.lineStrong, opacity: 0.9 }, width: 1.5,
            smooth: { type: 'cubicBezier', forceDirection: 'horizontal', roundness: 0.4 }
        };
    });

    var data = { nodes: new vis.DataSet(visNodes), edges: new vis.DataSet(visEdges) };
    dagEdgeSet = data.edges;
    var options = {
        groups: {
            progressNode: { shape: 'custom', ctxRenderer: progressNodeRenderer }
        },
        layout: { hierarchical: { enabled: false } },
        physics: { enabled: false },
        interaction: { dragNodes: true, dragView: true, zoomView: true, hover: true, tooltipDelay: 200 },
        edges: { font: { size: 10 } }
    };
    network = new vis.Network(container, data, options);

    // 双击算子节点 → 打开算子详情
    network.on('doubleClick', function (params) {
        if (params.nodes.length !== 1) return;
        var nid = String(params.nodes[0]);
        var idx = parseInt(nid.slice(1), 10);
        if (!isNaN(idx)) openOperatorDetail(idx);
    });

    // 居中视图
    (function centerView() {
        var nPos = network.getPositions();
        var minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
        Object.keys(nPos).forEach(function (nid) {
            var p = nPos[nid];
            if (p.x < minX) minX = p.x;
            if (p.x > maxX) maxX = p.x;
            if (p.y < minY) minY = p.y;
            if (p.y > maxY) maxY = p.y;
        });
        if (minX === Infinity) return;
        var cw = container.clientWidth, ch = container.clientHeight;
        var pad = 60;
        var sH = (ch - pad * 2) / ((maxY + 40) - (minY - 40) + 20);
        var sW = (cw - pad * 2) / ((maxX + 60) - (minX - 60) + 20);
        var scale = Math.max(0.08, Math.min(1.6, Math.min(sH, sW)));
        network.moveTo({ position: { x: (minX + maxX) / 2, y: (minY + maxY) / 2 }, scale: scale });
    })();

    startDagAnim();
}

/** 后续轮询：用最新快照刷节点状态/label，重绘。 */
function refreshDag(snap) {
    if (!network) return;
    var ops = (snap && snap.operators) || [];
    if (!ops.length) return;
    ops.forEach(function (op) {
        var id = opNodeId(op.idx);
        nodeProgress[id] = nodeInfoFor(op);
        nodeLabelCache[id] = opLabel(op);
    });
    // 同步末端 Output 节点（镜像查询级状态 + 根算子进度）
    if (nodeProgress[OUTPUT_NODE_ID]) {
        var rootOp = rootOpOf(ops);
        if (rootOp) {
            nodeProgress[OUTPUT_NODE_ID] = outputVisual(snap, rootOp);
            nodeLabelCache[OUTPUT_NODE_ID] = outputLabel(snap);
        }
    }
    network.redraw();
    startDagAnim();   // 新目标已就位 → 缓动补齐（轮询间隔内的视觉连续推进）
}
