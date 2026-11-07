// ==================== 手写 canvas 轻量图表 ====================
//
// 零依赖（不引图表库 / 不走外网 CDN，符合本项目「最小 vendor」原则）。
// 配色走本文件的 CHART_BRAND 品牌色板，几何工具用 utils.js 的 roundRectPath / fmtBytes / fmtRows
// （**不依赖 state.js 的 UI.\***：那套是全站主题色，与监控页品牌绿不是同一套）。
//
// 视觉口径（与监控页一致）：扁平、无阴影、2px 细线、**不填色块**，
// 只有水平参考线（不画竖网格），X 轴时间刻度刻意稀疏。
//
// 统一约定：
//   - 绘制函数接收 <canvas>，内部按 devicePixelRatio 放大画布，逻辑坐标 = CSS 像素；
//   - 调用方负责在容器尺寸变化后重绘（monitor.js 在 window resize 时重绘全部图表）；
//   - 数据为空时画一块「暂无数据」占位，不抛错。

/**
 * 监控页品牌色板（与 css/style.css 的 .monitor-view 变量同值，改色务必两处同步）。
 * 数据可视化元素**只许**用这里的颜色 —— 线条一律品牌绿，灰只给文字层级与描边。
 *
 * 浅色 / 深色两套：由 state.js 的 setThemePalette() → applyChartTheme() 切换（当前生效值存 CHART_BRAND）。
 * 深色下折线/网格/文字同步提亮，否则深底上原品牌深绿会糊成一片。
 */
var CHART_BRAND_LIGHT = {
    master: '#2E7D32',      // 主绿（深）：master 折线 / 迷你趋势线
    worker: '#7ED957',      // 辅助绿（亮）：worker 折线
    grid:   '#F1F5F9',      // 网格线
    axis:   '#E5E7EB',      // 零基线（比网格略深一档）
    guide:  '#94A3B8',      // 悬停竖线
    label:  '#94A3B8',      // 极小字号说明：轴刻度 / 图例数值 / 空态
    dim:    '#64748B',      // 标签 / 次要文字：图例名称
    ink:    '#1E293B',      // 主标题 / 数字：tooltip 数值
    ok:     '#10B981',      // 状态：正常
    warn:   '#F59E0B',      // 状态：警告
    err:    '#EF4444',      // 状态：异常
    tipBg:  'rgba(255,255,255,0.97)',   // tooltip 底
    pin:    '#FFFFFF'       // 悬停命中点的描边（与卡片底同色，做「挖空」效果）
};

/** 深色品牌色板（与 css/style.css 的 [data-theme="dark"] .monitor-view 同值，两处同步）。 */
var CHART_BRAND_DARK = {
    master: '#4ADE80',
    worker: '#86EFAC',
    grid:   '#232B39',
    axis:   '#2E3849',
    guide:  '#64748B',
    label:  '#94A3B8',
    dim:    '#94A3B8',
    ink:    '#E5E9F0',
    ok:     '#34D399',
    warn:   '#FBBF24',
    err:    '#F87171',
    tipBg:  'rgba(22,28,40,0.97)',
    pin:    '#161C28'
};

/** 当前生效色板（state.js setThemePalette 时整体替换）。 */
var CHART_BRAND = CHART_BRAND_LIGHT;

/** 由 state.js 的主题切换调用：整体替换色板（品牌色 + 折线兜底色）。 */
function applyChartTheme(dark) {
    CHART_BRAND = dark ? CHART_BRAND_DARK : CHART_BRAND_LIGHT;
    CHART_PALETTE = dark ? CHART_PALETTE_DARK : CHART_PALETTE_LIGHT;
}

/**
 * 折线配色：品牌绿打头（master 深绿 / worker 亮绿），其余为同色系兜底。
 * 实际取色由 monitor.js 的 monitorColorOf() 决定，这里只作无 color 时的回退。
 */
var CHART_PALETTE_LIGHT = [
    '#2E7D32', '#7ED957', '#C5E1A5', '#94A3B8'
];
var CHART_PALETTE_DARK = [
    '#4ADE80', '#86EFAC', '#C5E1A5', '#64748B'
];
var CHART_PALETTE = CHART_PALETTE_LIGHT;

/** 序列颜色：按序号取色，环绕。 */
function chartColor(i) {
    return CHART_PALETTE[((i % CHART_PALETTE.length) + CHART_PALETTE.length) % CHART_PALETTE.length];
}

/** 全站统一字体串（canvas 的 font 不认 CSS 变量，只能取一次实际值）。 */
function chartFont(weight, size) {
    var fam = getComputedStyle(document.body).fontFamily || 'sans-serif';
    return (weight ? weight + ' ' : '') + size + 'px ' + fam;
}

/**
 * 按 devicePixelRatio 准备画布。
 * @returns {{ctx:CanvasRenderingContext2D, w:number, h:number}} 逻辑宽高（CSS 像素）
 */
function chartPrep(canvas) {
    var dpr = window.devicePixelRatio || 1;
    var w = canvas.clientWidth || canvas.width || 300;
    var h = canvas.clientHeight || canvas.height || 120;
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
    var ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);   // 之后一律用逻辑坐标绘制
    ctx.clearRect(0, 0, w, h);
    return { ctx: ctx, w: w, h: h };
}

/** 空数据占位。 */
function chartEmpty(ctx, w, h, text) {
    ctx.fillStyle = CHART_BRAND.label;
    ctx.font = chartFont('', 12);
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(text || '暂无数据', w / 2, h / 2);
}

/**
 * 数值 → 图表文本。<b>与卡片共用同一套单位换算</b>（bytes 走 fmtBytes：
 * ≥1GB 用 GB、≥1MB 用 MB，自动换算），保证「轴 / 图例 / tooltip / 卡片」单位一致。
 */
function chartNum(v, unit) {
    if (unit === 'pct') return Math.round(Number(v) || 0) + '%';
    if (unit === 'count') return fmtRows(v);
    if (unit === 'bytesPerSec') return fmtBytes(v) + '/s';
    return fmtBytes(v);
}

/** 「好看」的步长：把区间切成 ~ticks 段，返回取整后的步长。 */
function niceStep(span, ticks) {
    if (!(span > 0)) return 1;
    var raw = span / Math.max(1, ticks);
    var mag = Math.pow(10, Math.floor(Math.log(raw) / Math.LN10));
    var norm = raw / mag;
    var step = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 2.5 ? 2.5 : norm <= 5 ? 5 : 10;
    return step * mag;
}

/** 毫秒 → HH:MM:SS。 */
function clockOf(ms) {
    var d = new Date(ms);
    var p = function (n) { return (n < 10 ? '0' : '') + n; };
    return p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
}

/** 序列最后一个有限值。 */
function lastVal(points) {
    for (var i = points.length - 1; i >= 0; i--) {
        if (isFinite(points[i].v)) return points[i].v;
    }
    return 0;
}

/**
 * 多序列折线图（2px 细线 / 无填充 / 仅水平参考线 / 稀疏时间刻度 / 悬停十字线）。
 *
 * <p>悬停走「整图重绘」：鼠标移动时用同一份几何重画一遍，多叠一条竖线与若干圆点 +
 * 一个画在画布上的 tooltip。比浮层 DOM 简单，且不会与 dpr 缩放打架。</p>
 *
 * @param {HTMLCanvasElement} canvas
 * @param {Array<{name:string, color:string=, points:Array<{t:number, v:number}>}>} series
 * @param {Object=} opts
 *        {unit:'pct'|'bytes'|'bytesPerSec'|'count', max:number=, min:number=, showLegend:boolean=,
 *         hover:boolean=, yTicks:number=, emptyText:string=}
 */
function lineChart(canvas, series, opts) {
    opts = opts || {};
    if (!canvas) return;
    var alive = (series || []).filter(function (s) { return s && s.points && s.points.length; });
    var unit = opts.unit || 'bytes';

    // ---- 时间范围（跨序列取并集）----
    var tMin = Infinity, tMax = -Infinity;
    alive.forEach(function (s) {
        s.points.forEach(function (p) {
            if (p.t < tMin) tMin = p.t;
            if (p.t > tMax) tMax = p.t;
        });
    });
    if (tMin === tMax) tMax = tMin + 1;

    // ---- 值范围（默认 0 基线；负值如 cpuPct = -1 不参与）----
    // opts.max 是调用方给的硬上限（如 CPU 恒为 100%）——必须原样尊重，不能再加余量往上抬，
    // 否则 100% 的轴会被算成 150%。只有自动量程时才留 12% 头顶余量。
    var fixedMax = opts.max !== undefined && opts.max !== null;
    var vMax = fixedMax ? Number(opts.max) : -Infinity;
    var vMin = opts.min !== undefined ? opts.min : 0;
    alive.forEach(function (s) {
        s.points.forEach(function (p) {
            if (!isFinite(p.v) || p.v < 0) return;
            if (!fixedMax && p.v > vMax) vMax = p.v;
        });
    });
    if (!isFinite(vMax) || vMax <= 0) vMax = 1;
    var span = (fixedMax ? vMax : vMax * 1.12) - vMin;
    var step = niceStep(span, opts.yTicks || 4);
    vMax = vMin + Math.ceil(span / step) * step;
    if (vMax <= vMin) vMax = vMin + step;

    function draw(hover) {
        var g = chartPrep(canvas);
        var ctx = g.ctx, w = g.w, h = g.h;
        if (!alive.length) { chartEmpty(ctx, w, h, opts.emptyText); return; }

        var legendH = opts.showLegend === false ? 0 : 20;
        // bytes / bytesPerSec 的轴标签最长（如 "39.0 GB" / "39.0 MB/s"），左侧留宽一点免得贴边
        var padL = unit === 'bytes' ? 56 : unit === 'bytesPerSec' ? 70 : 40;
        var padR = 12, padT = 8 + legendH, padB = 26;
        var plotW = Math.max(10, w - padL - padR);
        var plotH = Math.max(10, h - padT - padB);
        var X = function (t) { return padL + (t - tMin) / (tMax - tMin) * plotW; };
        var Y = function (v) {
            return padT + plotH
                    - (Math.max(vMin, Math.min(vMax, v)) - vMin) / (vMax - vMin) * plotH;
        };

        // ---- 水平参考线 + Y 轴刻度（不画竖网格）----
        ctx.font = chartFont('500', 10);
        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        for (var v = vMin; v <= vMax + 1e-9; v += step) {
            var y = Math.round(Y(v)) + 0.5;
            ctx.strokeStyle = (v === vMin) ? CHART_BRAND.axis : CHART_BRAND.grid;
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(padL, y);
            ctx.lineTo(padL + plotW, y);
            ctx.stroke();
            ctx.fillStyle = CHART_BRAND.label;
            ctx.fillText(chartNum(v, unit), padL - 8, y);
        }

        // ---- 折线（2px，无填充）----
        alive.forEach(function (s, si) {
            var color = s.color || chartColor(si);
            var pts = s.points;
            ctx.beginPath();
            ctx.moveTo(X(pts[0].t), Y(pts[0].v));
            for (var j = 1; j < pts.length; j++) ctx.lineTo(X(pts[j].t), Y(pts[j].v));
            ctx.strokeStyle = color;
            ctx.lineWidth = 2;
            ctx.lineJoin = 'round';
            ctx.lineCap = 'round';
            ctx.stroke();
            if (pts.length === 1) {
                ctx.beginPath();
                ctx.arc(X(pts[0].t), Y(pts[0].v), 2.6, 0, Math.PI * 2);
                ctx.fillStyle = color;
                ctx.fill();
            }
        });

        // ---- X 轴时间刻度：刻意稀疏（每 190px 一个，最多 4 段），
        //      6 分钟窗口下即每 1.5~2 分钟一个，避免时间标签糊成一片。----
        var segs = Math.max(2, Math.min(4, Math.floor(plotW / 190)));
        ctx.fillStyle = CHART_BRAND.label;
        ctx.font = chartFont('500', 10);
        ctx.textBaseline = 'top';
        for (var k = 0; k <= segs; k++) {
            var tk = tMin + (tMax - tMin) * k / segs;
            var xk = X(tk);
            var align = k === 0 ? 'left' : (k === segs ? 'right' : 'center');
            ctx.textAlign = align;
            ctx.fillText(clockOf(tk), xk, padT + plotH + 7);
        }

        // ---- 图例：短线样 + 名称 + 当前值 ----
        if (legendH) {
            var lx = padL;
            ctx.textAlign = 'left';
            ctx.textBaseline = 'middle';
            ctx.font = chartFont('500', 11);
            alive.forEach(function (s, si) {
                var color = s.color || chartColor(si);
                var label = s.name + ' ' + chartNum(lastVal(s.points), unit);
                var tw = ctx.measureText(label).width;
                if (lx + 18 + tw > w - padR) return;     // 超宽就少画几项，不换行
                ctx.strokeStyle = color;
                ctx.lineWidth = 2;
                ctx.lineCap = 'round';
                ctx.beginPath();
                ctx.moveTo(lx, legendH / 2);
                ctx.lineTo(lx + 12, legendH / 2);
                ctx.stroke();
                ctx.fillStyle = CHART_BRAND.dim;
                ctx.fillText(label, lx + 18, legendH / 2 + 0.5);
                lx += 18 + tw + 18;
            });
        }

        // ---- 悬停：竖线 + 命中点 + tooltip ----
        if (hover && hover.inside && hover.mx >= padL && hover.mx <= padL + plotW) {
            var ht = tMin + (hover.mx - padL) / plotW * (tMax - tMin);
            ht = Math.max(tMin, Math.min(tMax, ht));
            ctx.save();
            ctx.setLineDash([3, 3]);
            ctx.strokeStyle = CHART_BRAND.guide;
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(Math.round(X(ht)) + 0.5, padT);
            ctx.lineTo(Math.round(X(ht)) + 0.5, padT + plotH);
            ctx.stroke();
            ctx.restore();

            var rows = [];
            alive.forEach(function (s, si) {
                var best = null, bestD = Infinity;
                s.points.forEach(function (p) {
                    var d = Math.abs(p.t - ht);
                    if (d < bestD) { bestD = d; best = p; }
                });
                if (best) {
                    rows.push({ name: s.name, v: best.v, t: best.t,
                            color: s.color || chartColor(si) });
                    ctx.beginPath();
                    ctx.arc(X(best.t), Y(best.v), 3.2, 0, Math.PI * 2);
                    ctx.fillStyle = s.color || chartColor(si);
                    ctx.fill();
                    ctx.strokeStyle = CHART_BRAND.pin;
                    ctx.lineWidth = 1.2;
                    ctx.stroke();
                }
            });
            drawChartTooltip(ctx, rows, X(ht), hover.my, w, h, unit);
        }
    }

    // 绑定悬停（只绑一次；之后靠 _chartDraw 指向最新的绘制闭包）
    canvas._chartDraw = draw;
    if (!canvas._chartHoverBound && opts.hover !== false) {
        canvas._chartHoverBound = true;
        canvas._chartHoverState = { inside: false, mx: 0, my: 0 };
        canvas.addEventListener('mousemove', function (ev) {
            var st = canvas._chartHoverState;
            var rect = canvas.getBoundingClientRect();
            st.mx = ev.clientX - rect.left;
            st.my = ev.clientY - rect.top;
            st.inside = true;
            if (canvas._chartDraw) canvas._chartDraw(st);
        });
        canvas.addEventListener('mouseleave', function () {
            var st = canvas._chartHoverState;
            st.inside = false;
            if (canvas._chartDraw) canvas._chartDraw(st);
        });
    }
    draw(canvas._chartHoverState || { inside: false, mx: 0, my: 0 });
}

/** 折线图悬停 tooltip：画在画布上（白底 + 描边 + 每序列一行「色点 名称 值」）。 */
function drawChartTooltip(ctx, rows, anchorX, mouseY, canvasW, canvasH, unit) {
    if (!rows.length) return;
    var pad = 8, lineH = 16, w = 0;
    ctx.font = chartFont('500', 11);
    rows.forEach(function (r) {
        var tw = 10 + ctx.measureText(r.name).width + 8
                + ctx.measureText(chartNum(r.v, unit)).width + pad * 2;
        if (tw > w) w = tw;
    });
    var title = '时间 ' + clockOf(rows[0].t);
    ctx.font = chartFont('', 10);
    var tw2 = ctx.measureText(title).width + pad * 2;
    if (tw2 > w) w = tw2;
    w = Math.min(w, canvasW - 16);
    var h = rows.length * lineH + 22 + pad;

    // 贴近鼠标，但不越出画布
    var x = anchorX + 12;
    if (x + w > canvasW - 4) x = anchorX - w - 12;
    if (x < 4) x = 4;
    var y = mouseY + 10;
    if (y + h > canvasH - 4) y = canvasH - h - 4;
    if (y < 4) y = 4;

    ctx.save();
    ctx.beginPath();
    roundRectPath(ctx, x, y, w, h, 6);
    ctx.fillStyle = CHART_BRAND.tipBg;
    ctx.fill();
    ctx.strokeStyle = CHART_BRAND.axis;
    ctx.lineWidth = 1;
    ctx.stroke();

    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = CHART_BRAND.label;
    ctx.font = chartFont('', 10);
    ctx.fillText(title, x + pad, y + pad + 5);

    ctx.font = chartFont('500', 11);
    rows.forEach(function (r, i) {
        var ly = y + pad + 20 + i * lineH;
        ctx.fillStyle = r.color;
        ctx.beginPath();
        ctx.arc(x + pad + 4, ly, 3.2, 0, Math.PI * 2);
        ctx.fill();
        ctx.fillStyle = CHART_BRAND.dim;
        ctx.fillText(r.name, x + pad + 12, ly);
        ctx.fillStyle = CHART_BRAND.ink;
        ctx.textAlign = 'right';
        ctx.fillText(chartNum(r.v, unit), x + w - pad, ly);
        ctx.textAlign = 'left';
    });
    ctx.restore();
}

/**
 * 迷你趋势线（概览卡片内嵌，无轴无网格）。值域自动取 min/max。
 *
 * @param {HTMLCanvasElement} canvas
 * @param {number[]} values
 * @param {string=} color
 */
function sparkline(canvas, values, color) {
    if (!canvas) return;
    var g = chartPrep(canvas);
    var ctx = g.ctx, w = g.w, h = g.h;
    var vals = (values || []).filter(function (v) { return isFinite(v); });
    if (vals.length < 2) {
        // 点太少画一条淡底线，避免卡片高度塌陷
        ctx.strokeStyle = CHART_BRAND.grid;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(2, h - 2);
        ctx.lineTo(w - 2, h - 2);
        ctx.stroke();
        return;
    }
    var min = Math.min.apply(null, vals);
    var max = Math.max.apply(null, vals);
    if (max === min) max = min + 1;
    var padY = 4;
    var X = function (i) { return i / (vals.length - 1) * w; };
    var Y = function (v) { return h - padY - (v - min) / (max - min) * (h - padY * 2); };

    var col = color || CHART_BRAND.master;
    ctx.beginPath();
    ctx.moveTo(X(0), Y(vals[0]));
    for (var i = 1; i < vals.length; i++) ctx.lineTo(X(i), Y(vals[i]));
    ctx.strokeStyle = col;
    ctx.lineWidth = 1.5;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.stroke();

    ctx.beginPath();
    ctx.arc(X(vals.length - 1), Y(vals[vals.length - 1]), 2.2, 0, Math.PI * 2);
    ctx.fillStyle = col;
    ctx.fill();
}

// ==================== 状态色 / 进度条 ====================

/**
 * 占用率配色：**只在超过 80% 时才变色**（80-90% 黄、>90% 红），
 * 其余一律品牌主绿 —— 避免满屏彩色进度条把页面搅乱。
 *
 * @returns {string} CSS 颜色
 */
function usageColor(pct) {
    var p = Number(pct) || 0;
    if (p >= 90) return CHART_BRAND.err;
    if (p >= 80) return CHART_BRAND.warn;
    return CHART_BRAND.master;
}

/**
 * 细进度条（节点卡里一行一个指标）。
 * 纯 DOM（div 宽度）而非 canvas —— 要参与 flex 布局、随卡片拉伸、且无需重绘。
 *
 * @param {number} pct
 * @param {string=} color 不传则按 usageColor 的 80/90 阈值取色
 * @returns {string} HTML 片段
 */
function usageBar(pct, color) {
    var p = Math.max(0, Math.min(100, Number(pct) || 0));
    return '<div class="bar"><div class="bar-fill" style="width:' + p.toFixed(1)
            + '%;background:' + (color || usageColor(p)) + '"></div></div>';
}
