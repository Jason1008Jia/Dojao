// ==================== SQL 编辑器自动补全（DataGrip / DBeaver 式） ====================
//
// 目标：在裸 <textarea> 编辑器上给出主流 client 的基本补全 —— 边打字边弹、Ctrl/⌘+Space 强制、
// ↑/↓ 选、Tab/Enter 接受、Esc 收；候选覆盖 **SQL 关键字 + 函数（内置 + 自定义）+ 表名
// （catalog.schema.table）+ 字段名（带类型）**，输入 `别名.` / `schema.` 时自动切到列名 / 下一级。
//
// 两个关键取舍：
//   1. **光标定位用「镜像 div」**（textarea-caret-position 同款）。项目在 catalog.js 里刻意避开了
//      `caretRangeFromPoint` / 隐藏量尺 —— 那是给「失焦后找 drop 落点」用的；补全时 textarea 有焦点、
//      `selectionStart` 可用，镜像 div 是最可靠的做法，且不动现有高亮垫层（垫层负责显色，镜像只负责测量）。
//   2. **元数据首次触发时整树预取 + 内存缓存**。目录 API 是逐级懒加载的，补全要在本地过滤，所以
//      第一次唤起时并发拉齐 catalogs→schemas→tables→columns，**外加函数目录 `/api/functions`**
//      （内置 + 自定义统一由后端给出，前端不再维护静态函数表），之后纯本地匹配。失败逐级降级、不阻断。
//
// 依赖：utils.js 的 httpGet；sql-highlight.js 的 SQL_KEYWORDS（脚本顺序见 index.html）。

/** 输入防抖（ms）：太久不弹显得迟钝，太短则每键一次重算。 */
var AC_DEBOUNCE_MS = 200;

/** 元数据索引里的路径分隔符（避免与标识符里可能出现的 `.` 混淆）。 */
var AC_SEP = '\u0001';

/** 候选上限：弹窗再长也没人翻，够用即可。 */
var AC_MAX_ITEMS = 60;

/** 常用多词片段（单字关键字由 SQL_KEYWORDS 覆盖，这里只补「连写更有用」的组合）。 */
var AC_EXTRA_KEYWORDS = [
    'GROUP BY', 'ORDER BY', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'FULL OUTER JOIN',
    'INSERT INTO', 'CREATE TABLE', 'CREATE CATALOG', 'SHOW CATALOGS', 'SHOW SCHEMAS', 'SHOW TABLES'
];

// ---------------- 模块状态 ----------------

var acMenuEl = null;          // 弹窗单例 DOM
var acMenuBound = false;      // 全局收起监听是否已绑
var acRows = [];              // 行 DOM（供高亮切换）
var acItems = [];             // 当前候选
var acActive = 0;             // 高亮下标
var acCtx = null;             // 当前补全上下文（含替换范围）
var acTimer = null;           // 防抖定时器
var acSkipNextInput = false;  // 接受补全后合成 input 事件，避免又立刻弹一次
var acPending = false;        // 元数据未就绪时曾请求过弹窗 → 就绪后补弹

var acMirrorEl = null;        // 测量光标像素用的隐藏镜像
var acMirrorKey = '';         // 镜像已拷贝的排版度量指纹

var acMeta = null;            // { catalogs, schemas{cat:[s]}, tables{cat|s:[t]}, columns{cat|s|t:[col]}, functions:[{name,source,kind,backend,detail}] }
var acMetaLoading = false;
var acMetaFailed = false;

// ---------------- 初始化 ----------------

/** 绑定编辑器补全（首页初始化时调一次）。 */
function bindSqlAutocomplete() {
    var input = document.getElementById('query-input');
    if (!input) return;
    acEnsureMenu();
    input.addEventListener('input', function () { acOnInput(input); });
    input.addEventListener('keydown', function (e) { acOnKeydown(e, input); });
    // 失焦 / 编辑器自身滚动 → 收起（位置会失效）
    input.addEventListener('blur', function () { acHide(); });
    input.addEventListener('scroll', function () { acHide(); });
}

function acOnInput(input) {
    if (acSkipNextInput) { acSkipNextInput = false; return; }   // 这是 acAccept 合成的，别接着弹
    if (acTimer) { clearTimeout(acTimer); }
    acTimer = setTimeout(function () {
        acTimer = null;
        acMaybeShow(input, false);
    }, AC_DEBOUNCE_MS);
}

function acOnKeydown(e, input) {
    // Ctrl/⌘ + Enter = 运行：让给提交逻辑，顺手把补全收起（否则会盖在结果上）
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { acHide(); return; }
    // Ctrl/⌘ + Space：强制弹出（含默认被系统输入法占用的情况，先 preventDefault）
    if ((e.ctrlKey || e.metaKey) && !e.altKey && (e.key === ' ' || e.code === 'Space')) {
        e.preventDefault();
        acHide();
        acMaybeShow(input, true);
        return;
    }
    if (!acVisible()) return;
    if (e.key === 'ArrowDown') { e.preventDefault(); acMove(1); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); acMove(-1); }
    else if ((e.key === 'Enter' || e.key === 'Tab') && !e.ctrlKey && !e.metaKey && !e.altKey) {
        // 注意排除 Ctrl/⌘+Enter —— 那是「运行」，不能让补全把它吃掉
        e.preventDefault();
        acAccept();
    } else if (e.key === 'Escape') {
        e.preventDefault();
        acHide();
    }
}

// ---------------- 展示判定 ----------------

/**
 * 计算候选并决定是否弹出。
 *
 * @param {HTMLTextAreaElement} input
 * @param {boolean} force true = Ctrl/⌘+Space 强制（空半词也弹）
 */
function acMaybeShow(input, force) {
    if (!input) input = document.getElementById('query-input');
    if (!input) return;
    if (!force && document.activeElement !== input) { acHide(); return; }
    if (input.selectionStart !== input.selectionEnd) { acHide(); return; }   // 有选区不补全

    var ctx = acContext(input);
    // 自动模式：既没在打字（无半词）也没在点路径（无限定符）→ 不打扰
    if (!force && !ctx.word && !ctx.qualifier) { acHide(); return; }

    if (!acMeta && !acMetaFailed) acEnsureMetadata();

    var items = acBuildItems(ctx);
    var loading = !acMeta && acMetaLoading;
    if (!items.length && !loading) { acHide(); return; }

    acCtx = ctx;
    acItems = items;
    acActive = 0;
    acPending = !acMeta && acMetaLoading;
    acRender(input, loading);
}

function acVisible() {
    return !!(acMenuEl && !acMenuEl.classList.contains('hidden'));
}

function acHide() {
    if (acTimer) { clearTimeout(acTimer); acTimer = null; }
    acPending = false;
    acCtx = null;
    acItems = [];
    acRows = [];
    acActive = 0;
    if (acMenuEl) acMenuEl.classList.add('hidden');
}

function acMove(delta) {
    if (!acItems.length) return;
    acActive = (acActive + delta + acItems.length) % acItems.length;
    for (var i = 0; i < acRows.length; i++) {
        if (i === acActive) acRows[i].classList.add('active');
        else acRows[i].classList.remove('active');
    }
    var row = acRows[acActive];
    if (row && row.scrollIntoView) {
        try { row.scrollIntoView({ block: 'nearest' }); } catch (err) { /* 老浏览器忽略 */ }
    }
}

// ---------------- 上下文判定 ----------------

/** 半词字符（限定符路径里只认标识符与点）。 */
function acIsWordChar(c) {
    return !!c && (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
        || c === '_' || c === '$');
}

/**
 * 取光标处半词与左侧限定符。
 *
 * <p>`u.|` → word=''、qualifier='u'；`mysql.dga.|` → qualifier='mysql.dga'；
 * `sel|` → word='sel'、qualifier=null。</p>
 */
function acTokenAt(value, pos) {
    var s = pos;
    while (s > 0 && acIsWordChar(value.charAt(s - 1))) s--;
    var word = value.slice(s, pos);
    var qualifier = null;
    if (s > 0 && value.charAt(s - 1) === '.') {
        var qe = s - 1;
        var qs = qe;
        while (qs > 0 && (acIsWordChar(value.charAt(qs - 1)) || value.charAt(qs - 1) === '.')) qs--;
        qualifier = value.slice(qs, qe);
    }
    return { start: s, end: pos, word: word, qualifier: qualifier };
}

var AC_REF_RE = /\b(?:from|join|into|update)\s+([A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*){0,2})(?:\s+(?:as\s+)?([A-Za-z_][\w$]*))?/gi;

/**
 * 扫 query 里出现过的表引用，建立「别名/表名 → 列索引键」映射，并收集去重后的表键。
 * 用于「输入 `u.` 弹出 users 的字段」与默认上下文里只列相关表的字段。
 */
function acTableRefs(value) {
    var refs = { aliasMap: {}, tableKeys: [] };
    if (!acMeta) return refs;
    AC_REF_RE.lastIndex = 0;
    var m;
    while ((m = AC_REF_RE.exec(value)) !== null) {
        var path = m[1];
        var alias = m[2];
        var key = acFindColumnsKey(path);
        if (alias && !SQL_KEYWORDS.hasOwnProperty(alias.toUpperCase())) {
            if (key) refs.aliasMap[alias.toLowerCase()] = key;
        } else if (!alias) {
            var last = path.split('.').pop();
            if (key && !SQL_KEYWORDS.hasOwnProperty(last.toUpperCase())) {
                refs.aliasMap[last.toLowerCase()] = key;
            }
        }
        if (key && refs.tableKeys.indexOf(key) < 0) refs.tableKeys.push(key);
    }
    return refs;
}

/** catalog.schema.table（大小写不敏感）→ 元数据列索引键 "c\u0001s\u0001t"；先精确、后后缀匹配。 */
function acFindColumnsKey(path) {
    if (!acMeta) return null;
    var q = path.toLowerCase();
    var keys = Object.keys(acMeta.columns);
    var i;
    for (i = 0; i < keys.length; i++) {
        if (keys[i].split(AC_SEP).join('.').toLowerCase() === q) return keys[i];
    }
    for (i = 0; i < keys.length; i++) {
        var full = keys[i].split(AC_SEP).join('.').toLowerCase();
        if (full.length > q.length && full.charAt(full.length - q.length - 1) === '.'
                && full.slice(-q.length) === q) return keys[i];
    }
    return null;
}

/** 数组里大小写不敏感地找一个元素（返回原始大小写）。 */
function acFindCI(arr, lower) {
    for (var i = 0; i < arr.length; i++) {
        if (arr[i].toLowerCase() === lower) return arr[i];
    }
    return null;
}

/** 哪个 catalog 下有这个 schema（单段限定符是 schema 名时用）。 */
function acFindCatalogOfSchema(schemaLower) {
    if (!acMeta) return null;
    var cats = Object.keys(acMeta.schemas);
    for (var i = 0; i < cats.length; i++) {
        if (acFindCI(acMeta.schemas[cats[i]] || [], schemaLower)) return cats[i];
    }
    return null;
}

/**
 * 判定当前补全场景。
 *
 * <p>kind ∈ default（关键字+函数+表+相关列）/ table / column / schema / metadata（限定符识别不出）。</p>
 */
function acContext(input) {
    var value = input.value;
    var caret = input.selectionStart;
    var tok = acTokenAt(value, caret);
    var ctx = {
        start: tok.start, end: caret, word: tok.word, qualifier: tok.qualifier,
        kind: 'default', columnsKey: null, qualified: false, pathCat: null, pathSchema: null,
        refs: acTableRefs(value)
    };

    if (tok.qualifier) {
        var q = tok.qualifier.toLowerCase();
        // ① 别名 / 表名 → 该表字段
        if (ctx.refs.aliasMap[q]) { ctx.kind = 'column'; ctx.columnsKey = ctx.refs.aliasMap[q]; return ctx; }
        var key = acFindColumnsKey(tok.qualifier);
        if (key) { ctx.kind = 'column'; ctx.columnsKey = key; return ctx; }
        // ② 元数据路径 → 下一级
        if (acMeta) {
            var parts = q.split('.');
            if (parts.length === 1) {
                var cat = acFindCI(acMeta.catalogs, q);
                if (cat) { ctx.kind = 'schema'; ctx.pathCat = cat; return ctx; }
                var catOfSchema = acFindCatalogOfSchema(q);
                if (catOfSchema) {
                    ctx.kind = 'table'; ctx.qualified = true;
                    ctx.pathCat = catOfSchema;
                    ctx.pathSchema = acFindCI(acMeta.schemas[catOfSchema] || [], q);
                    return ctx;
                }
            } else if (parts.length === 2) {
                var cat2 = acFindCI(acMeta.catalogs, parts[0]);
                if (cat2 && acMeta.schemas[cat2]) {
                    var sch2 = acFindCI(acMeta.schemas[cat2], parts[1]);
                    if (sch2) {
                        ctx.kind = 'table'; ctx.qualified = true;
                        ctx.pathCat = cat2; ctx.pathSchema = sch2;
                        return ctx;
                    }
                }
            }
        }
        ctx.kind = 'metadata';   // 有 `.` 但认不出 → 只给元数据类，不给关键字
        return ctx;
    }

    // 无限定符：看是否紧跟 FROM/JOIN/INTO/UPDATE（表位）
    var before = value.slice(0, tok.start);
    if (/\b(?:from|join|into|update)\s+$/i.test(before)) { ctx.kind = 'table'; return ctx; }
    return ctx;
}

// ---------------- 候选构建 ----------------

function acBuildItems(ctx) {
    var out = [];
    var p = (ctx.word || '').toLowerCase();

    function add(text, kind, meta, insert, back) {
        out.push({ text: text, kind: kind, meta: meta || '', insert: insert || text, back: back || 0 });
    }
    // bare=true 时插入/显示裸表名（已带 catalog/schema 前缀的场景），否则插入全限定名
    function addTables(onlyCat, onlySchema, bare) {
        if (!acMeta) return;
        Object.keys(acMeta.columns).forEach(function (k) {
            var parts = k.split(AC_SEP);
            if (onlyCat != null && !(parts[0] === onlyCat && parts[1] === onlySchema)) return;
            var full = parts.join('.');
            add(bare ? parts[2] : full, 'table', parts[0] + '.' + parts[1], bare ? parts[2] : full);
        });
    }

    if (ctx.kind === 'column' && ctx.columnsKey && acMeta) {
        (acMeta.columns[ctx.columnsKey] || []).forEach(function (c) {
            add(c.name, 'column', c.dataType || '');
        });
    } else if (ctx.kind === 'table') {
        addTables(ctx.qualified ? ctx.pathCat : null, ctx.pathSchema, ctx.qualified);
    } else if (ctx.kind === 'schema') {
        ((acMeta && acMeta.schemas[ctx.pathCat]) || []).forEach(function (s) {
            add(s, 'schema', ctx.pathCat);
        });
    } else if (ctx.kind === 'metadata') {
        addTables(null, null, true);
    } else {
        // default：关键字 + 函数 + 表 + 查询里相关表的字段
        acPushKeywords(out);
        acPushFunctions(out);
        addTables(null, null, false);
        if (acMeta) {
            (ctx.refs.tableKeys || []).forEach(function (k) {
                (acMeta.columns[k] || []).forEach(function (c) {
                    add(c.name, 'column', c.dataType || '');
                });
            });
        }
    }

    // 前缀过滤（表名允许用末段也就是表名本身做前缀；函数用签名前缀，签名以函数名开头）
    var filtered = out.filter(function (it) {
        if (!p) return true;
        var t = it.text.toLowerCase();
        if (t.indexOf(p) === 0) return true;
        var last = t.lastIndexOf('.');
        return last >= 0 && t.slice(last + 1).indexOf(p) === 0;
    });

    // 去重（同名同类只留一个）
    var seen = {}, uniq = [];
    filtered.forEach(function (it) {
        var k = it.kind + '\u0000' + it.text.toLowerCase();
        if (seen[k]) return;
        seen[k] = 1;
        uniq.push(it);
    });

    // 排序：字段 / 表 优先，再函数、关键字；同类按字母
    var order = { column: 0, table: 1, schema: 0, function: 2, keyword: 3 };
    uniq.sort(function (a, b) {
        var oa = order[a.kind] == null ? 9 : order[a.kind];
        var ob = order[b.kind] == null ? 9 : order[b.kind];
        if (oa !== ob) return oa - ob;
        var ta = a.text.toLowerCase(), tb = b.text.toLowerCase();
        return ta < tb ? -1 : ta > tb ? 1 : 0;
    });

    return uniq.slice(0, AC_MAX_ITEMS);
}

function acPushKeywords(out) {
    Object.keys(SQL_KEYWORDS).forEach(function (k) {
        out.push({ text: k, kind: 'keyword', meta: '', insert: k, back: 0 });
    });
    AC_EXTRA_KEYWORDS.forEach(function (k) {
        out.push({ text: k, kind: 'keyword', meta: '', insert: k, back: 0 });
    });
}

function acPushFunctions(out) {
    if (!acMeta) return;
    (acMeta.functions || []).forEach(function (f) {
        if (!f || !f.name) return;
        var meta = f.source === 'custom'
                ? ('自定义' + (f.backend ? '·' + f.backend : ''))
                : ('内置' + (f.kind === 'aggregate' ? '·聚合' : f.kind === 'window' ? '·窗口' : ''));
        out.push({ text: f.name, kind: 'function', meta: meta, insert: f.name + '()', back: 1 });
    });
}

// ---------------- 接受补全 ----------------

function acAccept(index) {
    if (!acCtx) return;
    var it = acItems[index == null ? acActive : index];
    if (!it) return;
    var input = document.getElementById('query-input');
    if (!input) return;

    var start = acCtx.start, end = acCtx.end;
    input.setRangeText(it.insert, start, end, 'end');
    var caret = start + it.insert.length - (it.back || 0);
    try { input.setSelectionRange(caret, caret); } catch (e) { /* 忽略 */ }

    acHide();
    acSkipNextInput = true;
    // 合成一条 input 事件：同时驱动高亮垫层重画（sql-highlight.js）与草稿记忆 + 撤销栈（dashboard.js）
    try { input.dispatchEvent(new Event('input', { bubbles: true })); } catch (e) { acSkipNextInput = false; }
    input.focus();
}

// ---------------- 弹窗渲染 ----------------

function acEnsureMenu() {
    if (acMenuEl) return acMenuEl;
    acMenuEl = document.createElement('div');
    acMenuEl.className = 'ac-menu hidden';
    document.body.appendChild(acMenuEl);
    if (!acMenuBound) {
        acMenuBound = true;
        // 捕获阶段点别处 → 收起（点在菜单内则放过）
        document.addEventListener('mousedown', function (e) {
            if (acMenuEl && !acMenuEl.classList.contains('hidden')
                    && !acMenuEl.contains(e.target)) acHide();
        }, true);
        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') acHide();
        });
        window.addEventListener('resize', acHide);
        // capture=true：编辑器是内层滚动容器，scroll 不冒泡到 window；菜单自身滚动不算
        window.addEventListener('scroll', function (e) {
            if (acMenuEl && e.target && acMenuEl.contains(e.target)) return;
            acHide();
        }, true);
        window.addEventListener('blur', acHide);
    }
    return acMenuEl;
}

function acRender(input, loading) {
    var el = acEnsureMenu();
    el.innerHTML = '';
    acRows = [];

    acItems.forEach(function (it, i) {
        var row = document.createElement('div');
        row.className = 'ac-item' + (i === acActive ? ' active' : '');

        var name = document.createElement('span');
        name.className = 'ac-name';
        name.textContent = it.text;
        row.appendChild(name);

        if (it.meta) {
            var meta = document.createElement('span');
            meta.className = 'ac-meta';
            meta.textContent = it.meta;
            row.appendChild(meta);
        }
        var kind = document.createElement('span');
        kind.className = 'ac-kind';
        kind.textContent = acKindLabel(it.kind);
        row.appendChild(kind);

        // mousedown 而非 click：避免 textarea 先失焦再落点
        row.addEventListener('mousedown', function (e) {
            e.preventDefault();
            acAccept(i);
        });
        el.appendChild(row);
        acRows.push(row);
    });

    if (loading) {
        var tip = document.createElement('div');
        tip.className = 'ac-empty';
        tip.textContent = '正在加载元数据\u2026';
        el.appendChild(tip);
    }

    el.classList.remove('hidden');
    acPlace(input);
}

function acKindLabel(kind) {
    if (kind === 'keyword') return '关键字';
    if (kind === 'function') return '函数';
    if (kind === 'table') return '表';
    if (kind === 'column') return '字段';
    if (kind === 'schema') return '模式';
    if (kind === 'catalog') return '目录';
    return kind;
}

/** 把弹窗钉在光标下方；越界则往（上 / 左）收，保证整块在视口内。 */
function acPlace(input) {
    if (!acMenuEl) return;
    var pos = acCaretPixel(input, input.selectionStart);
    var mw = acMenuEl.offsetWidth, mh = acMenuEl.offsetHeight;
    var vw = document.documentElement.clientWidth, vh = document.documentElement.clientHeight;
    var left = pos.left;
    if (left + mw + 8 > vw) left = Math.max(4, vw - mw - 8);
    if (left < 4) left = 4;
    var top = pos.bottom + 2;
    if (top + mh + 8 > vh) top = Math.max(4, pos.top - mh - 2);
    acMenuEl.style.left = left + 'px';
    acMenuEl.style.top = top + 'px';
}

// ---------------- 光标 → 像素（镜像 div） ----------------

/** 取（或重建）排版与 textarea 完全一致的隐藏镜像；度量指纹变了就重建。 */
function acEnsureMirror(input) {
    var style = window.getComputedStyle(input);
    // 滚动条占掉的宽度：textarea 出滚动条后正文窄了一截，镜像必须补上同样的 padding-right，
    // 否则接近折行点的长行两边折在不同位置，算出来的光标 x 就偏了（垫层 syncSqlHighlight 同理）。
    var sbw = Math.max(0, input.offsetWidth - input.clientWidth);
    var key = [style.width, style.height, style.paddingTop, style.paddingRight, style.paddingBottom,
        style.paddingLeft, style.borderTopWidth, style.borderRightWidth, style.borderBottomWidth,
        style.borderLeftWidth, style.fontFamily, style.fontSize, style.fontWeight, style.lineHeight,
        style.letterSpacing, style.wordSpacing, style.boxSizing, sbw].join('|');
    if (acMirrorEl && acMirrorKey === key) return acMirrorEl;
    if (acMirrorEl && acMirrorEl.parentNode) acMirrorEl.parentNode.removeChild(acMirrorEl);

    var d = document.createElement('div');
    ['fontFamily', 'fontSize', 'fontWeight', 'fontStyle', 'letterSpacing', 'wordSpacing', 'lineHeight',
        'textTransform', 'textIndent', 'paddingTop', 'paddingRight', 'paddingBottom', 'paddingLeft',
        'borderTopWidth', 'borderRightWidth', 'borderBottomWidth', 'borderLeftWidth', 'borderStyle',
        'boxSizing', 'width', 'height', 'direction', 'tabSize'].forEach(function (p) {
        try { d.style[p] = style[p]; } catch (e) { /* 个别属性名不支持则跳过 */ }
    });
    var padRight = parseFloat(style.paddingRight) || 0;
    d.style.paddingRight = (padRight + sbw) + 'px';
    d.style.position = 'absolute';
    d.style.top = '0';
    d.style.left = '0';
    d.style.visibility = 'hidden';
    d.style.pointerEvents = 'none';
    d.style.whiteSpace = 'pre-wrap';
    d.style.wordWrap = 'break-word';
    d.style.overflowWrap = 'break-word';
    d.style.overflow = 'hidden';
    d.style.zIndex = '-1';
    document.body.appendChild(d);
    acMirrorEl = d;
    acMirrorKey = key;
    return d;
}

/**
 * 光标像素位置（视口坐标）。
 *
 * @returns {{left:number, top:number, bottom:number}} bottom 是光标行下沿，用来把弹窗贴在其下方
 */
function acCaretPixel(input, pos) {
    var mirror = acEnsureMirror(input);
    mirror.textContent = input.value.slice(0, pos);
    var marker = document.createElement('span');
    marker.textContent = '\u200b';
    mirror.appendChild(marker);

    var rect = input.getBoundingClientRect();
    var lh = parseFloat(window.getComputedStyle(input).lineHeight);
    if (!isFinite(lh) || lh <= 0) lh = 21;
    var left = rect.left + marker.offsetLeft - input.scrollLeft;
    var top = rect.top + marker.offsetTop - input.scrollTop;
    return { left: left, top: top, bottom: top + lh };
}

// ---------------- 元数据预取 ----------------

function acEnsureMetadata() {
    if (acMeta || acMetaLoading || acMetaFailed) return;
    acMetaLoading = true;
    acFetchAll().then(function (meta) {
        acMeta = meta;
        acMetaLoading = false;
        // 弹窗在等元数据 → 就绪后原地补弹一次
        if (acPending) {
            acPending = false;
            acMaybeShow(document.getElementById('query-input'), true);
        }
    }).catch(function () {
        acMetaLoading = false;
        acMetaFailed = true;
    });
}

/**
 * 整树预取：catalogs → schemas → tables → columns，**并与函数目录 `/api/functions` 并行**。
 * 逐级 catch，单点失败不影响其余（函数接口失败 → 空数组，仅少一类候选）。
 */
function acFetchAll() {
    var meta = { catalogs: [], schemas: {}, tables: {}, columns: {}, functions: [] };
    function get(url) {
        return httpGet(url).catch(function () { return null; });
    }

    var tree = get('/api/metadata/catalogs').then(function (d) {
        var cats = (d && d.catalogs) || [];
        meta.catalogs = cats;
        return Promise.all(cats.map(function (c) {
            return get('/api/metadata/catalogs/' + encodeURIComponent(c) + '/schemas').then(function (sd) {
                var schemas = (sd && sd.schemas) || [];
                meta.schemas[c] = schemas;
                return Promise.all(schemas.map(function (s) {
                    return get('/api/metadata/catalogs/' + encodeURIComponent(c)
                            + '/schemas/' + encodeURIComponent(s) + '/tables').then(function (td) {
                        var tables = (td && td.tables) || [];
                        meta.tables[c + AC_SEP + s] = tables;
                        return Promise.all(tables.map(function (t) {
                            return get('/api/metadata/catalogs/' + encodeURIComponent(c)
                                    + '/schemas/' + encodeURIComponent(s)
                                    + '/tables/' + encodeURIComponent(t) + '/columns').then(function (cd) {
                                meta.columns[c + AC_SEP + s + AC_SEP + t] = (cd && cd.columns) || [];
                            });
                        }));
                    });
                }));
            });
        }));
    });

    var fns = get('/api/functions').then(function (d) {
        return (d && d.functions) || [];
    });

    return Promise.all([tree, fns]).then(function (r) {
        meta.functions = r[1] || [];
        return meta;
    });
}

/** 目录树刷新后清空缓存，下次触发重新预取（供 catalog.js 的刷新按钮调用，可选）。 */
function invalidateSqlMetadata() {
    acMeta = null;
    acMetaFailed = false;
    acMetaLoading = false;
}
