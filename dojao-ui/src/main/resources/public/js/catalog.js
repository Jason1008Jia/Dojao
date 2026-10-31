// ==================== 首页：catalog → schema → table → column 目录树 ====================
// 数据源：GET /api/metadata/catalogs
//         GET /api/metadata/catalogs/{c}/schemas
//         GET /api/metadata/catalogs/{c}/schemas/{s}/tables
//         GET /api/metadata/catalogs/{c}/schemas/{s}/tables/{t}/columns
// 懒加载：点开才拉下一级。展开到表级别后，点表把「列信息」逐行铺在表下面。
// 表引用（catalog.schema.table）只通过**拖拽**落到 SQL 编辑器（点表不再回填）。
// 右键节点 → 「刷新」：重拉该节点的下一级（服务端不推送元数据变更，新建的表靠这个刷出来）。

var CATALOG_ARROW_OPEN = '\u2212';    // −（已展开）
var CATALOG_ARROW_CLOSED = '+';       // +（可展开）

/**
 * 加载顶层 catalog 列表并渲染整棵树。
 *
 * @return {Promise} 加载完成（含失败）后 resolve —— 供刷新按钮转动图标用
 */
function loadCatalogTree() {
    var box = document.getElementById('catalog-tree');
    if (!box) return Promise.resolve();
    box.innerHTML = '<div class="catalog-loading">加载中\u2026</div>';
    return httpGet('/api/metadata/catalogs')
        .then(function (d) {
            var cats = (d && d.catalogs) || [];
            if (!cats.length) {
                box.innerHTML = '<div class="catalog-empty">（无 catalog）</div>';
                return;
            }
            box.innerHTML = '';
            cats.forEach(function (c) {
                box.appendChild(buildTreeNode(c, 'catalog', [c]));
            });
        })
        .catch(function (err) {
            box.innerHTML = '<div class="catalog-error">目录加载失败：'
                + esc(oneLine(err && err.message ? err.message : err, 100)) + '</div>';
        });
}

/**
 * 「Catalog 浏览」标题右侧刷新按钮：<b>整棵树重拉</b>。
 *
 * <p>与节点右键的「刷新」分工不同 —— 右键刷新的是该节点的下一级（新表 / 新列），而这里重拉顶层
 * catalog 列表本身：新建 / 删除 catalog 之后，点它就能看到最新的 catalog 状态。
 * 副作用是已展开的层级会被收起（节点全部重建），这正是「重新拉一份最新的」的语义。</p>
 */
function refreshCatalogTree() {
    var btn = document.getElementById('catalog-refresh-btn');
    if (btn) { btn.disabled = true; btn.classList.add('spinning'); }
    loadCatalogTree().then(function () {
        if (btn) { btn.disabled = false; btn.classList.remove('spinning'); }
    });
}

/**
 * 给一个树行挂上「拖到右侧工作区」的能力。
 *
 * <p>拖拽载荷用 {@code text/plain}（跨组件通用，编辑器侧 {@code bindSqlDropZone} 按它判类型）。
 * 并用一个临时浮层做<b>自定义拖拽影像</b> —— 默认影像是整行截图，左侧栏本来就窄，
 * 拖到半路几乎看不出拖的是哪一项；这里直接把「将要插入的文本」显示出来。</p>
 *
 * @param {HTMLElement} row  树行（catalog / schema / table / column 通用）
 * @param {string} ref       落下时写进编辑器的文本（表引用或列名）
 * @param {string} hint      拖拽影像上显示的文字
 */
function bindTreeDrag(row, ref, hint) {
    row.draggable = true;
    row.ondragstart = function (e) {
        e.dataTransfer.setData('text/plain', ref);
        e.dataTransfer.effectAllowed = 'copy';
        row.classList.add('dragging');

        // 拖拽影像必须此刻在 DOM 里且可见，浏览器截完图立刻就能删（下一拍）。
        var ghost = document.createElement('div');
        ghost.className = 'tree-drag-ghost';
        ghost.textContent = hint;
        document.body.appendChild(ghost);
        if (e.dataTransfer.setDragImage) {
            try { e.dataTransfer.setDragImage(ghost, 8, 10); } catch (err) { /* 忽略：退回默认影像 */ }
        }
        setTimeout(function () {
            if (ghost.parentNode) ghost.parentNode.removeChild(ghost);
        }, 0);
    };
    row.ondragend = function () { row.classList.remove('dragging'); };
}

/**
 * 构造一个树节点（catalog / schema / table / column 四级通用）。
 *
 * @param {string} label 显示名
 * @param {string} level catalog | schema | table | column
 * @param {string[]} path [catalog] / [catalog,schema] / [catalog,schema,table] / 再加列名
 * @param {Object=} meta 列节点附加信息 {dataType, nullable, comment}
 */
function buildTreeNode(label, level, path, meta) {
    meta = meta || null;
    var node = document.createElement('div');
    node.className = 'tree-node tree-' + level;

    var row = document.createElement('div');
    row.className = 'tree-row';

    var arrow = document.createElement('span');
    arrow.className = 'tree-arrow';
    arrow.textContent = (level === 'column') ? '' : CATALOG_ARROW_CLOSED;
    row.appendChild(arrow);

    var name = document.createElement('span');
    name.className = 'tree-label';
    name.textContent = label;
    row.appendChild(name);

    // 列节点：名字后面跟一个类型小标签 + 字段注释，title 里带完整信息
    if (level === 'column') {
        var type = document.createElement('span');
        type.className = 'tree-col-type';
        type.textContent = (meta && meta.dataType) ? meta.dataType : '';
        row.appendChild(type);
        // 字段注释（如 users.name -> 「名字」）：有才展示。左侧看板窄，超长交给 CSS 省略号截断，
        // 完整内容仍在下面的 row.title 里（hover 可见）。
        if (meta && meta.comment) {
            var note = document.createElement('span');
            note.className = 'tree-col-comment';
            note.textContent = meta.comment;
            row.appendChild(note);
        }
        var bits = [path.join('.')];
        if (meta && meta.dataType) bits.push(meta.dataType);
        bits.push((meta && meta.nullable === false) ? 'NOT NULL' : 'NULL');
        if (meta && meta.comment) bits.push(meta.comment);
        bits.push('\uff08\u53ef\u62d6\u5230\u53f3\u4fa7\u7f16\u8f91\u5668\uff09');
        row.title = bits.join('  ');
    } else {
        row.title = path.join('.') + '\uff08\u53ef\u62d6\u5230\u53f3\u4fa7\u7f16\u8f91\u5668\uff09';
    }
    node.appendChild(row);

    // 列是叶子：不展开子节点，但同样可拖 —— 拖进编辑器的是**列名**（裸标识符，
    // 直接落到 SELECT / WHERE 里就能用），不像表节点要带 catalog.schema.table 前缀。
    if (level === 'column') {
        bindTreeDrag(row, label, label + '（列）');
        return node;
    }

    // catalog / schema / table 都可拖到右侧工作区：落下后写入 catalog[.schema[.table]]
    bindTreeDrag(row, path.join('.'), path.join('.'));

    var children = document.createElement('div');
    children.className = 'tree-children hidden';
    node.appendChild(children);

    var expanded = false;
    var loaded = false;

    /** 拉取本节点下一级并铺开（**强制重拉**：不看 loaded，直接丢弃旧子节点重建）。 */
    function loadChildren() {
        loaded = true;
        expanded = true;
        children.classList.remove('hidden');
        arrow.textContent = CATALOG_ARROW_OPEN;
        children.innerHTML = '<div class="catalog-loading">加载中\u2026</div>';
        fetchCatalogChildren(level, path)
            .then(function (items) {
                children.innerHTML = '';
                if (!items.length) {
                    children.innerHTML = '<div class="catalog-empty">（空）</div>';
                    return;
                }
                items.forEach(function (it) {
                    children.appendChild(buildTreeNode(it.label, it.level, it.path, it.meta));
                });
            })
            .catch(function (err) {
                loaded = false;   // 允许重试
                children.innerHTML = '<div class="catalog-error">加载失败：'
                    + esc(oneLine(err && err.message ? err.message : err, 80)) + '</div>';
            });
    }

    row.onclick = function () {
        if (!loaded) {
            loadChildren();
            return;
        }
        expanded = !expanded;
        children.classList.toggle('hidden', !expanded);
        arrow.textContent = expanded ? CATALOG_ARROW_OPEN : CATALOG_ARROW_CLOSED;
    };

    // 右键菜单：刷新本节点的下一级 —— 走的是同一套懒加载，重新拉一次并原地重建，
    // 用于把「刚在别处新建的库 / 表 / 列」刷出来（服务端元数据不推送，只能手动重拉）。
    // 只重拉本节点一层：schema 上的刷新 = 重拉 table 列表（新表即刻出现），
    // 已展开的更深层级会随之重建，需要时再点开即可。
    var menuLabel = level === 'catalog' ? '刷新 Schema 列表'
        : (level === 'schema' ? '刷新 Table 列表' : '刷新 Column 列表');
    row.oncontextmenu = function (e) {
        e.preventDefault();      // 屏蔽浏览器原生菜单
        e.stopPropagation();
        showContextMenu([{ label: menuLabel, onClick: loadChildren }], e.clientX, e.clientY);
    };

    return node;
}

/**
 * 拉取某节点的下一级。
 *
 * @return {Promise<Array<{label:string, level:string, path:string[], meta:Object}>>}
 */
function fetchCatalogChildren(level, path) {
    var base = '/api/metadata/catalogs/' + encodeURIComponent(path[0]);
    if (level === 'catalog') {
        return httpGet(base + '/schemas').then(function (d) {
            return ((d && d.schemas) || []).map(function (s) {
                return { label: s, level: 'schema', path: path.concat([s]) };
            });
        });
    }
    if (level === 'schema') {
        var url = base + '/schemas/' + encodeURIComponent(path[1]) + '/tables';
        return httpGet(url).then(function (d) {
            return ((d && d.tables) || []).map(function (t) {
                return { label: t, level: 'table', path: path.concat([t]) };
            });
        });
    }
    // table → 列信息
    var curl = base + '/schemas/' + encodeURIComponent(path[1])
        + '/tables/' + encodeURIComponent(path[2]) + '/columns';
    return httpGet(curl).then(function (d) {
        return ((d && d.columns) || []).map(function (c) {
            return {
                label: c.name, level: 'column', path: path.concat([c.name]),
                meta: { dataType: c.dataType, nullable: c.nullable, comment: c.comment }
            };
        });
    });
}

// ==================== SQL 编辑器「光标记忆」 ====================
//
// 从左侧目录树拖表名进来时，浏览器先处理树节点的 mousedown —— 一次按下就让 textarea 失焦
// （树行不可聚焦，焦点直接落到 body）。等 drop 事件到达时 `document.activeElement` 早已不是
// textarea、`selectionStart` 也就无从读起 —— 这正是「拖进来的表名永远贴在末尾」的根因。
//
// 好在**失焦那一刻光标仍在原处**：这里在 blur（外加键入 / 点选 / 选区变化的日常一拍）把
// `{start, end}` 记下来，drop 时按它落点。刻意不做坐标反查
// （`caretRangeFromPoint` / 隐藏量尺）—— 在「透明文字 + 垫层」这套结构里各家实现并不可靠。

/** 编辑器最近一次的光标 / 选区快照；null = 还没编辑过（落点退回末尾）。 */
var sqlCaretMemory = null;

/** 记下编辑器当前光标 / 选区（失焦时就靠这份快照落点）。 */
function rememberSqlCaret(input) {
    if (!input) return;
    var s = input.selectionStart;
    if (s === null || s === undefined || s < 0 || s > input.value.length) return;
    var e = input.selectionEnd;
    if (e === null || e === undefined || e < s || e > input.value.length) e = s;
    sqlCaretMemory = { start: s, end: e };
}

/** 绑定光标记忆（首页初始化时调一次）。 */
function bindSqlCaretMemory() {
    var input = document.getElementById('query-input');
    if (!input) return;
    ['keyup', 'mouseup', 'click', 'select', 'input'].forEach(function (evt) {
        input.addEventListener(evt, function () { rememberSqlCaret(input); });
    });
    // blur 是拖拽场景的关键一拍：mousedown 落左树 → textarea 失焦，此刻光标仍在原处。
    input.addEventListener('blur', function () { rememberSqlCaret(input); });
}

/**
 * 把引用文本写到 SQL 输入框。
 *
 * <p>编辑器仍有焦点时插在光标处；已被拖拽弄失焦时退回<b>光标记忆</b>里那一格，
 * 而不是无脑追加到末尾。若记忆里是一段选区，则整段替换掉（等同「把表名打进选区」）。</p>
 */
function insertRefAtCaret(input, ref) {
    var v = input.value || '';
    var focused = (document.activeElement === input);
    var start = focused ? input.selectionStart
        : (sqlCaretMemory ? sqlCaretMemory.start : v.length);
    var end = focused ? input.selectionEnd
        : (sqlCaretMemory ? sqlCaretMemory.end : start);
    if (start === null || start === undefined || start < 0 || start > v.length) start = v.length;
    if (end === null || end === undefined || end < start || end > v.length) end = start;

    var before = v.slice(0, start);
    var after = v.slice(end);
    var pad = (before && !/\s$/.test(before)) ? ' ' : '';
    var tail = (after && !/^\s/.test(after)) ? ' ' : '';

    input.value = before + pad + ref + tail + after;
    var caret = before.length + pad.length + ref.length;
    input.focus();
    input.setSelectionRange(caret, caret);
    sqlCaretMemory = { start: caret, end: caret };   // 连续拖第二张表 → 接在刚才那张后面
    // 程序化赋值不触发 input 事件：手动补一个，让编辑器的「草稿记忆 / Ctrl+Z 撤销栈」把这次
    // 拖拽也记成一（可撤销的）步 —— 否则拖进来的内容既存不进草稿，也撤不掉。
    input.dispatchEvent(new Event('input', { bubbles: true }));
}

/**
 * 让右侧工作区接受从左侧目录树拖来的引用。
 *
 * <p>落点判定框是整个 {@code .home-main}（编辑器 + 结果区），而不是只圈住编辑器外框 ——
 * 只圈外框时，手抖落到编辑器下面几像素就整段白拖，用户得瞄得很准。高亮仍打在编辑器外框上，
 * 让人一眼看到「会插到哪里」。</p>
 */
function bindSqlDropZone() {
    var zone = document.querySelector('.home-main') || document.querySelector('.sql-editor-wrap');
    var wrap = document.querySelector('.sql-editor-wrap');
    var input = document.getElementById('query-input');
    if (!zone || !input) return;

    function hasText(e) {
        var types = (e.dataTransfer && e.dataTransfer.types) || [];
        return Array.prototype.indexOf.call(types, 'text/plain') >= 0;
    }

    function highlight(on) {
        if (wrap) wrap.classList.toggle('drop-target', !!on);
    }

    zone.addEventListener('dragover', function (e) {
        if (!hasText(e)) return;
        e.preventDefault();                       // 允许落下
        e.dataTransfer.dropEffect = 'copy';
        highlight(true);
    });
    zone.addEventListener('dragleave', function (e) {
        // 在子元素之间移动不算离开
        if (e.relatedTarget && zone.contains(e.relatedTarget)) return;
        highlight(false);
    });
    zone.addEventListener('drop', function (e) {
        if (!hasText(e)) return;
        e.preventDefault();
        highlight(false);
        var ref = e.dataTransfer.getData('text/plain');
        if (ref) insertRefAtCaret(input, ref);
    });
}
