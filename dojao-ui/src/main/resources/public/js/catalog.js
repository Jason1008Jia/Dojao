// ==================== 首页：catalog → schema → table → column 目录树 ====================
// 数据源：GET /api/metadata/catalogs
//         GET /api/metadata/catalogs/{c}/schemas
//         GET /api/metadata/catalogs/{c}/schemas/{s}/tables
//         GET /api/metadata/catalogs/{c}/schemas/{s}/tables/{t}/columns
// 懒加载：点开才拉下一级。展开到表级别后，点表把「列信息」逐行铺在表下面。
// 表引用（catalog.schema.table）只通过**拖拽**落到 SQL 编辑器（点表不再回填）。
// 右键节点 → 「刷新」：重拉该节点的下一级（服务端不推送元数据变更，新建的表靠这个刷出来）。
// catalog 节点额外有「删除 Catalog」：二次确认后执行 DROP CATALOG（只注销元数据，不动数据源数据）。

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
            // 编辑器右侧的「默认 catalog」下拉框与目录树同源，同一份列表一起刷新
            populateSqlCatalogSelect(cats);
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
            populateSqlCatalogSelect([]);
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
 * 删除 catalog 的二次确认弹窗（危险操作，绝不点一下就删）。
 *
 * <p>确认后调 {@code DELETE /api/catalog/{name}}（服务端执行 DROP CATALOG）：只注销
 * 元数据注册，数据源里的数据原样保留；删除成功重拉整棵树与默认 catalog 下拉框。</p>
 */
function confirmDeleteCatalog(catalog) {
    var overlay = mgModal('删除 Catalog',
        '<div class="mg-confirm">'
        + '<span class="mg-confirm-ico">'
        + '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9"'
        + ' stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        + '<path d="M10.3 3.9 1.9 18a2 2 0 0 0 1.7 3h16.8a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/>'
        + '<path d="M12 9v5"/><path d="M12 17.6h.01"/></svg></span>'
        + '<div class="mg-confirm-main">'
        + '<div class="mg-confirm-title">确定删除 Catalog「<b>' + esc(catalog) + '</b>」？</div>'
        + '<div class="mg-confirm-desc">只会注销元数据注册，<b>不会删除数据源中的任何数据</b>。'
        + '删除后该 Catalog 下的表将无法再通过 Dojao 访问，且此操作不可撤销。</div>'
        + '</div></div>'
        + '<div class="mg-actions">'
        + '<button type="button" class="mg-btn" data-act="cancel">取消</button>'
        + '<button type="button" class="mg-btn danger" data-act="delete">删除 Catalog</button>'
        + '</div>');
    overlay.querySelector('.mg-modal').classList.add('mg-modal-sm');
    overlay.querySelector('[data-act="cancel"]').onclick = mgClose;
    overlay.querySelector('[data-act="delete"]').onclick = function () {
        var btn = overlay.querySelector('[data-act="delete"]');
        var cancel = overlay.querySelector('[data-act="cancel"]');
        btn.disabled = true;
        cancel.disabled = true;
        btn.textContent = '删除中\u2026';
        httpDelete('/api/catalog/' + encodeURIComponent(catalog))
            .then(function (d) {
                mgClose();
                showToast((d && d.message) || ('Catalog ' + catalog + ' 已删除'));
                refreshCatalogTree();
            })
            .catch(function (err) {
                btn.disabled = false;
                cancel.disabled = false;
                btn.textContent = '删除';
                showToast('删除失败：' + (err && err.message ? err.message : err));
            });
    };
}

// ==================== 编辑器默认 catalog：两段式 schema.table 自动补全 ====================
//
// 编辑器工具栏右侧的 catalog 下拉框 = 「当前 catalog」。选中后用户可以直接写两段式
// `schema.table`，提交前补全成引擎要的三段式 `catalog.schema.table`（见
// completeCatalogQualifiers，由 dashboard.js 的 doSubmitQuery 调用）。
//
// ★补全只在「表引用位置」进行（FROM / JOIN / INTO / UPDATE / TABLE 之后，以及 FROM 列表的
//   逗号之后），且两段的第一段必须是该 catalog 下真实的 schema —— 否则 `SELECT u.user_id`
//   这类「别名.列」会被误补成 `catalog.u.user_id`。

var SQL_CATALOG_PREF_KEY = 'dojao.sql.catalog';
/** catalog -> [schema]（选中时预取，供「第一段是不是 schema」判定）。 */
var sqlCatalogSchemas = {};

/** 当前选中的默认 catalog（'' = 未选择 / 未加载）。 */
function sqlCatalogSelection() {
    var sel = document.getElementById('sql-catalog-select');
    return sel ? sel.value : '';
}

/**
 * 用顶层 catalog 列表刷新下拉框（记住上次选择；已不存在的选择退回第一个）。
 * 由 {@link loadCatalogTree} 调用 —— 目录树与下拉框永远来自同一份列表。
 */
function populateSqlCatalogSelect(catalogs) {
    var sel = document.getElementById('sql-catalog-select');
    if (!sel) return;
    var cats = catalogs || [];
    var saved = '';
    try { saved = localStorage.getItem(SQL_CATALOG_PREF_KEY) || ''; } catch (e) { saved = ''; }
    sel.innerHTML = '';
    if (!cats.length) {
        var none = document.createElement('option');
        none.value = '';
        none.textContent = '（无 catalog）';
        sel.appendChild(none);
        sel.disabled = true;
        sel.title = '没有可用的 catalog：需写完整的 catalog.schema.table';
        syncSqlCatalogPick();
        return;
    }
    sel.disabled = false;
    cats.forEach(function (c) {
        var opt = document.createElement('option');
        opt.value = c;
        opt.textContent = c;
        sel.appendChild(opt);
    });
    sel.value = cats.indexOf(saved) >= 0 ? saved : cats[0];
    try { localStorage.setItem(SQL_CATALOG_PREF_KEY, sel.value); } catch (e) { /* 隐私模式忽略 */ }
    syncSqlCatalogPick();
    loadSqlCatalogSchemas(sel.value);
}

/** 下拉框切换：记住选择 + 预取该 catalog 的 schema 列表。 */
function onSqlCatalogChange() {
    var sel = document.getElementById('sql-catalog-select');
    if (!sel) return;
    try { localStorage.setItem(SQL_CATALOG_PREF_KEY, sel.value || ''); } catch (e) { /* 隐私模式忽略 */ }
    syncSqlCatalogPick();
    loadSqlCatalogSchemas(sel.value);
}

// ---------------- 自绘 catalog 下拉（原生 select 只作为数据源）----------------

/** 把隐藏 select 的当前值 / 可用性 / 提示同步到自绘胶囊。 */
function syncSqlCatalogPick() {
    var sel = document.getElementById('sql-catalog-select');
    var pick = document.getElementById('sql-catalog-pick');
    var btn = document.getElementById('sql-catalog-btn');
    var cur = document.getElementById('sql-catalog-current');
    if (!sel || !pick || !btn || !cur) return;
    cur.textContent = sel.value || '（无 catalog）';
    btn.disabled = !!sel.disabled;
    pick.title = sel.title || '当前 catalog';
    pick.classList.toggle('disabled', !!sel.disabled);
    if (sel.disabled) closeSqlCatalogMenu();
}

/** 按 select 的 options 渲染下拉弹层（当前项高亮）。 */
function renderSqlCatalogMenu() {
    var sel = document.getElementById('sql-catalog-select');
    var menu = document.getElementById('sql-catalog-menu');
    if (!sel || !menu) return;
    menu.innerHTML = '';
    if (!sel.options.length) {
        menu.innerHTML = '<div class="sql-catalog-opt-empty">（无 catalog）</div>';
        return;
    }
    Array.prototype.forEach.call(sel.options, function (opt) {
        var row = document.createElement('div');
        var active = (opt.value === sel.value);
        row.className = 'sql-catalog-opt' + (active ? ' active' : '');
        row.setAttribute('role', 'option');
        row.setAttribute('aria-selected', active ? 'true' : 'false');
        row.textContent = opt.textContent;
        row.title = opt.value;
        row.onclick = function () {
            sel.value = opt.value;
            closeSqlCatalogMenu();
            onSqlCatalogChange();   // 记住选择 + 预取 schema + 同步胶囊
        };
        menu.appendChild(row);
    });
}

/** 开 / 关下拉弹层。 */
function toggleSqlCatalogMenu(force) {
    var menu = document.getElementById('sql-catalog-menu');
    var pick = document.getElementById('sql-catalog-pick');
    var btn = document.getElementById('sql-catalog-btn');
    if (!menu || !pick || !btn || btn.disabled) return;
    var open = (typeof force === 'boolean') ? force : menu.classList.contains('hidden');
    if (!open) { closeSqlCatalogMenu(); return; }
    renderSqlCatalogMenu();
    menu.classList.remove('hidden');
    pick.classList.add('open');
    btn.setAttribute('aria-expanded', 'true');
}

/** 收起下拉弹层（幂等）。 */
function closeSqlCatalogMenu() {
    var menu = document.getElementById('sql-catalog-menu');
    var pick = document.getElementById('sql-catalog-pick');
    var btn = document.getElementById('sql-catalog-btn');
    if (menu) menu.classList.add('hidden');
    if (pick) pick.classList.remove('open');
    if (btn) btn.setAttribute('aria-expanded', 'false');
}

/** 绑定自绘下拉（首页初始化调一次）。 */
function bindSqlCatalogPicker() {
    var btn = document.getElementById('sql-catalog-btn');
    if (!btn || btn.dataset.bound) return;
    btn.dataset.bound = '1';
    btn.onclick = function (e) {
        e.stopPropagation();
        toggleSqlCatalogMenu();
    };
    // 点胶囊外面收起（捕获阶段：内部点击先判定 contains，弹层选项不受影响）
    document.addEventListener('mousedown', function (e) {
        var pick = document.getElementById('sql-catalog-pick');
        if (pick && !pick.contains(e.target)) closeSqlCatalogMenu();
    }, true);
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') closeSqlCatalogMenu();
    });
    window.addEventListener('resize', closeSqlCatalogMenu);
    window.addEventListener('scroll', closeSqlCatalogMenu, true);
    syncSqlCatalogPick();
}

/** 拉某 catalog 的 schema 名字（只用于补全判定，不做任何展示）。 */
function loadSqlCatalogSchemas(catalog) {
    var sel = document.getElementById('sql-catalog-select');
    if (!catalog) {
        if (sel) sel.title = '未选择 catalog：需写完整的 catalog.schema.table';
        syncSqlCatalogPick();
        return Promise.resolve();
    }
    return httpGet('/api/metadata/catalogs/' + encodeURIComponent(catalog) + '/schemas')
        .then(function (d) {
            sqlCatalogSchemas[catalog] = ((d && d.schemas) || []).slice();
            if (sel) {
                sel.title = '当前 catalog：' + catalog
                    + '（编辑器可直接写 schema.table，提交时自动补全为 ' + catalog + '.schema.table）';
            }
            syncSqlCatalogPick();
        })
        .catch(function () {
            sqlCatalogSchemas[catalog] = [];
            if (sel) sel.title = '当前 catalog：' + catalog + '（schema 列表加载失败，不会自动补全）';
            syncSqlCatalogPick();
        });
}

/** 表引用位置的关键字（其后紧跟的标识符是表名）。 */
var SQL_TABLE_REF_KEYWORDS = {
    from: 1, join: 1, into: 1, update: 1, table: 1, truncate: 1, describe: 1
};
/** 结束表引用列表（FROM 子句的其余部分）的关键字。 */
var SQL_TABLE_REF_END_KEYWORDS = {
    where: 1, group: 1, having: 1, order: 1, limit: 1, union: 1, intersect: 1,
    except: 1, qualify: 1, window: 1, on: 1, using: 1, set: 1, values: 1
};
/** 表名前后可能出现但从不是表名的关键字（`CREATE TABLE IF NOT EXISTS db.t` / `INSERT INTO TABLE db.t`）。 */
var SQL_TABLE_REF_SKIP_KEYWORDS = { if: 1, not: 1, exists: 1, only: 1, table: 1 };

/**
 * 把编辑器 SQL 里的两段式表引用补全成三段式。
 *
 * <p>只改「表引用位置」且第一段命中当前 catalog 的真实 schema 的两段名；字符串、注释、
 * 引号标识符内的文本原样保留。三段式（含更多段）不动，因此全限定的 SQL 完全不受影响。</p>
 *
 * @param {string} sql 用户书写的 SQL（可能是批量：按分号分隔）
 * @return {string} 补全后的 SQL；无 catalog / 无 schema 信息时原样返回
 */
function completeCatalogQualifiers(sql) {
    var text = String(sql || '');
    var catalog = sqlCatalogSelection();
    if (!catalog) return text;
    var schemas = sqlCatalogSchemas[catalog];
    if (!schemas || !schemas.length) return text;
    var schemaSet = {};
    schemas.forEach(function (s) { schemaSet[String(s).toLowerCase()] = 1; });
    var catalogLower = catalog.toLowerCase();

    var tokens = tokenizeSqlText(text);
    var inFrom = false;      // 是否处在 FROM 的表引用列表里（逗号后是下一张表）
    var expectTable = false; // 下一个标识符是否就是表名
    var sawSelect = false;   // 当前括号层是否见过 SELECT：只有「语句里的 FROM」才是表引用
    var lastIdent = '';      // 上一个标识符（`DELETE FROM` 的 FROM 前面是 delete）
    var stack = [];          // 括号栈：进子查询时保存外层状态
    var inserts = [];        // 待插入 `catalog.` 的位置（第一段表名的起始 offset）

    for (var i = 0; i < tokens.length; i++) {
        var t = tokens[i];
        if (t.type === 'punct') {
            if (t.text === '(') {
                stack.push({ inFrom: inFrom, expectTable: expectTable, sawSelect: sawSelect });
                inFrom = false;
                expectTable = false;
                sawSelect = false;
            } else if (t.text === ')') {
                var outer = stack.pop();
                inFrom = outer ? outer.inFrom : false;
                sawSelect = outer ? outer.sawSelect : false;
                expectTable = false;    // ')' 后面跟的是派生表别名，不是表名
            } else if (t.text === ',') {
                if (inFrom) expectTable = true;
            } else if (t.text === ';') {
                inFrom = false;
                expectTable = false;
                sawSelect = false;
                lastIdent = '';
            }
            continue;
        }
        if (t.type !== 'ident') continue;   // 字符串 / 引号标识符：不参与补全
        var low = t.text.toLowerCase();

        if (expectTable) {
            if (SQL_TABLE_REF_SKIP_KEYWORDS[low]) continue;
            // 收集表名的各段：a[.b[.c…]]
            var j = i + 1;
            var parts = 1;
            while (j + 1 < tokens.length && tokens[j].type === 'punct' && tokens[j].text === '.'
                    && tokens[j + 1].type === 'ident') {
                parts++;
                j += 2;
            }
            expectTable = false;
            if (parts === 2) {
                var first = t.text.toLowerCase();
                if (first !== catalogLower && schemaSet[first]) inserts.push(t.start);
            }
            i = j - 1;   // parts 段已消费（含点号）
            continue;
        }

        if (low === 'select') sawSelect = true;

        if (SQL_TABLE_REF_KEYWORDS[low]) {
            // ★ FROM 有两种：语句级 `SELECT … FROM t` / `DELETE FROM t` 才是表引用；
            //   函数内部的 `TRIM(x FROM col)` / `EXTRACT(year FROM col)` 不是 —— 误判会把
            //   `FROM col` 后面的列引用补成 catalog.schema.col。以此二判据区分。
            var isTableFrom = low !== 'from' || sawSelect || lastIdent === 'delete';
            if (isTableFrom) {
                if (low === 'from' || low === 'join') inFrom = true;
                expectTable = true;
            }
            lastIdent = low;
            continue;
        }
        if (SQL_TABLE_REF_END_KEYWORDS[low]) {
            inFrom = false;
            expectTable = false;
        }
        lastIdent = low;
    }

    if (!inserts.length) return text;
    inserts.sort(function (a, b) { return b - a; });
    var out = text;
    for (var k = 0; k < inserts.length; k++) {
        out = out.slice(0, inserts[k]) + catalog + '.' + out.slice(inserts[k]);
    }
    return out;
}

/**
 * 轻量 SQL 词法切分：只区分「标识符 / 标点 / 其它（字符串、引号标识符）」，
 * 字符串与 `--`、`/* *\/` 注释整段吞掉——补全绝不能改到它们的内容。
 *
 * @return {Array<{type:string, text:string, start:number, end:number}>}
 */
function tokenizeSqlText(text) {
    var out = [];
    var i = 0;
    var n = text.length;
    function isIdentStart(ch) { return /[A-Za-z_]/.test(ch); }
    function isIdentPart(ch) { return /[A-Za-z0-9_$]/.test(ch); }

    while (i < n) {
        var ch = text.charAt(i);
        if (/\s/.test(ch)) { i++; continue; }
        if (ch === '-' && text.charAt(i + 1) === '-') {
            i += 2;
            while (i < n && text.charAt(i) !== '\n') i++;
            continue;
        }
        if (ch === '/' && text.charAt(i + 1) === '*') {
            i += 2;
            while (i < n && !(text.charAt(i) === '*' && text.charAt(i + 1) === '/')) i++;
            i = Math.min(n, i + 2);
            continue;
        }
        if (ch === "'" || ch === '"' || ch === '`' || ch === '[') {
            var close = ch === '[' ? ']' : ch;
            var qs = i++;
            while (i < n) {
                if (text.charAt(i) === close) {
                    // `''` / `""` / `` `` `` 为转义，继续吞
                    if (close !== ']' && text.charAt(i + 1) === close) { i += 2; continue; }
                    i++;
                    break;
                }
                i++;
            }
            out.push({ type: 'other', text: text.slice(qs, i), start: qs, end: i });
            continue;
        }
        if (isIdentStart(ch)) {
            var s = i++;
            while (i < n && isIdentPart(text.charAt(i))) i++;
            out.push({ type: 'ident', text: text.slice(s, i), start: s, end: i });
            continue;
        }
        out.push({ type: 'punct', text: ch, start: i, end: i + 1 });
        i++;
    }
    return out;
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
        var items = [{ label: menuLabel, onClick: loadChildren }];
        // catalog 节点额外提供删除；schema / table 只刷新
        if (level === 'catalog') {
            items.push({ sep: true });
            items.push({
                label: '删除 Catalog',
                danger: true,
                onClick: function () { confirmDeleteCatalog(path[0]); }
            });
        }
        showContextMenu(items, e.clientX, e.clientY);
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
