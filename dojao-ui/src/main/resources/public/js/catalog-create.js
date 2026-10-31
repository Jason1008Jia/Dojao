// ==================== 新建 Catalog 向导 ====================
// 入口：左侧「Catalog 浏览」标题的 ＋（#catalog-create-btn，见 index.html）。
// 流程：GET /api/connectors 拉连接器卡片 → 点卡片显示表单 → 测试连接 / 创建。
// 创建由后端拼出并执行 CREATE CATALOG（见 DojaoUiApp.createCatalog），前端只负责填表。
// 字段完全来自服务端 describeProperties()，这里不维护第二份连接器配置清单。

var ccWizard = {
    connectors: null,   // 连接器描述缓存（服务启动后连接器清单不常变）
    failures: null,     // 加载失败的连接器 jar / 原因
    selected: null,     // 当前选中的连接器描述
    busy: false,        // 正在测试 / 创建：期间禁止关闭、返回、重复点击
    escBound: false
};

// 卡片 logo：使用 public/img/connectors/ 下的官方 logo；未知类型用通用「插头」图标。
// 路径是静态常量，不含用户输入，可以安全地拼进 innerHTML。
var CC_LOGOS = {
    mysql: 'img/connectors/mysql.png',
    tidb: 'img/connectors/tidb.png',
    doris: 'img/connectors/doris.png',
    postgresql: 'img/connectors/postgresql.png',
    sqlserver: 'img/connectors/sqlserver.png',
    oracle: 'img/connectors/oracle.png',
    hive2: 'img/connectors/hive.png',
    hive3: 'img/connectors/hive.png',
    clickhouse: 'img/connectors/clickhouse.png',
    excel: 'img/connectors/excel.png'
};

var CC_LOGO_FALLBACK = '<svg viewBox="0 0 48 48" aria-hidden="true">'
    + '<rect width="48" height="48" rx="12" fill="#2563eb"/>'
    + '<path d="M19 14v6M29 14v6M15 22h18v5a9 9 0 0 1-18 0z" fill="none" stroke="#fff"'
    + ' stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/>'
    + '<path d="M24 36v4" stroke="#fff" stroke-width="2.4" stroke-linecap="round"/>'
    + '</svg>';

/** 打开向导（显示连接器卡片；连接器清单按需加载并缓存）。 */
function openCatalogCreateDialog() {
    var overlay = document.getElementById('catalog-create-overlay');
    if (!overlay) return;
    overlay.classList.add('show');
    overlay.setAttribute('aria-hidden', 'false');
    if (!ccWizard.escBound) {
        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape' && overlay.classList.contains('show') && !ccWizard.busy) {
                closeCatalogCreate();
            }
        });
        ccWizard.escBound = true;
    }
    ccShowCards();
}

/** 关闭向导（测试 / 创建进行中不允许关，避免用户误以为已中止）。 */
function closeCatalogCreate() {
    if (ccWizard.busy) return;
    var overlay = document.getElementById('catalog-create-overlay');
    if (!overlay) return;
    overlay.classList.remove('show');
    overlay.setAttribute('aria-hidden', 'true');
    ccWizard.selected = null;
    ccSetStatus('', '');
}

/** 点遮罩空白处关闭（点弹窗内部不关）。 */
function catalogCreateBackdrop(e) {
    if (e.target === document.getElementById('catalog-create-overlay')) closeCatalogCreate();
}

/** 表单页「←」：回到连接器卡片列表。 */
function catalogCreateBack() {
    if (ccWizard.busy) return;
    ccShowCards();
}

// ==================== 连接器卡片 ====================

function ccShowCards() {
    ccWizard.selected = null;
    document.getElementById('cc-back').classList.add('hidden');
    document.getElementById('cc-title').textContent = '新建 Catalog';
    document.getElementById('cc-sub').textContent = '选择要连接的数据库类型';
    ccSetStatus('', '');
    ccSetButtons(false);

    var body = document.getElementById('cc-body');
    if (!ccWizard.connectors) {
        body.innerHTML = '<div class="cc-loading">加载连接器…</div>';
        httpGet('/api/connectors').then(function (d) {
            ccWizard.connectors = (d && d.connectors) || [];
            ccWizard.failures = (d && d.failures) || {};
            ccRenderCards();
        }).catch(function (err) {
            body.innerHTML = '<div class="cc-error">连接器清单加载失败：'
                + esc(oneLine(err && err.message ? err.message : err, 200)) + '</div>';
        });
        return;
    }
    ccRenderCards();
}

function ccRenderCards() {
    var body = document.getElementById('cc-body');
    var list = ccWizard.connectors || [];
    if (!list.length) {
        body.innerHTML = '<div class="cc-empty">没有可用的连接器。'
            + '请确认连接器 jar 已放到 lib/connectors/ 后重启服务。</div>';
        return;
    }

    var html = '';
    var failures = ccWizard.failures || {};
    var failKeys = Object.keys(failures);
    if (failKeys.length) {
        html += '<div class="cc-failures">有 ' + failKeys.length + ' 个连接器加载失败：'
            + esc(oneLine(failKeys.map(function (k) { return k + '：' + failures[k]; }).join('；'), 360))
            + '</div>';
    }
    html += '<div class="cc-grid">';
    list.forEach(function (c) {
        html += '<button type="button" class="cc-card" data-cc-type="' + esc(c.type) + '">'
            + '<span class="cc-card-logo">' + ccLogoFor(c.type) + '</span>'
            + '<span class="cc-card-main">'
            + '<span class="cc-card-name">' + esc(c.displayName) + '</span>'
            + '<span class="cc-card-desc">' + esc(c.description) + '</span>'
            + '<span class="cc-card-type">' + esc(c.type) + '</span>'
            + '</span></button>';
    });
    html += '</div>';
    body.innerHTML = html;

    Array.prototype.forEach.call(body.querySelectorAll('.cc-card'), function (card) {
        card.onclick = function () { catalogCreateSelect(card.getAttribute('data-cc-type')); };
    });
}

function ccLogoFor(type) {
    var src = CC_LOGOS[type];
    if (!src) return CC_LOGO_FALLBACK;
    return '<img class="cc-logo-img" src="' + src + '" alt="" aria-hidden="true"'
        + ' onerror="ccLogoImageError(this)">';
}

/** logo 文件缺失（旧部署包未带图）时退回通用插头图标，避免卡片出现破图。 */
function ccLogoImageError(img) {
    var box = img && img.parentNode;
    if (box) box.innerHTML = CC_LOGO_FALLBACK;
}

// ==================== 表单 ====================

function catalogCreateSelect(type) {
    if (ccWizard.busy) return;
    var chosen = null;
    (ccWizard.connectors || []).forEach(function (c) {
        if (c.type === type) chosen = c;
    });
    if (!chosen) return;
    ccWizard.selected = chosen;
    document.getElementById('cc-back').classList.remove('hidden');
    document.getElementById('cc-title').textContent = '新建 ' + chosen.displayName + ' Catalog';
    document.getElementById('cc-sub').textContent = chosen.description;
    ccSetStatus('', '');
    ccRenderForm(chosen);
    ccSetButtons(true);
}

function ccRenderForm(c) {
    var html = '<div class="cc-form">'
        + '<div class="cc-form-head">'
        + '<span class="cc-form-logo">' + ccLogoFor(c.type) + '</span>'
        + '<span class="cc-form-title"><span class="cc-form-name">' + esc(c.displayName) + '</span>'
        + '<span class="cc-form-desc">' + esc(c.description) + '</span></span>'
        + '</div>';

    html += ccFieldHtml({
        key: '__name', label: 'Catalog 名称', required: true, secret: false, kind: 'text',
        defaultValue: '', placeholder: '例如 mysql_prod',
        help: '唯一标识，只能包含字母、数字、下划线，且不能以数字开头'
    }, -1, 'cc-name');

    var fields = c.fields || [];
    fields.forEach(function (f, i) { html += ccFieldHtml(f, i, 'cc-f-' + i); });
    if (!fields.length) {
        html += '<div class="cc-note">该连接器没有声明额外属性，填好名称即可创建。</div>';
    }

    html += '<div class="cc-foot-hint">创建时后台执行：'
        + '<code>CREATE CATALOG … USING ' + esc(c.type) + ' WITH (…)</code></div>'
        + '</div>';
    document.getElementById('cc-body').innerHTML = html;

    var first = document.getElementById('cc-name');
    if (first) first.focus();
}

/**
 * 渲染一个字段。
 *
 * @param {Object} f  字段描述（服务端 CatalogField；名称字段是本地拼的同形状对象）
 * @param {number} index 字段下标（名称字段传 -1，不带 data-cc-key）
 * @param {string} id 输入框 id
 */
function ccFieldHtml(f, index, id) {
    var required = f.required ? ' <b class="cc-req">*</b>' : '';
    var help = f.help ? '<span class="cc-help">' + esc(f.help) + '</span>' : '';
    var attrs = ' id="' + id + '" class="cc-input"'
        + (index >= 0 ? ' data-cc-key="' + esc(f.key) + '"' : '');

    var input;
    if (f.kind === 'boolean') {
        var asTrue = f.defaultValue !== 'false';
        input = '<select' + attrs + '>'
            + '<option value="true"' + (asTrue ? ' selected' : '') + '>是</option>'
            + '<option value="false"' + (!asTrue ? ' selected' : '') + '>否</option>'
            + '</select>';
    } else if (f.kind === 'number') {
        input = '<input type="number"' + attrs + ' value="' + esc(f.defaultValue) + '"'
            + (f.placeholder ? ' placeholder="' + esc(f.placeholder) + '"' : '') + '>';
    } else {
        input = '<input type="' + (f.secret ? 'password' : 'text') + '"' + attrs
            + ' value="' + esc(f.defaultValue) + '"'
            + (f.placeholder ? ' placeholder="' + esc(f.placeholder) + '"' : '')
            + ' autocomplete="off" spellcheck="false">';
    }

    return '<label class="cc-field">'
        + '<span class="cc-label">' + esc(f.label) + required + '</span>'
        + input + help + '</label>';
}

/** 收集表单里的属性（空白值不提交，让连接器默认值生效；密码不 trim）。 */
function ccCollectValues() {
    var values = {};
    var c = ccWizard.selected;
    if (!c) return values;
    (c.fields || []).forEach(function (f, i) {
        var el = document.getElementById('cc-f-' + i);
        if (!el) return;
        var v = el.value == null ? '' : String(el.value);
        if (f.kind !== 'boolean') {
            if (!f.secret) v = v.trim();
            if (v === '') return;
        }
        values[f.key] = v;
    });
    return values;
}

function ccFormName() {
    var el = document.getElementById('cc-name');
    return el ? String(el.value || '').trim() : '';
}

/**
 * 客户端预检（服务端仍会完整校验，这里只为快速反馈）。
 *
 * @param {boolean} requireName 创建时名称必填；测试连接允许不填（服务端用 probe 占位）
 * @return {{name:string,values:Object}|null}
 */
function ccValidateForm(requireName) {
    var c = ccWizard.selected;
    if (!c) {
        ccSetStatus('err', '请先选择连接器');
        return null;
    }
    var name = ccFormName();
    if (requireName && !/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
        ccSetStatus('err', 'Catalog 名称只能包含字母、数字、下划线，且不能以数字开头');
        return null;
    }
    if (!requireName && name && !/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
        ccSetStatus('err', 'Catalog 名称只能包含字母、数字、下划线，且不能以数字开头');
        return null;
    }

    var values = ccCollectValues();
    var missing = [];
    (c.fields || []).forEach(function (f) {
        if (f.required && !values[f.key]) missing.push(f.label);
    });
    if (missing.length) {
        ccSetStatus('err', '请填写必填项：' + missing.join('、'));
        return null;
    }
    return { name: name, values: values };
}

// ==================== 测试连接 / 创建 ====================

function catalogCreateTest() {
    if (ccWizard.busy || !ccWizard.selected) return;
    var form = ccValidateForm(false);
    if (!form) return;

    ccSetBusy(true, '正在测试连接…');
    httpPost('/api/catalog/test', {
        type: ccWizard.selected.type,
        name: form.name || null,
        values: form.values
    }).then(function (r) {
        ccSetBusy(false);
        ccSetStatus('ok', (r && r.message) || '连接成功，可以创建了');
    }).catch(function (err) {
        ccSetBusy(false);
        ccSetStatus('err', err && err.message ? err.message : String(err));
    });
}

function catalogCreateSubmit() {
    if (ccWizard.busy || !ccWizard.selected) return;
    var form = ccValidateForm(true);
    if (!form) return;

    var type = ccWizard.selected.type;
    ccSetBusy(true, '正在执行 CREATE CATALOG…');
    httpPost('/api/catalog', { name: form.name, type: type, values: form.values })
        .then(function (r) {
            ccWizard.busy = false;
            ccSetButtons(true);
            ccSetStatus('ok', (r && r.message) || '创建成功');
            showToast('Catalog ' + form.name + ' 创建成功');
            refreshCatalogTree();
            // 留一小会儿让用户看到「创建成功」，再收起对话框
            setTimeout(function () {
                ccWizard.selected = null;
                closeCatalogCreate();
            }, 800);
        })
        .catch(function (err) {
            ccWizard.busy = false;
            ccSetButtons(true);
            ccSetStatus('err', err && err.message ? err.message : String(err));
        });
}

// ==================== 状态 ====================

/** @param {'ok'|'err'|'pending'|''} kind */
function ccSetStatus(kind, message) {
    var el = document.getElementById('cc-status');
    if (!el) return;
    el.className = 'cc-status' + (kind ? ' ' + kind : '');
    el.textContent = message || '';
    el.title = message || '';
}

function ccSetButtons(enabled) {
    var test = document.getElementById('cc-test');
    var create = document.getElementById('cc-create');
    if (test) test.disabled = !enabled;
    if (create) create.disabled = !enabled;
}

function ccSetBusy(busy, message) {
    ccWizard.busy = !!busy;
    ccSetButtons(!busy && !!ccWizard.selected);
    if (busy) {
        ccSetStatus('pending', message || '处理中…');
    }
}
