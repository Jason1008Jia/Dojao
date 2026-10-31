// ==================== 管理入口 + 弹窗 ====================
// 侧栏左下角【管理】与顶栏【设置】共用一套「富菜单」（showAppMenu）；三项目标
// （函数管理 / AI 模型管理 / 页面设置）的内容都走统一弹窗（mgModal）承载。
// 本文件只做 UI，不新增后端接口。

// —— 菜单项图标（stroke 风格，颜色由 .app-menu-ico 的 currentColor 决定）——
var APP_MENU_ICONS = {
    gear: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>',
    braces: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M8.5 3.5c-1.9 0-3 1.1-3 3v2.2c0 1.2-.7 1.9-2 2.3 1.3.4 2 1.1 2 2.3v2.2c0 1.9 1.1 3 3 3"/><path d="M15.5 3.5c1.9 0 3 1.1 3 3v2.2c0 1.2.7 1.9 2 2.3-1.3.4-2 1.1-2 2.3v2.2c0 1.9-1.1 3-3 3"/></svg>',
    spark: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 3.2l1.7 4.9 4.9 1.7-4.9 1.7L12 16.4l-1.7-4.9L5.4 9.8l4.9-1.7z"/><path d="M18.6 14.6l.7 2 2 .7-2 .7-.7 2-.7-2-2-.7 2-.7z"/></svg>',
    wand: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M15 4V2M15 16v-2M8 9h2M20 9h2M17.8 11.8l1.4 1.4M17.8 6.2l1.4-1.4M3 21l9.4-9.4M12.6 7.4l1.4-1.4M12.6 11.8L11.2 10.4"/></svg>',
    file: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"/><path d="M14 3v5h5"/><path d="M9 13h6"/><path d="M9 17h4"/></svg>',
    users: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>'
};

/** admin 才追加「用户管理」项（普通用户看不到入口）。 */
function pushUserManagerIfAdmin(items) {
    if (typeof Auth !== 'undefined' && Auth.isAdmin()) {
        items.push({ icon: APP_MENU_ICONS.users, label: '用户管理', desc: '账号与角色维护', onClick: openUserManager });
    }
}

/** 侧栏左下角【管理】：在按钮上方弹出富菜单。 */
function openCatalogManageMenu(ev) {
    var items = [
        { icon: APP_MENU_ICONS.braces, label: '函数管理', desc: '查看内置与自定义函数目录', onClick: openFunctionManager },
        { icon: APP_MENU_ICONS.file, label: 'SQL 脚本管理', desc: '保存 / 加载 / 删除我的 SQL', onClick: openSqlScriptManager },
        { icon: APP_MENU_ICONS.spark, label: 'AI 模型管理', desc: '模型提供方与服务配置', onClick: openAiModelManager },
        { icon: APP_MENU_ICONS.wand, label: 'Skill 管理', desc: '可复用分析角色：提示词 + 工具', onClick: openSkillManager },
        { icon: APP_MENU_ICONS.gear, label: '页面设置', desc: '主题 / 任务列表每页条数', onClick: openPageSettings }
    ];
    pushUserManagerIfAdmin(items);
    showAppMenu(items, document.getElementById('catalog-manage-btn'),
       { title: '管理', subtitle: '函数 · 脚本 · 模型 · 技能 · 页面', place: 'above' });
}

/** 顶栏【设置】：在按钮下方右侧弹出富菜单（顶栏在右上角，菜单朝左下展开）。 */
function openHeaderSettingsMenu(ev) {
    var items = [
        { icon: APP_MENU_ICONS.gear, label: '页面设置', desc: '主题 / 任务列表每页条数', onClick: openPageSettings },
        { icon: APP_MENU_ICONS.braces, label: '函数管理', desc: '查看内置与自定义函数目录', onClick: openFunctionManager },
        { icon: APP_MENU_ICONS.file, label: 'SQL 脚本管理', desc: '保存 / 加载 / 删除我的 SQL', onClick: openSqlScriptManager },
        { icon: APP_MENU_ICONS.spark, label: 'AI 模型管理', desc: '模型提供方与服务配置', onClick: openAiModelManager },
        { icon: APP_MENU_ICONS.wand, label: 'Skill 管理', desc: '可复用分析角色：提示词 + 工具', onClick: openSkillManager }
    ];
    pushUserManagerIfAdmin(items);
    showAppMenu(items, document.getElementById('settings-btn'),
       { title: '设置', subtitle: '页面 · 函数 · 脚本 · 模型 · 技能', place: 'below-right' });
}

// ---------------- 富菜单（图标 + 名称 + 描述）----------------

var appMenuEl = null;
var appMenuBound = false;

/**
 * 弹出富菜单。
 *
 * @param {Array<{icon:string, label:string, desc:string=, onClick:Function}>} items
 * @param {HTMLElement} anchorEl 定位锚点（触发按钮）
 * @param {{title:string=, subtitle:string=, place:'above'|'below-right'=}} opts
 */
function showAppMenu(items, anchorEl, opts) {
    opts = opts || {};
    var el = ensureAppMenu();
    el.innerHTML = '';

    var head = document.createElement('div');
    head.className = 'app-menu-head';
    head.innerHTML = '<div class="app-menu-title"></div><div class="app-menu-sub"></div>';
    head.querySelector('.app-menu-title').textContent = opts.title || '菜单';
    var sub = head.querySelector('.app-menu-sub');
    if (opts.subtitle) sub.textContent = opts.subtitle; else sub.remove();
    el.appendChild(head);

    (items || []).forEach(function (it) {
        var b = document.createElement('button');
        b.type = 'button';
        b.className = 'app-menu-item';
        b.innerHTML = '<span class="app-menu-ico">' + (it.icon || '') + '</span>'
            + '<span class="app-menu-text"><span class="app-menu-label"></span>'
            + (it.desc ? '<span class="app-menu-desc"></span>' : '') + '</span>';
        b.querySelector('.app-menu-label').textContent = it.label;
        if (it.desc) b.querySelector('.app-menu-desc').textContent = it.desc;
        b.onclick = function (e) {
            e.stopPropagation();
            hideAppMenu();
            it.onClick();
        };
        el.appendChild(b);
    });

    // 先按锚点定位（内部读 offsetWidth 触发一次 reflow），再加 .show 让淡入动画生效
    positionAppMenu(el, anchorEl, opts.place);
    el.classList.add('show');
}

function positionAppMenu(el, anchor, place) {
    var mw = el.offsetWidth, mh = el.offsetHeight;
    var vw = document.documentElement.clientWidth, vh = document.documentElement.clientHeight;
    var r = anchor ? anchor.getBoundingClientRect() : { left: 12, top: 12, right: 12, bottom: 12 };
    var left, top, origin;
    if (place === 'below-right') {
        // 顶栏按钮：菜单在其正下方，右缘对齐按钮右缘（朝左下展开）
        left = Math.max(8, Math.min(vw - mw - 8, r.right - mw));
        top = r.bottom + 8;
        origin = 'top right';
    } else {
        // 侧栏按钮：菜单在其正上方，左缘对齐按钮左缘（朝上展开）
        left = Math.max(8, Math.min(vw - mw - 8, r.left));
        top = r.top - mh - 8;
        if (top < 8) top = r.bottom + 8;   // 上方放不下就翻到下方
        origin = 'bottom left';
    }
    el.style.setProperty('--app-menu-origin', origin);
    el.style.left = left + 'px';
    el.style.top = top + 'px';
}

function hideAppMenu() {
    if (appMenuEl) appMenuEl.classList.remove('show');
}

function ensureAppMenu() {
    if (appMenuEl) return appMenuEl;
    appMenuEl = document.createElement('div');
    appMenuEl.className = 'app-menu';
    document.body.appendChild(appMenuEl);
    if (!appMenuBound) {
        appMenuBound = true;
        // 点菜单外收起；点触发按钮本身不收起（让按钮的 onclick 正常再弹一次）
        document.addEventListener('mousedown', function (e) {
            if (appMenuEl && appMenuEl.classList.contains('show') && !appMenuEl.contains(e.target)
                    && !(e.target.closest && e.target.closest('.app-menu-anchor'))) {
                hideAppMenu();
            }
        }, true);
        document.addEventListener('keydown', function (e) { if (e.key === 'Escape') hideAppMenu(); });
        window.addEventListener('resize', hideAppMenu);
        window.addEventListener('scroll', hideAppMenu, true);
        window.addEventListener('blur', hideAppMenu);
    }
    return appMenuEl;
}

// ---------------- 通用弹窗 ----------------

/** 懒建弹窗 shell（单例）并弹出；返回 overlay 元素供调用方往 .mg-body 里写内容。 */
function mgModal(title, bodyHtml) {
    var overlay = document.getElementById('mg-overlay');
    if (!overlay) {
        overlay = document.createElement('div');
        overlay.id = 'mg-overlay';
        overlay.className = 'mg-overlay';
        overlay.innerHTML = '<div class="mg-modal" role="dialog" aria-modal="true">'
            + '<div class="mg-head"><span class="mg-title"></span>'
            + '<button type="button" class="mg-close" aria-label="关闭">&times;</button></div>'
            + '<div class="mg-body"></div></div>';
        document.body.appendChild(overlay);
        // 点遮罩空白处 / 关闭按钮 / Esc 关闭
        overlay.addEventListener('mousedown', function (e) { if (e.target === overlay) mgClose(); });
        overlay.querySelector('.mg-close').onclick = mgClose;
        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape' && overlay.classList.contains('show')) mgClose();
        });
    }
    overlay.querySelector('.mg-title').textContent = title;
    overlay.querySelector('.mg-body').innerHTML = bodyHtml || '';
    overlay.classList.add('show');
    return overlay;
}

function mgClose() {
    var overlay = document.getElementById('mg-overlay');
    if (overlay) overlay.classList.remove('show');
}

// ---------------- 函数管理（只读目录：内置 + 自定义）----------------

/** 函数类别文案：内置区分标量/聚合/窗口，自定义标注后端语言。 */
function fnKindLabel(f) {
    if (f.source === 'custom') return f.backend ? ('自定义 · ' + f.backend) : '自定义';
    if (f.kind === 'aggregate') return '内置 · 聚合';
    if (f.kind === 'window') return '内置 · 窗口';
    if (f.kind === 'scalar') return '内置 · 标量';
    return '内置';
}

function openFunctionManager() {
    var overlay = mgModal('函数管理', '<div class="mg-loading">加载中\u2026</div>');
    httpGet('/api/functions').then(function (d) {
        renderFunctionManager(overlay, (d && d.functions) || []);
    }).catch(function (err) {
        overlay.querySelector('.mg-body').innerHTML = '<div class="mg-error">加载失败：'
            + esc(oneLine(err && err.message ? err.message : err, 140)) + '</div>';
    });
}

function renderFunctionManager(overlay, fns) {
    var all = fns.slice().sort(function (a, b) {
        return String(a.name || '').localeCompare(String(b.name || ''));
    });
    var groups = {
        builtin: all.filter(function (f) { return f.source !== 'custom'; }),
        custom: all.filter(function (f) { return f.source === 'custom'; })
    };

    overlay.querySelector('.mg-body').innerHTML =
        '<div class="mg-tabs" id="mg-fn-tabs">'
        + '<button type="button" class="mg-tab active" data-tab="builtin">内置函数'
        + '<span class="mg-tab-badge">' + groups.builtin.length + '</span></button>'
        + '<button type="button" class="mg-tab" data-tab="custom">自定义函数'
        + '<span class="mg-tab-badge">' + groups.custom.length + '</span></button>'
        + '</div>'
        + '<div class="mg-toolbar">'
        + '<input type="text" class="mg-search" id="mg-fn-search" placeholder="搜索 函数名 / 类别 / 说明\u2026">'
        + '<span class="mg-count" id="mg-fn-count"></span></div>'
        + '<div class="mg-fn-table-wrap"><table class="mg-fn-table">'
        + '<thead><tr><th>函数</th><th>类别</th><th>说明</th></tr></thead>'
        + '<tbody id="mg-fn-body"></tbody></table></div>';

    var tabsEl = document.getElementById('mg-fn-tabs');
    var input = document.getElementById('mg-fn-search');
    var bodyEl = document.getElementById('mg-fn-body');
    var countEl = document.getElementById('mg-fn-count');
    var activeTab = 'builtin';

    function draw() {
        var list = groups[activeTab];
        var q = (input.value || '').trim().toLowerCase();
        var shown = list.filter(function (f) {
            if (!q) return true;
            return String(f.name || '').toLowerCase().indexOf(q) >= 0
                || String(f.detail || '').toLowerCase().indexOf(q) >= 0
                || fnKindLabel(f).toLowerCase().indexOf(q) >= 0;
        });
        if (!shown.length) {
            bodyEl.innerHTML = '<tr><td colspan="3" class="mg-empty">'
                + (list.length ? '无匹配函数' : '暂无' + (activeTab === 'custom' ? '自定义' : '内置') + '函数')
                + '</td></tr>';
        } else {
            bodyEl.innerHTML = shown.map(function (f) {
                return '<tr><td class="mg-fn-name">' + esc(f.name) + '</td>'
                    + '<td>' + esc(fnKindLabel(f)) + '</td>'
                    + '<td class="mg-fn-detail">' + esc(f.detail || '') + '</td></tr>';
            }).join('');
        }
        countEl.textContent = shown.length + ' / ' + list.length + ' 个函数';
    }

    tabsEl.addEventListener('click', function (e) {
        var b = e.target.closest ? e.target.closest('.mg-tab') : null;
        if (!b) return;
        activeTab = b.getAttribute('data-tab');
        var tabs = tabsEl.querySelectorAll('.mg-tab');
        for (var i = 0; i < tabs.length; i++) tabs[i].classList.toggle('active', tabs[i] === b);
        draw();
    });

    input.addEventListener('input', draw);
    draw();
    input.focus();
}

// ---------------- AI 模型管理（OpenAI 兼容端点，按账号持久化到 ui_ai_config_user）----------------

function openAiModelManager() {
    var overlay = mgModal('AI 模型管理', '<div class="mg-loading">加载中\u2026</div>');
    httpGet('/api/ai/config').then(function (c) {
        renderAiConfigForm(overlay, c || {});
    }).catch(function (err) {
        overlay.querySelector('.mg-body').innerHTML = '<div class="mg-error">加载失败：'
            + esc(oneLine(err && err.message ? err.message : err, 140)) + '</div>';
    });
}

function renderAiConfigForm(overlay, c) {
    var keyPlaceholder = c.apiKeySet ? '\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022（已配置，留空=不修改）' : 'sk-...';
    var scopeText = c.scope === 'own' ? '<b>当前账号独立配置</b>'
        : (c.scope === 'inherited' ? '当前账号尚未单独配置，正在<b>沿用管理员配置</b>'
            : '当前账号尚未配置 API Key');
    overlay.querySelector('.mg-body').innerHTML =
        '<div class="mg-note">AI 对话使用 <b>OpenAI 兼容端点</b>（默认 <b>DeepSeek</b>，也可改为 OpenAI / Azure / 通义 等）。'
        + '服务地址与模型已预填默认值，通常<b>只需填写 API Key</b>。'
        + '每个账号一份配置（一账号一个模型）：' + scopeText + '，保存后只对当前账号生效。</div>'
        + '<div class="mg-form">'
        + '<label class="mg-field"><span>服务地址</span>'
        + '<input type="text" id="mg-ai-baseurl" value="' + esc(c.baseUrl || '') + '" placeholder="https://api.deepseek.com/"></label>'
        + '<label class="mg-field"><span>模型名称</span>'
        + '<input type="text" id="mg-ai-model" value="' + esc(c.model || '') + '" placeholder="deepseek-flash"></label>'
        + '<label class="mg-field"><span>温度</span>'
        + '<input type="text" id="mg-ai-temp" value="' + esc(c.temperature != null ? c.temperature : 0.2) + '" placeholder="0.2"></label>'
        + '<label class="mg-field"><span>API Key</span>'
        + '<input type="password" id="mg-ai-key" placeholder="' + esc(keyPlaceholder) + '"></label>'
        + '</div>'
        + '<div class="mg-actions">'
        + '<button type="button" class="mg-btn" data-act="cancel">取消</button>'
        + '<button type="button" class="mg-btn primary" data-act="ok">保存</button>'
        + '</div>';

    overlay.querySelector('[data-act="cancel"]').onclick = mgClose;
    var saveBtn = overlay.querySelector('[data-act="ok"]');
    saveBtn.onclick = function () {
        var baseUrl = (overlay.querySelector('#mg-ai-baseurl').value || '').trim();
        var model = (overlay.querySelector('#mg-ai-model').value || '').trim();
        var tempRaw = (overlay.querySelector('#mg-ai-temp').value || '').trim();
        var apiKey = overlay.querySelector('#mg-ai-key').value || '';
        if (!baseUrl) { showToast('服务地址不能为空'); return; }
        if (!model) { showToast('模型名称不能为空'); return; }
        var temperature = tempRaw === '' ? 0.2 : Number(tempRaw);
        if (!isFinite(temperature)) { showToast('温度必须是数字'); return; }
        saveBtn.disabled = true;
        httpPost('/api/ai/config', { baseUrl: baseUrl, model: model, temperature: temperature, apiKey: apiKey })
            .then(function () {
                saveBtn.disabled = false;
                mgClose();
                showToast('AI 配置已保存');
            })
            .catch(function (err) {
                saveBtn.disabled = false;
                showToast('保存失败：' + oneLine(err && err.message ? err.message : err, 100));
            });
    };
}

// ---------------- Skill 管理（可复用分析角色：提示词 + 工具白名单，见后端 SkillStore）----------------

/** 工具名 → 中文标签（技能表单里更可读）。 */
var SKILL_TOOL_LABELS = {
    execute_sql: '执行只读 SQL',
    execute_ddl: '执行 DDL',
    render_chart: '渲染图表',
    export_file: '导出可下载文件',
    list_catalogs: '列出数据源',
    list_tables: '列出表',
    describe_table: '查看表结构',
    describe_function: '查看函数详情',
    cluster_status: '查看集群负载',
    read_logs: '查看节点日志',
    list_queries: '查看查询列表',
    query_status: '查看查询详情',
    operator_status: '查看算子详情'
};

function skillToolLabel(name) {
    return SKILL_TOOL_LABELS[name] || name;
}

/** 打开技能管理：拉清单 → 列表页（可新建 / 编辑 / 删除）。 */
function openSkillManager() {
    var overlay = mgModal('Skill 管理', '<div class="mg-loading">加载中\u2026</div>');
    httpGet('/api/ai/skills').then(function (d) {
        if (d && d.availableTools && typeof aiAvailableTools !== 'undefined') {
            aiAvailableTools = d.availableTools;
        }
        renderSkillList(overlay, (d && d.skills) || []);
    }).catch(function (err) {
        overlay.querySelector('.mg-body').innerHTML = '<div class="mg-error">加载失败：'
            + esc(oneLine(err && err.message ? err.message : err, 140)) + '</div>';
    });
}

function renderSkillList(overlay, skills) {
    var body = overlay.querySelector('.mg-body');
    body.innerHTML =
        '<div class="mg-note">技能 = 一段可复用的分析角色：<b>自定义提示词</b>（业务口径 / 输出规范）'
        + ' + <b>允许使用的工具</b>。在 AI 对话底部的「技能」下拉里选中即生效。</div>'
        + '<div class="mg-toolbar"><span class="mg-count">' + skills.length + ' 个技能</span>'
        + '<button type="button" class="mg-btn primary" id="mg-skill-new">新建技能</button></div>'
        + (skills.length
            ? '<div class="mg-fn-table-wrap"><table class="mg-fn-table">'
                + '<thead><tr><th>名称</th><th>说明</th><th>工具</th><th>更新时间</th>'
                + '<th class="mg-script-act">操作</th></tr></thead>'
                + '<tbody id="mg-skill-body"></tbody></table></div>'
            : '<div class="mg-empty-block">还没有技能，点右上角「新建技能」创建一个。</div>');

    var newBtn = body.querySelector('#mg-skill-new');
    if (newBtn) newBtn.onclick = function () { renderSkillForm(overlay, null); };

    var tbody = body.querySelector('#mg-skill-body');
    if (!tbody) return;
    tbody.innerHTML = skills.map(function (s) {
        var tools = (s.tools && s.tools.length) ? s.tools.map(skillToolLabel).join('、') : '全部';
        return '<tr><td class="mg-fn-name">' + esc(s.name) + '</td>'
            + '<td class="mg-fn-detail">' + esc(s.description || '') + '</td>'
            + '<td class="mg-fn-detail">' + esc(tools) + '</td>'
            + '<td class="mg-fn-detail">' + esc(fmtScriptTime(s.updatedAt)) + '</td>'
            + '<td class="mg-script-act">'
            + '<button type="button" class="mg-mini" data-edit="' + esc(s.id) + '">编辑</button>'
            + '<button type="button" class="mg-mini danger" data-del="' + esc(s.id) + '"'
            + ' data-name="' + esc(s.name) + '">删除</button>'
            + '</td></tr>';
    }).join('');

    tbody.addEventListener('click', function (e) {
        var b = e.target.closest ? e.target.closest('button') : null;
        if (!b) return;
        if (b.hasAttribute('data-edit')) {
            openSkillEditor(overlay, b.getAttribute('data-edit'));
        } else if (b.hasAttribute('data-del')) {
            var id = b.getAttribute('data-del');
            var name = b.getAttribute('data-name');
            if (!window.confirm('删除技能「' + name + '」？此操作不可撤销。')) return;
            httpJson('DELETE', '/api/ai/skills/' + encodeURIComponent(id))
                .then(function () {
                    showToast('已删除技能：' + name);
                    if (typeof refreshAiSkills === 'function') refreshAiSkills();
                    openSkillManager();   // 重新拉取
                }).catch(function (err) {
                    showToast('删除失败：' + oneLine(err && err.message ? err.message : err, 80));
                });
        }
    });
}

/** 拉取技能全文后打开编辑表单（id 为空 = 新建）。 */
function openSkillEditor(overlay, id) {
    if (!id) { renderSkillForm(overlay, null); return; }
    httpGet('/api/ai/skills/' + encodeURIComponent(id)).then(function (s) {
        renderSkillForm(overlay, s || null);
    }).catch(function (err) {
        showToast('加载技能失败：' + oneLine(err && err.message ? err.message : err, 80));
    });
}

/**
 * 技能表单（新建 / 编辑）。
 *
 * @param {Object} overlay mgModal 的 overlay
 * @param {Object|null} skill 现有技能（null = 新建）
 */
function renderSkillForm(overlay, skill) {
    var isNew = !skill || !skill.id;
    var tools = (typeof aiAvailableTools !== 'undefined' && aiAvailableTools.length) ? aiAvailableTools.slice()
        : ['execute_sql', 'execute_ddl', 'render_chart', 'export_file', 'list_catalogs', 'list_tables',
            'describe_table', 'describe_function', 'cluster_status', 'read_logs', 'list_queries',
            'query_status', 'operator_status'];
    var selected = (skill && skill.tools && skill.tools.length) ? skill.tools.slice() : tools.slice();
    var checkedAll = (skill && skill.tools && skill.tools.length === 0) || !skill;   // 新建 / 空 = 默认全选
    var selSet = {};
    (checkedAll ? tools : selected).forEach(function (t) { selSet[t] = true; });

    var body = overlay.querySelector('.mg-body');
    body.innerHTML =
        '<div class="mg-form">'
        + '<label class="mg-field"><span>名称</span>'
        + '<input type="text" id="mg-skill-name" value="' + esc(skill ? skill.name : '')
        + '" placeholder="如：日活报表 / 留存分析"></label>'
        + '<label class="mg-field"><span>说明</span>'
        + '<input type="text" id="mg-skill-desc" value="' + esc(skill ? (skill.description || '') : '')
        + '" placeholder="一句话说明该技能做什么"></label>'
        + '<label class="mg-field mg-field-block"><span>提示词</span>'
        + '<textarea id="mg-skill-prompt" rows="6" placeholder="对该角色的附加指令，例如：\n'
        + '你是留存分析专家。先查用户行为表，用留存率口径（次日/7日）计算，输出 Markdown 表格。">'
        + esc(skill ? (skill.prompt || '') : '') + '</textarea></label>'
        + '<div class="mg-field mg-field-block"><span>可用工具</span>'
        + '<div class="mg-skill-tools">'
        + tools.map(function (t) {
            return '<label class="mg-skill-tool"><input type="checkbox" value="' + esc(t) + '"'
                + (selSet[t] ? ' checked' : '') + '><span>' + esc(skillToolLabel(t)) + '</span></label>';
        }).join('')
        + '</div><div class="mg-hint">全部勾选或全部不勾 = 不限制（使用全部工具）。</div></div>'
        + '</div>'
        + '<div class="mg-actions">'
        + '<button type="button" class="mg-btn" data-act="back">返回列表</button>'
        + '<button type="button" class="mg-btn primary" data-act="ok">' + (isNew ? '创建' : '保存') + '</button>'
        + '</div>';

    body.querySelector('[data-act="back"]').onclick = openSkillManager;
    var okBtn = body.querySelector('[data-act="ok"]');
    okBtn.onclick = function () {
        var name = (body.querySelector('#mg-skill-name').value || '').trim();
        var desc = (body.querySelector('#mg-skill-desc').value || '').trim();
        var prompt = body.querySelector('#mg-skill-prompt').value || '';
        var boxes = body.querySelectorAll('.mg-skill-tool input[type=checkbox]');
        var chosen = [];
        for (var i = 0; i < boxes.length; i++) {
            if (boxes[i].checked) chosen.push(boxes[i].value);
        }
        // 全选 = 不限制（存 [])；空列表 = 不限制。
        var toolsPayload = (chosen.length === tools.length) ? [] : chosen;
        if (!name) { showToast('技能名称不能为空'); return; }
        okBtn.disabled = true;
        var url = isNew ? '/api/ai/skills' : '/api/ai/skills/' + encodeURIComponent(skill.id);
        httpPost(url, { name: name, description: desc, prompt: prompt, tools: toolsPayload })
            .then(function () {
                okBtn.disabled = false;
                showToast(isNew ? '技能已创建' : '技能已保存');
                if (typeof refreshAiSkills === 'function') refreshAiSkills();
                openSkillManager();
            })
            .catch(function (err) {
                okBtn.disabled = false;
                showToast('保存失败：' + oneLine(err && err.message ? err.message : err, 100));
            });
    };
}

// ---------------- 用户管理（admin 专属：账号 CRUD + 角色分配）----------------

var USER_ROLE_LABELS = { ADMIN: '管理员', DEVELOPER: '开发者', ANALYST: '分析师' };

function userRoleLabel(role) {
    return USER_ROLE_LABELS[role] || (role === 'USER' ? '分析师' : (role || '-'));
}

/** admin 专属入口：拉账号列表 → 列表页（可新建 / 编辑 / 删除）。 */
function openUserManager() {
    var overlay = mgModal('用户管理', '<div class="mg-loading">加载中\u2026</div>');
    loadUserManager(overlay);
}

function loadUserManager(overlay) {
    httpGet('/api/users').then(function (d) {
        renderUserList(overlay, (d && d.users) || [], d && d.currentUser);
    }).catch(function (err) {
        overlay.querySelector('.mg-body').innerHTML = '<div class="mg-error">加载失败：'
            + esc(oneLine(err && err.message ? err.message : err, 140)) + '</div>';
    });
}

function renderUserList(overlay, users, currentUser) {
    var body = overlay.querySelector('.mg-body');
    body.innerHTML =
        '<div class="mg-note"><b>管理员</b>：数据能力 + 集群开发能力 + 用户管理；'
        + '<b>开发者</b>：数据能力 + 集群测试 / 压测 / 日志分析 / 查询诊断；'
        + '<b>分析师</b>：仅数据探查、报表、数据分析与挖掘（不能使用集群开发能力）。'
        + '角色同时决定 AI 对话的提示词与可用工具。</div>'
        + '<div class="mg-toolbar"><span class="mg-count">' + users.length + ' 个账号</span>'
        + '<button type="button" class="mg-btn primary" id="mg-user-new">新建用户</button></div>'
        + (users.length
            ? '<div class="mg-fn-table-wrap"><table class="mg-fn-table">'
                + '<thead><tr><th>用户名</th><th>显示名</th><th>角色</th><th>更新时间</th>'
                + '<th class="mg-script-act">操作</th></tr></thead>'
                + '<tbody id="mg-user-body"></tbody></table></div>'
            : '<div class="mg-empty-block">还没有账号。</div>');

    var newBtn = body.querySelector('#mg-user-new');
    if (newBtn) newBtn.onclick = function () { renderUserForm(overlay, null); };

    var tbody = body.querySelector('#mg-user-body');
    if (!tbody) return;
    tbody.innerHTML = users.map(function (u) {
        var isSelf = currentUser && u.username === currentUser;
        return '<tr><td class="mg-fn-name">' + esc(u.username)
            + (isSelf ? '<span class="mg-hint">（当前账号）</span>' : '') + '</td>'
            + '<td class="mg-fn-detail">' + esc(u.displayName || '') + '</td>'
            + '<td class="mg-fn-detail">' + esc(userRoleLabel(u.role)) + '</td>'
            + '<td class="mg-fn-detail">' + esc(fmtScriptTime(u.updatedAt)) + '</td>'
            + '<td class="mg-script-act">'
            + '<button type="button" class="mg-mini" data-edit="' + esc(u.username) + '">编辑</button>'
            + (isSelf ? ''
                : '<button type="button" class="mg-mini danger" data-del="' + esc(u.username) + '"'
                    + ' data-name="' + esc(u.username) + '">删除</button>')
            + '</td></tr>';
    }).join('');

    tbody.addEventListener('click', function (e) {
        var b = e.target.closest ? e.target.closest('button') : null;
        if (!b) return;
        if (b.hasAttribute('data-edit')) {
            var name = b.getAttribute('data-edit');
            var found = users.filter(function (u) { return u.username === name; })[0] || null;
            renderUserForm(overlay, found);
        } else if (b.hasAttribute('data-del')) {
            var delName = b.getAttribute('data-del');
            if (!window.confirm('删除用户「' + delName + '」？其脚本、AI 会话、技能与模型配置也会一并清理，且不可撤销。')) return;
            httpJson('DELETE', '/api/users/' + encodeURIComponent(delName))
                .then(function () {
                    showToast('已删除用户：' + delName);
                    loadUserManager(overlay);
                }).catch(function (err) {
                    showToast('删除失败：' + oneLine(err && err.message ? err.message : err, 100));
                });
        }
    });
}

/**
 * 用户表单（新建 / 编辑）。
 *
 * @param {Object} overlay mgModal 的 overlay
 * @param {Object|null} user 现有账号（null = 新建）
 */
function renderUserForm(overlay, user) {
    var isNew = !user || !user.username;
    var roles = ['ADMIN', 'DEVELOPER', 'ANALYST'];
    var currentRole = (user && user.role) || 'ANALYST';
    var body = overlay.querySelector('.mg-body');
    body.innerHTML =
        '<div class="mg-form">'
        + '<label class="mg-field"><span>用户名</span>'
        + '<input type="text" id="mg-user-name" value="' + esc(isNew ? '' : user.username) + '"'
        + (isNew ? ' placeholder="登录用户名（不含空白）"' : ' disabled') + '></label>'
        + '<label class="mg-field"><span>显示名</span>'
        + '<input type="text" id="mg-user-display" value="' + esc(isNew ? '' : (user.displayName || ''))
        + '" placeholder="可选，用于界面展示"></label>'
        + '<label class="mg-field"><span>角色</span><select id="mg-user-role">'
        + roles.map(function (r) {
            return '<option value="' + r + '"' + (r === currentRole ? ' selected' : '') + '>'
                + esc(userRoleLabel(r)) + '</option>';
        }).join('')
        + '</select></label>'
        + '<label class="mg-field"><span>' + (isNew ? '口令' : '重置口令') + '</span>'
        + '<input type="password" id="mg-user-pass" placeholder="'
        + (isNew ? '至少 6 位' : '留空 = 不修改') + '"></label>'
        + '</div>'
        + '<div class="mg-actions">'
        + '<button type="button" class="mg-btn" data-act="back">返回列表</button>'
        + '<button type="button" class="mg-btn primary" data-act="ok">' + (isNew ? '创建用户' : '保存') + '</button>'
        + '</div>';

    body.querySelector('[data-act="back"]').onclick = function () { loadUserManager(overlay); };
    var okBtn = body.querySelector('[data-act="ok"]');
    okBtn.onclick = function () {
        var username = isNew ? (body.querySelector('#mg-user-name').value || '').trim() : user.username;
        var displayName = (body.querySelector('#mg-user-display').value || '').trim();
        var role = body.querySelector('#mg-user-role').value;
        var password = body.querySelector('#mg-user-pass').value || '';
        if (!username) { showToast('用户名不能为空'); return; }
        if (password && password.length < 6) { showToast('口令至少 6 位'); return; }
        if (isNew && !password) { showToast('口令不能为空'); return; }
        okBtn.disabled = true;
        var payload = { displayName: displayName, role: role };
        if (password) payload.password = password;
        var req = isNew
            ? httpPost('/api/users', { username: username, password: password, role: role, displayName: displayName })
            : httpPost('/api/users/' + encodeURIComponent(username), payload);
        req.then(function () {
            okBtn.disabled = false;
            showToast(isNew ? '用户已创建' : '用户已保存');
            loadUserManager(overlay);
            if (isNew && typeof Auth !== 'undefined' && Auth.user()
                    && Auth.user().username === username) {
                Auth.load();   // 极端情况下新建的是当前账号（一般不会发生）：刷新顶栏
            }
        }).catch(function (err) {
            okBtn.disabled = false;
            showToast('保存失败：' + oneLine(err && err.message ? err.message : err, 100));
        });
    };
}

// ---------------- SQL 脚本管理（当前登录用户私有，见后端 SqlScriptStore）----------------

function openSqlScriptManager() {
    var overlay = mgModal('SQL 脚本管理', '<div class="mg-loading">加载中\u2026</div>');
    httpGet('/api/scripts').then(function (d) {
        renderSqlScriptList(overlay, (d && d.scripts) || []);
    }).catch(function (err) {
        overlay.querySelector('.mg-body').innerHTML = '<div class="mg-error">加载失败：'
            + esc(oneLine(err && err.message ? err.message : err, 140)) + '</div>';
    });
}

function renderSqlScriptList(overlay, scripts) {
    var body = overlay.querySelector('.mg-body');
    if (!scripts.length) {
        body.innerHTML = '<div class="mg-note">还没有保存的脚本。在 SQL 编辑器里'
            + '<b>右键 →「保存当前 SQL 文件」</b>即可保存，保存的脚本只对当前登录用户可见。</div>';
        return;
    }
    body.innerHTML =
        '<div class="mg-fn-table-wrap"><table class="mg-fn-table">'
        + '<thead><tr><th>名称</th><th>更新时间</th><th class="mg-script-act">操作</th></tr></thead>'
        + '<tbody id="mg-script-body"></tbody></table></div>';

    var tbody = body.querySelector('#mg-script-body');
    tbody.innerHTML = scripts.map(function (s) {
        return '<tr><td class="mg-fn-name">' + esc(s.name) + '</td>'
            + '<td class="mg-fn-detail">' + esc(fmtScriptTime(s.updatedAt)) + '</td>'
            + '<td class="mg-script-act">'
            + '<button type="button" class="mg-mini" data-load="' + esc(s.id) + '">加载</button>'
            + '<button type="button" class="mg-mini danger" data-del="' + esc(s.id) + '"'
            + ' data-name="' + esc(s.name) + '">删除</button>'
            + '</td></tr>';
    }).join('');

    tbody.addEventListener('click', function (e) {
        var b = e.target.closest ? e.target.closest('button') : null;
        if (!b) return;
        if (b.hasAttribute('data-load')) {
            loadSqlScriptIntoEditor(b.getAttribute('data-load'));
        } else if (b.hasAttribute('data-del')) {
            var name = b.getAttribute('data-name');
            if (!window.confirm('删除脚本「' + name + '」？此操作不可撤销。')) return;
            httpJson('DELETE', '/api/scripts/' + encodeURIComponent(b.getAttribute('data-del')))
                .then(function () {
                    showToast('已删除脚本：' + name);
                    openSqlScriptManager();   // 重新拉取
                }).catch(function (err) {
                    showToast('删除失败：' + oneLine(err && err.message ? err.message : err, 80));
                });
        }
    });
}

/** 脚本更新时间 → 本地可读串。 */
function fmtScriptTime(ms) {
    var d = new Date(Number(ms) || 0);
    if (isNaN(d.getTime()) || !ms) return '-';
    return d.toLocaleString();
}

// ---------------- 通用「输入名称」弹窗（保存 SQL 脚本等）----------------

/**
 * 简单取名弹窗（复用 mgModal）：确认后以输入值为参回调。
 *
 * @param {string} title  标题
 * @param {string} hint   顶部说明（纯文本）
 * @param {string} initial 初值
 * @param {Function} onOk 回调，参数为去除首尾空白后的非空名称
 */
function promptNameModal(title, hint, initial, onOk) {
    var overlay = mgModal(title, '');
    overlay.querySelector('.mg-body').innerHTML =
        (hint ? '<div class="mg-note">' + esc(hint) + '</div>' : '')
        + '<div class="mg-form"><label class="mg-field"><span>名称</span>'
        + '<input type="text" id="mg-name-input" value="' + esc(initial || '') + '" placeholder="输入名称"></label></div>'
        + '<div class="mg-actions">'
        + '<button type="button" class="mg-btn" data-act="cancel">取消</button>'
        + '<button type="button" class="mg-btn primary" data-act="ok">确定</button>'
        + '</div>';

    var input = overlay.querySelector('#mg-name-input');
    input.focus();
    input.select();

    function ok() {
        var v = (input.value || '').trim();
        if (!v) { input.focus(); return; }
        mgClose();
        onOk(v);
    }
    overlay.querySelector('[data-act="cancel"]').onclick = mgClose;
    overlay.querySelector('[data-act="ok"]').onclick = ok;
    input.addEventListener('keydown', function (e) {
        if (e.key === 'Enter') { e.preventDefault(); ok(); }
    });
}

// ---------------- 页面设置（主题 + 任务列表每页条数）----------------

function openPageSettings() {
    var dark = currentTheme() === 'dark';
    var overlay = mgModal('页面设置',
        '<div class="mg-form">'
        + '<div class="mg-field"><span>主题</span>'
        + '<div class="mg-seg" id="mg-theme-seg">'
        + '<button type="button" class="mg-seg-btn' + (dark ? '' : ' active') + '" data-theme-val="light">浅色</button>'
        + '<button type="button" class="mg-seg-btn' + (dark ? ' active' : '') + '" data-theme-val="dark">深色</button>'
        + '</div></div>'
        + '<label class="mg-field"><span>任务列表每页条数</span>'
        + '<select id="mg-perpage">'
        + [10, 20, 50, 100].map(function (n) {
            return '<option value="' + n + '"' + (n === jobPerPage ? ' selected' : '') + '>' + n + ' 条</option>';
        }).join('')
        + '</select></label>'
        + '</div>');

    overlay.querySelector('#mg-theme-seg').addEventListener('click', function (e) {
        var b = e.target.closest ? e.target.closest('.mg-seg-btn') : null;
        if (!b) return;
        var want = b.getAttribute('data-theme-val') === 'dark';
        if (want !== (currentTheme() === 'dark')) toggleTheme();
        var btns = overlay.querySelectorAll('.mg-seg-btn');
        for (var i = 0; i < btns.length; i++) btns[i].classList.toggle('active', btns[i] === b);
    });
    overlay.querySelector('#mg-perpage').addEventListener('change', function (e) {
        setJobPerPage(e.target.value);
    });
}

// ---------------- SQL 参数填写（submitQuery 检测到 ${name} 占位时）----------------

/**
 * 弹窗收集 SQL 里 ${name} 占位的取值，确认后回调替换结果。
 *
 * @param {string[]} names 去重后的参数名（按首次出现顺序）
 * @param {Function} onRun 回调，参数为 {name: value} —— 由调用方自行替换并执行
 */
function openSqlParamsDialog(names, onRun) {
    var overlay = mgModal('填写参数', '');
    var body = overlay.querySelector('.mg-body');

    body.innerHTML =
        '<div class="mg-note">这条 SQL 含参数占位（形如 <code>${name}</code>），'
        + '填写后点「运行」把它们替换成实际值再执行。</div>'
        + '<div class="mg-form" id="mg-param-form">'
        + (names || []).map(function (n) {
            return '<label class="mg-field"><span class="mg-param-key">${' + esc(n) + '}</span>'
                + '<input type="text" class="mg-param-input" data-param="' + esc(n) + '" '
                + 'placeholder="参数 ' + esc(n) + ' 的值"></label>';
        }).join('')
        + '</div>'
        + '<div class="mg-actions">'
        + '<button type="button" class="mg-btn" data-act="cancel">取消</button>'
        + '<button type="button" class="mg-btn primary" data-act="run">运行</button>'
        + '</div>';

    var inputs = overlay.querySelectorAll('.mg-param-input');
    if (inputs.length) inputs[0].focus();

    function collect() {
        var vals = {};
        for (var i = 0; i < inputs.length; i++) {
            vals[inputs[i].getAttribute('data-param')] = inputs[i].value;
        }
        return vals;
    }
    function run() {
        var vals = collect();
        mgClose();
        onRun(vals);
    }

    body.querySelector('[data-act="cancel"]').onclick = mgClose;
    body.querySelector('[data-act="run"]').onclick = run;
    // 参数框里回车 = 运行
    body.querySelector('#mg-param-form').addEventListener('keydown', function (e) {
        if (e.key === 'Enter') { e.preventDefault(); run(); }
    });
}
