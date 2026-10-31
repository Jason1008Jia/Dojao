// ==================== 控制台登录态 ====================
// 会话 cookie 由服务端下发（HttpOnly，JS 读不到也不该读）；这里只负责：
//   1. 拉当前用户 /api/auth/me（401 由 utils.js 的 httpJson 统一跳登录页）；
//   2. 顶栏渲染用户名 + 角色 + 登出；
//   3. 登录页表单提交。

var Auth = (function () {

    var current = null;

    /** 当前登录用户（未登录为 null）。 */
    function user() { return current; }

    /** 是否管理员（批 2 的横向菜单据此过滤模块）。 */
    function isAdmin() { return !!current && current.role === 'ADMIN'; }

    /** 是否开发者（数据能力 + 集群测试 / 压测 / 日志分析 / 查询诊断）。 */
    function isDeveloper() { return !!current && current.role === 'DEVELOPER'; }

    /** 是否分析师（仅数据探查 / 报表 / 分析 / 挖掘）。 */
    function isAnalyst() { return !!current && (current.role === 'ANALYST' || current.role === 'USER'); }

    /** 角色中文名。 */
    function roleLabel(role) {
        switch (role) {
            case 'ADMIN': return '管理员';
            case 'DEVELOPER': return '开发者';
            case 'ANALYST':
            case 'USER': return '分析师';
            default: return role || '未知';
        }
    }

    function load() {
        return httpGet('/api/auth/me').then(function (me) {
            current = me;
            return me;
        });
    }

    /** 顶栏：用户名 + 角色 + 登出按钮。 */
    function bindHeader() {
        return load().then(function (me) {
            var nameEl = document.getElementById('header-user');
            if (nameEl) {
                nameEl.textContent = (me.displayName || me.username)
                    + ' · ' + roleLabel(me.role);
            }
            var btn = document.getElementById('logout-btn');
            if (btn) btn.addEventListener('click', logout);
        }).catch(function () { /* 401 已整页跳转，这里无需处理 */ });
    }

    function logout() {
        httpPost('/api/auth/logout')
            .catch(function () { /* 会话可能已失效，照样回登录页 */ })
            .then(function () {
                window.__dojaoAllowUnload = true;   // 主动登出：不要触发「请勿刷新」拦截
                window.location.href = '/login.html';
            });
    }

    /** 登录页初始化（login.html 专用）。 */
    function initLoginPage() {
        var form = document.getElementById('login-form');
        if (!form) return;
        var errEl = document.getElementById('login-error');
        var btn = document.getElementById('login-btn');

        form.addEventListener('submit', function (ev) {
            ev.preventDefault();
            var username = document.getElementById('login-username').value.trim();
            var password = document.getElementById('login-password').value;
            if (!username || !password) {
                errEl.textContent = '请输入用户名和口令。';
                return;
            }
            errEl.textContent = '';
            btn.disabled = true;
            httpPost('/api/auth/login', { username: username, password: password })
                .then(function () { window.location.href = '/index.html'; })
                .catch(function (err) {
                    errEl.textContent = (err && err.message) ? err.message : '登录失败，请重试。';
                })
                .then(function () { btn.disabled = false; });
        });
    }

    return {
        user: user,
        isAdmin: isAdmin,
        isDeveloper: isDeveloper,
        isAnalyst: isAnalyst,
        roleLabel: roleLabel,
        load: load,
        bindHeader: bindHeader,
        logout: logout,
        initLoginPage: initLoginPage
    };
})();
