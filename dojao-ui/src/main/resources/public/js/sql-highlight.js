// ==================== SQL 语法高亮（textarea + 垫层） ====================
//
// 做法：编辑器本体仍是原生 <textarea> —— 光标、选区、输入法组字、浏览器原生滚动全部保持原生行为；
// 高亮靠它**背后**垫一层字体度量完全相同的 <pre>：textarea 的文字设为透明、只留光标色（caret-color），
// 于是眼睛看到的颜色全部来自垫层，而交互仍由 textarea 负责（垫层 pointer-events:none）。
//
// 为什么不上 contenteditable / 第三方编辑器（CodeMirror 之类）：
//   · contenteditable 会破坏原生撤销栈与输入法组字，还要自己实现光标/选区/粘贴；
//   · 引第三方编辑器 = 加依赖 + 重写「草稿记忆 / 自建撤销栈 / 拖拽落点」这套已跑通的接线；
//   · 本仓前端只有一个 vendored vis-network（DAG 图专用），编辑器这点高亮不值得再加一个。
//
// 染色范围只做三件事（用户口径）：**注释灰、字符串灰、关键字蓝**。字符串里的关键字、注释里的
// 关键字都不能被点亮 —— 所以必须按 token 切分，而不是对整段文本做全局替换。

/**
 * 高亮的关键字表（比较时统一转大写，故这里只写大写）。
 *
 * <p>取自 dojao-grammar 的 <code>SqlBase.g4</code> 里日常会写到的关键字，
 * 不求穷尽（多词关键字如 <code>GROUP BY</code> 会被逐个词点亮，视觉上即一整段）。</p>
 */
var SQL_KEYWORDS = {
    SELECT: 1, FROM: 1, WHERE: 1, GROUP: 1, BY: 1, ORDER: 1, HAVING: 1, LIMIT: 1, OFFSET: 1,
    JOIN: 1, INNER: 1, OUTER: 1, LEFT: 1, RIGHT: 1, FULL: 1, CROSS: 1, NATURAL: 1, ON: 1, USING: 1,
    AS: 1, AND: 1, OR: 1, NOT: 1, NULL: 1, IS: 1, IN: 1, BETWEEN: 1, LIKE: 1, EXISTS: 1,
    DISTINCT: 1, ALL: 1, UNION: 1, EXCEPT: 1, INTERSECT: 1, ANY: 1, SOME: 1,
    CASE: 1, WHEN: 1, THEN: 1, ELSE: 1, END: 1, IF: 1,
    ASC: 1, DESC: 1, NULLS: 1, FIRST: 1, LAST: 1,
    WITH: 1, OVER: 1, PARTITION: 1, ROWS: 1, RANGE: 1, PRECEDING: 1, FOLLOWING: 1,
    UNBOUNDED: 1, CURRENT: 1, ROW: 1, WINDOW: 1,
    ROLLUP: 1, CUBE: 1, GROUPING: 1, SETS: 1, CAST: 1, TYPE: 1,
    CREATE: 1, TABLE: 1, VIEW: 1, CATALOG: 1, SCHEMA: 1, DATABASE: 1, FUNCTION: 1, INDEX: 1,
    DROP: 1, ALTER: 1, ADD: 1, COLUMN: 1, RENAME: 1, COMMENT: 1,
    INSERT: 1, INTO: 1, VALUES: 1, UPDATE: 1, SET: 1, DELETE: 1, OVERWRITE: 1, REPLACE: 1,
    SHOW: 1, DESCRIBE: 1, DESC: 1, EXPLAIN: 1, USE: 1, CATALOGS: 1, SCHEMAS: 1, TABLES: 1, COLUMNS: 1,
    PRIMARY: 1, KEY: 1, UNIQUE: 1, DEFAULT: 1, EXTERNAL: 1, LOCATION: 1, STORED: 1, FORMAT: 1,
    TRUE: 1, FALSE: 1, INTERVAL: 1, ESCAPE: 1
};

/**
 * 单趟切 token 的正则（带 g 标志，用 exec 循环）。
 *
 * <p>顺序即优先级：注释 → 字符串 → 标识符 → 数字 → 空白 → 其他单字符。
 * 注释/字符串整段吞掉，故其中的关键字不会被单独点亮。</p>
 *
 * <ul>
 *   <li><code>--[^\n]*</code>：行注释（<code>--</code> 到行尾）</li>
 *   <li>块注释：以斜杠星号开始、可跨行，到「星号斜杠」为止（写法见下面正则本身）</li>
 *   <li>单引号 / 双引号字符串：允许反斜杠转义与 SQL 的 <code>''</code> 双写转义</li>
 *   <li>反引号标识符引用（<code>`库`.`表`</code>）：不是字符串，整段不染色</li>
 * </ul>
 */
var SQL_TOKEN_RE = /--[^\n]*|\/\*[\s\S]*?\*\/|'(?:[^'\\]|\\.|'')*'|"(?:[^"\\]|\\.|"")*"|`[^`]*`|[A-Za-z_][A-Za-z0-9_$]*|\d+(?:\.\d+)?|\s+|[\s\S]/g;

/** 单个 token 的 CSS 类名；返回空串表示「不染色」。 */
function sqlTokenClass(tok) {
    var c0 = tok.charAt(0);
    if (c0 === '-' && tok.charAt(1) === '-') return 'sql-tok-comment';
    if (c0 === '/' && tok.charAt(1) === '*') return 'sql-tok-comment';
    if (c0 === "'" || c0 === '"') return 'sql-tok-string';    // 单/双引号字符串整体染灰（含引号本身）
    if (c0 === '`') return '';                               // 反引号是标识符引用（库/表/列名），不是字符串
    if (/[A-Za-z_]/.test(c0) && SQL_KEYWORDS.hasOwnProperty(tok.toUpperCase())) {
        return 'sql-tok-keyword';
    }
    return '';
}

/**
 * 把编辑器内容重画一遍。
 *
 * <p>用 <code>createTextNode</code> / <code>textContent</code> 逐个拼节点，<b>不拼 innerHTML</b> ——
 * 编辑器里是用户输入的任意文本，拼 HTML 就是一个自伤的 XSS 口子。</p>
 *
 * <p>每次输入都整体重画：token 化几 KB 文本是微秒级，比做增量更新简单得多，也不会出现
 * 「删了两个字符颜色还在」这类状态不一致。</p>
 */
function renderSqlHighlight() {
    var host = document.getElementById('sql-highlight');
    var code = document.getElementById('sql-highlight-code');
    var input = document.getElementById('query-input');
    if (!host || !code || !input) return;

    var text = input.value || '';
    var frag = document.createDocumentFragment();
    var plain = '';                                    // 连续「不染色」片段攒成一个文本节点
    SQL_TOKEN_RE.lastIndex = 0;
    var m;
    while ((m = SQL_TOKEN_RE.exec(text)) !== null) {
        var tok = m[0];
        var cls = sqlTokenClass(tok);
        if (!cls) {
            plain += tok;
            continue;
        }
        if (plain) { frag.appendChild(document.createTextNode(plain)); plain = ''; }
        var span = document.createElement('span');
        span.className = cls;
        span.textContent = tok;
        frag.appendChild(span);
    }
    if (plain) frag.appendChild(document.createTextNode(plain));
    // 文本以换行结尾时 <pre> 不会把最后一个空行算进高度，补一个空格占位，保证行数与 textarea 一致
    if (text.charAt(text.length - 1) === '\n') frag.appendChild(document.createTextNode(' '));

    code.textContent = '';
    code.appendChild(frag);
    syncSqlHighlight(input, host);
}

/**
 * 对齐垫层：滚动位移 + 右内边距。
 *
 * <p>为什么内边距要动态算：textarea 出现滚动条时，正文宽度会比垫层少一个滚动条的宽度，
 * 长行的折行位置就此错开、越往下错得越多。这里量出实际差值补到垫层的 <code>padding-right</code> 上，
 * 两边正文宽度就一致（overlay 滚动条、无滚动条时为 0，等于不补）。</p>
 *
 * <p>位移用 <code>transform</code> 加在内层 <code>code</code> 上，而<b>不是</b>给垫层设
 * <code>scrollTop</code>：后者要求「内容确实溢出了盒」才有效，一旦盒高写成 auto（曾经的 bug）
 * 就静默失效——垫层不滚、textarea 滚，于是选中的阴影在文字上滑动、点击落点也整体错位。
 * transform 没有这个前提，垫层是 <code>overflow:hidden</code> 的定高盒时一定生效。</p>
 */
function syncSqlHighlight(input, host) {
    var padRight = parseFloat(window.getComputedStyle(input).paddingRight) || 0;
    var sbw = input.offsetWidth - input.clientWidth;   // border 为 0 → 差值就是滚动条宽度
    host.style.paddingRight = (padRight + Math.max(0, sbw)) + 'px';
    var code = document.getElementById('sql-highlight-code');
    if (code) {
        // 只可能为 0（pre-wrap 下没有横向滚动），一起带上以防将来关掉折行
        code.style.transform =
            'translate(' + (-input.scrollLeft) + 'px,' + (-input.scrollTop) + 'px)';
    }
}

/** 绑定编辑器高亮（首页初始化时调一次）。 */
function bindSqlHighlight() {
    var input = document.getElementById('query-input');
    var host = document.getElementById('sql-highlight');
    var body = document.querySelector('.sql-editor-body');
    if (!input || !host || !body) return;

    input.addEventListener('input', renderSqlHighlight);
    // 滚动只要跟住位移，不必重画 DOM（滚动是高频事件，重建整棵节点纯属浪费）
    input.addEventListener('scroll', function () { syncSqlHighlight(input, host); });

    // 输入法组字期间浏览器还没把候选串写进 value，垫层也就画不出来 —— 而 textarea 的文字是透明的，
    // 组字中的字会「看不见」。故组字期间把自带颜色还给 textarea、先把垫层藏起来，结束再换回来。
    input.addEventListener('compositionstart', function () { body.classList.add('sql-composing'); });
    input.addEventListener('compositionend', function () {
        body.classList.remove('sql-composing');
        renderSqlHighlight();
    });

    // 盒子一变（拖编辑器的高度把手、窗口缩放、滚动条出现/消失）折行位置与滚动条宽度就变了，
    // 必须重算 —— 垫层靠「和 textarea 的排版一模一样」立身，任何时候对不上都是落点错位。
    if (window.ResizeObserver) {
        new ResizeObserver(function () { renderSqlHighlight(); }).observe(input);
    }
    window.addEventListener('resize', renderSqlHighlight);   // 无 ResizeObserver 时的兜底
    renderSqlHighlight();
}
