package com.dojao.core.planner;

import com.dojao.spi.function.AggregateFunctionRegistry;
import com.dojao.spi.function.CollectAggregateRegistry;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * SQL 文本工具 —— 供物理 Planner 使用的纯文本解析助手：
 * 顶层切分（AND / 等号）、列引用去限定符、聚合/窗口函数文本解析、排序项解析。
 *
 * <p>注意：解析基于原始 SQL 文本（逻辑计划中大多为 {@code OpaqueExpr}），
 * 因此要求 SQL 写法规整（无嵌套字符串等极端情况）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/2
 */
public final class SqlTextUtil {

    private SqlTextUtil() {}

    /** 聚合函数解析结果。 */
    public record AggInfo(String function, String arg, boolean star) {}

    /** ORDER BY 项解析结果。 */
    public record OrderKey(String column, boolean ascending) {}

    // ==================== 顶层切分 ====================

    /** 在顶层（括号深度 0、字符串外）按关键字（如 AND / OR）切分。 */
    public static List<String> splitTopLevelKeyword(String text, String keyword) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int last = 0;
        char quote = 0;
        // BETWEEN lo AND hi 的 "AND" 是区间分隔符而非合取：刚扫到 BETWEEN 后遇到的下一个
        // 顶层 AND 必须跳过（否则 {@code a between 3 and 8} 被错切成 [a between 3, 8]，
        // 下推过滤变成 "between 3" 的非法 SQL，残留 "8" 也被误当表达式）。
        boolean inBetween = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') { quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && i + keyword.length() <= text.length()
                    && text.regionMatches(true, i, keyword, 0, keyword.length())) {
                int end = i + keyword.length();
                boolean leftOk = i == 0 || !isIdentChar(text.charAt(i - 1));
                boolean rightOk = end >= text.length() || !isIdentChar(text.charAt(end));
                if (leftOk && rightOk) {
                    if (keyword.equalsIgnoreCase("AND") && inBetween) {
                        // BETWEEN lo AND hi 的分隔符：跳过、不切分（upper bound 扫描开始）
                        inBetween = false;
                        i = end - 1;
                        continue;
                    }
                    String part = text.substring(last, i).trim();
                    if (!part.isEmpty()) parts.add(part);
                    last = end;
                    i = end - 1;
                }
            } else if (depth == 0 && i + "BETWEEN".length() <= text.length()
                    && text.regionMatches(true, i, "BETWEEN", 0, "BETWEEN".length())) {
                int end = i + "BETWEEN".length();
                boolean leftOk = i == 0 || !isIdentChar(text.charAt(i - 1));
                boolean rightOk = end >= text.length() || !isIdentChar(text.charAt(end));
                if (leftOk && rightOk) {
                    inBetween = true;
                    i = end - 1;
                }
            }
        }
        String remainder = text.substring(last).trim();
        if (!remainder.isEmpty()) parts.add(remainder);
        return parts;
    }

    /**
     * 拆出谓词的顶层 <b>AND 合取项</b>（每项再剥掉整体包裹的圆括号），供引擎侧 Filter 逐项求值。
     *
     * <p><b>为什么必须逐项。</b>{@code FilterOperator} 只编译「单个简单谓词」（列比较 /
     * IS [NOT] NULL / 标量函数谓词 / LIKE）。把整条合取串直接交给它，形如
     * {@code a = 1 AND b > 2} 会落进 {@code Form.UNKNOWN} —— 它对未知形态<b>保守放行全部行</b>
     * （放行 ≠ 报错，是静默多返回行）；{@code (a = 1) AND (b > 2)} 更糟：会拿 {@code "a"} 去比
     * 字符串 {@code "1) AND (b > 2"}，静默 0 行。拆开后逐项求值（全部通过才保留），
     * 未识别的单项才退回放行 —— 影响面从「整条谓词失效」缩到「单项失效」。</p>
     *
     * <p>切分本身复用 {@link #splitTopLevelKeyword}（括号深度 / 引号 / {@code BETWEEN lo AND hi}
     * 的分隔符 AND 都已处理）。</p>
     *
     * <p><b>★必须先剥「整体包裹的括号」再切</b>：派生表展平（{@code Planner.tryFlattenSimpleDerived}）
     * 产出的过滤是 {@code (inner.where)} 这种<b>整串被一对括号包住</b>的形态，此时 {@code AND}
     * 落在括号深度 1 ⇒ 顶层切分一无所获 ⇒ 整条合取被当作「单个谓词」交给 {@code FilterOperator}。
     * 实测事故：{@code (dt ='2026-08-25' and spu_id is not null)} 落进 {@code IS NOT NULL} 分支，
     * 尾部后缀匹配把左侧整段当成<b>列名</b> {@code "dt ='2026-08-25' and spu_id"} ⇒
     * ① 该伪列被并进扫描列集 ⇒ Hive 报「表中不存在列」；
     * ② 若没走投影，则 Filter 取不到向量恒假 ⇒ 静默 0 行。
     * 故这里循环：剥括号 → 顶层切 → 每段再剥括号并回炉（处理 {@code (a and b) and (c and d)}）。</p>
     *
     * @param expr 谓词表达式（可含多层括号包裹，如 {@code (p1) AND (p2)}）；null / 空 → 空表
     */
    public static List<String> splitConjuncts(String expr) {
        if (expr == null || expr.isBlank()) {
            return List.of();
        }
        List<String> out = new ArrayList<>(2);
        java.util.ArrayDeque<String> pending = new java.util.ArrayDeque<>();
        pending.add(expr);
        int guard = 0;
        while (!pending.isEmpty() && guard++ < 64) {
            String t = stripOuterParens(pending.poll());
            if (t.isEmpty()) {
                continue;
            }
            List<String> parts = splitTopLevelKeyword(t, "AND");
            if (parts.size() <= 1) {
                out.add(t);          // 剥完括号后再无顶层 AND → 叶子（单个简单谓词）
                continue;
            }
            for (String part : parts) {
                String p = stripOuterParens(part);
                if (!p.isEmpty()) {
                    pending.add(p);  // 回炉：可能还含被括号包住的合取
                }
            }
        }
        return out;
    }

    /** 谓词按「谁能求值」拆分的结果：{@code pushdown}（数据源认识的合取，可 null）+ {@code residual}（引擎注册函数谓词，可 null）。 */
    public record PredicateSplit(List<String> pushdownLeaves, List<String> residualLeaves) {
        /** 下推部分拼回单串（无 → null）。 */
        public String pushdown() {
            return pushdownLeaves.isEmpty() ? null : String.join(" AND ", pushdownLeaves);
        }

        /** 残留（引擎侧）部分拼回单串（无 → null）。 */
        public String residual() {
            return residualLeaves.isEmpty() ? null : String.join(" AND ", residualLeaves);
        }
    }

    /**
     * 把一条 {@code WHERE} 谓词按「数据源能否求值」拆成<b>下推部分</b>与<b>引擎残留部分</b> ——
     * 含<b>引擎注册标量函数</b>的合取项（如 {@code is_high(rating) = 1}）数据源不认识，
     * 只能由引擎自己的 {@code FilterOperator} 在数据所在地求值。
     *
     * <p><b>★Master 与 Worker 必须共用这一份实现</b>：规划期用它决定「要不要回退单机 / 别名该并进扫描列」，
     * 扫描节点用它决定「请求里的哪几项要在本节点补 {@code Filter}、哪几项留给连接器」。两边口径一旦
     * 分叉，就会出现「规划期以为数据源会过滤、实际没人过滤」的静默多返回行。</p>
     *
     * <p>切分口径 = {@link #splitConjuncts}（先剥外层括号再切顶层 AND，见该方法的括号陷阱说明）；
     * 叶子含<b>顶层 OR</b> 时拼回必须补回括号，否则 {@code (a=1 or b=2) and c=3} 拆后再 AND 拼接会
     * 丢掉括号、改变优先级。</p>
     */
    public static PredicateSplit splitByConnectorCapability(String cond) {
        List<String> pushable = new ArrayList<>(4);
        List<String> residual = new ArrayList<>(2);
        for (String part : splitConjuncts(cond)) {
            if (part.isEmpty()) {
                continue;
            }
            String leaf = splitTopLevelKeyword(part, "OR").size() > 1 ? "(" + part + ")" : part;
            if (com.dojao.core.function.scalar.ScalarExpr.isFunctionPredicate(part)) {
                residual.add(leaf);
            } else {
                pushable.add(leaf);
            }
        }
        return new PredicateSplit(List.copyOf(pushable), List.copyOf(residual));
    }

    /** 反复剥掉<b>整体包裹</b>的圆括号：{@code ((a = 1))} → {@code a = 1}；{@code (a) AND (b)} 不动。 */
    public static String stripOuterParens(String s) {
        if (s == null) {
            return "";
        }
        String t = s.trim();
        while (isWrapped(t)) {
            t = t.substring(1, t.length() - 1).trim();
        }
        return t;
    }

    /**
     * 首字符 {@code (} 与末字符 {@code )} 是否<b>整体配对</b> —— 只在开头那个括号恰好闭合于末尾时为 true。
     * 没有这层判定，{@code (a) AND (b)} 会被错剥成 {@code a) AND (b}。
     */
    private static boolean isWrapped(String t) {
        if (t.length() < 2 || t.charAt(0) != '(' || t.charAt(t.length() - 1) != ')') {
            return false;
        }
        int depth = 0;
        char quote = 0;
        for (int i = 0; i < t.length(); i++) {
            char c = t.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') { quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0 && i != t.length() - 1) {
                    return false;   // 开头括号中途就闭合了：不是整体包裹
                }
            }
        }
        return depth == 0;
    }

    /** 在顶层按纯等号 {@code =} 切分（忽略 {@code <= >= != <> ==}）。 */
    public static List<String> splitTopLevelEquals(String text) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int last = 0;
        char quote = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') { quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == '=' && depth == 0) {
                char prev = i > 0 ? text.charAt(i - 1) : 0;
                char next = i + 1 < text.length() ? text.charAt(i + 1) : 0;
                if (prev != '!' && prev != '<' && prev != '>' && prev != '=' && next != '=') {
                    parts.add(text.substring(last, i).trim());
                    last = i + 1;
                }
            }
        }
        parts.add(text.substring(last).trim());
        return parts;
    }

    // ==================== 限定符处理 ====================

    /** 列引用去限定符：{@code a.id} → {@code id}；同时去掉反引号。 */
    public static String stripQualifier(String ref) {
        String t = ref.trim().replace("`", "");
        int dot = t.lastIndexOf('.');
        if (dot >= 0) return t.substring(dot + 1).trim();
        return t;
    }

    /** 表达式内所有 {@code alias.col} 限定符去除（跳过字符串字面量）。 */
    public static String stripQualifiers(String expr) {
        String t = expr.replace("`", "");
        StringBuilder sb = new StringBuilder(t.length());
        int i = 0;
        char quote = 0;
        while (i < t.length()) {
            char c = t.charAt(i);
            if (quote != 0) {
                sb.append(c);
                if (c == quote) quote = 0;
                i++;
                continue;
            }
            if (c == '\'' || c == '"') { quote = c; sb.append(c); i++; continue; }
            if (isIdentStart(c)) {
                int j = i;
                while (j < t.length() && isIdentChar(t.charAt(j))) j++;
                int k = j;
                while (k < t.length() && Character.isWhitespace(t.charAt(k))) k++;
                if (k < t.length() && t.charAt(k) == '.') {
                    int m = k + 1;
                    while (m < t.length() && Character.isWhitespace(t.charAt(m))) m++;
                    if (m < t.length() && isIdentStart(t.charAt(m))) {
                        int n = m;
                        while (n < t.length() && isIdentChar(t.charAt(n))) n++;
                        sb.append(t, m, n); // 只保留列名
                        i = n;
                        continue;
                    }
                }
                sb.append(t, i, j);
                i = j;
                continue;
            }
            sb.append(c);
            i++;
        }
        return sb.toString();
    }

    /** 归一化：小写 + 去空白，用于文本匹配（如 HAVING 聚合替换）。 */
    public static String normalize(String s) {
        return s.toLowerCase(Locale.ROOT).replaceAll("\\s+", "");
    }

    // ==================== 聚合函数解析 ====================

    /**
     * 解析单个聚合函数调用文本，如 {@code COUNT(*)}、{@code SUM(quantity)}。
     * 支持内置 COUNT / SUM / AVG / MIN / MAX，以及已注册进
     * {@link AggregateFunctionRegistry} 的自定义聚合函数（CREATE FUNCTION 加载后）；
     * 其余返回空。
     */
    public static Optional<AggInfo> parseAggregate(String exprText) {
        String t = exprText.trim();
        int paren = t.indexOf('(');
        if (paren <= 0) return Optional.empty();
        String func = t.substring(0, paren).trim().toUpperCase(Locale.ROOT);
        if (!AGG_FUNCS.contains(func)
                && !AggregateFunctionRegistry.hasGlobal(func)
                && !CollectAggregateRegistry.hasGlobal(func)) return Optional.empty();
        int close = findMatchingParen(t, paren);
        if (close < 0) return Optional.empty();
        // 整个表达式必须是函数调用（不允许 count(*) + 1 之类）
        if (!t.substring(close + 1).trim().isEmpty()) return Optional.empty();
        String arg = t.substring(paren + 1, close).trim();
        boolean star = arg.isEmpty() || arg.equals("*");
        return Optional.of(new AggInfo(func, arg, star));
    }

    /**
     * 可解析为聚合调用的函数名。
     *
     * <p>{@code COUNT_IF / SUM_IF / MAX_IF / MIN_IF} 是条件聚合：规划期改写为
     * {@code CASE WHEN cond THEN value ELSE NULL END} 上的普通聚合（{@code count_if(cond)} →
     * {@code COUNT(CASE WHEN cond THEN 1 END)}），复用既有聚合 / 溢写 / 分布式链路。</p>
     */
    private static final Set<String> AGG_FUNCS = Set.of(
            "COUNT", "SUM", "AVG", "MIN", "MAX",
            "COUNT_IF", "SUM_IF", "MAX_IF", "MIN_IF");

    /**
     * 扫描表达式文本里出现的<b>所有</b>聚合调用（含「整串就是一个聚合调用」），返回原文片段。
     *
     * <p>{@link #parseAggregate} 只认整串；本方法用于「聚合套在表达式里」的形态：
     * {@code round(sum(f.amt), 2)} → {@code [sum(f.amt)]}、
     * {@code sum(a) * 100.0 / sum(b)} → {@code [sum(a), sum(b)]}。
     * 遇到非聚合的函数调用（{@code round(...)}）会<b>继续向内扫</b>其参数，故能挖出内层聚合；
     * 命中聚合调用后整体跳过（聚合套聚合是非法 SQL，不在支持范围）。</p>
     *
     * <p>片段按 {@link #normalize} 去重（保序），保证同一调用在多个 SELECT 项 / 多个位置
     * 只产出一次，调用方（Planner）据此为每个唯一调用建一个聚合规格。</p>
     */
    public static List<String> findAggregateCalls(String expr) {
        Set<String> seen = new LinkedHashSet<>();
        int i = 0;
        while (i < expr.length()) {
            char c = expr.charAt(i);
            if (isIdentStart(c)) {
                int j = i;
                while (j < expr.length() && isIdentChar(expr.charAt(j))) j++;
                int k = j;
                while (k < expr.length() && Character.isWhitespace(expr.charAt(k))) k++;
                if (k < expr.length() && expr.charAt(k) == '(') {
                    int close = findMatchingParen(expr, k);
                    if (close >= 0) {
                        // `sum(x) OVER (...)` 是窗口调用，不是聚合：内层 sum 不能当聚合处理，
                        // 否则整条 SELECT 会被误判为「含聚合」而多挂一个 HashAggregation。
                        if (followedByOver(expr, close + 1)) {
                            i = close + 1;
                            continue;
                        }
                        String candidate = expr.substring(i, close + 1);
                        if (parseAggregate(candidate).isPresent() && seen.add(normalize(candidate))) {
                            i = close + 1;
                            continue;
                        }
                    }
                }
                i = j;
                continue;
            }
            i++;
        }
        return new ArrayList<>(seen);
    }

    /** 在表达式中把所有 normalize 后命中 map 的聚合调用替换为 map 值（HAVING 用）。 */
    public static String replaceAggregates(String expr, Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < expr.length()) {
            char c = expr.charAt(i);
            if (isIdentStart(c)) {
                int j = i;
                while (j < expr.length() && isIdentChar(expr.charAt(j))) j++;
                int k = j;
                while (k < expr.length() && Character.isWhitespace(expr.charAt(k))) k++;
                if (k < expr.length() && expr.charAt(k) == '(') {
                    int close = findMatchingParen(expr, k);
                    if (close >= 0) {
                        // 窗口调用里的 `sum(x)` 不是聚合（见 findAggregateCalls），原样保留。
                        if (followedByOver(expr, close + 1)) {
                            sb.append(expr, i, close + 1);
                            i = close + 1;
                            continue;
                        }
                        String candidate = expr.substring(i, close + 1);
                        String out = map.get(normalize(candidate));
                        if (out != null) {
                            sb.append(out);
                            i = close + 1;
                            continue;
                        }
                    }
                }
                sb.append(expr, i, j);
                i = j;
                continue;
            }
            sb.append(c);
            i++;
        }
        return sb.toString();
    }

    // ==================== 窗口调用定位（表达式里的 OVER） ====================

    /** 表达式中的一段原文区间 {@code [start, end)}。 */
    public record Span(int start, int end) {}

    /**
     * 找出表达式里出现的<b>所有</b>窗口调用（{@code fn(...) OVER (...)} / {@code fn(...) OVER w}），
     * 返回覆盖「函数调用 + OVER 子句」的原文区间（按出现顺序，左闭右开）。
     *
     * <p>供 Planner 把<b>嵌在表达式里</b>的窗口（{@code gmv / sum(gmv) OVER ()}）物化成合成列后
     * 回写引用：只替换窗口片段本身，外层的 {@code + - * /} 与其它窗口保持原样。嵌套在普通函数
     * 参数里的窗口（{@code round(sum(x) OVER (), 2)}）同样能被找到。</p>
     */
    public static List<Span> findWindowCallSpans(String expr) {
        List<Span> spans = new ArrayList<>();
        int i = 0;
        while (i < expr.length()) {
            char c = expr.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {   // 跳过引号区域（字面量里的 OVER 不是窗口）
                int j = i + 1;
                while (j < expr.length()) {
                    if (expr.charAt(j) == c) {
                        if (j + 1 < expr.length() && expr.charAt(j + 1) == c) { j += 2; continue; }
                        break;
                    }
                    j++;
                }
                i = Math.min(expr.length(), j + 1);
                continue;
            }
            if (isIdentStart(c)) {
                int j = i;
                while (j < expr.length() && isIdentChar(expr.charAt(j))) j++;
                int k = j;
                while (k < expr.length() && Character.isWhitespace(expr.charAt(k))) k++;
                if (k < expr.length() && expr.charAt(k) == '(') {
                    int close = findMatchingParen(expr, k);
                    if (close >= 0) {
                        int end = overClauseEnd(expr, close + 1);
                        if (end > 0) {
                            spans.add(new Span(i, end));
                            i = end;
                            continue;
                        }
                    }
                }
                i = j;
                continue;
            }
            i++;
        }
        return spans;
    }

    /** {@code from} 起（跳过空白）是否紧跟 {@code OVER} 关键字（区分大小写不敏感、要求词边界）。 */
    private static boolean followedByOver(String text, int from) {
        int k = from;
        while (k < text.length() && Character.isWhitespace(text.charAt(k))) k++;
        if (k + 4 > text.length() || !text.regionMatches(true, k, "OVER", 0, 4)) return false;
        int after = k + 4;
        return after >= text.length() || !isIdentChar(text.charAt(after));
    }

    /**
     * {@code from} 起若是 OVER 子句则返回其结束下标（{@code OVER (...)} 到右括号之后、
     * {@code OVER windowName} 到窗口名之后），否则返回 -1。
     */
    private static int overClauseEnd(String text, int from) {
        if (!followedByOver(text, from)) return -1;
        int k = from;
        while (k < text.length() && Character.isWhitespace(text.charAt(k))) k++;
        int p = k + 4;
        while (p < text.length() && Character.isWhitespace(text.charAt(p))) p++;
        if (p < text.length() && text.charAt(p) == '(') {
            int close = findMatchingParen(text, p);
            return close < 0 ? -1 : close + 1;
        }
        if (p < text.length() && isIdentStart(text.charAt(p))) {
            int q = p;
            while (q < text.length() && isIdentChar(text.charAt(q))) q++;
            return q;
        }
        return -1;
    }

    /** 找到从 openParen 起始的匹配右括号下标；未找到返回 -1。 */
    public static int findMatchingParen(String text, int openParen) {
        int depth = 0;
        char quote = 0;
        for (int i = openParen; i < text.length(); i++) {
            char c = text.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"') { quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0) return i;
            }
        }
        return -1;
    }

    // ==================== ORDER BY 解析 ====================

    /** 解析排序项：{@code "t.action_time DESC"} → (action_time, false)；默认升序。 */
    public static OrderKey parseOrderKey(String orderByText) {
        String t = orderByText.trim();
        boolean ascending = true;
        String upper = t.toUpperCase(Locale.ROOT);
        if (upper.endsWith(" DESC")) {
            ascending = false;
            t = t.substring(0, t.length() - 5).trim();
        } else if (upper.endsWith(" ASC")) {
            t = t.substring(0, t.length() - 4).trim();
        }
        return new OrderKey(stripQualifier(t), ascending);
    }

    // ==================== 简单谓词判定（FilterOperator 可求值） ====================

    /**
     * 判定谓词是否可由 {@link FilterOperator} 求值：
     * {@code col op value} 或 {@code col IS [NOT] NULL}，且 {@code col} 为裸列名。
     */
    /** 匹配"裸列名 + 紧凑比较运算符"（如 rr<=10、flag=1、a.b>=5）。 */
    private static final java.util.regex.Pattern COMPACT_COMPARISON =
            java.util.regex.Pattern.compile(
                    "^\\s*([A-Za-z_][A-Za-z0-9_.]*)\\s*([=<>!]{1,2})\\s*(\\S.*)$");

    /**
     * 将紧凑比较过滤（如 {@code rr<=10}、{@code flag=1}）规范化为带空格形式
     * （{@code rr <= 10}），使后续按空白切分的解析（isSimpleFilter / FilterOperator）
     * 能正确拆出列、运算符、值。
     * <p>含引号字面量的过滤保持原样，避免破坏 {@code name = 'a=b'} 这类字符串值；
     * {@code IS [NOT] NULL} 不含比较运算符，也不受影响。
     */
    public static String normalizeFilter(String expr) {
        String t = expr.trim();
        if (t.isEmpty()) return t;
        if (t.indexOf('\'') >= 0 || t.indexOf('"') >= 0) {
            return t; // 含字符串字面量，保持原样
        }
        java.util.regex.Matcher m = COMPACT_COMPARISON.matcher(t);
        if (m.matches()) {
            return m.group(1) + " " + m.group(2) + " " + m.group(3);
        }
        return t;
    }

    public static boolean isSimpleFilter(String expr) {
        String t = normalizeFilter(expr);
        String upper = t.toUpperCase(Locale.ROOT);
        if (upper.endsWith(" IS NOT NULL") || upper.endsWith(" IS NULL")) {
            int suffix = upper.endsWith(" IS NOT NULL") ? " IS NOT NULL".length() : " IS NULL".length();
            return isSingleIdentifier(t.substring(0, t.length() - suffix).trim());
        }
        String[] parts = t.split("\\s+", 3);
        if (parts.length != 3) return false;
        if (!isSingleIdentifier(parts[0])) return false;
        return switch (parts[1]) {
            case "=", "==", "!=", "<>", ">", ">=", "<", "<=" -> true;
            default -> false;
        };
    }

    /** 是否为单个裸标识符（列名）。 */
    public static boolean isSingleIdentifier(String s) {
        if (s.isEmpty()) return false;
        if (!isIdentStart(s.charAt(0))) return false;
        for (int i = 1; i < s.length(); i++) {
            if (!isIdentChar(s.charAt(i))) return false;
        }
        return true;
    }

    /**
     * 是否为单个<b>列引用</b>：{@code ident} 或 {@code alias.ident}（至多一段限定符）。用于区分
     * 裸列与表达式 —— {@link #stripQualifier} 只取最后一个 {@code '.'} 之后的部分，直接拿它判
     * 「是否裸列」会把 {@code a.b + c.d} 误判为列 {@code d}。
     */
    public static boolean isColumnRef(String s) {
        String t = s.trim();
        int dot = t.indexOf('.');
        if (dot < 0) return isSingleIdentifier(t);
        return isSingleIdentifier(t.substring(0, dot)) && isSingleIdentifier(t.substring(dot + 1));
    }

    // ==================== LIKE 谓词解析 ====================

    /** {@code col [NOT] LIKE '字面量模式'} 的解析结果。 */
    public record LikeSpec(String column, java.util.regex.Pattern pattern, boolean negated) {

        /**
         * 逐行求值。NULL 与任何模式匹配结果都是未知 → 恒 false（{@code NOT LIKE} 亦然）——
         * 即 SQL 三值逻辑下「只留 TRUE」的同一口径（见 {@code FilterOperator} 的 COLUMN_LIKE 分支）。
         */
        public boolean matches(Object value) {
            if (value == null) return false;
            return negated != pattern.matcher(value.toString()).matches();
        }
    }

    /**
     * 解析 {@code col [NOT] LIKE '字面量模式'} —— 引擎侧<b>唯一</b>的 LIKE 形态：左侧须裸列
     * （可带限定符）、右侧须引号字面量。形状不符（{@code lower(x) LIKE 'a%'}、模式非字面量、
     * 顶层含 {@code AND}/{@code OR}）返回 {@code null}，由调用方回退其它形态或报不支持。
     *
     * <p><b>必须由三处共用本方法</b>（{@code FilterOperator}、{@code CaseProjectOperator}、
     * Planner 的 CASE 条件校验与列收集）：形状判定只要有一处不一致，就会出现「校验通过但求值落空」
     * 或「条件列被裁剪掉」的静默错误。</p>
     */
    public static LikeSpec parseLike(String expr) {
        if (expr == null) return null;
        String t = stripOuterParens(expr);
        if (t.isEmpty()) return null;
        List<String> parts = splitTopLevelKeyword(t, "LIKE");
        if (parts.size() != 2) return null;
        String left = parts.get(0).trim();
        String right = parts.get(1).trim();
        boolean not = false;
        if (left.toUpperCase(Locale.ROOT).endsWith("NOT")) {
            String head = left.substring(0, left.length() - "NOT".length()).trim();
            if (isSingleIdentifier(stripQualifier(head))) {
                not = true;
                left = head;
            }
        }
        String column = stripQualifier(left);
        if (!isSingleIdentifier(column) || !isQuotedLiteral(right)) return null;
        return new LikeSpec(column, compileLikePattern(unquoteLiteral(right)), not);
    }

    // ==================== IN 谓词解析 ====================

    /** {@code col [NOT] IN (字面量, ...)} 的解析结果。 */
    public record InSpec(String column, List<String> values, boolean negated) {

        /**
         * 逐行求值。SQL 三值逻辑：被比较值为 NULL 时 {@code NULL IN (...)} 结果未知 → 恒 false
         * （{@code NOT IN} 亦然，与 Hive / PostgreSQL 口径一致，WHERE 只保留 TRUE）。
         */
        public boolean matches(Object value) {
            if (value == null) return false;
            return negated != values.contains(value.toString());
        }
    }

    /**
     * 解析 {@code col [NOT] IN (v1, v2, ...)} —— 引擎侧<b>唯一</b>的 IN 形态：左侧须裸列
     * （可带限定符）、右侧须括号包裹的<b>字面量</b>列表（引号字符串 / 数字 / {@code true|false}）。
     *
     * <p><b>为什么必须有</b>：数据源不兑现谓词下推（如 Hive 直读 ORC）时，谓词整串落在扫描节点的
     * {@code FilterOperator} 求值；缺这一形态就退化成 {@code Form.UNKNOWN} → <b>静默放行全部行</b>。
     * 实测 CTAS：{@code event_type in ('purchase')} 被静默丢弃，写出的表含全部事件类型。</p>
     *
     * <p>形状不符（右侧非括号列表 / 列表项非字面量即列引用或子查询 / 顶层含 {@code AND}|{@code OR}）
     * 返回 {@code null}，由调用方回退其它形态（最终落 UNKNOWN 保守放行），绝不臆造列名。</p>
     */
    public static InSpec parseInList(String expr) {
        if (expr == null) return null;
        String t = stripOuterParens(expr);
        if (t.isEmpty()) return null;
        List<String> parts = splitTopLevelKeyword(t, "IN");
        if (parts.size() != 2) return null;
        String left = parts.get(0).trim();
        String right = parts.get(1).trim();
        boolean not = false;
        if (left.toUpperCase(Locale.ROOT).endsWith("NOT")) {
            String head = left.substring(0, left.length() - "NOT".length()).trim();
            if (isSingleIdentifier(stripQualifier(head))) {
                not = true;
                left = head;
            }
        }
        String column = stripQualifier(left);
        if (!isSingleIdentifier(column)) return null;
        // 右侧必须是 (v1, v2, ...)：首尾各一个括号（括号内的逗号由顶层切分处理）
        if (right.length() < 2 || right.charAt(0) != '(' || right.charAt(right.length() - 1) != ')') {
            return null;
        }
        String inner = right.substring(1, right.length() - 1).trim();
        if (inner.isEmpty()) return null;
        List<String> values = new ArrayList<>(4);
        for (String item : splitTopLevel(inner, ',')) {
            String v = item.trim();
            if (!isCompleteLiteral(v)) return null;   // 非字面量（列引用 / 子查询）→ 本形态不认
            values.add(unquoteLiteral(v));
        }
        if (values.isEmpty()) return null;
        return new InSpec(column, List.copyOf(values), not);
    }

    /**
     * 顶层（括号深度 0、引号外）按单个<b>分隔符</b>切分。
     *
     * <p>IN 列表的逗号不能用 {@link #splitTopLevelKeyword}：后者按「标识符边界」判定关键字，
     * 数字字面量前一位是数字（{@code isIdentChar} 为真）⇒ {@code 1, 2} 不切（整串当成一个词）。
     * 这里按字符切，引号内的分隔符与嵌套括号内的分隔符不动。</p>
     */
    private static List<String> splitTopLevel(String text, char sep) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        char quote = 0;
        int last = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') { quote = c; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && c == sep) {
                parts.add(text.substring(last, i).trim());
                last = i + 1;
            }
        }
        parts.add(text.substring(last).trim());
        return parts;
    }

    /** 整串是否<b>就是</b>一个字面量（引号串 / 数字 / {@code true|false|null}）—— 不含任何表达式成分。 */
    public static boolean isCompleteLiteral(String s) {
        String t = s == null ? "" : s.trim();
        if (t.isEmpty()) return false;
        if (isQuotedLiteral(t)) return true;
        if (t.matches("[+-]?\\d+(\\.\\d+)?([eE][+-]?\\d+)?")) return true;
        return t.equalsIgnoreCase("true") || t.equalsIgnoreCase("false") || t.equalsIgnoreCase("null");
    }

    /** 是否为引号包裹的字面量（{@code 'x'} / {@code "x"} / {@code `x`}）—— LIKE 右侧必须长这样。 */
    public static boolean isQuotedLiteral(String s) {
        String t = s.trim();
        if (t.length() < 2) return false;
        char c0 = t.charAt(0), c1 = t.charAt(t.length() - 1);
        return (c0 == '\'' || c0 == '"' || c0 == '`') && c1 == c0;
    }

    /** 去掉一层配对的<b>单/双</b>引号（{@code '%a%'} → {@code %a%}）；其余情况原样返回。 */
    public static String unquoteLiteral(String s) {
        String t = s.trim();
        if (t.length() >= 2) {
            char first = t.charAt(0), last = t.charAt(t.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return t.substring(1, t.length() - 1);
            }
        }
        return t;
    }

    /**
     * SQL LIKE 模式 → 正则。{@code %} → {@code .*}（任意长度含空）、{@code _} → {@code .}（任意单字符），
     * 其余字符全部按字面量转义（正则元字符不生效）。整体锚定、大小写敏感（对齐 Hive / PostgreSQL；
     * MySQL 大小写不敏感，但它自己下推执行，不走这里）。
     */
    public static java.util.regex.Pattern compileLikePattern(String sqlPattern) {
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < sqlPattern.length(); i++) {
            char c = sqlPattern.charAt(i);
            if (c == '%') {
                regex.append(".*");
            } else if (c == '_') {
                regex.append('.');
            } else {
                regex.append(java.util.regex.Pattern.quote(String.valueOf(c)));
            }
        }
        return java.util.regex.Pattern.compile(regex.append('$').toString(),
                java.util.regex.Pattern.DOTALL);
    }

    private static boolean isIdentStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private static boolean isIdentChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }
}
