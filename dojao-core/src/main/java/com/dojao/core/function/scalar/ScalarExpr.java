package com.dojao.core.function.scalar;

import com.dojao.core.planner.SqlTextUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * 标量函数表达式 —— 函数名 + 参数列表。
 *
 * <p>由 SQL 文本（逻辑计划中的 {@code OpaqueExpr}）解析而来。参数分两类：</p>
 * <ul>
 *   <li>列引用：{@code embedding} / {@code a.embedding} → 求值时从输入 batch 取该列行值</li>
 *   <li>字面量：{@code '[...]'} 字符串、数值、布尔、NULL → 求值时直接用</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/8/9
 */
public final class ScalarExpr {

    /** 参数：列引用 / 字面量 / 比较条件（{@code col op literal}）。 */
    public sealed interface ScalarArg {
        /** 列引用：按列名从输入 batch 取值 */
        record ColumnArg(String column) implements ScalarArg {}
        /** 字面量：已解析的值（NULL 为 null） */
        record LiteralArg(Object value) implements ScalarArg {}
        /** 比较条件：{@code col op literal}，逐行求值成 Boolean（供 IF 等条件函数） */
        record ComparisonArg(String left, String op, String right) implements ScalarArg {}
    }

    private final String function;
    private final List<ScalarArg> args;

    private ScalarExpr(String function, List<ScalarArg> args) {
        this.function = function;
        this.args = args;
    }

    /** 函数名（小写）。 */
    public String function() { return function; }

    public List<ScalarArg> args() { return args; }

    /**
     * 尝试把整段文本解析为一个已注册的标量函数调用。
     *
     * <p>要求：整段文本是单个函数调用（不允许 {@code func(...)+1} 之类），
     * 函数已注册，且所有参数可识别为列引用或字面量；否则返回空。</p>
     */
    public static Optional<ScalarExpr> tryParse(String text) {
        String t = text.trim();
        int paren = t.indexOf('(');
        if (paren <= 0) return Optional.empty();
        String fn = t.substring(0, paren).trim();
        if (!SqlTextUtil.isSingleIdentifier(fn)) return Optional.empty();
        int close = SqlTextUtil.findMatchingParen(t, paren);
        if (close < 0) return Optional.empty();
        if (!t.substring(close + 1).trim().isEmpty()) return Optional.empty();
        if (!ScalarFunctionRegistry.isRegistered(fn)) return Optional.empty();

        List<ScalarArg> args = new ArrayList<>();
        String argsText = t.substring(paren + 1, close).trim();
        if (!argsText.isEmpty()) {
            for (String part : splitTopLevelComma(argsText)) {
                ScalarArg arg = parseArg(part);
                if (arg == null) return Optional.empty();
                args.add(arg);
            }
        }
        return Optional.of(new ScalarExpr(fn.toLowerCase(), args));
    }

    private static ScalarArg parseArg(String argText) {
        String t = argText.trim();
        if (t.isEmpty()) return null;
        if (t.equalsIgnoreCase("NULL")) return new ScalarArg.LiteralArg(null);
        if (t.equalsIgnoreCase("TRUE")) return new ScalarArg.LiteralArg(Boolean.TRUE);
        if (t.equalsIgnoreCase("FALSE")) return new ScalarArg.LiteralArg(Boolean.FALSE);

        // 引号字面量：去引号（长度 < 2 的不可能是引号串，直接落到数值/列引用分支）
        if (t.length() >= 2) {
            char first = t.charAt(0), last = t.charAt(t.length() - 1);
            if (first == '\'' && last == '\'') return new ScalarArg.LiteralArg(t.substring(1, t.length() - 1));
            if (first == '"' && last == '"') return new ScalarArg.LiteralArg(t.substring(1, t.length() - 1));
        }
        try {
            return new ScalarArg.LiteralArg(Double.parseDouble(t));
        } catch (NumberFormatException ignored) {
            // 不是数值字面量 → 继续尝试比较条件 / 列引用
        }
        // 比较条件 col op literal（如 rating >= 5）：逐行求值成 Boolean，供 IF 等条件函数
        Optional<Comparison> cmp = splitComparison(t);
        if (cmp.isPresent()) {
            Comparison c = cmp.get();
            return new ScalarArg.ComparisonArg(c.left(), c.op(), c.right());
        }
        String col = SqlTextUtil.stripQualifier(t);
        if (SqlTextUtil.isSingleIdentifier(col)) return new ScalarArg.ColumnArg(col);
        return null;
    }

    /** 顶层逗号切分（括号深度 0、引号外）。包外供 {@link com.dojao.core.planner.Planner} 切分 IF 三参。 */
    public static List<String> splitTopLevelComma(String text) {
        List<String> parts = new ArrayList<>();
        int depth = 0, last = 0;
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
            else if (c == ',' && depth == 0) {
                parts.add(text.substring(last, i).trim());
                last = i + 1;
            }
        }
        String rem = text.substring(last).trim();
        if (!rem.isEmpty()) parts.add(rem);
        return parts;
    }

    // ==================== 比较谓词 ====================

    /** 比较谓词切分结果："左 操作符 右"。 */
    public record Comparison(String left, String op, String right) {}

    /**
     * 在顶层（括号深度 0、引号外）查找比较操作符并切分比较谓词。
     *
     * <p>优先匹配双字符操作符（&gt;= &lt;= != &lt;&gt;），再匹配单字符（= &gt; &lt;）。
     * 例：{@code cosine_similarity(a, '[0.5,...]') >= 0.8} → left=cosine_similarity(...), op=&gt;=, right=0.8。
     * 找不到顶层操作符（如 {@code IS [NOT] NULL}、纯函数调用）返回空。</p>
     */
    public static Optional<Comparison> splitComparison(String text) {
        String t = text.trim();
        int depth = 0;
        char quote = 0;
        for (int i = 0; i < t.length(); i++) {
            char c = t.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') { quote = c; continue; }
            if (c == '(') { depth++; continue; }
            if (c == ')') { depth--; continue; }
            if (depth > 0) continue;

            char next = i + 1 < t.length() ? t.charAt(i + 1) : 0;
            String op = null;
            int len = 1;
            if (c == '>' && next == '=') { op = ">="; len = 2; }
            else if (c == '<' && next == '=') { op = "<="; len = 2; }
            else if (c == '!' && next == '=') { op = "!="; len = 2; }
            else if (c == '<' && next == '>') { op = "<>"; len = 2; }
            else if (c == '=') { op = "="; len = 1; }
            else if (c == '>') { op = ">"; len = 1; }
            else if (c == '<') { op = "<"; len = 1; }
            if (op == null) continue;

            String left = t.substring(0, i).trim();
            String right = t.substring(i + len).trim();
            if (left.isEmpty() || right.isEmpty()) return Optional.empty();
            return Optional.of(new Comparison(left, op, right));
        }
        return Optional.empty();
    }

    /**
     * 是否为标量函数谓词："func(args) op 字面量"、"字面量 op func(args)" 或
     * "func(args) IS [NOT] NULL"，且函数为已注册标量函数。
     *
     * <p>要求另一侧必须是简单字面量（数值/引号字符串/布尔/NULL），从而排除
     * {@code func(a) > 1 AND func(b) < 2} 这类组合谓词（AND/OR 不在本次支持范围）。</p>
     */
    public static boolean isFunctionPredicate(String text) {
        String t = text.trim();
        String upper = t.toUpperCase(Locale.ROOT);
        if (upper.endsWith(" IS NOT NULL")) {
            return tryParse(t.substring(0, t.length() - " IS NOT NULL".length()).trim()).isPresent();
        }
        if (upper.endsWith(" IS NULL")) {
            return tryParse(t.substring(0, t.length() - " IS NULL".length()).trim()).isPresent();
        }
        Optional<Comparison> cmp = splitComparison(t);
        if (cmp.isEmpty()) return false;
        Comparison c = cmp.get();
        if (tryParse(c.left()).isPresent()) return isLiteral(c.right());
        if (tryParse(c.right()).isPresent()) return isLiteral(c.left());
        return false;
    }

    // ==================== 比较求值 ====================

    /** 去字面量引号（供 FilterOperator / CaseProjectOperator / ScalarEvaluator 复用）。 */
    public static String unquote(String s) {
        if (s.length() >= 2) {
            char first = s.charAt(0), last = s.charAt(s.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return s.substring(1, s.length() - 1);
            }
        }
        return s;
    }

    /**
     * 数值优先 / 字符串兜底的比较（供 FilterOperator / CaseProjectOperator / ScalarEvaluator 复用）。
     *
     * <p><b>★ 先判形态再解析</b>：{@code Long.parseLong} / {@code Double.parseDouble} 在<b>非数字
     * 文本</b>上要抛 {@link NumberFormatException} 并<b>填充栈</b>，单次约 1µs。原实现对每个文本行
     * 连探 4 次（两侧各 long / double 一遍），文本列的逐行比较因此变成 <b>3.5µs/行</b> ——
     * {@code SUM(IF(os = 'Android 14', 1, 0))} 这类「文本列 op 文本字面量」的 CASE 条件在
     * 3M 行上光比较就烧 <b>约 10s</b>（离线探针 {@code AggConsumerThroughputProbeTest} ⑤ 实测
     * 3.46µs/次，占整条消费链净成本的 94%）。故这里先用纯字符扫描判定「可能是数字」，
     * 文本行直接落到字符串比较，一次异常都不抛。</p>
     */
    public static boolean compare(String left, String op, String right) {
        if (isNumericAlphabet(left) && isNumericAlphabet(right)) {
            // 整数形态走 long（避免 double 精度损失）；位数超 18 有溢出风险 → 直落 double 路径
            if (isIntegralForm(left) && isIntegralForm(right)) {
                return compareNumeric(Long.parseLong(left.trim()), Long.parseLong(right.trim()), op);
            }
            Long lLong = parseLong(left), rLong = parseLong(right);
            if (lLong != null && rLong != null) {
                return compareNumeric(lLong, rLong, op);
            }
            Double lDouble = parseDouble(left), rDouble = parseDouble(right);
            if (lDouble != null && rDouble != null) {
                return compareNumeric(lDouble, rDouble, op);
            }
        }
        // 非数值（字符串/文本列）：退化为字符串比较
        int c = left.compareTo(right);
        return switch (op) {
            case "=", "==" -> c == 0;
            case "!=", "<>" -> c != 0;
            case ">" -> c > 0;
            case ">=" -> c >= 0;
            case "<" -> c < 0;
            case "<=" -> c <= 0;
            default -> true;
        };
    }

    /** 两侧均为整数时按 long 比较（避免 double 的精度损失）。 */
    private static boolean compareNumeric(long l, long r, String op) {
        return switch (op) {
            case "=", "==" -> l == r;
            case "!=", "<>" -> l != r;
            case ">" -> l > r;
            case ">=" -> l >= r;
            case "<" -> l < r;
            case "<=" -> l <= r;
            default -> true;
        };
    }

    /** 小数 / 科学计数法按 double 比较。 */
    private static boolean compareNumeric(double l, double r, String op) {
        return switch (op) {
            case "=", "==" -> l == r;
            case "!=", "<>" -> l != r;
            case ">" -> l > r;
            case ">=" -> l >= r;
            case "<" -> l < r;
            case "<=" -> l <= r;
            default -> true;
        };
    }

    /**
     * 廉价的「数值形态」预筛：只含 {@code 0-9 + - . e E} 与空白（{@code trim()} 口径），且<b>至少
     * 含一个数字</b>。文本（如 {@code "Android 14"}）在这里就被挡掉，绝不进 {@code parseXxx}。
     *
     * <p>刻意不认 {@code NaN} / {@code Infinity} / 十六进制浮点 / {@code 1d} 这类
     * {@code Double.parseDouble} 的 Java 特有写法 —— 它们只会出现在文本列里，按字符串比较才是
     * SQL 语义（旧实现会把 {@code 'NaN' = 'NaN'} 算成 false）。</p>
     */
    private static boolean isNumericAlphabet(String s) {
        boolean digit = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                digit = true;
            } else if (c != '+' && c != '-' && c != '.' && c != 'e' && c != 'E' && c > ' ') {
                return false;
            }
        }
        return digit;
    }

    /**
     * 整数形态（可带正负号与首尾空白，数字位数 ≤ 18 以保证 {@code Long.parseLong} 不溢出）。
     * 命中即可安全解析，无需 try/catch。
     */
    private static boolean isIntegralForm(String s) {
        int i = 0, end = s.length();
        while (i < end && s.charAt(i) <= ' ') i++;
        while (end > i && s.charAt(end - 1) <= ' ') end--;
        if (i < end && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
        int digits = end - i;
        if (digits == 0 || digits > 18) return false;
        for (; i < end; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') return false;
        }
        return true;
    }

    private static Long parseLong(String s) {
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double parseDouble(String s) {
        try {
            return Double.parseDouble(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 参数是否全为 {@link ScalarEvaluator} 能逐行直接求值的简单形态：列引用 / 字面量 /
     * {@code 列 op 字面量} 比较。
     *
     * <p>不满足者（如 {@code IF(rating-1 > 3, a, b)} 的比较左侧是表达式、{@code a > b} 的比较
     * 右侧是列）须回退通用表达式子系统（ExprParser/ExprEvaluator），否则比较参数会被当作
     * 裸列名解析 → 常量 null / 静默算错。</p>
     */
    public static boolean isSimpleArgs(ScalarExpr e) {
        for (ScalarArg a : e.args()) {
            if (a instanceof ScalarArg.ColumnArg || a instanceof ScalarArg.LiteralArg) continue;
            if (a instanceof ScalarArg.ComparisonArg c) {
                if (SqlTextUtil.isColumnRef(c.left()) && isLiteral(c.right())) continue;
            }
            return false;
        }
        return true;
    }

    /**
     * 是否为简单字面量：数值 / 引号字符串 / 布尔 / NULL。
     *
     * <p>供条件快路径的形状判定复用（如 {@code CaseProjectOperator} 只在
     * {@code 列 op 字面量} 时才走字符串比较快路径，否则回退通用表达式求值——
     * 否则 {@code a = b} 会被误当作「a 与字符串 'b' 比较」而静默算错）。</p>
     */
    public static boolean isLiteral(String s) {
        String t = s.trim();
        if (t.isEmpty()) return false;
        if (t.equalsIgnoreCase("NULL")) return true;
        if (t.equalsIgnoreCase("TRUE") || t.equalsIgnoreCase("FALSE")) return true;
        if (t.length() >= 2) {
            char f = t.charAt(0), l = t.charAt(t.length() - 1);
            if ((f == '\'' && l == '\'') || (f == '"' && l == '"')) return true;
        }
        try {
            Double.parseDouble(t);
            return true;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    /** 从函数谓词文本中收集标量函数引用的参数列（供列裁剪）。 */
    public static List<String> predicateColumnArgs(String text) {
        List<String> cols = new ArrayList<>();
        String t = text.trim();
        String upper = t.toUpperCase(Locale.ROOT);
        if (upper.endsWith(" IS NOT NULL")) {
            tryParse(t.substring(0, t.length() - " IS NOT NULL".length()).trim())
                    .ifPresent(e -> collectColumnArgs(e, cols));
        } else if (upper.endsWith(" IS NULL")) {
            tryParse(t.substring(0, t.length() - " IS NULL".length()).trim())
                    .ifPresent(e -> collectColumnArgs(e, cols));
        } else {
            splitComparison(t).ifPresent(c -> {
                tryParse(c.left()).ifPresent(e -> collectColumnArgs(e, cols));
                tryParse(c.right()).ifPresent(e -> collectColumnArgs(e, cols));
            });
        }
        return cols;
    }

    private static void collectColumnArgs(ScalarExpr e, List<String> cols) {
        for (ScalarArg a : e.args()) {
            if (a instanceof ScalarArg.ColumnArg c) cols.add(c.column());
            else if (a instanceof ScalarArg.ComparisonArg ca) {
                String col = SqlTextUtil.stripQualifier(ca.left());
                if (!cols.contains(col)) cols.add(col);
            }
        }
    }
}
