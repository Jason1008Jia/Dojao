package com.dojao.core.operator.expr;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code OpaqueExpr} 原始 SQL 文本 → {@link ExprNode} 的递归下降解析器。
 *
 * <p>手写小扫描器（不引第三方表达式库）：词法 = 数字 / 单引号字符串 / 反引号标识符 /
 * 标识符关键字 / 运算符 {@code + - * / % = != <> < <= > >= ( ) , .}；语法 = 完整布尔表达式
 * （{@code AND OR NOT}、比较、{@code IS [NOT] NULL}、{@code [NOT] IN}、{@code [NOT] BETWEEN}、
 * {@code [NOT] LIKE}）
 * + 算术表达式（{@code + - * / %}、一元负号）+ {@code CASE WHEN ... END} + {@code IF(a,b,c)}
 * （脱糖为单分支 CASE）+ 类型转换 {@code CAST(x AS type)} / {@code TRY_CAST(x AS type)}。</p>
 *
 * <p>运算符优先级（低→高）：OR → AND → NOT → 比较/IN/BETWEEN/LIKE/IS NULL → 加减 → 乘除模 →
 * 一元负号 → primary（字面量 / 列 / 函数调用 / 括号 / CASE）。</p>
 *
 * @author jiaxin
 * @date 2026/9/6
 */
public final class ExprParser {

    private final List<Tok> toks;
    private int p;

    private ExprParser(String text) {
        this.toks = tokenize(text);
        this.p = 0;
    }

    /** 解析一段标量表达式文本；失败抛 IllegalArgumentException（含原文定位）。 */
    public static ExprNode parse(String text) {
        if (text == null) throw new IllegalArgumentException("表达式为空");
        String src = text.trim();
        if (src.isEmpty()) throw new IllegalArgumentException("表达式为空");
        ExprParser parser = new ExprParser(src);
        ExprNode node = parser.parseOr();
        if (node == null) {
            throw new IllegalArgumentException("无法解析表达式: '" + src + "'");
        }
        if (parser.p != parser.toks.size()) {
            throw new IllegalArgumentException("表达式有多余内容: '" + src + "' @ "
                    + parser.toks.get(parser.p).text);
        }
        return node;
    }

    // ==================== 递归下降 ====================

    /** 顶层：OR（expr 的首操作数不允许为空）。 */
    private ExprNode parseOr() {
        ExprNode left = parseAnd();
        if (left == null) return null;
        while (isWord("OR")) {
            take();
            ExprNode right = parseAnd();
            if (right == null) throw err("OR 右侧缺表达式");
            left = new ExprNode.BoolOp(ExprNode.BoolOp.Op.OR, left, right);
        }
        return left;
    }

    private ExprNode parseAnd() {
        ExprNode left = parseNot();
        if (left == null) return null;
        while (isWord("AND")) {
            take();
            ExprNode right = parseNot();
            if (right == null) throw err("AND 右侧缺表达式");
            left = new ExprNode.BoolOp(ExprNode.BoolOp.Op.AND, left, right);
        }
        return left;
    }

    private ExprNode parseNot() {
        if (isWord("NOT")) {
            take();
            ExprNode inner = parseNot();
            if (inner == null) throw err("NOT 后缺表达式");
            return new ExprNode.Not(inner);
        }
        return parsePredicate();
    }

    private ExprNode parsePredicate() {
        ExprNode v = parseValue();
        if (v == null) return null;
        if (at(Kind.OP)) {
            String op = peek().text;
            ExprNode.Compare.Op cop = compareOp(op);
            if (cop != null) {
                take();
                ExprNode r = parseValue();
                if (r == null) throw err("比较 '" + op + "' 右侧缺表达式");
                return new ExprNode.Compare(cop, v, r);
            }
        }
        if (at(Kind.WORD)) {
            String w = peek().text.toUpperCase();
            switch (w) {
                case "IS" -> {
                    take();
                    boolean neg = false;
                    if (isWord("NOT")) { take(); neg = true; }
                    if (!isWord("NULL")) throw err("IS 需跟 NULL");
                    take();
                    return new ExprNode.IsNull(v, neg);
                }
                case "NOT" -> {
                    take();
                    if (isWord("IN")) {
                        take();
                        return new ExprNode.InList(v, parseParenList(), true);
                    }
                    if (isWord("BETWEEN")) {
                        take();
                        return between(v, true);
                    }
                    if (isWord("LIKE")) {
                        take();
                        return like(v, true);
                    }
                    throw err("NOT 后需跟 IN / BETWEEN 或 LIKE");
                }
                case "IN" -> {
                    take();
                    return new ExprNode.InList(v, parseParenList(), false);
                }
                case "BETWEEN" -> {
                    take();
                    return between(v, false);
                }
                case "LIKE" -> {
                    take();
                    return like(v, false);
                }
                default -> { /* 无后缀谓词 → 纯值 */ }
            }
        }
        return v;
    }

    /** {@code v [NOT] LIKE pattern}（已消费 LIKE）。模式侧允许任意值表达式（字面量 / 列 / 函数结果）。 */
    private ExprNode like(ExprNode v, boolean negated) {
        ExprNode pattern = parseValue();
        if (pattern == null) throw err("LIKE 右侧缺模式表达式");
        return new ExprNode.Like(v, pattern, negated);
    }

    private ExprNode between(ExprNode v, boolean negated) {
        ExprNode low = parseValue();
        if (low == null) throw err("BETWEEN 下限缺表达式");
        if (!isWord("AND")) throw err("BETWEEN 需 AND 连接上下限");
        take();
        ExprNode high = parseValue();
        if (high == null) throw err("BETWEEN 上限缺表达式");
        ExprNode both = new ExprNode.BoolOp(ExprNode.BoolOp.Op.AND,
                new ExprNode.Compare(ExprNode.Compare.Op.GE, v, low),
                new ExprNode.Compare(ExprNode.Compare.Op.LE, v, high));
        return negated ? new ExprNode.Not(both) : both;
    }

    private List<ExprNode> parseParenList() {
        List<ExprNode> list = new ArrayList<>();
        expect(Kind.LP, "'('");
        if (at(Kind.RP)) { take(); return list; }
        while (true) {
            ExprNode item = parseOr();
            if (item == null) throw err("IN 列表项缺表达式");
            list.add(item);
            if (at(Kind.COMMA)) { take(); continue; }
            break;
        }
        expect(Kind.RP, "')'");
        return list;
    }

    private ExprNode parseValue() {
        ExprNode left = parseMul();
        if (left == null) return null;
        while (at(Kind.OP)) {
            String op = peek().text;
            if ("+".equals(op) || "-".equals(op)) {
                take();
                ExprNode right = parseMul();
                if (right == null) throw err("算术 '" + op + "' 右侧缺表达式");
                left = new ExprNode.Arith("+".equals(op)
                        ? ExprNode.Arith.Op.ADD : ExprNode.Arith.Op.SUB, left, right);
            } else {
                break;
            }
        }
        return left;
    }

    private ExprNode parseMul() {
        ExprNode left = parseUnary();
        if (left == null) return null;
        while (at(Kind.OP)) {
            String op = peek().text;
            ExprNode.Arith.Op aop = switch (op) {
                case "*" -> ExprNode.Arith.Op.MUL;
                case "/" -> ExprNode.Arith.Op.DIV;
                case "%" -> ExprNode.Arith.Op.MOD;
                default -> null;
            };
            if (aop == null) break;
            take();
            ExprNode right = parseUnary();
            if (right == null) throw err("算术 '" + op + "' 右侧缺表达式");
            left = new ExprNode.Arith(aop, left, right);
        }
        return left;
    }

    private ExprNode parseUnary() {
        if (at(Kind.OP)) {
            String op = peek().text;
            if ("-".equals(op)) {
                take();
                ExprNode inner = parseUnary();
                if (inner == null) throw err("负号后缺表达式");
                return new ExprNode.Negate(inner);
            }
            if ("+".equals(op)) {
                take();
                ExprNode inner = parseUnary();
                if (inner == null) throw err("正号后缺表达式");
                return inner;
            }
        }
        return parsePrimary();
    }

    private ExprNode parsePrimary() {
        if (p >= toks.size()) return null;
        Tok t = peek();
        switch (t.kind) {
            case NUM -> {
                take();
                return new ExprNode.Literal(number(t.text));
            }
            case STR -> {
                take();
                return new ExprNode.Literal(t.text);
            }
            case LP -> {
                take();
                ExprNode inner = parseOr();
                if (inner == null) throw err("括号内缺表达式");
                expect(Kind.RP, "')'");
                return inner;
            }
            case RP, COMMA -> { return null; }   // 结构终止符：值段结束（THEN/ELSE 分支解析依赖）
            case DOT -> throw err("意外的 '.'");
            case OP -> throw err("意外的运算符 '" + t.text + "'");
            case WORD -> { return parseWord(t); }
        }
        return null;
    }

    private ExprNode parseWord(Tok t) {
        String up = t.text.toUpperCase();
        // 结构关键字：作为值段的终止符（THEN / ELSE / END / AND / OR 等）
        switch (up) {
            case "WHEN", "THEN", "ELSE", "END", "AND", "OR", "IS", "IN", "BETWEEN", "LIKE", "NOT" -> {
                return null;
            }
            case "NULL" -> { take(); return new ExprNode.Literal(null); }
            case "TRUE" -> { take(); return new ExprNode.Literal(Boolean.TRUE); }
            case "FALSE" -> { take(); return new ExprNode.Literal(Boolean.FALSE); }
            case "CASE" -> { take(); return parseCase(); }
            case "CAST", "TRY_CAST" -> {
                if (peek(1) != null && peek(1).kind == Kind.LP) {
                    boolean tryCast = "TRY_CAST".equals(up);
                    take();
                    return parseCast(tryCast);
                }
                take();   // 裸标识符（列名可叫 cast/try_cast）→ 按列引用
                return new ExprNode.Column(t.text);
            }
            case "IF" -> {
                if (peek(1) != null && peek(1).kind == Kind.LP) {
                    take();
                    return parseIf();
                }
                take();
                return new ExprNode.Column(t.text);
            }
            default -> { return parseIdentOrFunc(t); }
        }
    }

    private ExprNode parseIdentOrFunc(Tok t) {
        // 函数调用：IDENT '(' args ')'
        Tok next = peek(1);
        if (next != null && next.kind == Kind.LP) {
            take();   // 函数名
            expect(Kind.LP, "'('");
            List<ExprNode> args = new ArrayList<>();
            if (!at(Kind.RP)) {
                while (true) {
                    ExprNode arg = parseOr();
                    if (arg == null) throw err("函数参数缺表达式");
                    args.add(arg);
                    if (at(Kind.COMMA)) { take(); continue; }
                    break;
                }
            }
            expect(Kind.RP, "')'");
            return new ExprNode.FuncCall(t.text.toLowerCase(), args);
        }
        // 列引用：可能带限定符 a.b（取最末段为列名，物理层按裸列名解析）
        String name = t.text;
        take();
        while (at(Kind.DOT)) {
            Tok after = peek(1);
            if (after == null || after.kind != Kind.WORD) {
                throw err("限定符 '.' 后需列名");
            }
            take();   // '.'
            take();   // 列名
            name = after.text;
        }
        return new ExprNode.Column(name);
    }

    private ExprNode parseCase() {
        // 已消费 CASE；仅支持 searched CASE（WHEN 开头）；简单 CASE（CASE expr WHEN v）暂不支持
        List<ExprNode.WhenThen> branches = new ArrayList<>();
        while (isWord("WHEN")) {
            take();
            ExprNode cond = parseOr();
            if (cond == null) throw err("WHEN 条件缺表达式");
            if (!isWord("THEN")) throw err("CASE 缺 THEN");
            take();
            ExprNode val = parseValue();
            if (val == null) throw err("THEN 值缺表达式");
            branches.add(new ExprNode.WhenThen(cond, val));
        }
        if (branches.isEmpty()) {
            throw err("CASE 需 WHEN（简单 CASE 形式暂不支持，请用 searched CASE）");
        }
        ExprNode elseVal;
        if (isWord("ELSE")) {
            take();
            elseVal = parseValue();
            if (elseVal == null) throw err("ELSE 值缺表达式");
        } else {
            elseVal = new ExprNode.Literal(null);
        }
        if (!isWord("END")) throw err("CASE 缺 END");
        take();
        return new ExprNode.Case(branches, elseVal);
    }

    private ExprNode parseIf() {
        // 已消费 IF；IF(cond, a, b) → Case([WHEN cond THEN a], ELSE b)
        expect(Kind.LP, "'('");
        ExprNode cond = parseOr();
        if (cond == null) throw err("IF 条件缺表达式");
        expect(Kind.COMMA, "','");
        ExprNode a = parseOr();
        if (a == null) throw err("IF 第二参缺表达式");
        expect(Kind.COMMA, "','");
        ExprNode b = parseOr();
        if (b == null) throw err("IF 第三参缺表达式");
        expect(Kind.RP, "')'");
        List<ExprNode.WhenThen> branches = List.of(new ExprNode.WhenThen(cond, a));
        return new ExprNode.Case(branches, b);
    }

    /** {@code CAST(expr AS type)} / {@code TRY_CAST(expr AS type)}（已消费 CAST / TRY_CAST）。 */
    private ExprNode parseCast(boolean tryCast) {
        expect(Kind.LP, "'('");
        ExprNode inner = parseOr();
        if (inner == null) throw err("CAST 缺表达式");
        if (!isWord("AS")) throw err("CAST 需 AS type");
        take();
        CastType type = parseCastType();
        expect(Kind.RP, "')'");
        return new ExprNode.Cast(inner, type.canonical(), type.precision(), type.scale(), tryCast);
    }

    /** CAST 目标类型：规范名 + {@code DECIMAL(p,s)} 的精度/小数位（未书写为 -1）。 */
    private record CastType(String canonical, int precision, int scale) {}

    /**
     * 解析 CAST 的目标类型名：取首词为「基础类型」（{@link ExprEvaluator#normalizeCastType} 归一化并校验，
     * 未知类型直接报错 ⇒ 计划期即报「SELECT 表达式」不支持，而非运行期炸），随后解析类型参数
     * （{@code DECIMAL(p,s)} 的精度/小数位）并跳过其它修饰词（{@code (len)} / {@code PRECISION} /
     * {@code WITH TIME ZONE}）直到 CAST 的右括号。
     */
    private CastType parseCastType() {
        if (!at(Kind.WORD)) throw err("CAST 缺目标类型");
        String base = take().text;
        String canonical = ExprEvaluator.normalizeCastType(base);
        if (canonical == null) throw err("CAST 不支持的目标类型: " + base);
        int precision = -1, scale = -1;
        int depth = 0;
        int argIndex = 0;
        while (p < toks.size()) {
            Tok t = peek();
            if (t.kind == Kind.LP) { depth++; take(); continue; }
            if (t.kind == Kind.RP) {
                if (depth == 0) break;   // CAST 的右括号，留给 parseCast 消费
                depth--; take(); continue;
            }
            // 顶层括号内的前两个数字 = DECIMAL(p,s) 的精度 / 小数位
            if (depth == 1 && t.kind == Kind.NUM && argIndex < 2) {
                try {
                    int v = Integer.parseInt(t.text);
                    if (argIndex == 0) precision = v; else scale = v;
                } catch (NumberFormatException ignored) {
                    // 非整数参数（如 VARCHAR(10.5)）不当作 p/s
                }
                argIndex++;
            }
            take();   // 类型参数内的逗号 / 数字，或 PRECISION / TIME / ZONE 等修饰词
        }
        if (!"decimal".equals(canonical)) {
            precision = -1;
            scale = -1;
        }
        return new CastType(canonical, precision, scale);
    }

    // ==================== 词法 ====================

    private enum Kind { NUM, STR, WORD, LP, RP, COMMA, DOT, OP }

    private record Tok(Kind kind, String text) {}

    private static List<Tok> tokenize(String s) {
        List<Tok> out = new ArrayList<>();
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) { i++; continue; }
            if (Character.isDigit(c)) {
                int start = i;
                while (i < n && Character.isDigit(s.charAt(i))) i++;
                // 小数部分：'.' 后须跟数字才并入数值（避免吃掉 a.b 的限定符 '.'）
                if (i < n && s.charAt(i) == '.' && i + 1 < n && Character.isDigit(s.charAt(i + 1))) {
                    i++;
                    while (i < n && Character.isDigit(s.charAt(i))) i++;
                }
                out.add(new Tok(Kind.NUM, s.substring(start, i)));
                continue;
            }
            if (c == '\'') {
                StringBuilder sb = new StringBuilder();
                i++;   // 开引号
                boolean closed = false;
                while (i < n) {
                    char ch = s.charAt(i);
                    if (ch == '\'') {
                        if (i + 1 < n && s.charAt(i + 1) == '\'') { sb.append('\''); i += 2; continue; }
                        if (i + 1 < n && s.charAt(i + 1) == '\\') { /* 不识别 */ }
                        closed = true;
                        i++;
                        break;
                    }
                    if (ch == '\\' && i + 1 < n) {   // \' \\ 转义
                        char nx = s.charAt(i + 1);
                        if (nx == '\'' || nx == '\\') { sb.append(nx); i += 2; continue; }
                    }
                    sb.append(ch);
                    i++;
                }
                if (!closed) throw new IllegalArgumentException("字符串字面量缺闭合引号: " + s);
                out.add(new Tok(Kind.STR, sb.toString()));
                continue;
            }
            if (c == '`') {
                StringBuilder sb = new StringBuilder();
                i++;
                boolean closed = false;
                while (i < n) {
                    char ch = s.charAt(i);
                    if (ch == '`') {
                        if (i + 1 < n && s.charAt(i + 1) == '`') { sb.append('`'); i += 2; continue; }
                        closed = true;
                        i++;
                        break;
                    }
                    sb.append(ch);
                    i++;
                }
                if (!closed) throw new IllegalArgumentException("反引号标识符缺闭合: " + s);
                out.add(new Tok(Kind.WORD, sb.toString()));
                continue;
            }
            if (isIdentStart(c)) {
                int start = i;
                while (i < n && isIdentPart(s.charAt(i))) i++;
                out.add(new Tok(Kind.WORD, s.substring(start, i)));
                continue;
            }
            // 运算符 / 括号
            switch (c) {
                case '(' -> { out.add(new Tok(Kind.LP, "(")); i++; continue; }
                case ')' -> { out.add(new Tok(Kind.RP, ")")); i++; continue; }
                case ',' -> { out.add(new Tok(Kind.COMMA, ",")); i++; continue; }
                case '.' -> { out.add(new Tok(Kind.DOT, ".")); i++; continue; }
                default -> {
                    // 双字符操作符
                    if (i + 1 < n) {
                        String two = s.substring(i, i + 2);
                        if ("<>".equals(two) || "<=".equals(two) || ">=".equals(two) || "!=".equals(two)) {
                            out.add(new Tok(Kind.OP, two));
                            i += 2;
                            continue;
                        }
                    }
                    if (isOp(c)) {
                        out.add(new Tok(Kind.OP, String.valueOf(c)));
                        i++;
                        continue;
                    }
                    throw new IllegalArgumentException("无法识别的字符 '" + c + "' @ 位置 " + i + ": " + s);
                }
            }
        }
        return out;
    }

    private static boolean isIdentStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private static boolean isIdentPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    private static boolean isOp(char c) {
        return c == '+' || c == '-' || c == '*' || c == '/' || c == '%'
                || c == '=' || c == '<' || c == '>';
    }

    private static Object number(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException ignored) {
            return Double.parseDouble(text);
        }
    }

    private static ExprNode.Compare.Op compareOp(String op) {
        return switch (op) {
            case "=" -> ExprNode.Compare.Op.EQ;
            case "!=", "<>" -> ExprNode.Compare.Op.NE;
            case ">" -> ExprNode.Compare.Op.GT;
            case ">=" -> ExprNode.Compare.Op.GE;
            case "<" -> ExprNode.Compare.Op.LT;
            case "<=" -> ExprNode.Compare.Op.LE;
            default -> null;
        };
    }

    // ==================== 工具 ====================

    private Tok peek() { return peek(0); }

    private Tok peek(int k) {
        int idx = p + k;
        return idx < toks.size() ? toks.get(idx) : null;
    }

    private boolean at(Kind k) {
        Tok t = peek();
        return t != null && t.kind == k;
    }

    private boolean isWord(String kw) {
        Tok t = peek();
        return t != null && t.kind == Kind.WORD && t.text.equalsIgnoreCase(kw);
    }

    private Tok take() {
        Tok t = toks.get(p);
        p++;
        return t;
    }

    private void expect(Kind k, String what) {
        if (!at(k)) throw err("期望 " + what);
        take();
    }

    private IllegalArgumentException err(String msg) {
        String near = p < toks.size() ? " @ '" + toks.get(p).text + "'" : " @ 结尾";
        return new IllegalArgumentException(msg + near);
    }
}
