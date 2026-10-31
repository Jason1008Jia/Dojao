package com.dojao.core.function.scalar.string;

import com.dojao.core.function.scalar.ScalarFunctionRegistry;
import com.dojao.core.function.scalar.ScalarSupport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hive 标准字符串标量函数（{@code concat/upper/ucase/lower/lcase/length} 见 {@code ExprBuiltins}，
 * 此处不重复；返回数组的 {@code split} 本次不做）。
 *
 * <p>NULL 语义逐函数按 Hive：多数函数任参 null → null；{@code concat_ws} 跳过 null 参；
 * {@code find_in_set} 返回 0 表示未找到。</p>
 *
 * @author jiaxin
 * @date 2026/10/11
 */
public final class StringFunctions {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private StringFunctions() {
    }

    public static void registerAll() {
        reg("trim", "去首尾空格（仅空格字符）", args -> {
            ScalarSupport.requireArity("trim", args, 1);
            return args[0] == null ? null : trimSpaces(ScalarSupport.string(args[0]));
        });
        reg("ltrim", "去前导空格", args -> {
            ScalarSupport.requireArity("ltrim", args, 1);
            return args[0] == null ? null : ltrimSpaces(ScalarSupport.string(args[0]));
        });
        reg("rtrim", "去尾随空格", args -> {
            ScalarSupport.requireArity("rtrim", args, 1);
            return args[0] == null ? null : rtrimSpaces(ScalarSupport.string(args[0]));
        });
        reg("concat_ws", "用分隔符连接（跳过 null 参；分隔符 null → null）", args -> {
            ScalarSupport.requireAtLeast("concat_ws", args, 1);
            if (args[0] == null) return null;
            String sep = ScalarSupport.string(args[0]);
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (int i = 1; i < args.length; i++) {
                if (args[i] == null) continue;
                if (!first) sb.append(sep);
                sb.append(ScalarSupport.string(args[i]));
                first = false;
            }
            return sb.toString();
        });
        Function<Object[], Object> substr = args -> {
            ScalarSupport.requireArity("substr", args, 2, 3);
            if (args[0] == null || args[1] == null) return null;
            String s = ScalarSupport.string(args[0]);
            int start = ScalarSupport.asInt(args[1]);
            int begin = start > 0 ? start - 1 : start < 0 ? s.length() + start : 0;
            if (begin < 0) begin = 0;
            if (begin > s.length()) return "";
            if (args.length == 3) {
                if (args[2] == null) return null;
                int len = ScalarSupport.asInt(args[2]);
                if (len < 0) len = 0;
                int end = Math.min(s.length(), begin + len);
                return s.substring(begin, end);
            }
            return s.substring(begin);
        };
        reg("substr", "子串（1-based；负起点从末尾计；可选长度）", substr);
        reg("substring", "子串（substr 别名）", substr);
        reg("instr", "首次出现位置（1-based；未找到 0）", args -> {
            ScalarSupport.requireArity("instr", args, 2);
            if (args[0] == null || args[1] == null) return null;
            int idx = ScalarSupport.string(args[0]).indexOf(ScalarSupport.string(args[1]));
            return (long) (idx < 0 ? 0 : idx + 1);
        });
        reg("locate", "子串位置（1-based；locate(sub,str[,pos])；未找到 0）", args -> {
            ScalarSupport.requireArity("locate", args, 2, 3);
            if (args[0] == null || args[1] == null) return null;
            String sub = ScalarSupport.string(args[0]);
            String s = ScalarSupport.string(args[1]);
            int pos = args.length == 3 && args[2] != null ? ScalarSupport.asInt(args[2]) : 1;
            if (pos < 1) pos = 1;
            int idx = s.indexOf(sub, pos - 1);
            return (long) (idx < 0 ? 0 : idx + 1);
        });
        reg("replace", "字面替换 replace(s,from,to)", args -> {
            ScalarSupport.requireArity("replace", args, 3);
            if (args[0] == null || args[1] == null || args[2] == null) return null;
            return ScalarSupport.string(args[0]).replace(ScalarSupport.string(args[1]), ScalarSupport.string(args[2]));
        });
        reg("regexp_replace", "正则替换 regexp_replace(s,regex,repl)（$1 反向引用）", args -> {
            ScalarSupport.requireArity("regexp_replace", args, 3);
            if (args[0] == null || args[1] == null || args[2] == null) return null;
            return ScalarSupport.string(args[0])
                    .replaceAll(ScalarSupport.string(args[1]), ScalarSupport.string(args[2]));
        });
        reg("regexp_extract", "正则提取 regexp_extract(s,regex,idx)（idx 0=整体；无匹配→空串）", args -> {
            ScalarSupport.requireArity("regexp_extract", args, 3);
            if (args[0] == null || args[1] == null || args[2] == null) return null;
            int idx = ScalarSupport.asInt(args[2]);
            Matcher m = Pattern.compile(ScalarSupport.string(args[1])).matcher(ScalarSupport.string(args[0]));
            if (m.find() && idx >= 0 && m.groupCount() >= idx) {
                String g = m.group(idx);
                return g == null ? "" : g;
            }
            return "";
        });
        reg("repeat", "重复 repeat(s,n)（n<0 抛异常）", args -> {
            ScalarSupport.requireArity("repeat", args, 2);
            if (args[0] == null || args[1] == null) return null;
            int n = ScalarSupport.asInt(args[1]);
            if (n < 0) throw new IllegalArgumentException("repeat 次数不能为负: " + n);
            return ScalarSupport.string(args[0]).repeat(n);
        });
        reg("reverse", "反转字符串", args -> {
            ScalarSupport.requireArity("reverse", args, 1);
            return args[0] == null ? null : new StringBuilder(ScalarSupport.string(args[0])).reverse().toString();
        });
        reg("lpad", "左填充 lpad(s,len,pad)：左侧补 pad 至 len（超长截断）", args -> {
            ScalarSupport.requireArity("lpad", args, 3);
            if (args[0] == null || args[1] == null || args[2] == null) return null;
            return pad(ScalarSupport.string(args[0]), ScalarSupport.asInt(args[1]), ScalarSupport.string(args[2]), true);
        });
        reg("rpad", "右填充 rpad(s,len,pad)：右侧补 pad 至 len（超长截断）", args -> {
            ScalarSupport.requireArity("rpad", args, 3);
            if (args[0] == null || args[1] == null || args[2] == null) return null;
            return pad(ScalarSupport.string(args[0]), ScalarSupport.asInt(args[1]), ScalarSupport.string(args[2]), false);
        });
        reg("space", "n 个空格（n<0 抛异常）", args -> {
            ScalarSupport.requireArity("space", args, 1);
            if (args[0] == null) return null;
            int n = ScalarSupport.asInt(args[0]);
            if (n < 0) throw new IllegalArgumentException("space 参数不能为负: " + n);
            return " ".repeat(n);
        });
        reg("ascii", "首字符 ASCII 码（空串→0）", args -> {
            ScalarSupport.requireArity("ascii", args, 1);
            if (args[0] == null) return null;
            String s = ScalarSupport.string(args[0]);
            return s.isEmpty() ? 0L : (long) (int) s.charAt(0);
        });
        reg("chr", "码点转字符（非法码点抛异常）", args -> {
            ScalarSupport.requireArity("chr", args, 1);
            if (args[0] == null) return null;
            long n = ScalarSupport.asLong(args[0]);
            if (n < 0 || n > 65535) throw new IllegalArgumentException("chr 码点越界: " + n);
            return String.valueOf((char) n);
        });
        reg("soundex", "Soundex 编码（4 字符）", args -> {
            ScalarSupport.requireArity("soundex", args, 1);
            return args[0] == null ? null : soundex(ScalarSupport.string(args[0]));
        });
        reg("parse_url", "URL 解析：parse_url(url,part[,key])；part∈HOST/PATH/QUERY/REF/PROTOCOL/AUTHORITY/FILE/USERINFO", args -> {
            ScalarSupport.requireArity("parse_url", args, 2, 3);
            if (args[0] == null || args[1] == null) return null;
            String key = args.length == 3 && args[2] != null ? ScalarSupport.string(args[2]) : null;
            return parseUrl(ScalarSupport.string(args[0]), ScalarSupport.string(args[1]), key);
        });
        reg("get_json_object", "JSON 取值 get_json_object(json,path)（path 形如 $.a.b[0]）", args -> {
            ScalarSupport.requireArity("get_json_object", args, 2);
            if (args[0] == null || args[1] == null) return null;
            return getJsonObject(ScalarSupport.string(args[0]), ScalarSupport.string(args[1]));
        });
        reg("json_extract", "JSON 取值 json_extract(json,path[,path...])：返回合法 JSON 文本（字符串带引号；多路径返回数组）", args -> {
            ScalarSupport.requireAtLeast("json_extract", args, 2);
            if (args[0] == null) return null;
            String json = ScalarSupport.string(args[0]);
            List<JsonNode> nodes = new java.util.ArrayList<>(args.length - 1);
            for (int i = 1; i < args.length; i++) {
                if (args[i] == null) return null;
                nodes.add(resolveJsonNode(json, ScalarSupport.string(args[i])));
            }
            if (args.length == 2) {
                return jsonText(nodes.get(0));
            }
            com.fasterxml.jackson.databind.node.ArrayNode arr = MAPPER.createArrayNode();
            for (JsonNode n : nodes) {
                if (n == null) arr.addNull();
                else arr.add(n);
            }
            return arr.toString();
        });
        reg("find_in_set", "在逗号列表中查位置（1-based；未找到 0）", args -> {
            ScalarSupport.requireArity("find_in_set", args, 2);
            if (args[0] == null || args[1] == null) return null;
            String needle = ScalarSupport.string(args[0]);
            if (needle.indexOf(',') >= 0) return 0L;
            String[] parts = ScalarSupport.string(args[1]).split(",", -1);
            for (int i = 0; i < parts.length; i++) {
                if (parts[i].trim().equals(needle)) return (long) (i + 1);
            }
            return 0L;
        });
        reg("elt", "取第 n 个字符串 elt(n,s1,...)（越界→null）", args -> {
            ScalarSupport.requireAtLeast("elt", args, 2);
            if (args[0] == null) return null;
            int n = ScalarSupport.asInt(args[0]);
            if (n < 1 || n >= args.length) return null;
            return args[n] == null ? null : ScalarSupport.string(args[n]);
        });
        reg("initcap", "首字母大写（其余小写）", args -> {
            ScalarSupport.requireArity("initcap", args, 1);
            return args[0] == null ? null : initcap(ScalarSupport.string(args[0]));
        });
        reg("printf", "格式化 printf(fmt,...)（%s/%d/%f；整型字面量对 %d 自动归一）", args -> {
            ScalarSupport.requireAtLeast("printf", args, 1);
            if (args[0] == null) return null;
            String fmt = ScalarSupport.string(args[0]);
            Object[] fmtArgs = new Object[args.length - 1];
            System.arraycopy(args, 1, fmtArgs, 0, fmtArgs.length);
            try {
                return String.format(fmt, fmtArgs);
            } catch (java.util.IllegalFormatConversionException e) {
                Object[] conv = new Object[fmtArgs.length];
                for (int i = 0; i < fmtArgs.length; i++) {
                    Object a = fmtArgs[i];
                    conv[i] = (a instanceof Double d && !d.isInfinite() && d == Math.floor(d))
                            ? (Object) (long) d.doubleValue() : a;
                }
                return String.format(fmt, conv);
            }
        });
        reg("translate", "字符映射 translate(s,from,to)（to 更短则删除）", args -> {
            ScalarSupport.requireArity("translate", args, 3);
            if (args[0] == null || args[1] == null || args[2] == null) return null;
            return translate(ScalarSupport.string(args[0]), ScalarSupport.string(args[1]), ScalarSupport.string(args[2]));
        });
    }

    private static void reg(String name, String display, Function<Object[], Object> impl) {
        ScalarFunctionRegistry.register(ScalarSupport.fn(name, display, impl));
    }

    // ==================== 实现细节 ====================

    private static String trimSpaces(String s) {
        return rtrimSpaces(ltrimSpaces(s));
    }

    private static String ltrimSpaces(String s) {
        int b = 0;
        while (b < s.length() && s.charAt(b) == ' ') b++;
        return s.substring(b);
    }

    private static String rtrimSpaces(String s) {
        int e = s.length();
        while (e > 0 && s.charAt(e - 1) == ' ') e--;
        return s.substring(0, e);
    }

    private static String pad(String s, int len, String pad, boolean left) {
        if (len <= 0) return "";
        if (s.length() >= len) return s.substring(0, len);
        if (pad.isEmpty()) return s;
        int need = len - s.length();
        StringBuilder filler = new StringBuilder();
        while (filler.length() < need) filler.append(pad);
        filler.setLength(need);
        return left ? filler + s : s + filler;
    }

    private static String initcap(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        boolean boundary = true;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isLetterOrDigit(c)) {
                sb.append(boundary ? Character.toUpperCase(c) : Character.toLowerCase(c));
                boundary = false;
            } else {
                sb.append(c);
                boundary = true;
            }
        }
        return sb.toString();
    }

    private static String translate(String s, String from, String to) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            int idx = from.indexOf(c);
            if (idx < 0) sb.append(c);
            else if (idx < to.length()) sb.append(to.charAt(idx));
            // idx >= to.length() → 删除该字符
        }
        return sb.toString();
    }

    /** 标准 Soundex：首字母 + 3 位数字编码。 */
    private static String soundex(String s) {
        String up = s.toUpperCase(Locale.ROOT);
        if (up.isEmpty()) return "";
        char first = up.charAt(0);
        if (!Character.isLetter(first)) return "";
        StringBuilder code = new StringBuilder().append(first);
        char prev = code(up.charAt(0));
        for (int i = 1; i < up.length() && code.length() < 4; i++) {
            char c = up.charAt(i);
            if (!Character.isLetter(c)) {
                prev = 0;
                continue;
            }
            char cd = code(c);
            if (cd != '0' && cd != prev) code.append(cd);
            prev = cd;
            if (code.length() == 4) break;
        }
        while (code.length() < 4) code.append('0');
        return code.toString();
    }

    private static char code(char c) {
        return switch (Character.toUpperCase(c)) {
            case 'B', 'F', 'P', 'V' -> '1';
            case 'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z' -> '2';
            case 'D', 'T' -> '3';
            case 'L' -> '4';
            case 'M', 'N' -> '5';
            case 'R' -> '6';
            default -> '0';
        };
    }

    private static Object parseUrl(String url, String part, String key) {
        URI u;
        try {
            u = URI.create(url.trim());
        } catch (RuntimeException e) {
            return null;
        }
        return switch (part.trim().toUpperCase(Locale.ROOT)) {
            case "HOST" -> u.getHost();
            case "PATH" -> u.getPath();
            case "QUERY" -> key != null ? queryParam(u.getRawQuery(), key) : u.getQuery();
            case "REF" -> u.getFragment();
            case "PROTOCOL" -> u.getScheme();
            case "AUTHORITY" -> u.getRawAuthority();
            case "USERINFO" -> u.getRawUserInfo();
            case "FILE" -> {
                String p = u.getPath();
                String q = u.getRawQuery();
                if (q == null) yield p;
                yield (p == null ? "" : p) + "?" + q;
            }
            default -> null;
        };
    }

    private static String queryParam(String query, String key) {
        if (query == null) return null;
        for (String kv : query.split("&")) {
            int eq = kv.indexOf('=');
            String k = eq < 0 ? kv : kv.substring(0, eq);
            if (k.equals(key)) return eq < 0 ? "" : kv.substring(eq + 1);
        }
        return null;
    }

    /** Hive get_json_object：路径 {@code $.a.b[0]} / {@code $['a']['b']}；命中值转文本，未命中 null。 */
    private static String getJsonObject(String json, String path) {
        JsonNode node = resolveJsonNode(json, path);
        return node == null ? null : node.isValueNode() ? node.asText() : node.toString();
    }

    /** json_extract 口径：返回合法 JSON 文本（字符串保留引号），未命中/非法 → null。 */
    private static String jsonText(JsonNode node) {
        return node == null ? null : node.toString();
    }

    /** 解析 JSON 并按路径取值；非法 JSON、非法路径、缺失/JSON null → null。 */
    private static JsonNode resolveJsonNode(String json, String path) {
        String p = path.trim();
        if (!p.startsWith("$")) return null;
        JsonNode node;
        try {
            node = MAPPER.readTree(json);
        } catch (Exception e) {
            return null;
        }
        int i = 1;
        while (i < p.length()) {
            char c = p.charAt(i);
            if (c == '.') {
                int j = i + 1;
                StringBuilder name = new StringBuilder();
                if (j < p.length() && (p.charAt(j) == '\'' || p.charAt(j) == '"')) {
                    char q = p.charAt(j);
                    j++;
                    while (j < p.length() && p.charAt(j) != q) name.append(p.charAt(j++));
                    j++;   // 跳过收尾引号
                } else {
                    while (j < p.length() && p.charAt(j) != '.' && p.charAt(j) != '[') name.append(p.charAt(j++));
                }
                node = node.get(name.toString());
                i = j;
            } else if (c == '[') {
                int j = p.indexOf(']', i);
                if (j < 0) return null;
                String idx = p.substring(i + 1, j).trim();
                if ((idx.startsWith("'") && idx.endsWith("'")) || (idx.startsWith("\"") && idx.endsWith("\""))) {
                    node = node.get(idx.substring(1, idx.length() - 1));
                } else {
                    try {
                        node = node.get(Integer.parseInt(idx));
                    } catch (NumberFormatException e) {
                        return null;
                    }
                }
                i = j + 1;
            } else {
                i++;
            }
            if (node == null || node.isMissingNode() || node.isNull()) return null;
        }
        return node;
    }
}
