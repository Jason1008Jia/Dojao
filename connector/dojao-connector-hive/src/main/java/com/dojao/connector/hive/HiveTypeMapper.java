package com.dojao.connector.hive;

import com.dojao.connector.common.util.NestedFieldNames;
import com.dojao.spi.bean.DojaoType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hive 类型字符串 → Arrow 类型 / Dojao 标准类型。
 *
 * <p>Hive 的列类型在 MetaStore 里是<b>字符串</b>（{@code int} / {@code decimal(10,2)} /
 * {@code varchar(64)} / {@code array&lt;string&gt;} …）。标量精确映射到 Arrow；</p>
 *
 * <p>复合类型默认按<b>结构化</b>映射（{@code array} → Arrow List、{@code map} → Arrow Map、
 * {@code struct} → Arrow Struct），读出真实的嵌套向量，可参与 CTAS / INSERT 原样写回。
 * {@code uniontype} 没有对应的 Arrow 结构，仍按<b>文本</b>读出（如 {@code {0:1}}）。</p>
 *
 * <p>注意：LIST 元素字段统一命名 {@code item}、MAP 数据字段固定 {@code entries/key/value}
 * —— 与 {@code ORCFileArrowConverter} / {@code ParquetFileArrowConverter} 读侧、以及
 * {@code HiveConnector.alignToTableSchema} 的字段逐字节一致，避免「同类型但 Field 不等」
 * 退化到非零拷贝路径。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public final class HiveTypeMapper {

    /** {@code decimal(p,s)}；Hive 裸 {@code decimal} 等价于 {@code decimal(10,0)}。 */
    private static final Pattern DECIMAL =
            Pattern.compile("decimal\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)");

    /** Arrow 列表元素字段名（读/写两侧统一，取自 connector-common 的唯一定义）。 */
    public static final String LIST_ELEMENT_NAME = NestedFieldNames.LIST_ELEMENT;
    /** Arrow Map 的 entries / key / value 字段名（Arrow 规范固定值）。 */
    public static final String MAP_ENTRIES_NAME = NestedFieldNames.MAP_ENTRIES;
    public static final String MAP_KEY_NAME = NestedFieldNames.MAP_KEY;
    public static final String MAP_VALUE_NAME = NestedFieldNames.MAP_VALUE;

    private HiveTypeMapper() {
    }

    /** 去掉大小写与空白，便于匹配（Hive 类型不区分大小写）。 */
    public static String normalize(String hiveType) {
        return hiveType == null ? "string" : hiveType.trim().toLowerCase(Locale.ROOT);
    }

    /** 是否复合类型（array / map / struct / uniontype）。 */
    public static boolean isNested(String hiveType) {
        return isStructurallyNested(hiveType) || normalize(hiveType).startsWith("uniontype<");
    }

    /** 是否能映射成 Arrow 结构（array / map / struct；uniontype 除外）。 */
    public static boolean isStructurallyNested(String hiveType) {
        String t = normalize(hiveType);
        return t.startsWith("array<") || t.startsWith("map<") || t.startsWith("struct<");
    }

    /** Hive 类型 → Dojao 标准类型。 */
    public static DojaoType toDojaoType(String hiveType) {
        String t = normalize(hiveType);
        if (t.startsWith("array<")) return DojaoType.LIST;
        if (t.startsWith("map<")) return DojaoType.MAP;
        if (t.startsWith("struct<")) return DojaoType.STRUCT;
        // uniontype 没有 Arrow 对应结构 → 文本（读侧渲染成 {tag:值}）
        if (t.startsWith("uniontype<")) return DojaoType.STRING;
        return scalarDojaoType(t);
    }

    private static DojaoType scalarDojaoType(String t) {
        if (t.startsWith("tinyint")) return DojaoType.TINYINT;
        if (t.startsWith("smallint")) return DojaoType.SMALLINT;
        if (t.startsWith("bigint")) return DojaoType.BIGINT;
        if (t.equals("int") || t.equals("integer")) return DojaoType.INT;
        if (t.equals("boolean")) return DojaoType.BOOLEAN;
        if (t.startsWith("float")) return DojaoType.FLOAT;
        if (t.startsWith("double")) return DojaoType.DOUBLE;
        if (t.startsWith("decimal") || t.startsWith("numeric")) return DojaoType.DECIMAL;
        if (t.startsWith("date")) return DojaoType.DATE;
        if (t.startsWith("timestamp")) return DojaoType.TIMESTAMP;
        // string / varchar(n) / char(n) / binary / interval_* / 未知 → 文本
        return DojaoType.STRING;
    }

    /**
     * Hive 类型 → Arrow 物理类型（<b>不含子字段</b>）。
     *
     * <p>复合类型返回 {@code List/Map/Struct} 的裸 {@link ArrowType}；要构造可用的 Arrow
     * 字段请用 {@link #toField(String, String)}。</p>
     */
    public static ArrowType toArrowType(String hiveType) {
        String t = normalize(hiveType);
        if (t.startsWith("array<")) return new ArrowType.List();
        if (t.startsWith("map<")) return new ArrowType.Map(false);
        if (t.startsWith("struct<")) return new ArrowType.Struct();
        return scalarArrowType(t);
    }

    private static ArrowType scalarArrowType(String t) {
        if (t.startsWith("tinyint")) return new ArrowType.Int(8, true);
        if (t.startsWith("smallint")) return new ArrowType.Int(16, true);
        if (t.startsWith("bigint")) return new ArrowType.Int(64, true);
        if (t.equals("int") || t.equals("integer")) return new ArrowType.Int(32, true);
        if (t.equals("boolean")) return ArrowType.Bool.INSTANCE;
        if (t.startsWith("float")) return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        if (t.startsWith("double")) return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        if (t.startsWith("decimal") || t.startsWith("numeric")) {
            Matcher m = DECIMAL.matcher(t);
            if (m.find()) {
                int precision = Integer.parseInt(m.group(1));
                int scale = Integer.parseInt(m.group(2));
                return new ArrowType.Decimal(precision, scale, 128);
            }
            // Hive 默认 decimal = decimal(10,0)
            return new ArrowType.Decimal(10, 0, 128);
        }
        if (t.startsWith("date")) return new ArrowType.Date(DateUnit.DAY);
        if (t.startsWith("timestamp")) return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
        if (t.startsWith("binary")) return ArrowType.Binary.INSTANCE;
        // string / varchar / char / interval / uniontype / 未知 → 文本
        return ArrowType.Utf8.INSTANCE;
    }

    /**
     * 按列名 + Hive 类型构建可空 Arrow Field（复合类型递归构造子字段）。
     *
     * <p>{@code uniontype} 无 Arrow 结构，按 Utf8 文本处理；其内部类型文本一并忽略
     * （读侧由文件转换器渲染）。</p>
     */
    public static Field toField(String name, String hiveType) {
        TypeParser parser = new TypeParser(normalize(hiveType));
        Field field = parser.parseField(name);
        parser.skipWs();
        // 容错：尾部残余（如 uniontype<...> 消费后的空白）不视为错误
        return field;
    }

    /**
     * 按 {@code catalog.hive.nested.mode} 决定复合类型的 Arrow 形态。
     *
     * <p>{@code nestedNative=true}（默认）：{@code array/map/struct} 建真实嵌套向量；
     * {@code false}：整体按 {@code Utf8} 文本列（读侧渲染成 {@code [1,2]} / {"a":1}，写侧按原样字符串落文件）。
     * {@code uniontype} 无 Arrow 结构，两种模式下都是文本。</p>
     */
    public static Field toField(String name, String hiveType, boolean nestedNative) {
        if (!nestedNative && isNested(hiveType)) {
            return new Field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        }
        return toField(name, hiveType);
    }

    /**
     * Arrow Field → Hive 列类型字符串（CTAS 建表 DDL 用，{@link #toField} 的反向）。
     *
     * <p>无法精确表达的（Utf8 → {@code string}、未知 → {@code string}）走 Hive 最宽类型兜底，
     * 不抛错 —— CTAS 建表失败比类型略宽更糟。FixedSizeList 写成 {@code array<...>}（Hive 无定长数组）。</p>
     */
    public static String arrowToHiveType(Field field) {
        if (field == null) return "string";
        ArrowType type = field.getType();
        List<Field> children = field.getChildren();
        if (type instanceof ArrowType.List || type instanceof ArrowType.FixedSizeList) {
            Field elem = children.isEmpty() ? null : children.get(0);
            return "array<" + (elem == null ? "string" : arrowToHiveType(elem)) + ">";
        }
        if (type instanceof ArrowType.Map) {
            Field entries = children.isEmpty() ? null : children.get(0);
            List<Field> kv = entries == null ? List.of() : entries.getChildren();
            String key = kv.size() > 0 ? arrowToHiveType(kv.get(0)) : "string";
            String value = kv.size() > 1 ? arrowToHiveType(kv.get(1)) : "string";
            return "map<" + key + "," + value + ">";
        }
        if (type instanceof ArrowType.Struct) {
            StringBuilder sb = new StringBuilder("struct<");
            for (int i = 0; i < children.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(children.get(i).getName()).append(':')
                        .append(arrowToHiveType(children.get(i)));
            }
            return sb.append('>').toString();
        }
        return arrowToHiveType(type);
    }

    /** Arrow 标量类型 → Hive 类型字符串。 */
    public static String arrowToHiveType(ArrowType type) {
        if (type instanceof ArrowType.Bool) return "boolean";
        if (type instanceof ArrowType.Int i) {
            return switch (i.getBitWidth()) {
                case 8 -> "tinyint";
                case 16 -> "smallint";
                case 32 -> "int";
                default -> "bigint";
            };
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.SINGLE ? "float" : "double";
        }
        if (type instanceof ArrowType.Decimal d) {
            return "decimal(" + d.getPrecision() + "," + d.getScale() + ")";
        }
        if (type instanceof ArrowType.Date) return "date";
        if (type instanceof ArrowType.Timestamp) return "timestamp";
        if (type instanceof ArrowType.Binary) return "binary";
        // Utf8 / 其余未知 → string
        return "string";
    }

    // ==================== Hive 类型字符串递归解析 ====================

    /**
     * 极简递归下降解析器：只处理类型语法（{@code array<...>} / {@code map<...>} /
     * {@code struct<名:类型,...>}），标量原样交给 {@link #scalarArrowType(String)}。
     */
    private static final class TypeParser {
        private final String text;
        private int pos;

        TypeParser(String text) {
            this.text = text == null ? "string" : text;
        }

        Field parseField(String name) {
            skipWs();
            String base = readBaseToken();
            if (base.equals("array") && peek('<')) {
                pos++;
                Field element = parseField(LIST_ELEMENT_NAME);
                expect('>');
                return new Field(name, FieldType.nullable(new ArrowType.List()), List.of(element));
            }
            if (base.equals("map") && peek('<')) {
                pos++;
                Field key = parseField(MAP_KEY_NAME);
                key = new Field(MAP_KEY_NAME, FieldType.notNullable(key.getType()), key.getChildren());
                expect(',');
                Field value = parseField(MAP_VALUE_NAME);
                expect('>');
                Field entries = new Field(MAP_ENTRIES_NAME,
                        FieldType.notNullable(new ArrowType.Struct()), List.of(key, value));
                return new Field(name, FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
            }
            if (base.equals("struct") && peek('<')) {
                pos++;
                List<Field> children = new ArrayList<>();
                skipWs();
                while (!peek('>') && pos < text.length()) {
                    String childName = readUntil(':');
                    expect(':');
                    children.add(parseField(childName));
                    skipWs();
                    if (peek(',')) {
                        pos++;
                        skipWs();
                        continue;
                    }
                    break;
                }
                expect('>');
                return new Field(name, FieldType.nullable(new ArrowType.Struct()), children);
            }
            if (base.equals("uniontype") && peek('<')) {
                // 结构化 union 无 Arrow 对应结构：整体按文本，跳过内部类型描述
                skipGeneric();
                return new Field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
            }
            return new Field(name, FieldType.nullable(scalarArrowType(base)), null);
        }

        /** 读取到 {@code '<'} / {@code ','} / {@code '>'} / {@code ':'} / 结尾；括号整体保留。 */
        private String readBaseToken() {
            skipWs();
            int start = pos;
            while (pos < text.length()) {
                char c = text.charAt(pos);
                if (c == '<' || c == ',' || c == '>' || c == ':') break;
                if (c == '(') {
                    int depth = 0;
                    do {
                        char d = text.charAt(pos++);
                        if (d == '(') depth++;
                        else if (d == ')') depth--;
                    } while (depth > 0 && pos < text.length());
                    continue;
                }
                pos++;
            }
            return text.substring(start, pos).trim();
        }

        /** struct 字段名：读到冒号，去掉反引号与空白。 */
        private String readUntil(char stop) {
            skipWs();
            int start = pos;
            while (pos < text.length() && text.charAt(pos) != stop) pos++;
            String raw = text.substring(start, pos).trim();
            if (raw.length() >= 2 && raw.charAt(0) == '`' && raw.charAt(raw.length() - 1) == '`') {
                raw = raw.substring(1, raw.length() - 1);
            }
            return raw;
        }

        /** 消费一段 {@code <...>}（允许嵌套尖括号）。 */
        private void skipGeneric() {
            skipWs();
            if (!peek('<')) return;
            int depth = 0;
            do {
                char c = text.charAt(pos++);
                if (c == '<') depth++;
                else if (c == '>') depth--;
            } while (depth > 0 && pos < text.length());
        }

        private void expect(char c) {
            skipWs();
            if (pos < text.length() && text.charAt(pos) == c) pos++;
        }

        private boolean peek(char c) {
            return pos < text.length() && text.charAt(pos) == c;
        }

        void skipWs() {
            while (pos < text.length() && Character.isWhitespace(text.charAt(pos))) pos++;
        }
    }
}
