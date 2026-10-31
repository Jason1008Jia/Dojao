package com.dojao.spi.util;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.CallBack;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * JDBC ResultSet → Arrow VectorSchemaRoot 批量转换器。
 *
 * <p>两阶段批量转换：
 * <ol>
 *   <li>Phase 1（行扫描）：逐行读取 ResultSet，所有列值存入 typed 列数组</li>
 *   <li>Phase 2（列批量写）：逐列预分配 Arrow buffer，整列 unchecked set() 写入</li>
 * </ol>
 *
 * <p>避免逐行 setSafe() 的频繁容量检查与 VarChar 增量 re-alloc，
 * 利用 Arrow 列式内存布局实现单次 bulk write。
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/7/20
 */
public class JdbcArrowConverter {

    private final BufferAllocator allocator;
    private final int batchSize;
    private Schema cachedSchema;
    private List<ColumnWriter> cachedWriters;

    public JdbcArrowConverter(BufferAllocator allocator, int batchSize) {
        this.allocator = allocator;
        this.batchSize = batchSize;
    }

    // ==================== 批次转换（两阶段） ====================

    /** 从 ResultSet 读取下一批数据，无更多行时返回 EOF Page。 */
    public Page convertNextBatch(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        if (cachedWriters == null) {
            cachedWriters = buildWriters(md);
        }

        // 分配临时列存储
        for (ColumnWriter w : cachedWriters) {
            w.allocateTemp(batchSize);
        }

        // Phase 1: 行扫描 → 列数组
        int rowCount = 0;
        while (rowCount < batchSize && rs.next()) {
            for (int col = 0; col < cachedWriters.size(); col++) {
                cachedWriters.get(col).extract(rs, rowCount);
            }
            rowCount++;
        }

        if (rowCount == 0) {
            return Page.eof();
        }

        // Schema 在 Phase 1 之后构建：pgvector 的 vector 列维度需从数据中解析
        if (cachedSchema == null) {
            cachedSchema = buildSchema(md, cachedWriters);
        }

        // Phase 2: 列批量写 → Arrow VectorSchemaRoot
        List<FieldVector> vectors = new ArrayList<>();
        for (int col = 0; col < cachedWriters.size(); col++) {
            vectors.add(cachedWriters.get(col).writeBatch(allocator,
                    cachedSchema.getFields().get(col), rowCount));
        }

        // 收尾必须把 rowCount 下推到每一列：各 writer 只做 allocateNew(n) + set()/setNull()，
        // Arrow 的 valueCount 不会因此推进（恒为 0），而 ValueVector.getBufferSize() 在
        // valueCount==0 时返回 0 → Page.sizeInBytes 恒 0（监控里 Scan 段「Input Bytes」恒 0 B 的根因）。
        // 与 ORCFileArrowConverter.convertNextBatch 同款收尾。
        for (FieldVector v : vectors) {
            v.setValueCount(rowCount);
        }

        VectorSchemaRoot root = new VectorSchemaRoot(cachedSchema.getFields(), vectors, rowCount);
        return Page.fromRoot(root);
    }

    // ==================== Schema ====================

    /** 由 JDBC ResultSet 元数据推导 Arrow Schema（公开：连接器读方向 schema 推导复用，保证与 read 同类型）。 */
    public static Schema buildSchema(ResultSetMetaData md) throws SQLException {
        List<Field> fields = new ArrayList<>();
        int colCount = md.getColumnCount();
        for (int i = 1; i <= colCount; i++) {
            String name = columnName(md, i);
            fields.add(fieldFor(name, md.getColumnType(i), md.getPrecision(i),
                    md.getScale(i), safeTypeName(md, i)));
        }
        return new Schema(fields);
    }

    /** 带 writer 的 schema 构建：vector 列用 writer 解析出的维度（Phase 1 之后调用）。 */
    private Schema buildSchema(ResultSetMetaData md, List<ColumnWriter> writers)
            throws SQLException {
        List<Field> fields = new ArrayList<>();
        int colCount = md.getColumnCount();
        for (int i = 0; i < colCount; i++) {
            int colIdx = i + 1;
            fields.add(writers.get(i).field(columnName(md, colIdx), md, colIdx));
        }
        return new Schema(fields);
    }

    private static String columnName(ResultSetMetaData md, int colIdx) throws SQLException {
        String name = md.getColumnLabel(colIdx);
        if (name == null || name.isEmpty()) name = md.getColumnName(colIdx);
        return name;
    }

    /** 部分驱动（老版本 MySQL）的 getColumnTypeName 会抛异常 → 视为无类型名。 */
    static String safeTypeName(ResultSetMetaData md, int colIdx) {
        try {
            return md.getColumnTypeName(colIdx);
        } catch (SQLException | RuntimeException e) {
            return null;
        }
    }

    static List<ColumnWriter> buildWriters(ResultSetMetaData md) throws SQLException {
        List<ColumnWriter> writers = new ArrayList<>();
        int colCount = md.getColumnCount();
        for (int i = 1; i <= colCount; i++) {
            int jdbcType = md.getColumnType(i);
            String typeName = safeTypeName(md, i);
            if (isPgVector(jdbcType, typeName)) {
                writers.add(new PgVectorWriter(i));
            } else if (isArrayColumn(jdbcType, typeName)) {
                writers.add(new ListWriter(i));
            } else if (isMapColumn(typeName)) {
                writers.add(new MapWriter(i));
            } else {
                writers.add(ColumnWriter.forType(jdbcType, i));
            }
        }
        return writers;
    }

    /** 是否为 pgvector 的 vector 类型（JDBC 报 OTHER + typeName=vector）。 */
    static boolean isPgVector(int jdbcType, String typeName) {
        return jdbcType == Types.OTHER && "vector".equalsIgnoreCase(typeName);
    }

    // ==================== JDBC → Arrow 类型映射 ====================

    /** JDBC 类型 → Arrow 类型映射。 */
    public static ArrowType mapJdbcToArrow(int jdbcType, int precision, int scale) {
        return switch (jdbcType) {
            case Types.TINYINT                                           -> new ArrowType.Int(8, true);
            case Types.SMALLINT                                          -> new ArrowType.Int(16, true);
            case Types.INTEGER                                           -> new ArrowType.Int(32, true);
            case Types.BIGINT                                            -> new ArrowType.Int(64, true);
            case Types.FLOAT, Types.REAL                                 -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case Types.DOUBLE                                            -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case Types.DECIMAL, Types.NUMERIC                            -> new ArrowType.Decimal(precision, scale, 128);
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                 Types.CLOB, Types.NCHAR, Types.NVARCHAR, Types.NCLOB    -> ArrowType.Utf8.INSTANCE;
            case Types.DATE                                              -> new ArrowType.Date(DateUnit.DAY);
            case Types.TIME, Types.TIME_WITH_TIMEZONE                    -> new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE          -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case Types.BOOLEAN, Types.BIT                                -> ArrowType.Bool.INSTANCE;
            case Types.BINARY, Types.VARBINARY,
                 Types.LONGVARBINARY, Types.BLOB                         -> ArrowType.Binary.INSTANCE;
            default                                                      -> ArrowType.Utf8.INSTANCE;
        };
    }

    // ==================== 嵌套类型：数组 / Map（按驱动类型名解析） ====================

    /**
     * JDBC 列元数据 → Arrow {@link Field}（嵌套列带子字段，其余等价于
     * {@code new Field(name, nullable(mapJdbcToArrow(...)))}）。
     *
     * <p>嵌套形状由 JDBC 的 <b>类型名</b>决定（JDBC 标准类型码只区分 ARRAY，拿不到元素类型）：</p>
     * <ul>
     *   <li>PostgreSQL：{@code _int4} / {@code _text}（数组列 udt_name 前置下划线）；</li>
     *   <li>ClickHouse：{@code Array(Int64)} / {@code Map(String, Int64)}，
     *       可带 {@code Nullable(...)} / {@code LowCardinality(...)} 包装。</li>
     * </ul>
     */
    public static Field fieldFor(String name, int jdbcType, int precision, int scale,
                                 String typeName) {
        String tn = typeName == null ? "" : typeName.trim();
        if (isArrayColumn(jdbcType, tn)) {
            return listFieldOf(name, tn);
        }
        if (isMapColumn(tn)) {
            return fieldOfTypeName(name, tn);
        }
        return new Field(name, FieldType.nullable(mapJdbcToArrow(jdbcType, precision, scale)), null);
    }

    /**
     * 是否数组列：JDBC 标准 ARRAY，或类型名是 PG（{@code _int4}）/ ClickHouse（{@code Array(...)}）。
     *
     * <p>ClickHouse 的 {@code Nullable(Array(Int64))} / {@code LowCardinality(...)} 外壳必须先剥掉，
     * 否则裸 {@code Array(} 前缀匹配不到，嵌套列会被当成标量字符串读。</p>
     */
    static boolean isArrayColumn(int jdbcType, String typeName) {
        if (jdbcType == Types.ARRAY) return true;
        String tn = stripWrappers(typeName);
        return tn.startsWith("_") || tn.regionMatches(true, 0, "Array(", 0, 6);
    }

    /** 是否 Map 列：ClickHouse {@code Map(K, V)}（PG 无内置 map 类型）；同样先剥外壳。 */
    static boolean isMapColumn(String typeName) {
        return stripWrappers(typeName).regionMatches(true, 0, "Map(", 0, 4);
    }

    /** 数组列 → {@code List<element>}；PG 的 {@code _int4} 按标量元素处理（外壳已剥）。 */
    private static Field listFieldOf(String name, String typeName) {
        String tn = stripWrappers(typeName);
        Field element;
        if (tn.startsWith("_")) {
            element = new Field("item", FieldType.nullable(pgTypeOf(tn.substring(1))), null);
        } else if (tn.regionMatches(true, 0, "Array(", 0, 6)) {
            element = fieldOfTypeName("item", innerOf(tn, "Array"));
        } else {
            element = new Field("item", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        }
        return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE), List.of(element));
    }

    /**
     * 驱动类型名 → Arrow Field（递归：{@code Array(T)} / {@code Map(K,V)}，
     * 标量走 {@link #scalarTypeOf(String)}；{@code Nullable/LowCardinality} 外壳剥掉）。
     */
    static Field fieldOfTypeName(String name, String rawTypeName) {
        String t = stripWrappers(rawTypeName);
        if (isMapColumn(t)) {
            String[] kv = splitTopLevel(innerOf(t, "Map"));
            Field key = fieldOfTypeName("key", kv.length > 0 ? kv[0] : "String");
            key = new Field("key", FieldType.notNullable(key.getType()), key.getChildren());
            Field value = fieldOfTypeName("value", kv.length > 1 ? kv[1] : "String");
            Field entries = new Field("entries",
                    FieldType.notNullable(new ArrowType.Struct()), List.of(key, value));
            return new Field(name, FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
        }
        if (t.startsWith("_") || t.regionMatches(true, 0, "Array(", 0, 6)) {
            return listFieldOf(name, t);
        }
        return new Field(name, FieldType.nullable(scalarTypeOf(t)), null);
    }

    /** 标量类型名 → Arrow 类型（PG udt / ClickHouse 名称 / 常见 SQL 名称）。 */
    static ArrowType scalarTypeOf(String rawTypeName) {
        String t = stripWrappers(rawTypeName);
        if (t.isEmpty()) return ArrowType.Utf8.INSTANCE;
        if (t.startsWith("_")) return pgTypeOf(t.substring(1));
        if (t.regionMatches(true, 0, "FixedString(", 0, 12)) return ArrowType.Utf8.INSTANCE;
        String lower = t.toLowerCase(Locale.ROOT);
        return switch (lower) {
            case "bool", "boolean" -> ArrowType.Bool.INSTANCE;
            case "int8", "tinyint" -> new ArrowType.Int(8, true);
            case "uint8" -> new ArrowType.Int(8, false);
            case "int16", "smallint" -> new ArrowType.Int(16, true);
            case "uint16" -> new ArrowType.Int(16, false);
            case "int32", "int", "integer" -> new ArrowType.Int(32, true);
            case "uint32" -> new ArrowType.Int(32, false);
            case "int64", "bigint" -> new ArrowType.Int(64, true);
            case "uint64" -> new ArrowType.Int(64, false);
            case "float32", "float", "float4", "real" ->
                    new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "float64", "double", "float8" ->
                    new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "date", "date32", "date64" -> new ArrowType.Date(DateUnit.DAY);
            case "datetime", "datetime64", "timestamp", "timestamptz" ->
                    new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case "bytea", "binary" -> ArrowType.Binary.INSTANCE;
            default -> decimalOrText(t, lower);
        };
    }

    /** 逐层剥 {@code Nullable(...)} / {@code LowCardinality(...)}（可叠加）。 */
    private static String stripWrappers(String typeName) {
        String t = typeName == null ? "" : typeName.trim();
        boolean changed = true;
        while (changed) {
            changed = false;
            String next = unwrap(t, "Nullable");
            if (!next.equals(t)) { t = next; changed = true; }
            next = unwrap(t, "LowCardinality");
            if (!next.equals(t)) { t = next; changed = true; }
        }
        return t;
    }

    private static ArrowType decimalOrText(String t, String lower) {
        if (lower.startsWith("decimal") || lower.startsWith("numeric")) {
            int[] ps = decimalPrecisionScale(t);
            return new ArrowType.Decimal(ps[0], ps[1], 128);
        }
        return ArrowType.Utf8.INSTANCE;
    }

    /** {@code Decimal(18, 2)} / {@code numeric(18,2)} → [precision, scale]，缺省 (38,10)。 */
    private static int[] decimalPrecisionScale(String typeName) {
        int open = typeName.indexOf('(');
        int close = typeName.lastIndexOf(')');
        if (open < 0 || close <= open) return new int[]{38, 10};
        String[] parts = typeName.substring(open + 1, close).split(",");
        try {
            int p = Integer.parseInt(parts[0].trim());
            int s = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
            return new int[]{p, s};
        } catch (NumberFormatException e) {
            return new int[]{38, 10};
        }
    }

    /** PostgreSQL {@code information_schema.columns.udt_name}（已去下划线）→ Arrow 类型。 */
    private static ArrowType pgTypeOf(String udt) {
        return switch (udt.toLowerCase(Locale.ROOT)) {
            case "bool" -> ArrowType.Bool.INSTANCE;
            case "int2" -> new ArrowType.Int(16, true);
            case "int4" -> new ArrowType.Int(32, true);
            case "int8" -> new ArrowType.Int(64, true);
            case "float4" -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "float8" -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "numeric", "money" -> new ArrowType.Decimal(38, 10, 128);
            case "date" -> new ArrowType.Date(DateUnit.DAY);
            case "timestamp", "timestamptz" -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case "bytea" -> ArrowType.Binary.INSTANCE;
            default -> ArrowType.Utf8.INSTANCE;
        };
    }

    /** 去掉 {@code Wrapper(...)} 外壳；不匹配则原样返回。 */
    private static String unwrap(String typeName, String wrapper) {
        String t = typeName.trim();
        int prefix = wrapper.length();
        if (t.length() > prefix + 1 && t.regionMatches(true, 0, wrapper + "(", 0, prefix + 1)
                && t.endsWith(")")) {
            return t.substring(prefix + 1, t.length() - 1).trim();
        }
        return t;
    }

    /** {@code Name(a, b)} → {@code a, b}（未命中返回原串）。 */
    private static String innerOf(String typeName, String name) {
        String t = typeName.trim();
        if (t.length() > name.length() + 1
                && t.regionMatches(true, 0, name + "(", 0, name.length() + 1) && t.endsWith(")")) {
            return t.substring(name.length() + 1, t.length() - 1);
        }
        return t;
    }

    /** 按顶层逗号切分（忽略括号内的逗号，如 {@code Map(String, Array(Int64))}）。 */
    static String[] splitTopLevel(String text) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(' || c == '<') depth++;
            else if (c == ')' || c == '>') depth--;
            else if (c == ',' && depth == 0) {
                parts.add(text.substring(start, i).trim());
                start = i + 1;
            }
        }
        String last = text.substring(start).trim();
        if (!last.isEmpty() || !parts.isEmpty()) parts.add(last);
        return parts.toArray(new String[0]);
    }

    // ==================== 列写入策略（两阶段批量转换） ====================

    /**
     * 列写入策略 —— 两阶段：
     * <ol>
     *   <li>{@link #allocateTemp} / {@link #extract}：行扫描存入临时数组</li>
     *   <li>{@link #writeBatch}：列批量写入 Arrow vector</li>
     * </ol>
     */
    abstract static class ColumnWriter {
        protected final int columnIndex;

        protected ColumnWriter(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        /** 分配临时存储。 */
        abstract void allocateTemp(int maxRows);

        /** 从 ResultSet 当前行提取值，写入临时数组的 {@code row} 位置。 */
        abstract void extract(ResultSet rs, int row) throws SQLException;

        /** 从临时数组批量写入 Arrow vector，返回填充后的 vector。 */
        abstract FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount);

        /**
         * 该列的 Arrow Field。默认按 JDBC 标准映射；pgvector（维度要等首批数据解析）与
         * 嵌套列（元素类型只存在于驱动类型名里）各自覆盖。
         */
        Field field(String name, ResultSetMetaData md, int colIdx) throws SQLException {
            return fieldFor(name, md.getColumnType(colIdx), md.getPrecision(colIdx),
                    md.getScale(colIdx), safeTypeName(md, colIdx));
        }

        /** 根据 JDBC 类型创建对应的 ColumnWriter。 */
        static ColumnWriter forType(int jdbcType, int columnIndex) {
            return switch (jdbcType) {
                case Types.TINYINT -> new TinyIntWriter(columnIndex);
                case Types.SMALLINT -> new SmallIntWriter(columnIndex);
                case Types.INTEGER -> new IntWriter(columnIndex);
                case Types.BIGINT -> new BigIntWriter(columnIndex);
                case Types.FLOAT, Types.REAL -> new FloatWriter(columnIndex);
                case Types.DOUBLE -> new DoubleWriter(columnIndex);
                case Types.DECIMAL, Types.NUMERIC -> new DecimalWriter(columnIndex);
                case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                     Types.CLOB, Types.NCHAR, Types.NVARCHAR, Types.NCLOB ->
                        new VarCharWriter(columnIndex);
                case Types.DATE -> new DateWriter(columnIndex);
                case Types.TIME, Types.TIME_WITH_TIMEZONE -> new TimeWriter(columnIndex);
                case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE ->
                        new TimestampWriter(columnIndex);
                case Types.BOOLEAN, Types.BIT -> new BoolWriter(columnIndex);
                case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY,
                     Types.BLOB -> new BinaryWriter(columnIndex);
                default -> new VarCharWriter(columnIndex);
            };
        }
    }

    // ==================== 具体 Writer 实现 ====================

    /** TINYINT writer：临时 byte[] → TinyIntVector。 */
    static final class TinyIntWriter extends ColumnWriter {
        private byte[] values;
        private boolean[] nulls;

        TinyIntWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new byte[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            values[row] = rs.getByte(columnIndex);
            nulls[row] = rs.wasNull();
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            TinyIntVector vec = new TinyIntVector(field, allocator);
            vec.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) vec.setNull(r);
                else vec.set(r, values[r]);
            }
            return vec;
        }
    }

    /** SMALLINT writer：临时 short[] → SmallIntVector。 */
    static final class SmallIntWriter extends ColumnWriter {
        private short[] values;
        private boolean[] nulls;

        SmallIntWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new short[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            values[row] = rs.getShort(columnIndex);
            nulls[row] = rs.wasNull();
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            SmallIntVector vec = new SmallIntVector(field, allocator);
            vec.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) vec.setNull(r);
                else vec.set(r, values[r]);
            }
            return vec;
        }
    }

    /** INT writer：临时 int[] → IntVector。 */
    static final class IntWriter extends ColumnWriter {
        private int[] values;
        private boolean[] nulls;

        IntWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new int[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            values[row] = rs.getInt(columnIndex);
            nulls[row] = rs.wasNull();
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            IntVector vec = new IntVector(field, allocator);
            vec.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) vec.setNull(r);
                else vec.set(r, values[r]);
            }
            return vec;
        }
    }

    /** BIGINT writer：临时 long[] → BigIntVector。 */
    static final class BigIntWriter extends ColumnWriter {
        private long[] values;
        private boolean[] nulls;

        BigIntWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new long[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            values[row] = rs.getLong(columnIndex);
            nulls[row] = rs.wasNull();
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            BigIntVector vec = new BigIntVector(field, allocator);
            vec.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) vec.setNull(r);
                else vec.set(r, values[r]);
            }
            return vec;
        }
    }

    /** FLOAT writer：临时 float[] → Float4Vector。 */
    static final class FloatWriter extends ColumnWriter {
        private float[] values;
        private boolean[] nulls;

        FloatWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new float[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            values[row] = rs.getFloat(columnIndex);
            nulls[row] = rs.wasNull();
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            Float4Vector vec = new Float4Vector(field, allocator);
            vec.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) vec.setNull(r);
                else vec.set(r, values[r]);
            }
            return vec;
        }
    }

    /** DOUBLE writer：临时 double[] → Float8Vector。 */
    static final class DoubleWriter extends ColumnWriter {
        private double[] values;
        private boolean[] nulls;

        DoubleWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new double[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            values[row] = rs.getDouble(columnIndex);
            nulls[row] = rs.wasNull();
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            Float8Vector vec = new Float8Vector(field, allocator);
            vec.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) vec.setNull(r);
                else vec.set(r, values[r]);
            }
            return vec;
        }
    }

    /** DECIMAL writer：临时 BigDecimal[] → DecimalVector。 */
    static final class DecimalWriter extends ColumnWriter {
        private java.math.BigDecimal[] values;
        private boolean[] nulls;

        DecimalWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new java.math.BigDecimal[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            values[row] = rs.getBigDecimal(columnIndex);
            nulls[row] = rs.wasNull();
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            DecimalVector vec = new DecimalVector(field, allocator);
            vec.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) vec.setNull(r);
                else vec.set(r, values[r]);
            }
            return vec;
        }
    }

    /** VARCHAR writer：临时 String[] → VarCharVector，预计算总字节数一次性分配。 */
    static final class VarCharWriter extends ColumnWriter {
        private String[] values;
        private boolean[] nulls;

        VarCharWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new String[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            values[row] = rs.getString(columnIndex);
            nulls[row] = rs.wasNull();
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            // 先预计算总字节数 + 编码
            byte[][] rowBytes = new byte[rowCount][];
            int totalBytes = 0;
            for (int r = 0; r < rowCount; r++) {
                if (!nulls[r] && values[r] != null) {
                    rowBytes[r] = values[r].getBytes(StandardCharsets.UTF_8);
                    totalBytes += rowBytes[r].length;
                }
            }

            // 一次性分配 exact 容量的 Arrow buffer
            VarCharVector vec = new VarCharVector(field, allocator);
            vec.allocateNew(totalBytes, rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r] || rowBytes[r] == null) {
                    vec.setNull(r);
                } else {
                    vec.set(r, rowBytes[r]);
                }
            }
            return vec;
        }
    }

    /** DATE writer：临时 int[] (epoch days) → DateDayVector。 */
    static final class DateWriter extends ColumnWriter {
        private int[] values;
        private boolean[] nulls;

        DateWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new int[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            java.sql.Date val = rs.getDate(columnIndex);
            nulls[row] = rs.wasNull();
            if (!nulls[row]) values[row] = (int) val.toLocalDate().toEpochDay();
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            DateDayVector vec = new DateDayVector(field, allocator);
            vec.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) vec.setNull(r);
                else vec.set(r, values[r]);
            }
            return vec;
        }
    }

    /** TIME writer：临时 int[] (ms of day) → TimeMilliVector。 */
    static final class TimeWriter extends ColumnWriter {
        private int[] values;
        private boolean[] nulls;

        TimeWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new int[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            java.sql.Time val = rs.getTime(columnIndex);
            nulls[row] = rs.wasNull();
            if (!nulls[row]) values[row] = (int) (val.toLocalTime().toNanoOfDay() / 1_000_000);
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            TimeMilliVector vec = new TimeMilliVector(field, allocator);
            vec.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) vec.setNull(r);
                else vec.set(r, values[r]);
            }
            return vec;
        }
    }

    /** TIMESTAMP writer：临时 long[] (epoch ms) → TimeStampMilliVector。 */
    static final class TimestampWriter extends ColumnWriter {
        private long[] values;
        private boolean[] nulls;

        TimestampWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new long[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            java.sql.Timestamp val = rs.getTimestamp(columnIndex);
            nulls[row] = rs.wasNull();
            if (!nulls[row]) {
                // 朴素 DATETIME/TIMESTAMP（无时区）按 UTC 基线存储：val.getTime() 是把墙钟当 JVM 本地时区
                // 实例（Asia/Shanghai 会 -8h），而 Arrow 无时区 TimestampMilliVector 解码固定走 UTC
                // （DateUtility.getLocalDateTimeFromEpochMilli 硬编码 UTC）→ 读到 8h 前的墙钟，
                // 导致 year()/month() 分组把每月 1 日 00:00-08:00 的行滚进上月。改成 wall-clock 的
                // UTC 基线 epoch，解码即还原墙钟，与 DATE(epochDay)/TIME(nanoOfDay) 写器约定一致。
                values[row] = val.toLocalDateTime()
                        .atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
            }
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            TimeStampMilliVector vec = new TimeStampMilliVector(field, allocator);
            vec.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) vec.setNull(r);
                else vec.set(r, values[r]);
            }
            return vec;
        }
    }

    /** BOOLEAN writer：临时 boolean[] → BitVector。 */
    static final class BoolWriter extends ColumnWriter {
        private boolean[] values;
        private boolean[] nulls;

        BoolWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new boolean[maxRows]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            values[row] = rs.getBoolean(columnIndex);
            nulls[row] = rs.wasNull();
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            BitVector vec = new BitVector(field, allocator);
            vec.allocateNew(rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) vec.setNull(r);
                else vec.set(r, values[r] ? 1 : 0);
            }
            return vec;
        }
    }

    /** BINARY writer：临时 byte[][] → VarBinaryVector。 */
    static final class BinaryWriter extends ColumnWriter {
        private byte[][] values;
        private boolean[] nulls;

        BinaryWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) { values = new byte[maxRows][]; nulls = new boolean[maxRows]; }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            values[row] = rs.getBytes(columnIndex);
            nulls[row] = rs.wasNull();
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            int totalBytes = 0;
            for (int r = 0; r < rowCount; r++) {
                if (!nulls[r] && values[r] != null) {
                    totalBytes += values[r].length;
                }
            }
            VarBinaryVector vec = new VarBinaryVector(field, allocator);
            vec.allocateNew(totalBytes, rowCount);
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r] || values[r] == null) {
                    vec.setNull(r);
                } else {
                    vec.set(r, values[r]);
                }
            }
            return vec;
        }
    }

    /**
     * 数组 / List 列 writer —— {@link java.sql.Array}、驱动直接返回的 {@code Object[]} / Collection
     * → Arrow {@code List<元素>}（元素类型由驱动类型名解析，见 {@link #fieldFor}）。
     *
     * <p>与 Hive 嵌套读同一套口径：<b>NULL 容器与空数组不同</b> —— NULL 走
     * {@code setNull}，空数组只写一段长度为 0 的 offset。</p>
     */
    static final class ListWriter extends ColumnWriter {
        private Object[][] values;
        private boolean[] nulls;

        ListWriter(int ci) {
            super(ci);
        }

        @Override void allocateTemp(int maxRows) {
            values = new Object[maxRows][];
            nulls = new boolean[maxRows];
        }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            Object raw = rs.getObject(columnIndex);
            boolean isNull = raw == null || rs.wasNull();
            nulls[row] = isNull;
            if (!isNull) values[row] = toArray(raw);
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            ListVector vec = (ListVector) field.createVector(allocator);
            FieldVector child = vec.getDataVector();
            for (int r = 0; r < rowCount; r++) {
                int start = vec.startNewValue(r);
                if (nulls[r] || values[r] == null) {
                    vec.endValue(r, 0);
                    vec.setNull(r);
                    continue;
                }
                Object[] elems = values[r];
                for (int i = 0; i < elems.length; i++) {
                    writeValue(child, start + i, elems[i]);
                }
                vec.endValue(r, elems.length);
            }
            vec.setValueCount(rowCount);
            return vec;
        }
    }

    /**
     * Map 列 writer（ClickHouse {@code Map(K,V)}）—— 驱动返回的 {@link Map}（或
     * {@code [[k,v],...]} 形态的二维数组）→ Arrow {@link MapVector}。
     */
    static final class MapWriter extends ColumnWriter {
        private Map<?, ?>[] values;
        private boolean[] nulls;

        MapWriter(int ci) {
            super(ci);
        }

        @Override void allocateTemp(int maxRows) {
            values = new Map[maxRows];
            nulls = new boolean[maxRows];
        }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            Object raw = rs.getObject(columnIndex);
            boolean isNull = raw == null || rs.wasNull();
            nulls[row] = isNull;
            if (!isNull) values[row] = toMap(raw);
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            MapVector vec = (MapVector) field.createVector(allocator);
            StructVector entries = (StructVector) vec.getDataVector();
            FieldVector keys = entries.getChildrenFromFields().get(0);
            FieldVector vals = entries.getChildrenFromFields().get(1);
            for (int r = 0; r < rowCount; r++) {
                int start = vec.startNewValue(r);
                if (nulls[r] || values[r] == null) {
                    vec.endValue(r, 0);
                    vec.setNull(r);
                    continue;
                }
                int i = start;
                for (Map.Entry<?, ?> e : values[r].entrySet()) {
                    writeValue(keys, i, e.getKey());
                    if (e.getValue() == null) vals.setNull(i);
                    else writeValue(vals, i, e.getValue());
                    // ★ entries 是非空 struct：必须逐条置位，否则引擎拷贝/溢写后整张 map 退化
                    entries.setIndexDefined(i);
                    i++;
                }
                vec.endValue(r, i - start);
            }
            vec.setValueCount(rowCount);
            return vec;
        }
    }

    // ==================== 嵌套值写入（Java 对象 → Arrow 向量） ====================

    /**
     * 递归写一行值。容器按需展开（{@code startNewValue/endValue}），标量走
     * {@link #setScalar}；NULL 一律 {@code setNull}。
     */
    static void writeValue(FieldVector vec, int index, Object value) {
        if (value == null) {
            if (vec instanceof MapVector mv) {
                mv.startNewValue(index);
                mv.endValue(index, 0);
            } else if (vec instanceof ListVector lv) {
                lv.startNewValue(index);
                lv.endValue(index, 0);
            }
            vec.setNull(index);
            return;
        }
        // MapVector 是 ListVector 的子类，必须先判 Map 再判 List
        if (vec instanceof MapVector mv) {
            int start = mv.startNewValue(index);
            Map<?, ?> entries = toMap(value);
            StructVector entryVec = (StructVector) mv.getDataVector();
            FieldVector keys = entryVec.getChildrenFromFields().get(0);
            FieldVector vals = entryVec.getChildrenFromFields().get(1);
            int i = start;
            for (Map.Entry<?, ?> e : entries.entrySet()) {
                writeValue(keys, i, e.getKey());
                if (e.getValue() == null) vals.setNull(i);
                else writeValue(vals, i, e.getValue());
                entryVec.setIndexDefined(i);
                i++;
            }
            mv.endValue(index, i - start);
        } else if (vec instanceof ListVector lv) {
            int start = lv.startNewValue(index);
            Object[] elems = toArray(value);
            FieldVector child = lv.getDataVector();
            for (int i = 0; i < elems.length; i++) {
                writeValue(child, start + i, elems[i]);
            }
            lv.endValue(index, elems.length);
        } else {
            setScalar(vec, index, value);
        }
    }

    /** 标量写入（按向量物理类型分派；字符串/二进制统一按 UTF-8 取字节）。 */
    static void setScalar(FieldVector vec, int index, Object value) {
        if (vec instanceof BitVector v) {
            v.setSafe(index, toBoolean(value) ? 1 : 0);
        } else if (vec instanceof TinyIntVector v) {
            v.setSafe(index, (byte) toLong(value));
        } else if (vec instanceof SmallIntVector v) {
            v.setSafe(index, (short) toLong(value));
        } else if (vec instanceof IntVector v) {
            v.setSafe(index, (int) toLong(value));
        } else if (vec instanceof BigIntVector v) {
            v.setSafe(index, toLong(value));
        } else if (vec instanceof Float4Vector v) {
            v.setSafe(index, (float) toDouble(value));
        } else if (vec instanceof Float8Vector v) {
            v.setSafe(index, toDouble(value));
        } else if (vec instanceof DecimalVector v) {
            v.setSafe(index, toDecimal(value, v.getScale()));
        } else if (vec instanceof DateDayVector v) {
            v.setSafe(index, (int) toEpochDay(value));
        } else if (vec instanceof TimeStampMilliVector v) {
            v.setSafe(index, toEpochMilli(value));
        } else if (vec instanceof TimeMilliVector v) {
            v.setSafe(index, (int) toMillisOfDay(value));
        } else if (vec instanceof VarCharVector v) {
            v.setSafe(index, toBytes(value));
        } else if (vec instanceof VarBinaryVector v) {
            v.setSafe(index, toBytes(value));
        } else {
            throw new IllegalStateException("嵌套列写回不支持的向量类型: " + vec.getClass().getName());
        }
    }

    /** {@code java.sql.Array} / Java 数组 / Collection → Object[]。 */
    static Object[] toArray(Object raw) {
        if (raw == null) return null;
        if (raw instanceof java.sql.Array sqlArray) {
            Object arr;
            try {
                arr = sqlArray.getArray();
            } catch (SQLException e) {
                throw new IllegalStateException("读取 SQL 数组失败", e);
            } finally {
                try {
                    sqlArray.free();
                } catch (SQLException ignored) {
                    // 部分驱动不支持 free，交给 ResultSet 关闭时释放
                }
            }
            return arr == raw ? new Object[0] : toArray(arr);
        }
        if (raw instanceof Object[] arr) return arr;
        if (raw.getClass().isArray()) {
            int n = Array.getLength(raw);
            Object[] out = new Object[n];
            for (int i = 0; i < n; i++) out[i] = Array.get(raw, i);
            return out;
        }
        if (raw instanceof Collection<?> c) return c.toArray();
        return new Object[]{raw};
    }

    /** Java Map / {@code [[k,v],...]} / {@code Map.Entry[]} → Map。 */
    static Map<?, ?> toMap(Object raw) {
        if (raw == null) return null;
        if (raw instanceof Map<?, ?> m) return m;
        Map<Object, Object> out = new LinkedHashMap<>();
        if (raw instanceof Object[] arr) {
            for (Object o : arr) {
                if (o instanceof Object[] pair && pair.length >= 2) out.put(pair[0], pair[1]);
                else if (o instanceof Map.Entry<?, ?> e) out.put(e.getKey(), e.getValue());
            }
            return out;
        }
        throw new IllegalStateException("无法把 " + raw.getClass().getName() + " 解析为 Map");
    }

    private static boolean toBoolean(Object value) {
        if (value instanceof Boolean b) return b;
        if (value instanceof Number n) return n.doubleValue() != 0d;
        return Boolean.parseBoolean(value.toString().trim());
    }

    private static long toLong(Object value) {
        if (value instanceof Number n) return n.longValue();
        if (value instanceof Boolean b) return b ? 1L : 0L;
        String s = value.toString().trim();
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return new BigDecimal(s).longValue();
        }
    }

    private static double toDouble(Object value) {
        if (value instanceof Number n) return n.doubleValue();
        if (value instanceof Boolean b) return b ? 1d : 0d;
        return Double.parseDouble(value.toString().trim());
    }

    private static BigDecimal toDecimal(Object value, int scale) {
        if (value instanceof BigDecimal bd) return bd.setScale(scale, java.math.RoundingMode.HALF_UP);
        if (value instanceof Number n) return BigDecimal.valueOf(n.doubleValue())
                .setScale(scale, java.math.RoundingMode.HALF_UP);
        return new BigDecimal(value.toString().trim()).setScale(scale, java.math.RoundingMode.HALF_UP);
    }

    private static byte[] toBytes(Object value) {
        if (value instanceof byte[] b) return b;
        if (value instanceof String s) return s.getBytes(StandardCharsets.UTF_8);
        // 兜底标量列（如 ClickHouse Array(Tuple(...))：元素没有独立 Arrow 类型）
        // 也按文本渲染，避免写出 [Ljava.lang.Object;@1a2b3c 这种垃圾
        return renderText(value).getBytes(StandardCharsets.UTF_8);
    }

    /** 嵌套 Java 容器 → 可读文本（{@code [1,"a"]} / {@code {"k":1}}），标量走 toString。 */
    private static String renderText(Object value) {
        StringBuilder sb = new StringBuilder();
        appendText(sb, value);
        return sb.toString();
    }

    private static void appendText(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof Map<?, ?> map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                appendText(sb, e.getKey());
                sb.append(':');
                appendText(sb, e.getValue());
            }
            sb.append('}');
        } else if (value instanceof Object[] arr) {
            sb.append('[');
            for (int i = 0; i < arr.length; i++) {
                if (i > 0) sb.append(',');
                appendText(sb, arr[i]);
            }
            sb.append(']');
        } else if (value instanceof Collection<?> collection) {
            appendText(sb, collection.toArray());
        } else if (value instanceof String s) {
            sb.append('"');
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '\\' || c == '"') sb.append('\\');
                sb.append(c);
            }
            sb.append('"');
        } else {
            sb.append(value);
        }
    }

    private static long toEpochDay(Object value) {
        if (value instanceof LocalDate d) return d.toEpochDay();
        if (value instanceof java.sql.Date d) return d.toLocalDate().toEpochDay();
        if (value instanceof Number n) return n.longValue();
        return LocalDate.parse(value.toString().trim()).toEpochDay();
    }

    private static long toEpochMilli(Object value) {
        if (value instanceof LocalDateTime t) return t.toInstant(ZoneOffset.UTC).toEpochMilli();
        if (value instanceof Instant i) return i.toEpochMilli();
        if (value instanceof java.sql.Timestamp ts) return ts.getTime();
        if (value instanceof java.util.Date d) return d.getTime();
        if (value instanceof Number n) return n.longValue();
        return java.sql.Timestamp.valueOf(value.toString().trim()).getTime();
    }

    private static long toMillisOfDay(Object value) {
        if (value instanceof java.sql.Time t) return t.getTime() % 86_400_000L;
        if (value instanceof java.time.LocalTime t) {
            return t.toSecondOfDay() * 1000L + t.getNano() / 1_000_000L;
        }
        if (value instanceof Number n) return n.longValue();
        return java.sql.Time.valueOf(value.toString().trim()).getTime() % 86_400_000L;
    }

    /**
     * pgvector vector writer：把 PG 返回的 "[v1,v2,...]" 文本解析为 float[]，
     * 批量写入 FixedSizeList&lt;Float32&gt; 列（维度由首个非空行解析，schema 在 Phase 1 后构建）。
     */
    static final class PgVectorWriter extends ColumnWriter {
        private float[][] values;
        private boolean[] nulls;
        private int dim = -1;

        PgVectorWriter(int ci) { super(ci); }

        @Override void allocateTemp(int maxRows) {
            values = new float[maxRows][];
            nulls = new boolean[maxRows];
        }

        @Override void extract(ResultSet rs, int row) throws SQLException {
            String s = rs.getString(columnIndex);
            nulls[row] = rs.wasNull();
            if (!nulls[row] && s != null) {
                float[] v = parseVector(s);
                values[row] = v;
                if (dim < 0) dim = v.length;
            }
        }

        /** 维度来自 Phase 1 解析（首批数据），不能走标准 JDBC 映射。 */
        @Override Field field(String name, ResultSetMetaData md, int colIdx) {
            return new Field(name, FieldType.nullable(arrowType()), childFields());
        }

        @Override FieldVector writeBatch(BufferAllocator allocator, Field field, int rowCount) {
            int listSize = Math.max(0, dim);
            int schemaListSize = ((ArrowType.FixedSizeList) field.getType()).getListSize();
            if (schemaListSize != listSize) {
                throw new IllegalStateException("vector 列维度解析不一致: schema=" + schemaListSize
                        + ", 实际=" + listSize + "（首批若全为 NULL 无法推断维度）");
            }
            // Arrow 15：FixedSizeListVector 构造需 CallBack；子向量默认是 ZeroVector，必须 initializeChildrenFromFields 建出 Float4Vector
            FixedSizeListVector vec = new FixedSizeListVector(field, allocator, new CallBack() {
                @Override public void doWork() { }
            });
            vec.initializeChildrenFromFields(field.getChildren());
            // allocateNew() 无参重载，先用 setInitialCapacity 把 parent validity 与 child 容量定为 rowCount / rowCount*listSize
            vec.setInitialCapacity(rowCount);
            vec.allocateNew();
            Float4Vector child = (Float4Vector) vec.getDataVector();
            // parent validity 全置有效；空行用 setNull 清位（allocateNew 后 validity 是全 0，必须预填）
            int parentBytes = (rowCount + 7) / 8;
            ArrowBuf parentVal = vec.getValidityBuffer();
            for (int b = 0; b < parentBytes; b++) {
                parentVal.setByte(b, (byte) 0xFF);
            }
            // child validity 全置有效：setSafe(int,float) 只写 value buffer、不动 validity，预填保证非空行被识别为有效
            int childElems = rowCount * listSize;
            ArrowBuf childVal = child.getValidityBuffer();
            for (int b = 0; b < (childElems + 7) / 8; b++) {
                childVal.setByte(b, (byte) 0xFF);
            }
            for (int r = 0; r < rowCount; r++) {
                if (nulls[r]) {
                    vec.setNull(r);
                } else {
                    float[] vals = values[r];
                    for (int e = 0; e < listSize; e++) {
                        child.setSafe(r * listSize + e, vals[e]);
                    }
                }
            }
            vec.setValueCount(rowCount);
            return vec;
        }

        ArrowType arrowType() {
            // Arrow 15 的 FixedSizeList 类型只携带 listSize，元素类型在 child Field 上
            return new ArrowType.FixedSizeList(Math.max(0, dim));
        }

        List<Field> childFields() {
            return List.of(new Field("item",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                    null));
        }

        /** 解析 pgvector 文本 "[v1,v2,...]"（支持 NaN/Infinity）。 */
        static float[] parseVector(String s) {
            String t = s.trim();
            if (t.length() >= 2 && t.charAt(0) == '[' && t.charAt(t.length() - 1) == ']') {
                t = t.substring(1, t.length() - 1);
            }
            if (t.isBlank()) return new float[0];
            String[] parts = t.split(",");
            float[] out = new float[parts.length];
            for (int i = 0; i < parts.length; i++) {
                out[i] = Float.parseFloat(parts[i].trim());
            }
            return out;
        }
    }

    // ==================== 重置 ====================

    /** 清除缓存的 Schema，下次 {@link #convertNextBatch(ResultSet)} 会重新构建。 */
    public void reset() {
        cachedSchema = null;
        cachedWriters = null;
    }

    public Schema getCachedSchema() {
        return cachedSchema;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
