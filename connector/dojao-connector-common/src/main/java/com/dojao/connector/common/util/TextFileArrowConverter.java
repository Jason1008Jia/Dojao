package com.dojao.connector.common.util;

import com.dojao.common.data.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * 分隔文本文件 → Arrow 批量转换器（Hive TEXTFILE / LazySimpleSerDe 这一类）。
 *
 * <p>文本文件不自描述 schema，所以必须由调用方给出目标 Arrow {@link Schema}（Hive 侧取自
 * MetaStore 的列定义）以及分隔符 / 转义符 / NULL 串。</p>
 *
 * <p>逐行按分隔符切分（支持单字符转义），按目标列类型做解析；解析失败或 {@code \N} 记为 NULL。
 * 行内字段少于列数 → 补 NULL；多于列数 → 忽略多余字段。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public class TextFileArrowConverter implements AutoCloseable {

    /** Hive 默认 NULL 串。 */
    public static final String DEFAULT_NULL_STRING = "\\N";
    /** Hive 默认字段分隔符（Ctrl-A）。 */
    public static final char DEFAULT_DELIMITER = '\001';
    /** Hive 默认转义符。 */
    public static final char DEFAULT_ESCAPE = '\\';

    private final BufferAllocator allocator;
    private final int batchSize;
    private final Schema arrowSchema;
    private final char delimiter;
    private final char escape;
    private final String nullString;
    private final BufferedReader reader;
    private boolean exhausted;

    public TextFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs,
                                  Schema arrowSchema, int batchSize) throws IOException {
        this(allocator, path, fs, arrowSchema, DEFAULT_DELIMITER, DEFAULT_ESCAPE,
                DEFAULT_NULL_STRING, batchSize);
    }

    public TextFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs,
                                  Schema arrowSchema, char delimiter, char escape,
                                  String nullString, int batchSize) throws IOException {
        this.allocator = allocator;
        this.batchSize = batchSize;
        this.arrowSchema = arrowSchema;
        this.delimiter = delimiter;
        this.escape = escape;
        this.nullString = (nullString == null || nullString.isEmpty())
                ? DEFAULT_NULL_STRING : nullString;
        this.reader = new BufferedReader(
                new InputStreamReader(fs.open(path), StandardCharsets.UTF_8));
    }

    public TextFileArrowConverter(BufferAllocator allocator, Path path, Configuration conf,
                                  Schema arrowSchema, int batchSize) throws IOException {
        this(allocator, path, path.getFileSystem(conf), arrowSchema, batchSize);
    }

    // ==================== 批次转换 ====================

    public Page convertNextBatch() throws IOException {
        if (exhausted) return Page.eof();

        List<FieldVector> vectors = createVectors(arrowSchema);
        int rowCount = 0;
        String line;
        while (rowCount < batchSize && (line = reader.readLine()) != null) {
            // 末尾 \r（CRLF 文件）裁掉
            if (!line.isEmpty() && line.charAt(line.length() - 1) == '\r') {
                line = line.substring(0, line.length() - 1);
            }
            fillRow(vectors, line, rowCount);
            rowCount++;
        }
        if (rowCount == 0) {
            exhausted = true;
            for (FieldVector v : vectors) v.close();
            return Page.eof();
        }

        for (FieldVector v : vectors) {
            v.setValueCount(rowCount);
        }
        VectorSchemaRoot root = new VectorSchemaRoot(arrowSchema.getFields(), vectors, rowCount);
        return Page.fromRoot(root);
    }

    // ==================== 行填充 ====================

    private void fillRow(List<FieldVector> vectors, String line, int row) {
        String[] fields = split(line, delimiter, escape, nullString);
        for (int col = 0; col < vectors.size(); col++) {
            FieldVector vec = vectors.get(col);
            if (col >= fields.length || fields[col] == null || nullString.equals(fields[col])) {
                vec.setNull(row);
                continue;
            }
            try {
                fillValue(vec, fields[col], row);
            } catch (RuntimeException e) {
                // 解析失败（脏数据 / 类型不符）→ 该字段记 NULL，不让一行坏数据打断整批
                vec.setNull(row);
            }
        }
    }

    /**
     * 按目标列类型把一个字符串字面量解析进向量第 {@code row} 行。
     *
     * <p><b>公开</b>供分区列常量物化复用：Hive 的分区列不在数据文件里，其值来自路径
     * {@code key=value}，最终同样要按列类型解析后逐行写入（见 {@code HiveConnector}）。</p>
     */
    public static void fillValue(FieldVector vec, String raw, int row) {
        // 字符串列保留原样（Hive 的 LazySimpleSerDe 也不 trim 字符串）；数值 / 日期等解析前再 trim
        if (vec instanceof VarCharVector v) {
            v.setSafe(row, raw.getBytes(StandardCharsets.UTF_8));
            return;
        }
        if (vec instanceof VarBinaryVector v) {
            v.setSafe(row, raw.getBytes(StandardCharsets.UTF_8));
            return;
        }
        String s = raw.trim();
        if (vec instanceof BitVector v) {
            v.setSafe(row, parseBool(s) ? 1 : 0);
        } else if (vec instanceof TinyIntVector v) {
            v.setSafe(row, (byte) Long.parseLong(s));
        } else if (vec instanceof SmallIntVector v) {
            v.setSafe(row, (short) Long.parseLong(s));
        } else if (vec instanceof IntVector v) {
            v.setSafe(row, Integer.parseInt(s));
        } else if (vec instanceof BigIntVector v) {
            v.setSafe(row, Long.parseLong(s));
        } else if (vec instanceof Float4Vector v) {
            v.setSafe(row, Float.parseFloat(s));
        } else if (vec instanceof Float8Vector v) {
            v.setSafe(row, Double.parseDouble(s));
        } else if (vec instanceof DecimalVector v) {
            v.setSafe(row, new BigDecimal(s));
        } else if (vec instanceof DateDayVector v) {
            v.setSafe(row, (int) LocalDate.parse(s).toEpochDay());
        } else if (vec instanceof TimeStampMilliVector v) {
            v.setSafe(row, parseTimestampMillis(s));
        }
    }

    private static boolean parseBool(String s) {
        return "true".equalsIgnoreCase(s) || "1".equals(s) || "t".equalsIgnoreCase(s)
                || "yes".equalsIgnoreCase(s) || "y".equalsIgnoreCase(s);
    }

    /** 支持 {@code yyyy-MM-dd HH:mm:ss[.fff]} 与 {@code yyyy-MM-dd'T'HH:mm:ss[.fff]}。 */
    static long parseTimestampMillis(String s) {
        String normalized = s.indexOf('T') >= 0 ? s.replace('T', ' ') : s;
        try {
            return java.sql.Timestamp.valueOf(normalized).getTime();
        } catch (IllegalArgumentException e) {
            // 仅日期 → 当天 00:00:00
            return LocalDate.parse(s).atStartOfDay()
                    .atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
    }

    // ==================== 切分 ====================

    /**
     * 按分隔符切分，支持单字符转义（{@code escape} 后一个字符按字面处理）。
     * 保留空字段（与 Hive LazySimpleSerDe 一致：空串是合法值，NULL 由 {@code \N} 表达）。
     *
     * <p><b>NULL 判在转义之前</b>：{@code \N} 里的 {@code \} 同时是转义符，先转义就会被解成
     * {@code N}；因此每个字段同时累积「文件里的原文」，原文与 {@code nullString} 逐字相等时
     * 该字段返回 {@code null}（由调用方写成 Arrow NULL）。</p>
     */
    static String[] split(String line, char delimiter, char escape, String nullString) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        StringBuilder raw = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == delimiter && !escaped) {
                out.add(segment(cur, raw, nullString));
                cur.setLength(0);
                raw.setLength(0);
                continue;
            }
            raw.append(c);
            if (escaped) {
                cur.append(c);
                escaped = false;
            } else if (c == escape) {
                escaped = true;
            } else {
                cur.append(c);
            }
        }
        out.add(segment(cur, raw, nullString));
        return out.toArray(new String[0]);
    }

    private static String segment(StringBuilder decoded, StringBuilder raw, String nullString) {
        return raw.toString().equals(nullString) ? null : decoded.toString();
    }

    // ==================== Vector 创建 ====================

    private List<FieldVector> createVectors(Schema schema) {
        List<FieldVector> vectors = new ArrayList<>();
        for (Field field : schema.getFields()) {
            vectors.add(field.createVector(allocator));
        }
        return vectors;
    }

    // ==================== 访问器 ====================

    public Schema getArrowSchema() { return arrowSchema; }
    public int getBatchSize() { return batchSize; }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
