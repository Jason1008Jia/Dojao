package com.dojao.connector.common.util;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Arrow VectorSchemaRoot → Hive 分隔文本文件（TEXTFILE / LazySimpleSerDe）的写入器
 * （{@link TextFileArrowConverter} 的反向）。
 *
 * <p>一行一条记录、字段按分隔符拼接、行尾 {@code \n}；NULL 写 {@code \N}；转义符与分隔符
 * 在值内加转义。默认参数即 Hive 的
 * {@code field.delim=\001 / escape.delim=\\ / serialization.null.format=\N}，与读侧同一口径。</p>
 *
 * <p>值里的换行符会<b>直接报错</b>：文本格式以换行分行，写出去会把一条记录拆成两条 ——
 * 需要存换行请改 ORC / Parquet 目标表。</p>
 *
 * <p>值格式尽量贴近 Hive 自身的文本输出：boolean → {@code true/false}、decimal →
 * {@code toPlainString()}、date → {@code yyyy-MM-dd}、timestamp → {@code yyyy-MM-dd HH:mm:ss[.SSS]}
 * （无毫秒时省略小数，Hive 默认 {@code timestamp.formats} 可直接解析）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public class TextArrowWriter implements ArrowFileWriter {

    private static final DateTimeFormatter TS_SECONDS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TS_MILLIS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final BufferedWriter out;
    private final List<Field> dataFields;
    /** 文本文件列 → 源（Page）向量下标。 */
    private final int[] sourceIndexes;
    private final char delimiter;
    private final char escape;
    private final String nullString;
    private final StringBuilder line = new StringBuilder(256);
    private boolean closed;

    public TextArrowWriter(Configuration conf, Path path, FileSystem fs,
                           List<Field> dataFields, int[] sourceIndexes) throws IOException {
        this(conf, path, fs, dataFields, sourceIndexes,
                TextFileArrowConverter.DEFAULT_DELIMITER, TextFileArrowConverter.DEFAULT_ESCAPE,
                TextFileArrowConverter.DEFAULT_NULL_STRING);
    }

    /**
     * @param dataFields    需要落文本文件的列（顺序 = 文件列序 = Hive 数据列序）
     * @param sourceIndexes 与 {@code dataFields} 一一对应，指向写入页里的列下标
     */
    public TextArrowWriter(Configuration conf, Path path, FileSystem fs,
                           List<Field> dataFields, int[] sourceIndexes,
                           char delimiter, char escape, String nullString) throws IOException {
        this.dataFields = List.copyOf(dataFields);
        this.sourceIndexes = sourceIndexes.clone();
        this.delimiter = delimiter;
        this.escape = escape;
        this.nullString = (nullString == null || nullString.isEmpty())
                ? TextFileArrowConverter.DEFAULT_NULL_STRING : nullString;
        this.out = new BufferedWriter(
                new OutputStreamWriter(fs.create(path, true), StandardCharsets.UTF_8), 64 * 1024);
    }

    @Override
    public void write(VectorSchemaRoot root) throws IOException {
        int rows = root.getRowCount();
        for (int r = 0; r < rows; r++) {
            line.setLength(0);
            for (int col = 0; col < sourceIndexes.length; col++) {
                if (col > 0) line.append(delimiter);
                FieldVector src = root.getVector(sourceIndexes[col]);
                if (r >= src.getValueCount() || src.isNull(r)) {
                    line.append(nullString);
                } else {
                    appendEscaped(valueText(src, r));
                }
            }
            out.write(line.toString());
            out.write('\n');
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        out.close();
    }

    /**
     * 转义：转义符自身与分隔符前加转义符（与读侧 {@code TextFileArrowConverter.split} 对称）。
     *
     * <p>换行符<b>直接报错</b>：LazySimpleSerDe / Hive 文本文件都以换行分行，值里的换行写出去
     * 就是「一条记录变两条」—— 与其静默写坏，不如让调用方改目标表格式。</p>
     */
    private void appendEscaped(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\n' || c == '\r') {
                throw new IllegalArgumentException("Hive 文本表不能写入含换行符的值（LazySimpleSerDe 以换行分行，"
                        + "值: " + preview(value) + "）；请改用 ORC / Parquet 目标表");
            }
            if (c == escape || c == delimiter) {
                line.append(escape);
            }
            line.append(c);
        }
    }

    private static String preview(String value) {
        String oneLine = value.replace('\n', ' ').replace('\r', ' ');
        return oneLine.length() <= 32 ? oneLine : oneLine.substring(0, 32) + "…";
    }

    private static String valueText(FieldVector v, int row) {
        if (v instanceof BitVector bv) return bv.get(row) != 0 ? "true" : "false";
        if (v instanceof TinyIntVector x) return Integer.toString(x.get(row));
        if (v instanceof SmallIntVector x) return Integer.toString(x.get(row));
        if (v instanceof IntVector x) return Integer.toString(x.get(row));
        if (v instanceof BigIntVector x) return Long.toString(x.get(row));
        if (v instanceof Float4Vector x) return Float.toString(x.get(row));
        if (v instanceof Float8Vector x) return Double.toString(x.get(row));
        if (v instanceof DecimalVector x) {
            BigDecimal d = (BigDecimal) x.getObject(row);
            return d == null ? "" : d.toPlainString();
        }
        if (v instanceof DateDayVector x) return LocalDate.ofEpochDay(x.get(row)).toString();
        if (v instanceof TimeStampMilliVector x) {
            long millis = x.get(row);
            LocalDateTime ts = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
            return (ts.getNano() == 0 ? TS_SECONDS : TS_MILLIS).format(ts);
        }
        if (v instanceof VarCharVector x) return new String(x.get(row), StandardCharsets.UTF_8);
        if (v instanceof VarBinaryVector x) return new String(x.get(row), StandardCharsets.UTF_8);
        throw new IllegalStateException("文本写入器不支持的列向量类型: " + v.getClass().getName());
    }

    /** 按文件列序把 Page 列名映射成下标；缺列抛错（宁可报错也不要错列）。 */
    public static int[] sourceIndexes(VectorSchemaRoot root, List<String> dataColumnNames) {
        List<String> names = new ArrayList<>();
        for (FieldVector v : root.getFieldVectors()) names.add(v.getField().getName());
        int[] out = new int[dataColumnNames.size()];
        for (int i = 0; i < dataColumnNames.size(); i++) {
            int idx = -1;
            for (int n = 0; n < names.size(); n++) {
                if (names.get(n).equalsIgnoreCase(dataColumnNames.get(i))) {
                    idx = n;
                    break;
                }
            }
            if (idx < 0) {
                throw new IllegalArgumentException("写入页缺少 Hive 数据列: " + dataColumnNames.get(i)
                        + "（页列: " + names + "）");
            }
            out[i] = idx;
        }
        return out;
    }
}
