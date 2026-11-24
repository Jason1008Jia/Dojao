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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 分隔文本文件 → Arrow 批量转换器（Hive TEXTFILE / LazySimpleSerDe 这一类）。
 *
 * <p>文本文件不自描述 schema，所以必须由调用方给出目标 Arrow {@link Schema}（Hive 侧取自
 * MetaStore 的列定义）以及分隔符 / 转义符 / NULL 串。</p>
 *
 * <p>逐行按分隔符切分（支持单字符转义），按目标列类型做解析；解析失败或 {@code \N} 记为 NULL。
 * 行内字段少于列数 → 补 NULL；多于列数 → 忽略多余字段。</p>
 *
 * <p><b>分批口径</b>：行数（{@code batchSize}）与字节（{@link #DEFAULT_MAX_BATCH_BYTES}）双上界，
 * 先到先收批 —— 宽行表（如 ES 落地的 JSON 全量串）只按行数分批会产出几百 MB 的页，回传链路
 * 同时持有源页 / 流根拷贝 / Master 入站缓冲 / copyPage 拷贝，几页即吃穿分配器。</p>
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
    /**
     * 单批<b>字节</b>上界（默认 32MB，与 {@code JdbcArrowConverter#DEFAULT_MAX_BATCH_BYTES} 同口径）：
     * 只按行数分批时，宽行（大 JSON / 长文本）会出现「10000 行一批 = 几百 MB 的 Arrow 页」——
     * 页在回传链路上至少被复制两次（Worker 流根、Master 入站 + {@code copyPage}），
     * 几页就能把分配器吃穿（线上 {@code staging_es_to_skb_info} 的 {@code raw_json_data} 即此形态）。
     * 行数到顶或字节到顶都收批 —— 页小一点只多几次网络往返，爆内存则查询直接失败。
     */
    public static final long DEFAULT_MAX_BATCH_BYTES = 32L * 1024 * 1024;

    private final BufferAllocator allocator;
    private final int batchSize;
    private final long maxBatchBytes;
    private final Schema arrowSchema;
    private final char delimiter;
    private final char escape;
    private final String nullString;
    private final LineReader reader;
    private boolean exhausted;
    private boolean shapeChecked;

    public TextFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs,
                                  Schema arrowSchema, int batchSize) throws IOException {
        this(allocator, path, fs, arrowSchema, DEFAULT_DELIMITER, DEFAULT_ESCAPE,
                DEFAULT_NULL_STRING, batchSize);
    }

    public TextFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs,
                                  Schema arrowSchema, char delimiter, char escape,
                                  String nullString, int batchSize) throws IOException {
        this(allocator, path, fs, arrowSchema, delimiter, escape, nullString, batchSize,
                DEFAULT_MAX_BATCH_BYTES);
    }

    public TextFileArrowConverter(BufferAllocator allocator, Path path, FileSystem fs,
                                  Schema arrowSchema, char delimiter, char escape,
                                  String nullString, int batchSize, long maxBatchBytes)
            throws IOException {
        this.allocator = allocator;
        this.batchSize = batchSize;
        this.maxBatchBytes = maxBatchBytes > 0 ? maxBatchBytes : DEFAULT_MAX_BATCH_BYTES;
        this.arrowSchema = arrowSchema;
        this.delimiter = delimiter;
        this.escape = escape;
        this.nullString = (nullString == null || nullString.isEmpty())
                ? DEFAULT_NULL_STRING : nullString;
        this.reader = new LineReader(openDataStream(fs, path), LINE_BUFFER_BYTES);
    }

    // ==================== 压缩 ====================

    /**
     * 每份 {@link Configuration} 一个编解码器工厂：构造它要按 {@code io.compression.codecs}
     * 实例化全部 codec（含探测本地库），逐文件新建在上千分片的表上是纯浪费。
     * 按配置对象 identity 缓存（每个 catalog 一份 conf，条目数受控）。
     */
    private static final Map<Configuration, CompressionCodecFactory> CODEC_FACTORIES =
            Collections.synchronizedMap(new IdentityHashMap<>());

    private static CompressionCodecFactory codecFactory(Configuration conf) {
        synchronized (CODEC_FACTORIES) {
            CompressionCodecFactory factory = CODEC_FACTORIES.get(conf);
            if (factory == null) {
                factory = new CompressionCodecFactory(conf);
                CODEC_FACTORIES.put(conf, factory);
            }
            return factory;
        }
    }

    /**
     * 打开数据流，并按文件名后缀自动解压（{@code .gz} / {@code .bz2} / {@code .deflate} / {@code .snappy} …）。
     *
     * <p><b>必须解压</b>：Hive 的 TEXTFILE 也是先过 Hadoop 的 {@link CompressionCodecFactory}。
     * 不解压时 gzip 字节会被当成 UTF-8 明文解码 —— 行被随机的 {@code 0x0A} 字节切碎、每个字段都是
     * 乱码（线上 {@code staging_es_to_skb_info} 的 {@code part-*.txt.gz} 就是这个形态）。
     * 后缀明示是压缩格式但本机没有对应 codec 时<b>报错</b>，绝不把压缩字节当明文吐出去。</p>
     */
    private static InputStream openDataStream(FileSystem fs, Path path) throws IOException {
        Configuration conf = fs.getConf() != null ? fs.getConf() : new Configuration();
        CompressionCodec codec = codecFactory(conf).getCodec(path);
        boolean gzip = looksGzip(path.getName());
        if (codec == null && !gzip && looksCompressed(path.getName())) {
            throw new IOException("Hive 文本文件是压缩格式，但本机没有对应的解压 codec："
                    + path + "（把对应 hadoop-* codec 加入 classpath，或先解压后再读）");
        }
        // 1MB 读缓冲：文本表动辄几百个小文件，逐文件的小块 HDFS read 调用可观
        InputStream raw = fs.open(path, READ_BUFFER_BYTES);
        // gzip 走 JDK 的 GZIPInputStream：与 Hadoop GzipCodec 实测同速（本机 native zlib 未加载时
        // 两者都是纯 Java 解码，56MB 样本 1.0x），但 JDK 路径不依赖 io.compression.codecs 注册表
        // —— 用户自定义 codec 列表时 .gz 仍能读。
        if (gzip) {
            return new java.util.zip.GZIPInputStream(raw, READ_BUFFER_BYTES);
        }
        if (codec == null) {
            return raw;
        }
        try {
            return codec.createInputStream(raw);
        } catch (IOException | RuntimeException e) {
            try { raw.close(); } catch (IOException ignored) { }
            throw e;
        }
    }

    /** 文本读取的缓冲大小（HDFS 读缓冲 / gzip 解压缓冲共用）。 */
    private static final int READ_BUFFER_BYTES = 1024 * 1024;

    /**
     * 字节级按行读取器：直接在字节缓冲里找 {@code \n} / {@code \r}，每行做一次
     * {@code new String(bytes, UTF_8)}。
     *
     * <p>等价于 {@code new BufferedReader(new InputStreamReader(in, UTF_8))} 的 {@code readLine()}
     * （同样把 {@code \n} / {@code \r} / {@code \r\n} 当行尾），但省掉 InputStreamReader 的
     * char 缓冲与 readLine 的逐字符拷贝。文本表读取是逐行的热循环，实测整条读取链路快约 20%；
     * 超过初始缓冲的行会翻倍扩容（上限 {@link #MAX_LINE_BYTES}）。</p>
     */
    static final class LineReader implements java.io.Closeable {

        private final InputStream in;
        private byte[] buf;
        private int pos;
        private int limit;
        private boolean eof;
        /** 上一行在文件里的 UTF-8 字节数（含行尾）—— 供 {@link #convertNextBatch} 的字节上界计数。 */
        private long lastLineBytes;

        LineReader(InputStream in, int bufferBytes) {
            this.in = in;
            this.buf = new byte[Math.max(1024, bufferBytes)];
        }

        /** @return 下一行（不含行尾），没有更多数据时返回 null。 */
        String readLine() throws IOException {
            while (true) {
                if (pos >= limit && !fill()) {
                    return null;
                }
                int start = pos;
                for (int i = start; i < limit; i++) {
                    byte b = buf[i];
                    if (b != '\n' && b != '\r') {
                        continue;
                    }
                    String line = new String(buf, start, i - start, StandardCharsets.UTF_8);
                    int consumed = i - start + 1;
                    pos = i + 1;
                    if (b == '\r') {
                        // CRLF 时吃掉紧跟的 '\n'（可能落在下一次读块的第一个字节）
                        if (pos >= limit) {
                            fill();
                        }
                        if (pos < limit && buf[pos] == '\n') {
                            pos++;
                            consumed++;
                        }
                    }
                    lastLineBytes = consumed;
                    return line;
                }
                // 本块内没有行尾 → 把这段行首挪到 0、必要时扩容，再读更多字节
                if (!fill()) {
                    String last = new String(buf, 0, limit, StandardCharsets.UTF_8);
                    lastLineBytes = limit;
                    pos = limit;
                    return last;
                }
            }
        }

        /** 上一行读入的字节数（含行尾；末行无行尾 = 行内容字节数）。 */
        long lastLineBytes() {
            return lastLineBytes;
        }

        /** 把 {@code [pos, limit)} 挪到 0 并至少读到一个新字节；EOF 返回 false。 */
        private boolean fill() throws IOException {
            if (pos > 0) {
                System.arraycopy(buf, pos, buf, 0, limit - pos);
                limit -= pos;
                pos = 0;
            }
            if (eof) {
                return false;
            }
            if (limit == buf.length) {
                if (buf.length >= MAX_LINE_BYTES) {
                    throw new IOException("文本文件单行超过 " + MAX_LINE_BYTES + " 字节，无法按行切分");
                }
                buf = java.util.Arrays.copyOf(buf, (int) Math.min((long) buf.length * 2, MAX_LINE_BYTES));
            }
            int n;
            do {
                n = in.read(buf, limit, buf.length - limit);
            } while (n == 0);
            if (n < 0) {
                eof = true;
                return false;
            }
            limit += n;
            return true;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /** 行缓冲初始大小（超过则翻倍扩容）。 */
    private static final int LINE_BUFFER_BYTES = 256 * 1024;
    /** 单行字节上限（防止畸形文件把内存吃穿）。 */
    private static final int MAX_LINE_BYTES = 64 * 1024 * 1024;

    private static boolean looksGzip(String name) {
        String n = name.toLowerCase(Locale.ROOT);
        return n.endsWith(".gz") || n.endsWith(".gzip");
    }

    /** 常见压缩后缀（Hadoop codec 未注册时据此响亮失败，而不是静默读乱码）。 */
    private static boolean looksCompressed(String name) {
        String n = name.toLowerCase(Locale.ROOT);
        return n.endsWith(".gz") || n.endsWith(".gzip") || n.endsWith(".bz2") || n.endsWith(".deflate")
                || n.endsWith(".snappy") || n.endsWith(".lz4") || n.endsWith(".zst")
                || n.endsWith(".zstd") || n.endsWith(".lzo");
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
        long bytes = 0L;
        String line;
        while (rowCount < batchSize && (line = reader.readLine()) != null) {
            bytes += reader.lastLineBytes();
            fillRow(vectors, line, rowCount);
            rowCount++;
            if (bytes >= maxBatchBytes) break;   // 行数到顶或字节到顶都收批
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
        if (!shapeChecked) {
            shapeChecked = true;
            warnShapeIfMismatched(vectors.size(), fields.length);
        }
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
            // Arrow 要求 BigDecimal 的 scale 与列定义完全一致：文本里的 "123"（scale 0）
            // 或小数位少于列定义的值直接 setSafe 会抛 UnsupportedOperationException: 0 != 2。
            // 按列 scale 补齐（与 Hive 一致，超出位数按 HALF_UP 收敛）。
            BigDecimal bd = new BigDecimal(s);
            int scale = v.getScale();
            if (bd.scale() != scale) {
                bd = bd.setScale(scale, RoundingMode.HALF_UP);
            }
            v.setSafe(row, bd);
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

    /** 已报警过的「列数 / 切分段数」组合（同表同形态只报一次，别刷屏）。 */
    private static final java.util.Set<String> SHAPE_WARNED =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * 首行切出来的段数与表列数不一致 → 大声告警。
     *
     * <p>分隔符取错（比如 SerDe 里 {@code serialization.format=1} 被当成字符 {@code '1'}）时，
     * 数据不会报错，只会<b>静默串列</b>；这里把「按分隔符切成几段 vs 表有几列」直接摆出来，
     * 一眼能看出分隔符不对。</p>
     */
    private void warnShapeIfMismatched(int columns, int actual) {
        if (columns <= 0 || actual == columns || SHAPE_WARNED.size() > 200) {
            return;
        }
        String key = columns + "/" + actual + "/" + (int) delimiter;
        if (!SHAPE_WARNED.add(key)) {
            return;
        }
        System.err.printf("[文本表] 首行按分隔符切成 %d 段，与表列数 %d 不一致（分隔符=%s，转义=%s）"
                        + " → 元数据里的分隔符可能不对，请核对 SerDe 参数 field.delim /"
                        + " serialization.format；按错误分隔符切分会静默串列%n",
                actual, columns,
                Character.isISOControl(delimiter) ? String.format("U+%04X", (int) delimiter)
                        : "'" + delimiter + "'",
                Character.isISOControl(escape) ? String.format("U+%04X", (int) escape) : "'" + escape + "'");
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
    public long getMaxBatchBytes() { return maxBatchBytes; }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
