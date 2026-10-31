package com.dojao.connector.hive;

import com.dojao.connector.common.util.TextFileArrowConverter;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Hive 表的物理存储格式 —— 由 {@code StorageDescriptor} 的 inputFormat / SerDe 反推，
 * 反过来也提供写入时（CTAS / INSERT）要登记的 inputFormat / outputFormat / SerDe 描述。
 *
 * <p>Dojao 对 ORC 与 Parquet <b>同等对待</b>（各自有专门的 File→Arrow 转换器，走同一条
 * {@code convertNextBatch() -> Page} 路径），不存在「偏向某一种格式」的问题；其余任何
 * SerDe（TextInputFormat + LazySimpleSerDe / OpenCSVSerde 等）统一按 {@link #TEXT}
 * 用分隔符解析读取。</p>
 *
 * <p>写侧三种格式都支持：新建表（CTAS）按 catalog 属性 {@code catalog.hive.default.format}
 * 落 ORC / Parquet / 分隔文本，已有表按目标表自身的格式写。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public enum HiveStorageFormat {

    /** ORC（{@code OrcInputFormat} / {@code OrcSerde}）。 */
    ORC,

    /** Parquet（{@code ParquetInputFormat} / {@code MapredParquetInputFormat} / {@code ParquetHiveSerDe}）。 */
    PARQUET,

    /** 其余一律按分隔文本读取（TEXTFILE / LazySimpleSerDe / RegexSerDe / OpenCSVSerde …）。 */
    TEXT;

    /**
     * 由 inputFormat 类名与 SerDe 类名判定格式（都不匹配 → {@link #TEXT} 兜底）。
     *
     * @param inputFormat 表的 {@code StorageDescriptor.inputFormat}
     * @param serdeLib    表的 {@code SerDeInfo.serializationLib}
     */
    public static HiveStorageFormat detect(String inputFormat, String serdeLib) {
        String in = inputFormat == null ? "" : inputFormat.toLowerCase(Locale.ROOT);
        String serde = serdeLib == null ? "" : serdeLib.toLowerCase(Locale.ROOT);
        if (in.contains("orc") || serde.contains("orc")) return ORC;
        if (in.contains("parquet") || serde.contains("parquet")) return PARQUET;
        return TEXT;
    }

    /**
     * 把 catalog 属性值（{@code orc} / {@code parquet} / {@code text} / {@code textfile}）解析成格式。
     *
     * @throws IllegalArgumentException 取值无法识别（报错而不是默默回退，避免写错格式）
     */
    public static HiveStorageFormat fromCatalogValue(String raw) {
        String v = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
        return switch (v) {
            case "orc" -> ORC;
            case "parquet" -> PARQUET;
            case "text", "textfile" -> TEXT;
            default -> throw new IllegalArgumentException("无法识别的 catalog.hive.default.format 取值: "
                    + raw + "（可选 orc / parquet / text）");
        };
    }

    /** 写进 HMS 的 {@code StorageDescriptor.inputFormat}。 */
    public String inputFormat() {
        return switch (this) {
            case ORC -> "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
            case PARQUET -> "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
            case TEXT -> "org.apache.hadoop.mapred.TextInputFormat";
        };
    }

    /** 写进 HMS 的 {@code StorageDescriptor.outputFormat}。 */
    public String outputFormat() {
        return switch (this) {
            case ORC -> "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
            case PARQUET -> "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
            case TEXT -> "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
        };
    }

    /** 写进 HMS 的 {@code SerDeInfo.serializationLib}。 */
    public String serdeLib() {
        return switch (this) {
            case ORC -> "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
            case PARQUET -> "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
            case TEXT -> "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
        };
    }

    /**
     * 写进 HMS 的 SerDe 参数：ORC / Parquet 只需 {@code serialization.format=1}；
     * 文本额外给分隔符 / 行分隔 / 转义 / NULL 串（与读侧同一默认）。
     */
    public Map<String, String> serdeParameters() {
        Map<String, String> params = new LinkedHashMap<>();
        if (this == TEXT) {
            char delim = TextFileArrowConverter.DEFAULT_DELIMITER;
            params.put("field.delim", String.valueOf(delim));
            params.put("serialization.format", String.valueOf(delim));
            params.put("line.delim", "\n");
            params.put("escape.delim", String.valueOf(TextFileArrowConverter.DEFAULT_ESCAPE));
            params.put("serialization.null.format", TextFileArrowConverter.DEFAULT_NULL_STRING);
        } else {
            params.put("serialization.format", "1");
        }
        return params;
    }

    /** 数据文件扩展名（Hive 按 inputFormat 读，扩展名只影响可读性）。 */
    public String fileSuffix() {
        return switch (this) {
            case ORC -> ".orc";
            case PARQUET -> ".parquet";
            case TEXT -> ".txt";
        };
    }
}
