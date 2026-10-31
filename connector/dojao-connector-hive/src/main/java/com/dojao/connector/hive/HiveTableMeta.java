package com.dojao.connector.hive;

import com.dojao.spi.bean.Column;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 一张 Hive 表的读侧元数据快照（由 {@link HiveMetastore} 从 HMS 翻译而来，之后不再回查 HMS）。
 *
 * <p>关键点：<b>分区列不在数据文件里</b>。ORC/Parquet/文本文件只存常规列，分区列的值来自
 * 文件所在目录的 {@code key=value} 路径段。因此这里把列拆成
 * {@link #dataColumns()}（文件里有的）与 {@link #partitionColumns()}（要从路径补的），
 * 读路径按此把分区列物化成常量向量拼回表结构。</p>
 *
 * @param location         表根 location（分区表的「分区父目录」也在此之下）。
 * @param transactional    Hive ACID（事务表）标记 —— 本次只做「识别 + 明确拒绝」，真正的合并读法留待后续实现。
 * @param dataColumns      常规列（数据文件里的列，顺序即文件内列序）。
 * @param partitionColumns 分区列（值从路径补，顺序即 Hive 分区定义序）。
 * @param bucketCols       分桶列名（{@code CLUSTERED BY}，保序）；无分桶为空。
 * @param numBuckets       桶数（{@code INTO N BUCKETS}）；无分桶为 0。
 * @param sortCols         分桶内排序列（{@code SORTED BY}）；无为空。当前仅留存元数据，读路径不用。
 * @param textDelimiter    ---- 仅 TEXT 格式有意义：SerDe 分隔参数 ----
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public record HiveTableMeta(String catalogName, String database, String table, String location,
                            HiveStorageFormat format, String inputFormat, String serdeLib, boolean transactional,
                            Map<String, String> parameters, List<Column> dataColumns, List<Column> partitionColumns,
                            List<String> bucketCols, int numBuckets, List<String> sortCols,
                            char textDelimiter, char textEscape, String textNullString) {

    public HiveTableMeta(String catalogName, String database, String table, String location,
                         HiveStorageFormat format, String inputFormat, String serdeLib,
                         boolean transactional, Map<String, String> parameters,
                         List<Column> dataColumns, List<Column> partitionColumns,
                         List<String> bucketCols, int numBuckets, List<String> sortCols,
                         char textDelimiter, char textEscape, String textNullString) {
        this.catalogName = catalogName;
        this.database = database;
        this.table = table;
        this.location = location;
        this.format = format;
        this.inputFormat = inputFormat;
        this.serdeLib = serdeLib;
        this.transactional = transactional;
        this.parameters = parameters;
        this.dataColumns = List.copyOf(dataColumns);
        this.partitionColumns = List.copyOf(partitionColumns);
        this.bucketCols = List.copyOf(bucketCols);
        this.numBuckets = numBuckets;
        this.sortCols = List.copyOf(sortCols);
        this.textDelimiter = textDelimiter;
        this.textEscape = textEscape;
        this.textNullString = textNullString;
    }

    /** 是否分桶表（有分桶列且桶数 &gt; 0）。 */
    public boolean bucketed() {
        return numBuckets > 0 && !bucketCols.isEmpty();
    }

    /**
     * 本表实际生效的「原生嵌套」开关。
     *
     * <p><b>TEXT 格式表恒按文本</b>：分隔文本里没有结构信息，嵌套列在文件里只有字符串，
     * 强行按 {@code List/Map/Struct} 建列只会得到整列 NULL（{@code TextFileArrowConverter}
     * 无法填充嵌套向量）。ORC / Parquet 才跟随 catalog 属性 {@code catalog.hive.nested.mode}。</p>
     */
    public boolean effectiveNestedNative(boolean catalogNestedNative) {
        return catalogNestedNative && format != HiveStorageFormat.TEXT;
    }

    /**
     * 分桶列的声明类型（顺序同 {@link #bucketCols()}）；某分桶列在表结构里找不到时给空串
     * （调用方视为「类型未知」→ 不裁剪）。
     */
    public List<String> bucketColumnTypes() {
        List<Column> all = allColumns();
        List<String> types = new ArrayList<>(bucketCols.size());
        for (String name : bucketCols) {
            String type = "";
            for (Column c : all) {
                if (c.name().equalsIgnoreCase(name)) {
                    type = c.dataType();
                    break;
                }
            }
            types.add(type);
        }
        return types;
    }

    /**
     * Hive 表结构序 = 常规列 + 分区列。
     */
    public List<Column> allColumns() {
        if (partitionColumns.isEmpty()) return dataColumns;
        List<Column> all = new ArrayList<>(dataColumns.size() + partitionColumns.size());
        all.addAll(dataColumns);
        all.addAll(partitionColumns);
        return all;
    }

    public boolean partitioned() {
        return !partitionColumns.isEmpty();
    }

    /**
     * 分区列名（保序）。
     */
    public List<String> partitionKeys() {
        List<String> keys = new ArrayList<>(partitionColumns.size());
        for (Column c : partitionColumns) keys.add(c.name());
        return keys;
    }

    /**
     * 数据列 Arrow Field（顺序 = 文件内列序），复合类型按原生嵌套结构建。
     */
    public List<Field> dataFields() {
        return dataFields(true);
    }

    /**
     * 数据列 Arrow Field；{@code nestedNative=false} 时 {@code array/map/struct} 按 {@code Utf8} 文本列建
     * （对应 {@code catalog.hive.nested.mode=text}）。
     */
    public List<Field> dataFields(boolean nestedNative) {
        return fieldsOf(dataColumns, nestedNative);
    }

    /**
     * 分区列 Arrow Field。
     */
    public List<Field> partitionFields() {
        return partitionFields(true);
    }

    public List<Field> partitionFields(boolean nestedNative) {
        return fieldsOf(partitionColumns, nestedNative);
    }

    /**
     * 全列 Arrow Field（= 读路径最终产出的表结构）。
     */
    public List<Field> allFields() {
        return allFields(true);
    }

    public List<Field> allFields(boolean nestedNative) {
        return fieldsOf(allColumns(), nestedNative);
    }

    private static List<Field> fieldsOf(List<Column> columns, boolean nestedNative) {
        List<Field> fields = new ArrayList<>(columns.size());
        for (Column c : columns) fields.add(HiveTypeMapper.toField(c.name(), c.dataType(), nestedNative));
        return fields;
    }
}
