package com.dojao.connector.hive;

import com.dojao.common.data.Page;
import com.dojao.connector.common.util.NestedFieldNames;
import com.dojao.connector.common.util.ORCFileArrowConverter;
import com.dojao.connector.common.util.ParquetFileArrowConverter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * <b>Hive ORC / Parquet 原生嵌套读</b>的离线探针（本地文件，无 HDFS / 无 HMS）。
 *
 * <p>两种模式各验一遍：</p>
 * <ul>
 *   <li><b>native</b>（{@code catalog.hive.nested.mode=native}，默认）—— array/map/struct
 *       读成真实的 Arrow List/Map/Struct 向量，元素数为 0 = 空数组、validity=0 = NULL；</li>
 *   <li><b>text</b>（{@code text}）—— 同一文件读成 JSON 风格字符串（{@code ["a","b"]} /
 *       {@code {"x":1.5}} / {@code {"city":"bj","age":30}}）。</li>
 * </ul>
 *
 * <p>数据三行覆盖三种边界：正常值 / 空数组与空 map / 整列为 NULL。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public final class HiveNestedProbeTest {

    private HiveNestedProbeTest() {
    }

    public static void main(String[] args) throws Exception {
        java.nio.file.Path dir = Files.createTempDirectory("dojao-hive-nested-");
        Counter counter = new Counter();
        try (BufferAllocator allocator = new RootAllocator(256L * 1024 * 1024)) {
            String orc = writeOrcNested(dir);
            try (Page page = readOrc(allocator, orc, true)) {
                System.out.println("=== ORC 原生嵌套读 ===");
                checkNative(counter, page, "ORC");
            }
            try (Page page = readOrc(allocator, orc, false)) {
                System.out.println("=== ORC 嵌套文本读 ===");
                checkText(counter, page, "ORC");
            }

            String parquet = writeParquetNested(dir);
            try (Page page = readParquet(allocator, parquet, true)) {
                System.out.println("=== Parquet 原生嵌套读 ===");
                checkNative(counter, page, "Parquet");
            }
            try (Page page = readParquet(allocator, parquet, false)) {
                System.out.println("=== Parquet 嵌套文本读 ===");
                checkText(counter, page, "Parquet");
            }

            System.out.println("=== TEXT 格式表恒按文本 ===");
            check(counter, "TEXT 表忽略 nested.mode=native（否则整列 NULL）", () -> {
                HiveTableMeta textMeta = table(HiveStorageFormat.TEXT);
                HiveTableMeta orcMeta = table(HiveStorageFormat.ORC);
                HiveTableMeta parquetMeta = table(HiveStorageFormat.PARQUET);
                return !textMeta.effectiveNestedNative(true)
                        && orcMeta.effectiveNestedNative(true)
                        && parquetMeta.effectiveNestedNative(true)
                        && !orcMeta.effectiveNestedNative(false);
            });

            System.out.println("=== HiveTypeMapper 与 Arrow 子字段口径 ===");
            check(counter, "List 元素名 / Map entries·key·value 与转换器一致", () -> {
                Field list = HiveTypeMapper.toField("tags", "array<string>");
                Field map = HiveTypeMapper.toField("scores", "map<string,double>");
                return list.getChildren().get(0).getName().equals(NestedFieldNames.LIST_ELEMENT)
                        && map.getChildren().get(0).getName().equals(NestedFieldNames.MAP_ENTRIES)
                        && map.getChildren().get(0).getChildren().get(0).getName().equals(NestedFieldNames.MAP_KEY)
                        && map.getChildren().get(0).getChildren().get(1).getName().equals(NestedFieldNames.MAP_VALUE);
            });
        }
        System.out.println("=== 结果 ===");
        System.out.printf("通过 %d，失败 %d%n", counter.pass, counter.fail);
        System.out.println("临时目录: " + dir);
        if (counter.fail > 0) System.exit(1);
    }

    // ==================== 断言 ====================

    private static void checkNative(Counter counter, Page page, String tag) {
        Schema schema = page.getRoot().getSchema();
        check(counter, tag + " : schema 为 List/Map/Struct 且子字段命名正确", () -> {
            Field tags = schema.findField("tags");
            Field scores = schema.findField("scores");
            Field info = schema.findField("info");
            if (!(tags.getType() instanceof ArrowType.List)) return false;
            if (!(scores.getType() instanceof ArrowType.Map)) return false;
            if (!(info.getType() instanceof ArrowType.Struct)) return false;
            if (!tags.getChildren().get(0).getName().equals(NestedFieldNames.LIST_ELEMENT)) return false;
            Field entries = scores.getChildren().get(0);
            return entries.getName().equals(NestedFieldNames.MAP_ENTRIES)
                    && entries.getChildren().get(0).getName().equals(NestedFieldNames.MAP_KEY)
                    && entries.getChildren().get(1).getName().equals(NestedFieldNames.MAP_VALUE);
        });

        ListVector tags = (ListVector) page.getRoot().getVector("tags");
        VarCharVector tagData = (VarCharVector) tags.getDataVector();
        check(counter, tag + " : 第 0 行 array = [a, b]", () -> {
            int s = tags.getElementStartIndex(0);
            int e = tags.getElementEndIndex(0);
            return e - s == 2
                    && tagData.getObject(s).toString().equals("a")
                    && tagData.getObject(s + 1).toString().equals("b");
        });
        check(counter, tag + " : 第 1 行 array 为空数组（非 NULL）", () ->
                !tags.isNull(1) && tags.getElementEndIndex(1) == tags.getElementStartIndex(1));
        check(counter, tag + " : 第 2 行 array 为 NULL", () -> tags.isNull(2));

        MapVector scores = (MapVector) page.getRoot().getVector("scores");
        StructVector entries = (StructVector) scores.getDataVector();
        VarCharVector keys = (VarCharVector) entries.getChild(NestedFieldNames.MAP_KEY);
        Float8Vector values = (Float8Vector) entries.getChild(NestedFieldNames.MAP_VALUE);
        check(counter, tag + " : 第 0 行 map = {x=1.5, y=2.5}", () -> {
            int s = scores.getElementStartIndex(0);
            int e = scores.getElementEndIndex(0);
            if (e - s != 2) return false;
            boolean first = keys.getObject(s).toString().equals("x") && values.get(s) == 1.5;
            boolean second = keys.getObject(s + 1).toString().equals("y") && values.get(s + 1) == 2.5;
            return first && second;
        });
        check(counter, tag + " : 第 1 行 map 为空 map（非 NULL）", () ->
                !scores.isNull(1) && scores.getElementEndIndex(1) == scores.getElementStartIndex(1));
        check(counter, tag + " : 第 2 行 map 为 NULL", () -> scores.isNull(2));

        StructVector info = (StructVector) page.getRoot().getVector("info");
        VarCharVector city = (VarCharVector) info.getChild("city");
        IntVector age = (IntVector) info.getChild("age");
        check(counter, tag + " : 第 0 行 struct = {city=bj, age=30}", () ->
                !info.isNull(0)
                        && city.getObject(0).toString().equals("bj")
                        && age.get(0) == 30);
        check(counter, tag + " : 第 1 行 struct 为 NULL", () -> info.isNull(1));
        check(counter, tag + " : 第 2 行 struct = {city=sh, age=25}", () ->
                !info.isNull(2)
                        && city.getObject(2).toString().equals("sh")
                        && age.get(2) == 25);
    }

    private static void checkText(Counter counter, Page page, String tag) {
        VarCharVector tags = (VarCharVector) page.getRoot().getVector("tags");
        VarCharVector scores = (VarCharVector) page.getRoot().getVector("scores");
        VarCharVector info = (VarCharVector) page.getRoot().getVector("info");
        check(counter, tag + " : 文本模式列类型为 Utf8", () ->
                page.getRoot().getSchema().findField("tags").getType() instanceof ArrowType.Utf8
                        && page.getRoot().getSchema().findField("scores").getType() instanceof ArrowType.Utf8
                        && page.getRoot().getSchema().findField("info").getType() instanceof ArrowType.Utf8);
        check(counter, tag + " : array → [\"a\",\"b\"] / [] / NULL", () ->
                tags.getObject(0).toString().equals("[\"a\",\"b\"]")
                        && tags.getObject(1).toString().equals("[]")
                        && tags.isNull(2));
        check(counter, tag + " : map → {\"x\":1.5,\"y\":2.5} / {} / NULL", () ->
                scores.getObject(0).toString().equals("{\"x\":1.5,\"y\":2.5}")
                        && scores.getObject(1).toString().equals("{}")
                        && scores.isNull(2));
        check(counter, tag + " : struct → {\"city\":\"bj\",\"age\":30} / NULL / {\"city\":\"sh\",\"age\":25}", () ->
                info.getObject(0).toString().equals("{\"city\":\"bj\",\"age\":30}")
                        && info.isNull(1)
                        && info.getObject(2).toString().equals("{\"city\":\"sh\",\"age\":25}"));
    }

    /** 造一张只有一列嵌套列的表元数据（探针用，不连 HMS）。 */
    private static HiveTableMeta table(HiveStorageFormat format) {
        return new HiveTableMeta("hive", "db", "t", "/tmp/t", format, "", "",
                false, java.util.Map.of(), List.of(new com.dojao.spi.bean.Column("tags", "array<string>", "", "")),
                List.of(), List.of(), 0, List.of(), '\u0001', '\\', "\\N");
    }

    private static void check(Counter counter, String name, BooleanSupplier body) {
        boolean ok;
        try {
            ok = body.getAsBoolean();
        } catch (RuntimeException e) {
            System.out.printf("[FAIL] %s（异常: %s: %s）%n", name,
                    e.getClass().getSimpleName(), e.getMessage());
            counter.fail++;
            return;
        }
        System.out.printf("[%s] %s%n", ok ? "PASS" : "FAIL", name);
        if (ok) counter.pass++;
        else counter.fail++;
    }

    private static final class Counter {
        int pass;
        int fail;
    }

    // ==================== ORC ====================

    private static String writeOrcNested(java.nio.file.Path dir) throws Exception {
        TypeDescription schema = TypeDescription.fromString(
                "struct<id:int,tags:array<string>,scores:map<string,double>,"
                        + "info:struct<city:string,age:int>>");
        Configuration conf = new Configuration();
        Path file = new Path(dir.resolve("nested.orc").toAbsolutePath().toString());
        try (org.apache.orc.Writer writer = OrcFile.createWriter(file,
                OrcFile.writerOptions(conf).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch(3);
            LongColumnVector id = (LongColumnVector) batch.cols[0];
            ListColumnVector tags = (ListColumnVector) batch.cols[1];
            MapColumnVector scores = (MapColumnVector) batch.cols[2];
            StructColumnVector info = (StructColumnVector) batch.cols[3];

            id.vector[0] = 1;
            id.vector[1] = 2;
            id.vector[2] = 3;

            BytesColumnVector tagChild = (BytesColumnVector) tags.child;
            tagChild.ensureSize(8, false);
            tags.offsets[0] = 0;
            tags.lengths[0] = 2;
            tagChild.setVal(0, "a".getBytes(StandardCharsets.UTF_8));
            tagChild.setVal(1, "b".getBytes(StandardCharsets.UTF_8));
            tags.offsets[1] = 2;
            tags.lengths[1] = 0;
            tags.offsets[2] = 2;
            tags.lengths[2] = 0;
            tags.isNull[2] = true;
            tags.noNulls = false;

            BytesColumnVector keys = (BytesColumnVector) scores.keys;
            DoubleColumnVector values = (DoubleColumnVector) scores.values;
            keys.ensureSize(8, false);
            values.ensureSize(8, false);
            scores.offsets[0] = 0;
            scores.lengths[0] = 2;
            keys.setVal(0, "x".getBytes(StandardCharsets.UTF_8));
            values.vector[0] = 1.5;
            keys.setVal(1, "y".getBytes(StandardCharsets.UTF_8));
            values.vector[1] = 2.5;
            scores.offsets[1] = 2;
            scores.lengths[1] = 0;
            scores.offsets[2] = 2;
            scores.lengths[2] = 0;
            scores.isNull[2] = true;
            scores.noNulls = false;

            BytesColumnVector city = (BytesColumnVector) info.fields[0];
            LongColumnVector age = (LongColumnVector) info.fields[1];
            city.setVal(0, "bj".getBytes(StandardCharsets.UTF_8));
            age.vector[0] = 30;
            info.isNull[1] = true;
            info.noNulls = false;
            city.setVal(2, "sh".getBytes(StandardCharsets.UTF_8));
            age.vector[2] = 25;

            batch.size = 3;
            writer.addRowBatch(batch);
        }
        return file.toString();
    }

    private static Page readOrc(BufferAllocator allocator, String file, boolean nestedNative)
            throws Exception {
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(new Configuration());
        try (ORCFileArrowConverter converter = new ORCFileArrowConverter(
                allocator, path, fs, 1024, 0L, -1L, List.of(), nestedNative)) {
            Page page = converter.convertNextBatch();
            if (page == null) throw new IllegalStateException("ORC 读不到任何行: " + file);
            return page;
        }
    }

    // ==================== Parquet ====================

    private static String writeParquetNested(java.nio.file.Path dir) throws Exception {
        MessageType schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
                .optionalGroup()
                    .as(LogicalTypeAnnotation.listType())
                    .repeatedGroup()
                        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                            .as(LogicalTypeAnnotation.stringType())
                            .named("element")
                    .named("list")
                .named("tags")
                .optionalGroup()
                    .as(LogicalTypeAnnotation.mapType())
                    .repeatedGroup()
                        .required(PrimitiveType.PrimitiveTypeName.BINARY)
                            .as(LogicalTypeAnnotation.stringType())
                            .named("key")
                        .optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("value")
                    .named("key_value")
                .named("scores")
                .optionalGroup()
                    .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("city")
                    .optional(PrimitiveType.PrimitiveTypeName.INT32).named("age")
                .named("info")
                .named("m");

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        Path file = new Path(dir.resolve("nested.parquet").toAbsolutePath().toString());
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
                .withConf(conf)
                .withType(schema)
                .build()) {
            // 第 0 行：正常值
            SimpleGroup row0 = new SimpleGroup(schema);
            row0.add("id", 1);
            Group tags0 = row0.addGroup("tags");
            tags0.addGroup("list").append("element", "a");
            tags0.addGroup("list").append("element", "b");
            Group scores0 = row0.addGroup("scores");
            Group kv0 = scores0.addGroup("key_value");
            kv0.add("key", "x");
            kv0.add("value", 1.5);
            Group kv1 = scores0.addGroup("key_value");
            kv1.add("key", "y");
            kv1.add("value", 2.5);
            Group info0 = row0.addGroup("info");
            info0.add("city", "bj");
            info0.add("age", 30);
            writer.write(row0);

            // 第 1 行：空数组 + 空 map + NULL struct
            SimpleGroup row1 = new SimpleGroup(schema);
            row1.add("id", 2);
            row1.addGroup("tags");        // list 组存在但 0 个元素
            row1.addGroup("scores");
            writer.write(row1);

            // 第 2 行：array / map 整列 NULL，struct 有值
            SimpleGroup row2 = new SimpleGroup(schema);
            row2.add("id", 3);
            Group info2 = row2.addGroup("info");
            info2.add("city", "sh");
            info2.add("age", 25);
            writer.write(row2);
        }
        return file.toString();
    }

    private static Page readParquet(BufferAllocator allocator, String file, boolean nestedNative)
            throws Exception {
        try (ParquetFileArrowConverter converter = new ParquetFileArrowConverter(
                allocator, new Path(file), new Configuration(), 1024, 0L, -1L,
                new ArrayList<>(), nestedNative)) {
            Page page = converter.convertNextBatch();
            if (page == null) throw new IllegalStateException("Parquet 读不到任何行: " + file);
            return page;
        }
    }
}
