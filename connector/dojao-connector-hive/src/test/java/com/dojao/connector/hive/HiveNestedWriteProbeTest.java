package com.dojao.connector.hive;

import com.dojao.common.data.Page;
import com.dojao.connector.common.util.ORCArrowWriter;
import com.dojao.connector.common.util.ORCFileArrowConverter;
import com.dojao.connector.common.util.ParquetArrowWriter;
import com.dojao.connector.common.util.ParquetFileArrowConverter;
import com.dojao.spi.bean.Column;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * <b>Hive ORC / Parquet 原生嵌套写</b>的离线探针（本地文件，无 HDFS / 无 HMS）。
 *
 * <p>验证三件事：</p>
 * <ol>
 *   <li>ORC / Parquet 写入器能为 array / map / struct 建出<b>真实的嵌套 schema</b>
 *       （不是退化成一列字符串）；</li>
 *   <li>写出的文件能被各自的转换器<b>原样读回</b> —— 值、空容器（{@code []} / <code>{}</code>）、
 *       NULL 容器、NULL 元素 / NULL map 值都区分得开；</li>
 *   <li>文本格式（TEXTFILE）遇到嵌套列时<b>明确拒绝</b>，不做「写进去读不回来」的静默降级。</li>
 * </ol>
 *
 * <p>数据 5 行：正常值 / 空容器 + NULL struct / 容器整体 NULL / 含 NULL 元素与 NULL map 值 /
 * struct 子字段为 NULL。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public final class HiveNestedWriteProbeTest {

    private static final int ROWS = 5;

    private HiveNestedWriteProbeTest() {
    }

    public static void main(String[] args) throws Exception {
        java.nio.file.Path dir = Files.createTempDirectory("dojao-hive-nested-write-");
        Counter counter = new Counter();
        Configuration conf = new Configuration();
        try (BufferAllocator allocator = new RootAllocator(256L * 1024 * 1024)) {
            List<Field> fields = probeFields();

            System.out.println("=== 写入 schema（ORC / Parquet） ===");
            checkSchema(counter, fields);

            String orc = dir.resolve("roundtrip.orc").toAbsolutePath().toString();
            try (VectorSchemaRoot root = buildRoot(allocator, fields)) {
                Path path = new Path(orc);
                FileSystem fs = path.getFileSystem(conf);
                try (ORCArrowWriter writer = new ORCArrowWriter(conf, path, fs, fields, identity(fields.size()))) {
                    writer.write(root);
                }
            }
            try (Page page = readOrc(allocator, orc)) {
                System.out.println("=== ORC 写入 → 读回 ===");
                checkRoundTrip(counter, page, "ORC");
            }

            String parquet = dir.resolve("roundtrip.parquet").toAbsolutePath().toString();
            try (VectorSchemaRoot root = buildRoot(allocator, fields)) {
                Path path = new Path(parquet);
                FileSystem fs = path.getFileSystem(conf);
                try (ParquetArrowWriter writer =
                             new ParquetArrowWriter(conf, path, fs, fields, identity(fields.size()))) {
                    writer.write(root);
                }
            }
            try (Page page = readParquet(allocator, parquet)) {
                System.out.println("=== Parquet 写入 → 读回 ===");
                checkRoundTrip(counter, page, "Parquet");
            }

            System.out.println("=== 文本格式拒绝嵌套列 ===");
            checkTextGuard(counter);
        }
        System.out.println("=== 结果 ===");
        System.out.printf("通过 %d，失败 %d%n", counter.pass, counter.fail);
        System.out.println("临时目录: " + dir);
        if (counter.fail > 0) System.exit(1);
    }

    // ==================== 源数据 ====================

    private static List<Field> probeFields() {
        return List.of(
                HiveTypeMapper.toField("id", "bigint"),
                HiveTypeMapper.toField("tags", "array<string>"),
                HiveTypeMapper.toField("scores", "map<string,double>"),
                HiveTypeMapper.toField("info", "struct<city:string,age:int>"));
    }

    private static VectorSchemaRoot buildRoot(BufferAllocator alloc, List<Field> fields) {
        BigIntVector id = (BigIntVector) fields.get(0).createVector(alloc);
        ListVector tags = (ListVector) fields.get(1).createVector(alloc);
        MapVector scores = (MapVector) fields.get(2).createVector(alloc);
        StructVector info = (StructVector) fields.get(3).createVector(alloc);

        id.allocateNew(ROWS);
        tags.setInitialCapacity(ROWS);
        tags.allocateNew();
        scores.setInitialCapacity(ROWS);
        scores.allocateNew();
        info.setInitialCapacity(ROWS);
        info.allocateNew();

        VarCharVector tagData = (VarCharVector) tags.getDataVector();
        StructVector entries = (StructVector) scores.getDataVector();
        VarCharVector keys = (VarCharVector) entries.getChild("key");
        Float8Vector values = (Float8Vector) entries.getChild("value");
        VarCharVector city = (VarCharVector) info.getChild("city");
        IntVector age = (IntVector) info.getChild("age");

        int tagElems = 0;
        int mapEntries = 0;
        for (int r = 0; r < ROWS; r++) {
            id.setSafe(r, r);
            switch (r) {
                case 0 -> {
                    tagElems += writeTags(tags, tagData, r, "a", "b");
                    mapEntries += writeMap(scores, keys, values, r,
                            new String[]{"x", "y"}, new double[]{1.5, 2.5}, null);
                    info.setIndexDefined(r);
                    city.setSafe(r, "bj".getBytes(StandardCharsets.UTF_8));
                    age.setSafe(r, 30);
                }
                case 1 -> {
                    writeEmptyList(tags, r);
                    writeEmptyMap(scores, r);
                    info.setNull(r);
                }
                case 2 -> {
                    writeNullList(tags, r);
                    writeNullMap(scores, r);
                    info.setIndexDefined(r);
                    city.setSafe(r, "sh".getBytes(StandardCharsets.UTF_8));
                    age.setSafe(r, 25);
                }
                case 3 -> {
                    // 数组含 NULL 元素；map 含 NULL 值
                    int off = tags.startNewValue(r);
                    tagData.setSafe(off, "c".getBytes(StandardCharsets.UTF_8));
                    tagData.setNull(off + 1);
                    tags.endValue(r, 2);
                    tagElems += 2;
                    mapEntries += writeMap(scores, keys, values, r,
                            new String[]{"z"}, new double[]{0d}, new boolean[]{true});
                    info.setIndexDefined(r);
                    city.setNull(r);
                    age.setSafe(r, 7);
                }
                default -> {
                    tagElems += writeTags(tags, tagData, r, "d");
                    mapEntries += writeMap(scores, keys, values, r,
                            new String[]{"w"}, new double[]{4.5}, null);
                    info.setIndexDefined(r);
                    city.setSafe(r, "gz".getBytes(StandardCharsets.UTF_8));
                    age.setNull(r);
                }
            }
        }

        id.setValueCount(ROWS);
        tagData.setValueCount(tagElems);
        tags.setValueCount(ROWS);
        entries.setValueCount(mapEntries);
        keys.setValueCount(mapEntries);
        values.setValueCount(mapEntries);
        scores.setValueCount(ROWS);
        city.setValueCount(ROWS);
        age.setValueCount(ROWS);
        info.setValueCount(ROWS);

        List<FieldVector> vectors = new ArrayList<>();
        vectors.add(id);
        vectors.add(tags);
        vectors.add(scores);
        vectors.add(info);
        return new VectorSchemaRoot(fields, vectors, ROWS);
    }

    private static int writeTags(ListVector v, VarCharVector data, int row, String... values) {
        int off = v.startNewValue(row);
        for (int i = 0; i < values.length; i++) {
            data.setSafe(off + i, values[i].getBytes(StandardCharsets.UTF_8));
        }
        v.endValue(row, values.length);
        return values.length;
    }

    private static void writeEmptyList(ListVector v, int row) {
        v.startNewValue(row);
        v.endValue(row, 0);
    }

    private static void writeNullList(ListVector v, int row) {
        writeEmptyList(v, row);
        v.setNull(row);
    }

    private static int writeMap(MapVector v, VarCharVector keys, Float8Vector values, int row,
                                String[] names, double[] numbers, boolean[] nullValue) {
        int off = v.startNewValue(row);
        StructVector entries = (StructVector) v.getDataVector();
        for (int i = 0; i < names.length; i++) {
            // ★ entries 是非空 struct：Arrow 拷贝 / IPC 会看它的 validity，必须逐条置位
            entries.setIndexDefined(off + i);
            keys.setSafe(off + i, names[i].getBytes(StandardCharsets.UTF_8));
            if (nullValue != null && nullValue[i]) values.setNull(off + i);
            else values.setSafe(off + i, numbers[i]);
        }
        v.endValue(row, names.length);
        return names.length;
    }

    private static void writeEmptyMap(MapVector v, int row) {
        v.startNewValue(row);
        v.endValue(row, 0);
    }

    private static void writeNullMap(MapVector v, int row) {
        writeEmptyMap(v, row);
        v.setNull(row);
    }

    // ==================== 断言 ====================

    private static void checkSchema(Counter counter, List<Field> fields) {
        check(counter, "ORC schema：array / map / struct 都是真实嵌套结构", () -> {
            TypeDescription orc = ORCArrowWriter.orcSchemaOf(fields);
            List<TypeDescription> children = orc.getChildren();
            return children.get(1).getCategory() == TypeDescription.Category.LIST
                    && children.get(1).getChildren().get(0).getCategory() == TypeDescription.Category.STRING
                    && children.get(2).getCategory() == TypeDescription.Category.MAP
                    && children.get(2).getChildren().get(0).getCategory() == TypeDescription.Category.STRING
                    && children.get(2).getChildren().get(1).getCategory() == TypeDescription.Category.DOUBLE
                    && children.get(3).getCategory() == TypeDescription.Category.STRUCT
                    && children.get(3).getFieldNames().equals(List.of("city", "age"));
        });

        check(counter, "Parquet schema：3-level LIST / MAP key_value / struct group", () -> {
            MessageType parquet = ParquetArrowWriter.parquetSchemaOf(fields);
            Type tags = parquet.getType("tags");
            Type scores = parquet.getType("scores");
            Type info = parquet.getType("info");
            GroupType listGroup = tags.asGroupType();
            GroupType mapGroup = scores.asGroupType();
            GroupType kv = mapGroup.getType(0).asGroupType();
            return tags.getLogicalTypeAnnotation()
                            instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation
                    && listGroup.getFieldCount() == 1
                    && listGroup.getType(0).isRepetition(Type.Repetition.REPEATED)
                    && listGroup.getType(0).asGroupType().getFieldCount() == 1
                    && scores.getLogicalTypeAnnotation()
                            instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation
                    && kv.getFieldCount() == 2
                    && kv.getType(0).isRepetition(Type.Repetition.REQUIRED)
                    && kv.getType(1).isRepetition(Type.Repetition.OPTIONAL)
                    && !info.isPrimitive()
                    && info.asGroupType().getFieldCount() == 2;
        });
    }

    private static void checkRoundTrip(Counter counter, Page page, String tag) {
        VectorSchemaRoot root = page.getRoot();
        check(counter, tag + " : schema 保持 List / Map / Struct", () -> {
            Field tags = root.getSchema().findField("tags");
            Field scores = root.getSchema().findField("scores");
            Field info = root.getSchema().findField("info");
            return tags.getType() instanceof ArrowType.List
                    && scores.getType() instanceof ArrowType.Map
                    && info.getType() instanceof ArrowType.Struct;
        });

        ListVector tags = (ListVector) root.getVector("tags");
        VarCharVector tagData = (VarCharVector) tags.getDataVector();
        check(counter, tag + " : 第 0 行 array = [a, b]", () -> {
            int s = tags.getElementStartIndex(0);
            return tags.getElementEndIndex(0) - s == 2
                    && tagData.getObject(s).toString().equals("a")
                    && tagData.getObject(s + 1).toString().equals("b");
        });
        check(counter, tag + " : 第 1 行 array 为空数组（非 NULL）", () ->
                !tags.isNull(1) && tags.getElementEndIndex(1) == tags.getElementStartIndex(1));
        check(counter, tag + " : 第 2 行 array 为 NULL", () -> tags.isNull(2));
        check(counter, tag + " : 第 3 行 array = [c, NULL]", () -> {
            int s = tags.getElementStartIndex(3);
            return tags.getElementEndIndex(3) - s == 2
                    && tagData.getObject(s).toString().equals("c")
                    && tagData.isNull(s + 1);
        });
        check(counter, tag + " : 第 4 行 array = [d]", () -> {
            int s = tags.getElementStartIndex(4);
            return tags.getElementEndIndex(4) - s == 1
                    && tagData.getObject(s).toString().equals("d");
        });

        MapVector scores = (MapVector) root.getVector("scores");
        StructVector entries = (StructVector) scores.getDataVector();
        VarCharVector keys = (VarCharVector) entries.getChild("key");
        Float8Vector values = (Float8Vector) entries.getChild("value");
        check(counter, tag + " : 第 0 行 map = {x=1.5, y=2.5}", () -> {
            int s = scores.getElementStartIndex(0);
            return scores.getElementEndIndex(0) - s == 2
                    && keys.getObject(s).toString().equals("x") && values.get(s) == 1.5
                    && keys.getObject(s + 1).toString().equals("y") && values.get(s + 1) == 2.5;
        });
        check(counter, tag + " : 第 1 行 map 为空 map（非 NULL）", () ->
                !scores.isNull(1) && scores.getElementEndIndex(1) == scores.getElementStartIndex(1));
        check(counter, tag + " : 第 2 行 map 为 NULL", () -> scores.isNull(2));
        check(counter, tag + " : 第 3 行 map = {z=NULL}", () -> {
            int s = scores.getElementStartIndex(3);
            return scores.getElementEndIndex(3) - s == 1
                    && keys.getObject(s).toString().equals("z")
                    && values.isNull(s);
        });
        check(counter, tag + " : 第 4 行 map = {w=4.5}", () -> {
            int s = scores.getElementStartIndex(4);
            return scores.getElementEndIndex(4) - s == 1
                    && keys.getObject(s).toString().equals("w")
                    && values.get(s) == 4.5;
        });

        StructVector info = (StructVector) root.getVector("info");
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
        check(counter, tag + " : 第 3 行 struct 子字段 city 为 NULL", () ->
                !info.isNull(3) && city.isNull(3) && age.get(3) == 7);
        check(counter, tag + " : 第 4 行 struct 子字段 age 为 NULL", () ->
                !info.isNull(4)
                        && city.getObject(4).toString().equals("gz")
                        && age.isNull(4));
    }

    private static void checkTextGuard(Counter counter) {
        HiveTableMeta nested = textMeta(List.of(
                new Column("id", "bigint", "", null),
                new Column("tags", "array<string>", "", null)));
        check(counter, "TEXT 表 + array 列 → 明确拒绝（不是静默写字符串）", () -> {
            try {
                HiveConnector.checkWritableTarget(nested);
                return false;
            } catch (UnsupportedOperationException e) {
                return e.getMessage().contains("不支持写入嵌套类型列")
                        && e.getMessage().contains("tags")
                        && e.getMessage().contains("ORC / Parquet");
            }
        });

        HiveTableMeta scalar = textMeta(List.of(
                new Column("id", "bigint", "", null),
                new Column("name", "string", "", null)));
        check(counter, "TEXT 表 + 纯标量列 → 放行", () -> {
            HiveConnector.checkWritableTarget(scalar);
            return true;
        });

        HiveTableMeta orcNested = new HiveTableMeta("cat", "db", "t", "/tmp/t",
                HiveStorageFormat.ORC, HiveStorageFormat.ORC.inputFormat(),
                HiveStorageFormat.ORC.serdeLib(), false, Map.of(),
                List.of(new Column("tags", "array<string>", "", null)),
                List.of(), List.of(), 0, List.of(), (char) 1, '\\', null);
        check(counter, "ORC 表 + array 列 → 放行", () -> {
            HiveConnector.checkWritableTarget(orcNested);
            return true;
        });
    }

    private static HiveTableMeta textMeta(List<Column> columns) {
        return new HiveTableMeta("cat", "db", "t", "/tmp/t",
                HiveStorageFormat.TEXT, HiveStorageFormat.TEXT.inputFormat(),
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", false, Map.of(),
                columns, List.of(), List.of(), 0, List.of(), '\t', '\\', null);
    }

    // ==================== 读 ====================

    private static Page readOrc(BufferAllocator allocator, String file) throws Exception {
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(new Configuration());
        try (ORCFileArrowConverter converter = new ORCFileArrowConverter(
                allocator, path, fs, 1024, 0L, -1L, List.of(), true)) {
            Page page = converter.convertNextBatch();
            if (page == null) throw new IllegalStateException("ORC 读不到任何行: " + file);
            return page;
        }
    }

    private static Page readParquet(BufferAllocator allocator, String file) throws Exception {
        try (ParquetFileArrowConverter converter = new ParquetFileArrowConverter(
                allocator, new Path(file), new Configuration(), 1024, 0L, -1L,
                new ArrayList<>(), true)) {
            Page page = converter.convertNextBatch();
            if (page == null) throw new IllegalStateException("Parquet 读不到任何行: " + file);
            return page;
        }
    }

    // ==================== 工具 ====================

    private static int[] identity(int n) {
        int[] out = new int[n];
        for (int i = 0; i < n; i++) out[i] = i;
        return out;
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
}
