package com.dojao.connector.clickhouse;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * <b>ClickHouse 嵌套类型（Array / Map）映射</b>的离线探针 —— 只验纯类型推导，不连库。
 *
 * <p>CTAS 建表必须声明真实的 {@code Array(T)} / {@code Map(K, V)}：旧实现统一写
 * {@code String}，后续 INSERT 传 {@code Object[]} 会被驱动拒绝。元素 / Map 值按可空性
 * 包 {@code Nullable(...)}，Map 的 key 永不包（ClickHouse 不允许可空键）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public final class ClickHouseNestedTypeProbeTest {

    private static int pass;
    private static int fail;

    private ClickHouseNestedTypeProbeTest() {
    }

    public static void main(String[] args) {
        scenario("ClickHouse 标量映射不变", ClickHouseNestedTypeProbeTest::scalars);
        scenario("ClickHouse Array 映射", ClickHouseNestedTypeProbeTest::arrays);
        scenario("ClickHouse Map 映射", ClickHouseNestedTypeProbeTest::maps);
        scenario("ClickHouse STRUCT 显式拒绝", ClickHouseNestedTypeProbeTest::rejectsStruct);
        System.out.println("==== " + pass + " PASS / " + fail + " FAIL ====");
        if (fail > 0) System.exit(1);
    }

    private static void scalars() {
        check("Utf8 → String", () -> "String".equals(ch(field("c", true, ArrowType.Utf8.INSTANCE))));
        check("Int8 → Int8", () -> "Int8".equals(ch(field("c", true, new ArrowType.Int(8, true)))));
        check("Int32 → Int32", () -> "Int32".equals(ch(field("c", true, new ArrowType.Int(32, true)))));
        check("Int64 → Int64", () -> "Int64".equals(ch(field("c", true, new ArrowType.Int(64, true)))));
        check("Float32 → Float32", () -> "Float32".equals(ch(field("c", true,
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)))));
        check("Float64 → Float64", () -> "Float64".equals(ch(field("c", true,
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))));
        check("Decimal(18,2)", () -> "Decimal(18, 2)".equals(ch(field("c", true,
                new ArrowType.Decimal(18, 2, 128)))));
        check("Bool → Bool", () -> "Bool".equals(ch(field("c", true, ArrowType.Bool.INSTANCE))));
        check("Date → Date", () -> "Date".equals(ch(field("c", true, new ArrowType.Date(DateUnit.DAY)))));
        check("Timestamp → DateTime64(3)", () -> "DateTime64(3)".equals(ch(field("c", true,
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)))));
    }

    private static void arrays() {
        check("可空元素 Array(Nullable(Int64))", () -> "Array(Nullable(Int64))".equals(ch(
                list(item("item", true, new ArrowType.Int(64, true))))));
        check("非空元素 Array(Int64)", () -> "Array(Int64)".equals(ch(
                list(item("item", false, new ArrowType.Int(64, true))))));
        check("字符串元素 Array(Nullable(String))", () -> "Array(Nullable(String))".equals(ch(
                list(item("item", true, ArrowType.Utf8.INSTANCE)))));
        check("嵌套数组 Array(Array(Int8))", () -> "Array(Array(Int8))".equals(ch(
                list(list(item("item", false, new ArrowType.Int(8, true)))))));
        check("容器子字段不包 Nullable（CH 禁止）", () -> "Array(Array(Nullable(Int8)))".equals(ch(
                list(list(item("item", true, new ArrowType.Int(8, true)))))));
        check("Map 值为可空数组 → Map(String, Array(Int8))",
                () -> "Map(String, Array(Int8))".equals(ch(map(
                        item("key", false, ArrowType.Utf8.INSTANCE),
                        list(item("item", false, new ArrowType.Int(8, true)))))));
        check("FixedSizeList 也写 Array", () -> "Array(Nullable(Float32))".equals(ch(
                new Field("xs", FieldType.nullable(new ArrowType.FixedSizeList(3)),
                        List.of(item("item", true,
                                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)))))));
        check("空子字段兜底 Array(String)", () -> "Array(String)".equals(ch(
                new Field("xs", FieldType.nullable(ArrowType.List.INSTANCE), List.of()))));
    }

    private static void maps() {
        check("Map(String, Nullable(Int64))", () -> "Map(String, Nullable(Int64))".equals(ch(
                map(item("key", true, ArrowType.Utf8.INSTANCE),
                        item("value", true, new ArrowType.Int(64, true))))));
        check("Map 值非空 Map(String, Int64)", () -> "Map(String, Int64)".equals(ch(
                map(item("key", false, ArrowType.Utf8.INSTANCE),
                        item("value", false, new ArrowType.Int(64, true))))));
        check("Map key 永不可空", () -> !ch(map(item("key", true, ArrowType.Utf8.INSTANCE),
                item("value", true, new ArrowType.Int(64, true)))).startsWith("Map(Nullable("));
    }

    private static void rejectsStruct() {
        Field struct = new Field("s", FieldType.nullable(new ArrowType.Struct()),
                List.of(item("a", true, new ArrowType.Int(32, true))));
        check("STRUCT 抛 UnsupportedOperationException", () -> {
            try {
                ch(struct);
                return false;
            } catch (UnsupportedOperationException expected) {
                return true;
            } catch (RuntimeException other) {
                return false;
            }
        });
    }

    // ==================== 构造工具 ====================

    private static String ch(Field field) {
        return ClickHouseConnector.arrowTypeToClickHouse(field);
    }

    private static Field field(String name, boolean nullable, ArrowType type) {
        return new Field(name, nullable ? FieldType.nullable(type) : FieldType.notNullable(type), null);
    }

    private static Field item(String name, boolean nullable, ArrowType type) {
        return field(name, nullable, type);
    }

    private static Field list(Field element) {
        return new Field("xs", FieldType.nullable(ArrowType.List.INSTANCE), List.of(element));
    }

    private static Field map(Field key, Field value) {
        Field keyField = new Field("key", FieldType.notNullable(key.getType()), key.getChildren());
        Field valueField = new Field("value", value.getFieldType().isNullable()
                ? FieldType.nullable(value.getType()) : FieldType.notNullable(value.getType()),
                value.getChildren());
        Field entries = new Field("entries", FieldType.notNullable(new ArrowType.Struct()),
                List.of(keyField, valueField));
        return new Field("m", FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
    }

    // ==================== 断言工具 ====================

    private static void scenario(String name, Runnable body) {
        int before = fail;
        try {
            body.run();
        } catch (Throwable t) {
            fail++;
            System.out.println("[FAIL] " + name + " —— " + t);
            t.printStackTrace(System.out);
        }
        if (fail == before) System.out.println("[PASS] " + name);
    }

    private static void check(String name, BooleanSupplier assertion) {
        try {
            if (assertion.getAsBoolean()) {
                pass++;
            } else {
                fail++;
                System.out.println("  [FAIL] " + name);
            }
        } catch (Throwable t) {
            fail++;
            System.out.println("  [FAIL] " + name + " —— " + t);
        }
    }
}
