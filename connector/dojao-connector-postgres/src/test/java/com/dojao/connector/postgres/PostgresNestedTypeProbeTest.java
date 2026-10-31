package com.dojao.connector.postgres;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * <b>PostgreSQL 嵌套类型（数组）映射</b>的离线探针 —— 只验纯类型推导，不连库。
 *
 * <p>CTAS 建表 DDL 与 {@code createArrayOf} 的元素类型必须同口径：多维数组逐层
 * {@code 元素类型[]}，而 {@code createArrayOf} 永远只给<b>最内层</b>基础类型。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/25
 */
public final class PostgresNestedTypeProbeTest {

    private static int pass;
    private static int fail;

    private PostgresNestedTypeProbeTest() {
    }

    public static void main(String[] args) {
        scenario("PG 数组 → CTAS 列类型", PostgresNestedTypeProbeTest::ctasTypes);
        scenario("PG createArrayOf 基础类型", PostgresNestedTypeProbeTest::arrayElementTypes);
        scenario("PG Map / STRUCT 显式拒绝", PostgresNestedTypeProbeTest::rejectsComposite);
        System.out.println("==== " + pass + " PASS / " + fail + " FAIL ====");
        if (fail > 0) System.exit(1);
    }

    private static void ctasTypes() {
        check("Int32[]", () -> "INTEGER[]".equals(PostgresConnector.arrowTypeToPg(
                list(item("item", new ArrowType.Int(32, true))))));
        check("多维度 Int64[][]", () -> "BIGINT[][]".equals(PostgresConnector.arrowTypeToPg(
                list(list(item("item", new ArrowType.Int(64, true)))))));
        check("TEXT[]", () -> "TEXT[]".equals(PostgresConnector.arrowTypeToPg(
                list(item("item", ArrowType.Utf8.INSTANCE)))));
        check("NUMERIC[]", () -> "NUMERIC[]".equals(PostgresConnector.arrowTypeToPg(
                list(item("item", new ArrowType.Decimal(38, 10, 128))))));
        check("TIMESTAMP[]", () -> "TIMESTAMP[]".equals(PostgresConnector.arrowTypeToPg(
                list(item("item", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))))));
        check("Float32[] → REAL[]", () -> "REAL[]".equals(PostgresConnector.arrowTypeToPg(
                list(item("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))))));
        check("空子字段兜底 TEXT[]", () -> "TEXT[]".equals(PostgresConnector.arrowTypeToPg(
                new Field("xs", FieldType.nullable(ArrowType.List.INSTANCE), List.of()))));
    }

    private static void arrayElementTypes() {
        check("Int8/16 → int2", () -> "int2".equals(PostgresConnector.pgArrayElementType(
                list(item("item", new ArrowType.Int(8, true)))))
                && "int2".equals(PostgresConnector.pgArrayElementType(
                list(item("item", new ArrowType.Int(16, true))))));
        check("Int32 → int4", () -> "int4".equals(PostgresConnector.pgArrayElementType(
                list(item("item", new ArrowType.Int(32, true))))));
        check("Int64 → int8", () -> "int8".equals(PostgresConnector.pgArrayElementType(
                list(item("item", new ArrowType.Int(64, true))))));
        check("多维取最内层 int8", () -> "int8".equals(PostgresConnector.pgArrayElementType(
                list(list(item("item", new ArrowType.Int(64, true)))))));
        check("Float32 → float4", () -> "float4".equals(PostgresConnector.pgArrayElementType(
                list(item("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))))));
        check("Float64 → float8", () -> "float8".equals(PostgresConnector.pgArrayElementType(
                list(item("item", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))))));
        check("Decimal → numeric", () -> "numeric".equals(PostgresConnector.pgArrayElementType(
                list(item("item", new ArrowType.Decimal(18, 2, 128))))));
        check("Bool → bool", () -> "bool".equals(PostgresConnector.pgArrayElementType(
                list(item("item", ArrowType.Bool.INSTANCE)))));
        check("Date → date", () -> "date".equals(PostgresConnector.pgArrayElementType(
                list(item("item", new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY))))));
        check("Timestamp → timestamp", () -> "timestamp".equals(PostgresConnector.pgArrayElementType(
                list(item("item", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))))));
        check("Binary → bytea", () -> "bytea".equals(PostgresConnector.pgArrayElementType(
                list(item("item", ArrowType.Binary.INSTANCE)))));
        check("Utf8 → text", () -> "text".equals(PostgresConnector.pgArrayElementType(
                list(item("item", ArrowType.Utf8.INSTANCE)))));
    }

    private static void rejectsComposite() {
        Field map = new Field("m", FieldType.nullable(new ArrowType.Map(false)), List.of(
                new Field("entries", FieldType.notNullable(new ArrowType.Struct()), List.of(
                        new Field("key", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null),
                        new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null)))));
        Field struct = new Field("s", FieldType.nullable(new ArrowType.Struct()), List.of(
                item("a", new ArrowType.Int(32, true))));
        check("Map 抛 UnsupportedOperationException", () -> throwsUnsupported(() ->
                PostgresConnector.arrowTypeToPg(map)));
        check("STRUCT 抛 UnsupportedOperationException", () -> throwsUnsupported(() ->
                PostgresConnector.arrowTypeToPg(struct)));
    }

    // ==================== 构造工具 ====================

    private static Field item(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    private static Field list(Field element) {
        return new Field("xs", FieldType.nullable(ArrowType.List.INSTANCE), List.of(element));
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static boolean throwsUnsupported(ThrowingRunnable body) {
        try {
            body.run();
            return false;
        } catch (UnsupportedOperationException expected) {
            return true;
        } catch (Exception other) {
            return false;
        }
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
