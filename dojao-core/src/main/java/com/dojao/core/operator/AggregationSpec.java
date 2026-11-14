package com.dojao.core.operator;

import com.dojao.core.function.aggregate.*;
import com.dojao.spi.function.AggregateFunction;
import com.dojao.spi.function.CollectAggregateFunction;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;

/**
 * 聚合规格 —— 描述一个聚合表达式：聚合函数 + 输入列 + 输出列名。
 *
 * <p>两种形态（互斥）：
 * <ul>
 *   <li><b>增量型</b>（{@code aggregate != null}）：内置五函数 + 自定义 {@link AggregateFunction}，
 *       四阶段 createState/accumulate/merge/evaluate，状态 Java/Python 侧来回。</li>
 *   <li><b>collect-evaluate 型</b>（{@code collect != null}）：框架按 key 攒值（Java 侧 Arrow 列），
 *       外部语言只写 evaluate（{@link CollectAggregateFunction}）。主力推广形态。</li>
 * </ul>
 * 本轮单 GROUP BY 内只支持其一（不混用），且 collect 型每查询一个。</p>
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/7/15
 */
public record AggregationSpec(
        String inputColumn,
        String outputColumn,
        AggregateFunction<?> aggregate,
        CollectAggregateFunction collect,
        List<String> collectInputColumns) {

    /** 增量型构造（collect 留空）。 */
    public AggregationSpec(String inputColumn, String outputColumn, AggregateFunction<?> aggregate) {
        this(inputColumn, outputColumn, aggregate, null, List.of());
    }

    /** 是否 collect-evaluate 型。 */
    public boolean isCollect() {
        return collect != null;
    }

    // ==================== 输出类型（用于 Arrow 向量创建） ====================

    /** 内建聚合函数对应的 Arrow 输出类型 */
    private static final ArrowType INT64 = new ArrowType.Int(64, true);
    private static final ArrowType FLOAT64 = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    private static final ArrowType LIST = new ArrowType.List();

    /** 获取该聚合的输出 Arrow 类型 */
    public ArrowType outputArrowType() {
        if (collect != null) return collect.outputType();
        String name = aggregate.functionName();
        return switch (name) {
            case "sum", "avg", "min", "max" -> FLOAT64;
            case "count", "count_distinct", "sum_int64" -> INT64;
            // collect_list / collect_set：元素类型来自输入列，ArrowType 本身不含子字段，
            // 声明完整 schema 请用 outputField(...)
            case "collect_list", "collect_set" -> LIST;
            default -> FLOAT64; // 自定义聚合默认 double
        };
    }

    /** 是否 collect_list / collect_set（输出嵌套 List，字段需带子字段）。 */
    public boolean isCollectArray() {
        if (aggregate == null) return false;
        String name = aggregate.functionName();
        return "collect_list".equals(name) || "collect_set".equals(name);
    }

    /**
     * 聚合结果列 Field（带子字段的类型声明）。
     *
     * <p>{@link ArrowType} 无法表达 {@code list<元素类型>} —— {@code ArrowType.List} 本身不含子字段，
     * 直接 {@code new Field(name, type, null)} 建不出可写的 ListVector。所有“声明输出 schema”的地方
     * （单机 buildOutput / 分布式 CTAS 声明 / DTO 重建）都必须走本方法。</p>
     *
     * @param inputField 聚合输入列的 Field（元素类型来源）；{@code null} 时（输入尚未到达）回退
     *                   {@code list<Utf8>}，仅用于空输入兜底
     */
    public Field outputField(String name, Field inputField) {
        if (isCollectArray()) {
            Field child = inputField != null
                    ? new Field("item", inputField.getFieldType(), inputField.getChildren())
                    : new Field("item", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
            return new Field(name, FieldType.nullable(LIST), List.of(child));
        }
        return new Field(name, FieldType.nullable(outputArrowType(inputField)), null);
    }

    /**
     * 按<b>输入列类型</b>推导输出类型：
     * <ul>
     *   <li><b>MIN/MAX 与输入严格同型</b>（时间戳 / 日期 / 字符串 / 布尔 / Decimal / 二进制，
     *       以及数值：{@code max(int_col)} → INT、{@code min(float_col)} → FLOAT —— 不再漂成 DOUBLE）；</li>
     *   <li><b>SUM(整型) → BIGINT</b>（整数累加，结果仍是整数；INT32 相加可能溢出 32 位，按 SQL 惯例
     *       扩到 64 位）（{@link #outputArrowType()} 静态口径仍是 DOUBLE —— 输入未知时
     *       无法判断，只能保守）；SUM(DECIMAL)/AVG(DECIMAL) → 同族 DECIMAL（精度放宽）；</li>
     *   <li>其余聚合走 {@link #outputArrowType()}。输入字段未知（null）时回退静态口径。</li>
     * </ul>
     *
     * <p>与 {@code AggregationHashTable.resultTypeFor} 同一规则，供 CTAS / 分布式声明列复用。</p>
     */
    public ArrowType outputArrowType(org.apache.arrow.vector.types.pojo.Field inputField) {
        if (inputField != null && aggregate != null) {
            String name = aggregate.functionName();
            ArrowType t = inputField.getType();
            if ("min".equals(name) || "max".equals(name)) {
                return t;
            }
            // ★SUM/AVG(DECIMAL) 必须输出 DECIMAL（不能退回 DOUBLE），否则累加结果带浮点尾差。
            //   精度按「整数位 + 10」放宽（MySQL 口径），scale 保持不变 ⇒ 精确到分。
            if (("sum".equals(name) || "avg".equals(name)) && t instanceof ArrowType.Decimal d) {
                return widenDecimal(d);
            }
            // ★SUM(整型) → BIGINT：准确累加且结果列不再按 double 展示（sum(CAST(x AS BIGINT))）。
            if ("sum".equals(name) && t instanceof ArrowType.Int) {
                return INT64;
            }
        }
        return outputArrowType();
    }

    /** DECIMAL 聚合输出类型：precision 放宽 10 位（封顶 38），scale 不变。 */
    public static ArrowType widenDecimal(ArrowType.Decimal d) {
        int precision = Math.min(38, Math.max(d.getPrecision() + 10, d.getScale() + 1));
        return new ArrowType.Decimal(precision, d.getScale(), 128);
    }

    // ==================== 工厂方法 ====================

    public static AggregationSpec count(String outputColumn) {
        return new AggregationSpec(null, outputColumn, CountAggregateFunction.INSTANCE);
    }

    public static AggregationSpec count(String inputColumn, String outputColumn) {
        return new AggregationSpec(inputColumn, outputColumn, CountAggregateFunction.INSTANCE);
    }

    /** COUNT(DISTINCT inputColumn)：状态为去重集合，输出 BIGINT。 */
    public static AggregationSpec countDistinct(String inputColumn, String outputColumn) {
        return new AggregationSpec(inputColumn, outputColumn, CountDistinctAggregateFunction.INSTANCE);
    }

    public static AggregationSpec sum(String inputColumn, String outputColumn) {
        return new AggregationSpec(inputColumn, outputColumn, SumAggregateFunction.INSTANCE);
    }

    public static AggregationSpec avg(String inputColumn, String outputColumn) {
        return new AggregationSpec(inputColumn, outputColumn, AvgAggregateFunction.INSTANCE);
    }

    public static AggregationSpec min(String inputColumn, String outputColumn) {
        return new AggregationSpec(inputColumn, outputColumn, MinAggregateFunction.INSTANCE);
    }

    public static AggregationSpec max(String inputColumn, String outputColumn) {
        return new AggregationSpec(inputColumn, outputColumn, MaxAggregateFunction.INSTANCE);
    }

    /**
     * 内部 BIGINT 求和：只供二级 COUNT(DISTINCT) 改写的外层合并使用（保留 BIGINT，
     * 不像普通 {@code sum} 那样漂成 DOUBLE）。不注册进 SQL 函数目录。
     */
    public static AggregationSpec sumInt64(String inputColumn, String outputColumn) {
        return new AggregationSpec(inputColumn, outputColumn, SumInt64AggregateFunction.INSTANCE);
    }

    /** COLLECT_LIST / ARRAY_AGG：收集为 {@code list<输入类型>}（NULL 跳过，空组 → NULL）。 */
    public static AggregationSpec collectList(String inputColumn, String outputColumn) {
        return new AggregationSpec(inputColumn, outputColumn, CollectListAggregateFunction.INSTANCE);
    }

    /** COLLECT_SET：去重收集为 {@code list<输入类型>}（NULL 跳过，空组 → NULL）。 */
    public static AggregationSpec collectSet(String inputColumn, String outputColumn) {
        return new AggregationSpec(inputColumn, outputColumn, CollectSetAggregateFunction.INSTANCE);
    }

    /** 注册自定义聚合函数（增量型） */
    public static AggregationSpec aggregate(String inputColumn, String outputColumn, AggregateFunction<?> custom) {
        return new AggregationSpec(inputColumn, outputColumn, custom);
    }

    /**
     * collect-evaluate 型：{@code inputColumns} = <b>SQL 调用的实际收集列</b>（引擎按位收集后按
     * key 分组交付给 {@code fn}）。{@link CollectAggregateFunction#inputColumns()} 的声明名只作
     * 参数位标签 / 元数据 —— <b>不要求与 SQL 列同名</b>（如 {@code @collect_aggregate(("num",))}
     * 声明的函数可作用于任意列 {@code stddev(discount_amount)}，位置与语义由外挂函数决定）。
     */
    public static AggregationSpec collect(String outputColumn, CollectAggregateFunction fn,
                                          List<String> inputColumns) {
        return new AggregationSpec(null, outputColumn, null, fn, List.copyOf(inputColumns));
    }
}
