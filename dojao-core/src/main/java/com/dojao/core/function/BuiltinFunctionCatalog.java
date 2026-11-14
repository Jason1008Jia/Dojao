package com.dojao.core.function;

import com.dojao.core.function.scalar.ScalarFunctionRegistry;
import com.dojao.core.operator.WindowFunctionType;
import com.dojao.core.operator.expr.ExprBuiltins;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 内置函数目录 —— 供 UI 补全 / 文档一次性枚举出「引擎里真能跑」的内置函数。
 *
 * <p>内置函数分散在多处（各有各的注册/解析路径），这里做**只读汇总**，不引入新的注册中心：</p>
 * <ul>
 *   <li><b>标量（注册式）</b>：{@link ScalarFunctionRegistry} 静态块里 4 个
 *       （{@code cosine_similarity / l2_distance / inner_product / if}），
 *       {@code detail} 取 {@link com.dojao.spi.function.Function#display()}。</li>
 *   <li><b>标量（表达式内置）</b>：{@link ExprBuiltins#names()}（日期/数学/字符串共 15 个）。</li>
 *   <li><b>聚合（内建快速路径）</b>：{@code count / count_distinct / sum / avg / min / max}
 *       与条件聚合 {@code count_if / sum_if / max_if / min_if}，
 *       与 {@code FragmentExecutor.aggregateFor} / {@code AggregationSpec} 的硬编码 switch 同源。</li>
 *   <li><b>窗口</b>：{@link WindowFunctionType} 全部枚举名（小写），跳过仅作占位的 {@code AGGREGATE}。</li>
 * </ul>
 *
 * <p><b>维护约定</b>：改动上述任一来源（增删函数、改 switch 分支）时应同步本目录；
 * 由于来源即各实现本身（枚举遍历 / 集合读取），多数改动会自动反映，只有硬编码名字需手工对齐。
 * <b>不要</b>改用 {@code SemanticAnalyzer.AGGREGATE_FUNCTIONS} 之类的「语法白名单」——那里含大量
 * 尚未实现的名字（如 {@code VAR_POP / MEDIAN}），补全会提示出跑不通的函数。</p>
 *
 * @author jiaxin
 * @date 2026/9/29
 */
public final class BuiltinFunctionCatalog {

    /** 内置函数条目。{@code kind} = {@code scalar | aggregate | window}。 */
    public record Entry(String name, String kind, String detail) {}

    /** 注册式标量函数的 SQL 名（与 {@link ScalarFunctionRegistry} 静态块逐一对齐）。 */
    private static final List<String> REGISTERED_SCALARS = List.of(
            // —— 原生 SPI 标量（余弦/距离/条件）——
            "cosine_similarity", "l2_distance", "inner_product", "if",
            // —— 数学（BuiltinScalars / MathFunctions）——
            "rand", "pow", "power", "sqrt", "exp", "ln", "log", "log10", "log2", "pmod",
            "sign", "bin", "hex", "unhex", "factorial", "greatest", "least", "negative", "positive",
            // —— 字符串（BuiltinScalars / StringFunctions）——
            "trim", "ltrim", "rtrim", "concat_ws", "substr", "substring", "instr", "locate",
            "replace", "regexp_replace", "regexp_extract", "repeat", "reverse", "lpad", "rpad",
            "space", "ascii", "chr", "soundex", "parse_url", "get_json_object", "json_extract",
            "find_in_set",
            "elt", "initcap", "printf", "translate",
            // —— 日期时间（BuiltinScalars / DateTimeFunctions）——
            "current_date", "current_timestamp", "unix_timestamp", "from_unixtime", "to_date",
            "hour", "minute", "second", "dayofmonth", "weekofyear", "dayofweek", "datediff",
            "date_add", "date_sub", "add_months", "months_between", "last_day", "next_day",
            "trunc", "date_format", "from_utc_timestamp", "to_utc_timestamp",
            // —— 条件（BuiltinScalars / ConditionalFunctions）——
            "nvl", "ifnull", "if_null", "coalesce", "isnull", "isnotnull", "nullif", "assert_true",
            // —— 加密（BuiltinScalars / CryptoFunctions）——
            "md5", "sha1", "sha2", "crc32", "uuid", "hash");

    /** 内建聚合（与 FragmentExecutor.aggregateFor / AggregationSpec 的 switch 同源）。 */
    private static final List<String> AGGREGATES = List.of(
            "count", "count_distinct", "sum", "avg", "min", "max",
            "count_if", "sum_if", "max_if", "min_if",
            "collect_list", "collect_set", "array_agg");

    private BuiltinFunctionCatalog() {
    }

    /** 全部内置函数（按名字排序；同名只留一条，{@code aggregate} 优先于 {@code window}）。 */
    public static List<Entry> all() {
        // 同名去重：sum/avg/count/min/max 同时是聚合与窗口，聚合语义更常用，先入先留。
        Map<String, Entry> byName = new LinkedHashMap<>();

        for (String name : REGISTERED_SCALARS) {
            String detail = "";
            try {
                detail = ScalarFunctionRegistry.resolve(name).display();
            } catch (RuntimeException ignored) {
                // 未注册（不该发生）→ 仍列出名字，detail 留空
            }
            byName.putIfAbsent(name, new Entry(name, "scalar", detail));
        }
        for (String name : ExprBuiltins.names()) {
            byName.putIfAbsent(name, new Entry(name, "scalar", ""));
        }
        for (String name : AGGREGATES) {
            byName.putIfAbsent(name, new Entry(name, "aggregate", ""));
        }
        for (WindowFunctionType t : WindowFunctionType.values()) {
            if (t == WindowFunctionType.AGGREGATE) {
                continue;   // 自定义聚合占位枚举，非具体函数
            }
            String name = t.name().toLowerCase();
            byName.putIfAbsent(name, new Entry(name, "window", ""));
        }

        List<Entry> out = new ArrayList<>(byName.values());
        out.sort((a, b) -> a.name().compareTo(b.name()));
        return out;
    }
}
