package com.dojao.core.function.scalar.conditional;

import com.dojao.core.function.scalar.ScalarFunctionRegistry;
import com.dojao.core.function.scalar.ScalarSupport;

import java.util.function.Function;

/**
 * Hive 标准条件标量函数（{@code if} 见 {@link com.dojao.core.function.scalar.IfFunction}，
 * {@code case when} 已是表达式语法，此处不重复）。
 *
 * @author jiaxin
 * @date 2026/10/11
 */
public final class ConditionalFunctions {

    private ConditionalFunctions() {
    }

    public static void registerAll() {
        reg("nvl", "空值替换 nvl(a,b)：a 为 null 返回 b，否则 a", args -> {
            ScalarSupport.requireArity("nvl", args, 2);
            return args[0] != null ? args[0] : args[1];
        });
        reg("coalesce", "首个非 null 值（全 null → null）", args -> {
            ScalarSupport.requireAtLeast("coalesce", args, 1);
            for (Object a : args) {
                if (a != null) return a;
            }
            return null;
        });
        reg("isnull", "是否为空（null → true）", args -> {
            ScalarSupport.requireArity("isnull", args, 1);
            return args[0] == null;
        });
        reg("isnotnull", "是否非空", args -> {
            ScalarSupport.requireArity("isnotnull", args, 1);
            return args[0] != null;
        });
        reg("nullif", "相等则返回 null，否则返回 a", args -> {
            ScalarSupport.requireArity("nullif", args, 2);
            if (args[0] == null) return null;
            return equal(args[0], args[1]) ? null : args[0];
        });
        reg("assert_true", "断言为真，否则抛异常（可选消息）assert_true(cond[,msg])", args -> {
            ScalarSupport.requireArity("assert_true", args, 1, 2);
            if (!ScalarSupport.truthy(args[0])) {
                String msg = args.length == 2 && args[1] != null
                        ? ScalarSupport.string(args[1]) : "assert_true 断言失败";
                throw new IllegalArgumentException(msg);
            }
            return null;
        });
    }

    private static void reg(String name, String display, Function<Object[], Object> impl) {
        ScalarFunctionRegistry.register(ScalarSupport.fn(name, display, impl));
    }

    /** 等值判定：两数值按 double 值比较；其余按字符串比较。 */
    private static boolean equal(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(ScalarSupport.asDouble(a), ScalarSupport.asDouble(b)) == 0;
        }
        return ScalarSupport.string(a).equals(ScalarSupport.string(b));
    }
}
