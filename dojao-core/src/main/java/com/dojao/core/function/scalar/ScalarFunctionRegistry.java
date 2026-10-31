package com.dojao.core.function.scalar;

import com.dojao.spi.function.ScalarFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内置标量函数注册中心。
 *
 * <p>对标 {@code AggregateFunctionRegistry}，后续可扩展为 Java SPI 发现 + 运行时注册。</p>
 *
 * @author jiaxin
 * @date 2026/8/9
 */
public final class ScalarFunctionRegistry {

    private static final Map<String, ScalarFunction> FUNCTIONS = new ConcurrentHashMap<>();

    static {
        register(CosineSimilarityFunction.INSTANCE);
        register(L2DistanceFunction.INSTANCE);
        register(InnerProductFunction.INSTANCE);
        register(IfFunction.INSTANCE);
        BuiltinScalars.registerAll();
    }

    private ScalarFunctionRegistry() {}

    /** 注册一个标量函数（SQL 名大小写不敏感）。 */
    public static void register(ScalarFunction fn) {
        FUNCTIONS.put(fn.functionName().toLowerCase(), fn);
    }

    /**
     * 按显式 SQL 名注册一个标量函数。
     * 用于后端加载场景：以 SQL 函数名（FunctionDefinition.name）为准，不依赖类内 functionName()。
     */
    public static void registerGlobal(String name, ScalarFunction fn) {
        FUNCTIONS.put(name.toLowerCase(), fn);
    }

    /** 函数名是否已注册。 */
    public static boolean isRegistered(String name) {
        return FUNCTIONS.containsKey(name.toLowerCase());
    }

    /** 注销一个已注册的标量函数（DROP FUNCTION）。 */
    public static void unregisterGlobal(String name) {
        FUNCTIONS.remove(name.toLowerCase());
    }

    /** 按名解析函数，未注册抛 UnsupportedOperationException。 */
    public static ScalarFunction resolve(String name) {
        ScalarFunction fn = FUNCTIONS.get(name.toLowerCase());
        if (fn == null) throw new UnsupportedOperationException("未知标量函数: " + name);
        return fn;
    }
}
