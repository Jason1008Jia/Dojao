package com.dojao.core.function.table;

import com.dojao.spi.function.TableFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 表函数注册中心 —— 后端加载的用户自定义表函数（FROM TABLE(...)）。
 *
 * <p>对标 {@code ScalarFunctionRegistry} / {@code AggregateFunctionRegistry}：
 * CREATE FUNCTION 时由 {@code FunctionManager.ensureLoadedFrom} 调后端 {@code loadTable}，
 * 以 SQL 函数名（FunctionDefinition.name）注册（大小写不敏感）；查询期 Planner 按名 resolve。</p>
 */
public final class TableFunctionRegistry {

    private static final Map<String, TableFunction> FUNCTIONS = new ConcurrentHashMap<>();

    private TableFunctionRegistry() {}

    /** 按显式 SQL 名注册一个表函数（大小写不敏感）。 */
    public static void registerGlobal(String name, TableFunction fn) {
        FUNCTIONS.put(name.toLowerCase(), fn);
    }

    /** 函数名是否已注册。 */
    public static boolean isRegistered(String name) {
        return FUNCTIONS.containsKey(name.toLowerCase());
    }

    /** 注销一个已注册的表函数（DROP FUNCTION）。 */
    public static void unregisterGlobal(String name) {
        FUNCTIONS.remove(name.toLowerCase());
    }

    /** 按名解析表函数，未注册抛 UnsupportedOperationException（fail-fast）。 */
    public static TableFunction resolve(String name) {
        TableFunction fn = FUNCTIONS.get(name.toLowerCase());
        if (fn == null) throw new UnsupportedOperationException("未知表函数: " + name);
        return fn;
    }
}
