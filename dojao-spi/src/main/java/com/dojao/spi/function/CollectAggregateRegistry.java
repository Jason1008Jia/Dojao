package com.dojao.spi.function;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 收集-求值型聚合函数注册中心（collect-evaluate 形态）。
 *
 * <p>与 {@link AggregateFunctionRegistry}（四阶段增量）并存、按 SQL 名区分：
 * 后端加载时按 CREATE 元数据把 collect 型注册到这里、增量型注册到 AggregateFunctionRegistry；
 * Planner 查聚合时先查本表再查增量表。全部静态（无 session 语义，与增量全局表对称）。</p>
 *
 * <p>放 SPI（与 {@link AggregateFunctionRegistry} 同层）：语义分析器（dojao-grammar，只依赖
 * SPI/MetaStore）在 GROUP BY 校验时也要识别 collect 聚合表达式，放 core 会造成 grammar→core
 * 循环依赖。</p>
 */
public final class CollectAggregateRegistry {

    private static final Map<String, CollectAggregateFunction> GLOBAL = new ConcurrentHashMap<>();

    private CollectAggregateRegistry() {
    }

    public static void registerGlobal(String name, CollectAggregateFunction fn) {
        GLOBAL.put(name.toLowerCase(), fn);
    }

    public static boolean hasGlobal(String name) {
        return GLOBAL.containsKey(name.toLowerCase());
    }

    public static CollectAggregateFunction getGlobal(String name) {
        return GLOBAL.get(name.toLowerCase());
    }

    public static void unregisterGlobal(String name) {
        GLOBAL.remove(name.toLowerCase());
    }
}
