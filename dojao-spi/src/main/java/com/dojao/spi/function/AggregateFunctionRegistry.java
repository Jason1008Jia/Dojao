package com.dojao.spi.function;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 聚合函数注册中心 —— Session 级别。
 *
 * <h3>加载优先级</h3>
 * <ol>
 *   <li>Session 临时函数（{@link #registerTemporary}）—— CREATE TEMPORARY FUNCTION 时注册</li>
 *   <li>全局内置函数（Java SPI 发现 + 硬编码注册）—— 启动时加载</li>
 *   <li>Python 动态函数（{@link PyAggregateBridge}）—— 运行时 CREATE TEMPORARY FUNCTION ... WITH (type='python', ...) 注册</li>
 * </ol>
 *
 * <h3>线程安全</h3>
 * 所有读操作（{@link #get}, {@link #has}）无锁；写操作（注册/注销）仅 CREATE/DROP FUNCTION 时触发。
 *
 * <p>典型用法：
 * <pre>{@code
 *   // 启动时：注册内置函数（com.dojao.core.function.aggregate.*）
 *   registry.registerGlobal(SumAggregateFunction.INSTANCE);
 *   registry.registerGlobal(AvgAggregateFunction.INSTANCE);
 *
 *   // 运行时：SQL 执行 CREATE TEMP FUNCTION
 *   registry.registerTemporary("my_custom", new PyAggregateBridge(host, port, "my_module.MyAggregate"));
 *
 *   // 执行 SQL 时查找
 *   AggregateFunction<?> fn = registry.get("my_custom");
 * }</pre>
 *
 * @author jiaxin
 * @since 1.2
 */
public final class AggregateFunctionRegistry {

    /** 全局内置函数（启动时加载，所有 Session 共享 —— 无状态，已为函数实现而非实例） */
    private static final Map<String, AggregateFunction<?>> GLOBAL = new ConcurrentHashMap<>();

    /** Session 级临时函数（Session 关闭时清空） */
    private final Map<String, AggregateFunction<?>> sessionFunctions = new ConcurrentHashMap<>();

    // ==================== 全局注册（启动时） ====================

    /** 注册一个全局内置函数 */
    public static void registerGlobal(AggregateFunction<?> fn) {
        GLOBAL.put(fn.functionName().toLowerCase(), fn);
    }

    /**
     * 按显式名字注册一个全局函数。
     * 用于后端加载场景：以 SQL 函数名（FunctionDefinition.name）为准，不依赖类内 functionName()。
     */
    public static void registerGlobal(String name, AggregateFunction<?> fn) {
        GLOBAL.put(name.toLowerCase(), fn);
    }

    /** 通过 Java SPI 加载 classpath 上的所有聚合函数实现 */
    public static void loadFromSpi() {
        for (AggregateFunction<?> fn : ServiceLoader.load(AggregateFunction.class)) {
            registerGlobal(fn);
        }
    }

    /** 全局（含后端加载）是否已注册某函数名。 */
    public static boolean hasGlobal(String name) {
        return GLOBAL.containsKey(name.toLowerCase());
    }

    /** 按名取全局函数；未注册返回 null。 */
    public static AggregateFunction<?> getGlobal(String name) {
        return GLOBAL.get(name.toLowerCase());
    }

    /** 注销一个全局函数（DROP FUNCTION）。 */
    public static void unregisterGlobal(String name) {
        GLOBAL.remove(name.toLowerCase());
    }

    // ==================== Session 级注册（运行时） ====================

    /** 查找函数：Session 临时函数 > 全局函数 */
    public AggregateFunction<?> get(String name) {
        AggregateFunction<?> fn = sessionFunctions.get(name.toLowerCase());
        return fn != null ? fn : GLOBAL.get(name.toLowerCase());
    }

    /** 是否注册（包括全局和临时） */
    public boolean has(String name) {
        return sessionFunctions.containsKey(name.toLowerCase())
                || GLOBAL.containsKey(name.toLowerCase());
    }

    /**
     * 注册临时函数（CREATE TEMPORARY FUNCTION 时调用）。
     * 会话级生命周期：Session 关闭时由框架调用 {@link #clearTemporaryFunctions()}。
     */
    public void registerTemporary(String name, AggregateFunction<?> fn) {
        sessionFunctions.put(name.toLowerCase(), fn);
    }

    /** 注销临时函数（DROP TEMPORARY FUNCTION） */
    public void removeTemporary(String name) {
        sessionFunctions.remove(name.toLowerCase());
    }

    /** 清空所有 Session 级临时函数（Session 关闭时调用） */
    public void clearTemporaryFunctions() {
        sessionFunctions.clear();
    }
}
