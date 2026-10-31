package com.dojao.core.function;

import com.dojao.spi.bean.FunctionDefinition;
import com.dojao.spi.function.AggregateFunction;
import com.dojao.spi.function.AggregateFunctionRegistry;
import com.dojao.spi.function.CollectAggregateFunction;
import com.dojao.spi.function.CollectAggregateRegistry;
import com.dojao.spi.function.FunctionBackend;
import com.dojao.spi.function.ScalarFunction;
import com.dojao.spi.function.TableFunction;
import com.dojao.spi.function.WindowFunction;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Java 函数后端 —— type = {@code "java"}，进程内 {@link URLClassLoader} 加载。
 *
 * <p>从 {@code jar_path} + {@code class_name} 实例化用户函数：类必须实现
 * {@link ScalarFunction} / {@link AggregateFunction} / {@link WindowFunction} 之一，
 * 否则抛 {@link IllegalArgumentException}（fail-fast）。</p>
 *
 * <h3>类加载缓存</h3>
 * <p>同一 jar 只建一个 {@link URLClassLoader}（按规范化的 jar 路径缓存）。
 * 否则同一 jar 被两个类加载器各加载一次，会产生"同名不同类"，导致
 * {@code ClassCastException}，也会重复执行 static 初始化。</p>
 */
public final class JavaBackend implements FunctionBackend {

    /** jar 路径（规范化） → 类加载器。 */
    private final Map<String, URLClassLoader> classLoaders = new ConcurrentHashMap<>();

    @Override
    public String type() {
        return "java";
    }

    @Override
    public ScalarFunction loadScalar(FunctionDefinition def) {
        return load(def, ScalarFunction.class, "标量函数");
    }

    /**
     * 加载聚合函数并注册（以 SQL 函数名 def.name() 为准）。<b>collect-evaluate 型优先</b>：
     * 类实现 {@link CollectAggregateFunction}（框架攒值、外部只写 evaluate）→ 注册进
     * {@link CollectAggregateRegistry}（Planner 先查本表再查增量表）；否则按增量四阶段
     * {@link AggregateFunction} → {@link AggregateFunctionRegistry}。
     * <p>void 契约：后端自行完成"加载 + 注册"，框架之后按名从注册表取用。</p>
     */
    @Override
    public void loadAggregate(FunctionDefinition def) {
        try {
            CollectAggregateFunction collect = load(def, CollectAggregateFunction.class, "collect 聚合函数");
            CollectAggregateRegistry.registerGlobal(def.name(), collect);
            return;
        } catch (IllegalArgumentException e) {
            // 非 collect 契约 → 按增量聚合加载
        }
        AggregateFunction<?> agg = load(def, AggregateFunction.class, "聚合函数");
        AggregateFunctionRegistry.registerGlobal(def.name(), agg);
    }

    @Override
    public TableFunction loadTable(FunctionDefinition def) {
        return load(def, TableFunction.class, "表函数");
    }

    @Override
    public WindowFunction loadWindow(FunctionDefinition def) {
        return load(def, WindowFunction.class, "窗口函数");
    }

    @Override
    public void validate(FunctionDefinition def) {
        if (def.className() == null || def.className().isBlank()) {
            throw new IllegalArgumentException("java 后端需要 class_name（全限定类名）");
        }
        // CREATE 冒烟校验：类加载 + 实例化，函数契约任一满足即通过（对称 Python check_function）。
        // 类本身加载失败 → IllegalStateException 原样抛出（fail-fast）。
        boolean ok = tryLoad(def, ScalarFunction.class)
                || tryLoad(def, CollectAggregateFunction.class)
                || tryLoad(def, AggregateFunction.class)
                || tryLoad(def, TableFunction.class)
                || tryLoad(def, WindowFunction.class);
        if (!ok) {
            throw new IllegalArgumentException(
                    "函数 [" + def.className() + "] 未实现任何函数契约"
                            + "（ScalarFunction/CollectAggregateFunction/AggregateFunction/TableFunction/WindowFunction）");
        }
    }

    /** 冒烟：按某契约加载，不满足该契约返回 false（加载失败原样抛出）。 */
    private boolean tryLoad(FunctionDefinition def, Class<?> expected) {
        try {
            load(def, expected, "函数");
            return true;
        } catch (IllegalArgumentException e) {
            return false; // 类能加载但不是该契约 → 试下一个
        }
    }

    @Override
    public void close() {
        classLoaders.values().forEach(loader -> {
            try {
                loader.close();
            } catch (Exception ignored) {
                // 回收尽力而为
            }
        });
        classLoaders.clear();
    }

    // ==================== 内部 ====================

    private <T> T load(FunctionDefinition def, Class<T> expected, String kind) {
        String className = def.className();
        if (className == null || className.isBlank()) {
            throw new IllegalArgumentException("java 后端需要 class_name（全限定类名）");
        }
        try {
            ClassLoader loader = loaderFor(def);
            Class<?> clazz = Class.forName(className, true, loader);
            Object instance = clazz.getDeclaredConstructor().newInstance();
            if (!expected.isInstance(instance)) {
                throw new IllegalArgumentException(
                        kind + " [" + className + "] 未实现 " + expected.getSimpleName());
            }
            return expected.cast(instance);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
                 | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(
                    "加载函数失败: " + className + "（jar=" + def.jarPath() + "）", e);
        }
    }

    private ClassLoader loaderFor(FunctionDefinition def) {
        String code = def.jarPath();
        if (code == null || code.isBlank()) {
            // 未给 code_path：用上下文类加载器（函数类已在 classpath 上）
            return Thread.currentThread().getContextClassLoader();
        }
        // code_path 统一经 FunctionPathReader 解析（file/shared 直通，http/hdfs/s3 先下载到缓存）
        ResolvedCode rc = PathReaders.resolve(code);
        return classLoaders.computeIfAbsent(rc.localPath(), p -> {
            try {
                return new URLClassLoader(new URL[]{toUrl(rc.localPath())},
                        Thread.currentThread().getContextClassLoader());
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("无效函数代码路径: " + rc.localPath(), e);
            }
        });
    }

    private static URL toUrl(String path) throws MalformedURLException {
        if (path.startsWith("file:") || path.startsWith("jar:")) {
            return new URL(path);
        }
        return Path.of(path).toUri().toURL();
    }
}
