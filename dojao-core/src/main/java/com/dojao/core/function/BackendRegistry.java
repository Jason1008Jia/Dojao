package com.dojao.core.function;

import com.dojao.spi.function.FunctionBackend;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 函数后端注册表 —— 多语言拓展外缘的<b>注册表 + 分发器</b>。
 *
 * <p>镜像 {@code MetaStore.connectorFactories} 的模式：</p>
 * <ul>
 *   <li>启动时 ServiceLoader 发现 classpath 上所有 {@link FunctionBackend} 实现；</li>
 *   <li>内置注册 Java 后端（{@link JavaBackend}，进程内加载）；</li>
 *   <li>CREATE FUNCTION 时按 {@code type} {@link #resolve(String)} 分发 —— 引擎只认
 *       {@code type} 字符串，语言差异全部被压在各自的 FunctionBackend 里。</li>
 * </ul>
 *
 * <p><b>新增语言零引擎改动</b>：写一个 FunctionBackend → 放 classpath（或调用
 * {@link #register(FunctionBackend)}）→ 该语言即可被 CREATE FUNCTION 使用。</p>
 */
public final class BackendRegistry {

    /** type（小写）→ 后端。 */
    private static final Map<String, FunctionBackend> BACKENDS = new ConcurrentHashMap<>();

    static {
        // 内置 Java 后端（进程内 URLClassLoader），先注册保证优先
        register(new JavaBackend());
        // ServiceLoader 自动发现 classpath 上其余语言的 FunctionBackend 实现；
        // 只补空缺（putIfAbsent），不覆盖已内置的同名 type
        for (FunctionBackend backend : ServiceLoader.load(FunctionBackend.class)) {
            BACKENDS.putIfAbsent(backend.type().toLowerCase(), backend);
        }
    }

    private BackendRegistry() {
    }

    /** 注册一个后端（重复注册同 type 时后者覆盖，保持幂等）。 */
    public static void register(FunctionBackend backend) {
        BACKENDS.put(backend.type().toLowerCase(), backend);
    }

    /** 按 type 解析后端；未注册的类型返回 empty（CREATE FUNCTION 时应报"不支持的类型"）。 */
    public static Optional<FunctionBackend> resolve(String type) {
        return type == null ? Optional.empty()
                : Optional.ofNullable(BACKENDS.get(type.toLowerCase()));
    }

    /** 该 type 是否已有后端。 */
    public static boolean has(String type) {
        return type != null && BACKENDS.containsKey(type.toLowerCase());
    }

    /** 当前已注册的全部后端类型（java / python / rust / …）。 */
    public static Set<String> types() {
        return Collections.unmodifiableSet(BACKENDS.keySet());
    }

    /** 关闭所有已注册后端（节点退出回收，关侧车 / 释放类加载器）。 */
    public static void closeAll() {
        BACKENDS.values().forEach(FunctionBackend::close);
    }
}
