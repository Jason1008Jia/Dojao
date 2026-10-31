package com.dojao.spi.service;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * 连接器工厂注册表 —— 把「系统里到底有哪些连接器可用」这件事集中到一处。
 *
 * <p>两个来源，合在一张表里：</p>
 * <ol>
 *   <li><b>classpath</b>：{@link ServiceLoader} 扫 {@code META-INF/services/com.dojao.spi.service.ConnectorFactory}
 *       —— 官方连接器（mysql / postgres / hive2 / hive3…）走这条；</li>
 *   <li><b>插件目录</b>：{@code lib/connectors/*.jar}，每个 jar 一个独立类加载器
 *       —— 第三方连接器按下面的约定打包丢进来即可，<b>不必改任何 Dojao 代码</b>。</li>
 * </ol>
 *
 * <h3>第三方连接器打包约定</h3>
 * <ul>
 *   <li>jar 里必须有 {@code META-INF/services/com.dojao.spi.service.ConnectorFactory}，
 *       内容是实现类全限定名（一行一个）；</li>
 *   <li>{@code dojao-spi} / {@code dojao-common} <b>不要打进去</b>（用 {@code provided} 作用域）。
 *       这两个包由父加载器供，打进 jar 反而会让类型对不上 —— SPI 接口不是同一个 Class 对象，
 *       转换时就抛 {@code ClassCastException}；</li>
 *   <li>其余依赖（JDBC 驱动、数据湖 SDK…）请打进去（fat jar），或确保服务端 classpath 上已有
 *       —— 插件类加载器是<b>子优先</b>的，自带副本会盖住父加载器里的同名类。</li>
 * </ul>
 *
 * <p>本类刻意不依赖任何日志框架（dojao-spi 要保持零依赖），发现失败时<b>不抛异常也不吞掉</b>，
 * 而是记进 {@link #failures()}，由调用方决定怎么报 —— 「连接器 jar 加载不了」这种事必须让人看见，
 * 不能变成一句「未知的连接器类型」。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/20
 */
public final class ConnectorRegistry {

    /**
     * 当前 SPI 契约版本。{@link ConnectorFactory#apiVersion()} 高于它的插件会被拒绝加载
     * —— 插件是针对更新的 SPI 编的，硬塞进来只会在运行时炸得更晚、更难查。
     */
    public static final int API_VERSION = 1;

    /** 插件目录系统属性（优先级最高）。 */
    public static final String PLUGIN_DIR_PROPERTY = "system.connector.plugins.dir";

    /** 部署根目录系统属性（插件目录的次级推断依据：{@code <dojao.home>/lib/connectors}）。 */
    public static final String HOME_PROPERTY = "dojao.home";

    /** 相对部署根目录的插件目录名（assembly 产出的就是这个布局）。 */
    public static final String PLUGIN_DIR_NAME = "lib/connectors";

    /** type → 工厂。 */
    private final Map<String, ConnectorFactory> byType = new ConcurrentHashMap<>();

    /** type → 来源描述（jar 名 / "classpath"），仅用于错误提示。 */
    private final Map<String, String> origins = new ConcurrentHashMap<>();

    /** 失败记录：来源（jar 名前缀 / 类名）→ 原因。空 = 一切正常。 */
    private final Map<String, String> failures = new ConcurrentHashMap<>();

    /** 已打开的插件类加载器（保活 + 关闭用）。 */
    private final List<URLClassLoader> pluginLoaders = new ArrayList<>();

    /** 实际扫描的插件目录；未配置 / 不存在 = 空。 */
    private final Path pluginDir;

    private ConnectorRegistry(Path pluginDir) {
        this.pluginDir = pluginDir;
    }

    // ==================== 装载 ====================

    /**
     * 建一个注册表：先扫 classpath，再扫插件目录（{@link #resolvePluginDir()}）。
     * 插件目录不存在就当没有插件，不算失败。
     */
    public static ConnectorRegistry load() {
        return load(resolvePluginDir().orElse(null));
    }

    /**
     * 建一个注册表：classpath + 指定插件目录。
     *
     * @param pluginDir 插件目录（可为 null = 只扫 classpath）
     */
    public static ConnectorRegistry load(Path pluginDir) {
        ConnectorRegistry registry = new ConnectorRegistry(pluginDir);
        registry.scanClasspath();
        if (pluginDir != null) registry.scanPluginDir(pluginDir);
        return registry;
    }

    /** 插件目录推断：{@code -Dsystem.connector.plugins.dir} &gt; {@code -Ddoja.home/lib/connectors} &gt; {@code ./lib/connectors}。 */
    public static Optional<Path> resolvePluginDir() {
        String explicit = System.getProperty(PLUGIN_DIR_PROPERTY);
        if (explicit != null && !explicit.isBlank()) {
            return Optional.of(Paths.get(explicit.trim()));
        }
        String home = System.getProperty(HOME_PROPERTY);
        if (home != null && !home.isBlank()) {
            return Optional.of(Paths.get(home.trim()).resolve(PLUGIN_DIR_NAME));
        }
        return Optional.of(Paths.get(PLUGIN_DIR_NAME));
    }

    /** 人话描述插件目录，用在「找不到连接器」的报错里。 */
    public static String pluginDirDescription() {
        Path dir = resolvePluginDir().orElse(null);
        if (dir == null) return PLUGIN_DIR_NAME;
        return dir.toAbsolutePath() + "（或 -D" + PLUGIN_DIR_PROPERTY + "=<目录>）";
    }

    private void scanClasspath() {
        try {
            for (ConnectorFactory factory : ServiceLoader.load(ConnectorFactory.class)) {
                registerInternal(factory, "classpath");
            }
        } catch (ServiceConfigurationError e) {
            failures.put("classpath", String.valueOf(e));
        }
    }

    private void scanPluginDir(Path dir) {
        if (!Files.isDirectory(dir)) return;
        List<Path> jars;
        try (Stream<Path> files = Files.list(dir)) {
            jars = files.filter(p -> p.getFileName().toString().endsWith(".jar"))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            failures.put(dir.toString(), "列举插件目录失败: " + e);
            return;
        }
        for (Path jar : jars) {
            scanPluginJar(jar);
        }
    }

    /**
     * 一个 jar 一个类加载器 —— 好处是「哪个 jar 坏了」能指名道姓地报出来，
     * 而且两个插件各自带的依赖版本不会互相污染。
     */
    private void scanPluginJar(Path jar) {
        String name = jar.getFileName().toString();
        URLClassLoader loader;
        try {
            loader = new PluginClassLoader(new URL[]{jar.toUri().toURL()}, getClass().getClassLoader());
        } catch (IOException e) {
            failures.put(name, "无法读取 jar: " + e);
            return;
        }
        boolean any = false;
        try {
            for (ConnectorFactory factory : ServiceLoader.load(ConnectorFactory.class, loader)) {
                registerInternal(factory, name);
                any = true;
            }
            if (!any) {
                failures.put(name, "jar 里没有 META-INF/services/" + ConnectorFactory.class.getName()
                        + " 声明（第三方连接器必须声明自己的工厂）");
            }
        } catch (ServiceConfigurationError | LinkageError e) {
            // 类对不上（多半是把 dojao-spi 打进了 jar）或依赖缺失 —— 记下来，别拖垮其他插件
            failures.put(name, String.valueOf(e));
        } catch (RuntimeException e) {
            failures.put(name, e.toString());
        } finally {
            pluginLoaders.add(loader);   // 保活（已注册的工厂还要用它的类）
        }
    }

    private void registerInternal(ConnectorFactory factory, String origin) {
        if (factory == null) return;
        String type = factory.type();
        if (type == null || type.isBlank()) {
            failures.put(origin, "ConnectorFactory " + factory.getClass().getName() + " 返回了空 type()");
            return;
        }
        if (factory.apiVersion() > API_VERSION) {
            failures.put(origin, "连接器类型 '" + type + "' 的工厂声明 apiVersion="
                    + factory.apiVersion() + "，高于本服务端支持的 " + API_VERSION
                    + "（插件是针对更新的 dojao-spi 编译的，请升级服务端或改用匹配的插件版本）");
            return;
        }
        ConnectorFactory previous = byType.putIfAbsent(type, factory);
        origins.putIfAbsent(type, origin);
        if (previous != null) {
            // 同名 type 两个实现 = 配置事故（改了 type() 忘了卸旧 jar）→ 说清楚谁赢了
            failures.put(origin, "连接器类型 '" + type + "' 与 " + origins.get(type)
                    + " 的同名实现冲突，本次忽略: " + factory.getClass().getName());
        }
    }

    // ==================== 查询 ====================

    /** 手动登记一个工厂（覆盖同 type；SPI 覆盖不到的场景，如测试）。 */
    public void register(ConnectorFactory factory) {
        byType.put(factory.type(), factory);
        origins.put(factory.type(), "manual");
        failures.remove(factory.type());
    }

    /** 按类型取工厂。 */
    public Optional<ConnectorFactory> factory(String type) {
        return Optional.ofNullable(type == null ? null : byType.get(type));
    }

    /** 是否支持该类型。 */
    public boolean supports(String type) {
        return type != null && byType.containsKey(type);
    }

    /** 全部已发现的类型（排序）。 */
    public List<String> types() {
        List<String> types = new ArrayList<>(byType.keySet());
        Collections.sort(types);
        return types;
    }

    /** 类型 → 来源（classpath / jar 名），供诊断展示。 */
    public Map<String, String> origins() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(origins));
    }

    /** 加载失败记录（jar / 来源 → 原因）；空表示没有异常。 */
    public Map<String, String> failures() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(failures));
    }

    /** 插件目录（可能不存在）。 */
    public Path pluginDir() {
        return pluginDir;
    }

    /** 关闭插件类加载器（进程退出 / 测试收尾）。已注册的工厂随之不可再用。 */
    public void close() {
        for (URLClassLoader loader : pluginLoaders) {
            try {
                loader.close();
            } catch (IOException ignored) {
                // 关闭失败无补救动作，且进程即将退出
            }
        }
        pluginLoaders.clear();
    }

    // ==================== 插件类加载器 ====================

    /**
     * 插件类加载器：<b>子优先</b>。
     *
     * <p>插件自带的依赖（比如它自己要的某个 JDBC 驱动版本）优先于服务端 classpath 上的同名类，
     * 这样插件才不会被服务端的版本绑死。但契约包必须<b>回父加载器</b>取：{@code java.*} /
     * {@code javax.*} 等 JDK 类自不必说，{@code com.dojao.spi.*} 与 {@code com.dojao.common.*}
     * 更是关键 —— SPI 接口必须全系统同一个 Class 对象，插件自带一份的话
     * {@code @Override} 就会变成「实现了另一个接口」。</p>
     */
    private static final class PluginClassLoader extends URLClassLoader {

        /** 强制回父加载器的包前缀。 */
        private static final String[] PARENT_FIRST = {
                "java.", "javax.", "jdk.", "sun.", "com.sun.",
                "com.dojao.spi.", "com.dojao.common.",
        };

        PluginClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    if (parentFirst(name)) {
                        loaded = super.loadClass(name, false);
                    } else {
                        try {
                            loaded = findClass(name);          // 先看插件自己的 jar
                        } catch (ClassNotFoundException e) {
                            loaded = super.loadClass(name, false);   // 再退回父加载器
                        }
                    }
                }
                if (resolve) resolveClass(loaded);
                return loaded;
            }
        }

        private static boolean parentFirst(String name) {
            for (String prefix : PARENT_FIRST) {
                if (name.startsWith(prefix)) return true;
            }
            return false;
        }
    }
}
