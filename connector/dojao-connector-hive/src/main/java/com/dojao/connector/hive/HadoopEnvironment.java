package com.dojao.connector.hive;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Hadoop 运行环境准备 —— 把「Windows 上跑 Hadoop FS 必须的前置条件」和「公网/HDFS 连接参数」
 * 收敛到一处，任何构造 {@link Configuration} 的地方都从这里走。
 *
 * <p><b>Windows 三坑</b>（造数程序踩过的同一套）：</p>
 * <ol>
 *   <li>没有 {@code hadoop.home.dir} + {@code bin/winutils.exe} 时，任何
 *       {@code FileSystem.get()} 都会在 {@code Shell.<clinit>} 直接抛 —— 即使只写 hdfs://。
 *       这里用<b>空占位 winutils.exe</b> 兜底（Hadoop 只查存在性，hdfs 路径够用）。</li>
 *   <li>HDFS 访问身份由 {@code HADOOP_USER_NAME} 决定，需与集群侧一致。</li>
 *   <li>公网链路被 NAT 掐断：NameNode/DataNode 的 RPC 连接空闲 ~30s 会被 RST。把
 *       {@code ipc.ping.interval} 压到 20s（客户端在 socket 读超时时发 ping 刷新映射）
 *       并放宽连接/重试参数，长连接才不会被单方面回收。</li>
 * </ol>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public final class HadoopEnvironment {

    private static final String PROP_HADOOP_HOME = "hadoop.home.dir";

    private HadoopEnvironment() {
    }

    /** 幂等准备：设置 HDFS 访问用户 + 兜底 hadoop.home.dir。 */
    public static synchronized void prepare(Map<String, String> props) {
        String user = firstNonBlank(props.get("hadoop.user.name"), props.get("HADOOP_USER_NAME"));
        if (user != null) {
            System.setProperty("HADOOP_USER_NAME", user);
        }
        ensureHadoopHome();
    }

    private static void ensureHadoopHome() {
        String existing = System.getProperty(PROP_HADOOP_HOME);
        if (existing != null && !existing.isBlank()) {
            if (!isWindows() || new File(existing, "bin/winutils.exe").exists()) return;
        }
        String env = System.getenv("HADOOP_HOME");
        File home = (env != null && !env.isBlank())
                ? new File(env)
                : new File(System.getProperty("java.io.tmpdir"), "dojao-hadoop-home");
        File bin = new File(home, "bin");
        if (!bin.isDirectory() && !bin.mkdirs() && !bin.isDirectory()) {
            System.err.println("[Hive] 无法创建 hadoop.home.dir: " + home);
            return;
        }
        if (isWindows()) {
            File winutils = new File(bin, "winutils.exe");
            if (!winutils.exists()) {
                try {
                    if (!winutils.createNewFile()) {
                        System.err.println("[Hive] 无法创建占位 winutils.exe: " + winutils);
                    }
                } catch (IOException ignored) {
                    // 创建失败只影响本地 file:// 路径；hdfs:// 仍可用
                }
            }
        }
        System.setProperty(PROP_HADOOP_HOME, home.getAbsolutePath());
    }

    /**
     * 由 catalog properties 构造 Hadoop Configuration（HDFS 读文件 与 HMS Thrift 客户端共用）。
     *
     * <p>可识别键（catalog 规范写法 {@code catalog.hive.<键>}，归一化后即这些裸键）：
     * {@code fs.defaultFS}、{@code hive.metastore.uris}，以及一批可覆盖的连接调优键
     * （{@code ipc.ping.interval} / {@code dfs.client.socket-timeout} …）。</p>
     */
    public static Configuration newConfiguration(Map<String, String> props) {
        Configuration conf = new Configuration();
        copy(conf, props, "fs.defaultFS");
        copy(conf, props, "fs.default.name");
        copy(conf, props, "dfs.nameservices");
        copy(conf, props, "hive.metastore.uris");

        // DataNode 返回主机名而非 IP：NAT 环境下必须按主机名回连（集群内网不适用时显式关掉）
        conf.setBoolean("dfs.client.use.datanode.hostname",
                boolProp(props, "dfs.client.use.datanode.hostname", true));

        // 公网/长连接保活与重试（对齐造数程序的实测参数）
        conf.set("ipc.ping.interval", prop(props, "ipc.ping.interval", "20000"));
        conf.set("ipc.client.connect.timeout", prop(props, "ipc.client.connect.timeout", "30000"));
        conf.setInt("ipc.client.connect.max.retries",
                intProp(props, "ipc.client.connect.max.retries", 20));
        conf.set("dfs.client.socket-timeout", prop(props, "dfs.client.socket-timeout", "120000"));
        conf.set("dfs.client.retry.policy.max.time.sec",
                prop(props, "dfs.client.retry.policy.max.time.sec", "1800"));

        // Hive3 客户端 ↔ Hive2 MetaStore：Thrift 协议兼容，但默认的能力校验会在握手时报版本不匹配，
        // 关掉校验即可「单客户端通吃 hive2/hive3」（Trino / Iceberg 同款做法）
        conf.setBoolean("hive.metastore.client.capability.check",
                boolProp(props, "hive.metastore.client.capability.check", false));
        conf.setBoolean("hive.metastore.sasl.enabled",
                boolProp(props, "hive.metastore.sasl.enabled", false));
        return conf;
    }

    private static void copy(Configuration conf, Map<String, String> props, String key) {
        String v = props.get(key);
        if (v != null && !v.isBlank()) conf.set(key, v.trim());
    }

    private static String prop(Map<String, String> props, String key, String fallback) {
        String v = props.get(key);
        return (v == null || v.isBlank()) ? fallback : v.trim();
    }

    private static int intProp(Map<String, String> props, String key, int fallback) {
        try {
            return Integer.parseInt(prop(props, key, String.valueOf(fallback)));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static boolean boolProp(Map<String, String> props, String key, boolean fallback) {
        String v = props.get(key);
        return (v == null || v.isBlank()) ? fallback : Boolean.parseBoolean(v.trim());
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v.trim();
        }
        return null;
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }
}
