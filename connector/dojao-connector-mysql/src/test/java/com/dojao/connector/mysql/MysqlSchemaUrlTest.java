package com.dojao.connector.mysql;

/**
 * {@link MysqlConnector#buildSchemaUrl(String, String)} 的离线回归测试 —— <b>不连任何数据源</b>，
 * 纯字符串断言，直接 Run main()。
 *
 * <p>守的是一条不变量：<b>改写 JDBC URL 换库名时，authority（host:port）与 {@code ?} 之后的连接参数
 * 必须原样保留</b>。这条不变量曾经被打破且<b>在本机开发环境完全看不出来</b>：旧实现拼出
 * {@code jdbc:mysql:/metastore}（host 与参数全丢），而 Connector/J 对缺失 authority 的 URL 不报
 * 格式错、而是兜底 {@code localhost:3306} —— 开发机本机就有 MySQL，于是「连上了」；只有集群里
 * （Master 本机没有 3306）才炸成 {@code Schema check failed … Communications link failure}。</p>
 *
 * <p>所以这个测试刻意<b>不从 localhost 出发</b>：全部用假主机名，一旦 host 被改写，断言立刻失败。</p>
 *
 * @author jiaxin
 * @date 2026/9/22
 */
public class MysqlSchemaUrlTest {

    private static int passed = 0;

    public static void main(String[] args) {
        // 1) 带库名 + 带参数：host、端口、参数一个都不能少
        assertEq("jdbc:mysql://aliyun002:3306/metastore"
                        + "?useUnicode=true&serverTimezone=Asia/Shanghai"
                        + "&useSSL=false&allowPublicKeyRetrieval=true",
                MysqlConnector.buildSchemaUrl(
                        "jdbc:mysql://aliyun002:3306/metastore"
                                + "?useUnicode=true&serverTimezone=Asia/Shanghai"
                                + "&useSSL=false&allowPublicKeyRetrieval=true",
                        "metastore"),
                "完整 URL 换同名库：host:port 与全部参数原样保留");

        // 2) 换库名（schema 与 url 里的库不同）
        assertEq("jdbc:mysql://db-host:3307/main",
                MysqlConnector.buildSchemaUrl("jdbc:mysql://db-host:3307/other", "main"),
                "换库名：只改库名，authority 不动");

        // 3) URL 里没有库名（只到 host:port）
        assertEq("jdbc:mysql://db-host:3306/dga",
                MysqlConnector.buildSchemaUrl("jdbc:mysql://db-host:3306", "dga"),
                "URL 无库名：补上 /schema");

        // 4) 结尾多个斜杠（有人会手滑写成 .../3306/）
        assertEq("jdbc:mysql://db-host:3306/dga",
                MysqlConnector.buildSchemaUrl("jdbc:mysql://db-host:3306/", "dga"),
                "URL 以 / 结尾：不产生 //");

        // 5) 多主机（replication / loadbalance）+ 参数
        assertEq("jdbc:mysql:replication://h1:3306,h2:3306/dga?useSSL=false",
                MysqlConnector.buildSchemaUrl(
                        "jdbc:mysql:replication://h1:3306,h2:3306/metastore?useSSL=false", "dga"),
                "replication 多主机 + 参数");

        // 6) 反例守卫：绝不允许再出现被截断的 authority
        String mangled = MysqlConnector.buildSchemaUrl("jdbc:mysql://h:3306/db?useSSL=false", "db");
        assertTrue(mangled.startsWith("jdbc:mysql://"),
                "结果必须以 jdbc:mysql:// 开头（旧 bug 会得到 jdbc:mysql:/）: " + mangled);
        assertTrue(mangled.contains("h:3306"), "结果必须含 host:port（旧 bug 丢 host）: " + mangled);
        assertTrue(mangled.contains("useSSL=false"), "结果必须含连接参数（旧 bug 丢参数）: " + mangled);

        System.err.println("MysqlSchemaUrlTest: " + passed + " 项全 PASS");
    }

    // ==================== 断言 ====================

    private static void assertEq(String expected, String actual, String msg) {
        if (!expected.equals(actual)) {
            throw new AssertionError(msg + "\n  期望: " + expected + "\n  实为: " + actual);
        }
        passed++;
        System.err.println("  - " + msg + " ✓");
    }

    private static void assertTrue(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
        passed++;
        System.err.println("  - " + msg + " ✓");
    }
}
