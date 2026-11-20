package com.dojao.connector.hive;

import com.dojao.spi.util.CatalogProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * hive2 / hive3 两个工厂共用的属性说明与准入校验 —— 两者属性完全相同，
 * 差异只在 {@link HiveVersion}，没必要各写一份。
 *
 * <p>属性键遵循 catalog 命名规范 {@code catalog.<类型>.<属性名>}（见
 * {@link CatalogProperties}）；Hive 系共用家族前缀 {@code catalog.hive.}，
 * 故 hive2 / hive3 都写 {@code catalog.hive.metastore.uris} 这一份即可。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/20
 */
final class HiveConnectorFactorySupport {

    /** 属性说明（供错误提示 / Web UI 建 catalog 表单）。 */
    static final Map<String, String> DESCRIBE;

    static {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("catalog.hive.metastore.uris", "必填。Hive MetaStore Thrift 地址，如 thrift://host:9083");
        props.put("catalog.hive.fs.defaultFS", "HDFS 默认文件系统，如 hdfs://host:9000"
                + "（Hive 表 location 若是完整 URI，本项可省）");
        props.put("catalog.hive.default.format", "可选，默认 orc。新建表（CTAS）的物理格式：orc / parquet / text"
                + "（写已有表时按目标表自身格式）");
        props.put("catalog.hive.default.cts.table", "可选，默认 external（hive3）/ managed（hive2）。"
                + "CTAS 建表类型：external 建外部表（DROP 不删数据、不会被自动翻成 ACID 事务表）；"
                + "managed 建管理表（manager 同义）");
        props.put("catalog.hive.batch.size", "可选。单批读取行数，默认取引擎默认批大小");
        props.put("catalog.hive.partition.limit", "可选。分区枚举上限，默认 10000");
        props.put("catalog.hive.bucket.pruning", "可选，默认 true。分桶表桶裁剪开关 —— 分桶列被等值/IN 命中时只读命中桶文件");
        props.put("catalog.hive.acid.read", "可选，默认 false。ACID（事务表）读取开关 —— 开启后按写事务快照过滤 base/delta 并做行级删除/更新合并");
        props.put("catalog.hive.acid.write", "可选，默认 true。ACID（事务表）写入开关 —— INSERT INTO 走写事务 + delta 目录（Hive2 用事务 id，Hive3 用 writeId）；关闭则明确拒绝写事务表");
        props.put("catalog.hive.compact.target.bytes", "可选。小文件合并目标大小（字节，可写 64M/1G），默认取 HDFS 块大小");
        props.put("catalog.hive.split.file.min.bytes", "可选。单文件大到该值才尝试文件内切割，默认 1G");
        props.put("catalog.hive.metastore.kerberos.principal", "可选。Kerberos 认证主体（HMS 开启 Kerberos 时需要）");
        props.put("catalog.hive.hadoop.user.name", "可选。HDFS / HMS 访问身份（集群侧需存在该用户）。"
                + "per-catalog 生效：本 catalog 的 HMS 连接与 HDFS 文件系统都按此身份访问，"
                + "同 JVM 多 hive catalog 可各用各的用户");
        props.put("catalog.hive.dfs.client.use.datanode.hostname",
                "可选，默认 true。DataNode 回连用主机名；外网部署且只能解析 IP 时设为 false");

        // ---- HDFS HA：fs.defaultFS 写成 hdfs://<nameservice>（无端口）时这四项必填 ----
        props.put("catalog.hive.dfs.nameservices",
                "HDFS HA 必填。nameservice 名，多个用逗号分隔，须与 fs.defaultFS 的 hdfs://<名> 一致");
        props.put("catalog.hive.dfs.ha.namenodes.<nameservice>",
                "HDFS HA 必填。该 nameservice 下的 NameNode 逻辑 id，如 nn1,nn2");
        props.put("catalog.hive.dfs.namenode.rpc-address.<nameservice>.<nn>",
                "HDFS HA 必填。NameNode RPC 地址，如 10.24.62.6:8020（端口映射时填映射后的地址）");
        props.put("catalog.hive.dfs.client.failover.proxy.provider.<nameservice>",
                "HDFS HA 必填。HDFS 用 org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        DESCRIBE = Map.copyOf(props);
    }

    private HiveConnectorFactorySupport() {
    }

    /**
     * 准入校验：只查「必填项在不在、写法像不像」，不连 HMS ——
     * 连通性由 {@code new HiveConnector(...)} 在落库前真实构造时验证。
     */
    static void validate(HiveVersion version, String catalogName, Map<String, String> properties) {
        Map<String, String> props = CatalogProperties.normalize(
                version.type(), version.family(), properties);
        if (firstNonBlank(props.get("hive.metastore.uris"), props.get("metastore.uris")) == null) {
            throw new IllegalArgumentException("Hive catalog '" + catalogName
                    + "' 缺少必填属性 'catalog.hive.metastore.uris'（Hive MetaStore Thrift 地址）。可用属性: "
                    + DESCRIBE);
        }
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v.trim();
        }
        return null;
    }
}
