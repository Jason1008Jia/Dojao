package com.dojao.connector.hive;

import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.ColumnStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnState;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hive MetaStore（Thrift）客户端封装 —— 数据探查的唯一入口。
 *
 * <p>只做「读元数据」：库/表/列/分区定义、统计参数。不谈数据面 —— 数据由连接器直连 HDFS 读文件，
 * 不经 HiveServer2、不走 Hive-JDBC。</p>
 *
 * <p><b>单客户端通吃 hive2 / hive3</b>：用 {@code hive-standalone-metastore} 一个版本的客户端
 * 连两个版本的 HMS（Thrift 协议兼容，同 Trino / Iceberg）。{@code type='hive2'|'hive3'} 决定
 * catalog 类型标识，以及 ACID 读快照的取法 —— 见 {@link #validWriteIds}（Hive3）与
 * {@link #validTxns}（Hive2）。</p>
 *
 * <p>惰性连接：首个探查请求才握手；表元数据按 {@code db.table} 缓存（读路径短 TTL，
 * 写 / DDL 路径回源 HMS —— 见 {@link #tableFresh}）。</p>
 *
 * <p><b>线程安全</b>：{@link HiveMetaStoreClient} 不是线程安全的（单 socket + 无同步），而一个
 * 连接器实例会被多个扫描线程并发使用（并行切片各调 {@code openSplit} → {@code table(..)} 探表）。
 * 故所有走 client 的方法都 {@code synchronized} —— 元数据调用量小且表缓存命中即返回，
 * 串行化的代价远小于「并发复用同一 Thrift 连接」的风险。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public class HiveMetastore implements AutoCloseable {

    private final String catalogName;
    private final String uris;
    private final Configuration conf;
    private final boolean acidRead;
    /** 只有 Hive2 需要 txn-id 快照（Hive3 用 write-id 快照），故记版本。 */
    private final HiveVersion version;
    /** catalog 的 Hadoop 访问身份（{@code hadoop.user.name}）；null = 用进程默认 UGI。 */
    private final String user;

    /**
     * 表元数据缓存（{@code db.table} → 快照 + 抓取时刻）—— <b>必须带 TTL</b>：连接器实例是
     * <b>进程级</b>的（Worker 启动时注册、跨查询复用；Master 与 Worker 各有独立缓存），外部或在
     * 另一进程里 DROP / CREATE 表都没有通知渠道。没有 TTL 时一次旧快照会跟随进程到退出：CTAS 在
     * Master 侧按新列建表，Worker 仍拿旧列校验写入页（「写入页缺少 Hive 数据列」）。
     * 正确性关键路径（写会话建立 / DDL）另由 {@link #tableFresh} 强制回源。
     */
    private final Map<String, CachedTable> tableCache = new ConcurrentHashMap<>();

    /** 读路径快照有效期：只限制「外部 DDL 后的可见延迟」，不参与写 / DDL 的正确性保证。 */
    private static final long TABLE_CACHE_TTL_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(60);

    private record CachedTable(HiveTableMeta meta, long fetchedNanos) { }

    private volatile HiveMetaStoreClient client;

    public HiveMetastore(String catalogName, String uris, Configuration conf, boolean acidRead,
                         HiveVersion version) {
        this(catalogName, uris, conf, acidRead, version, null);
    }

    /**
     * @param user Hadoop 访问身份（catalog 属性 {@code hadoop.user.name}），可为 null。
     *             非空时 HMS 连接在 {@code createRemoteUser(user).doAs(...)} 里建立 ——
     *             {@link HiveMetaStoreClient} 建连会对连接执行 {@code set_ugi(用户, 组)}，
     *             HMS 之后所有 HDFS 权限检查都按这个身份执行（HMS 侧按连接记住，后续 RPC 无需
     *             再次 doAs）。null 则沿用进程默认 UGI（{@code HADOOP_USER_NAME} / OS 用户）。
     */
    public HiveMetastore(String catalogName, String uris, Configuration conf, boolean acidRead,
                         HiveVersion version, String user) {
        this.catalogName = catalogName;
        this.uris = uris;
        this.conf = conf;
        this.acidRead = acidRead;
        this.version = version;
        this.user = user == null || user.isBlank() ? null : user.trim();
    }

    public String uris() { return uris; }

    /** catalog 声明的 Hadoop 访问身份；null = 进程默认（见构造器 javadoc）。 */
    public String user() { return user; }

    /** 是否已开启 ACID 表读取（catalog 属性 {@code hive.acid.read}）。 */
    public boolean acidReadEnabled() { return acidRead; }

    /** 是否 Hive2 catalog —— 决定 ACID 用 txn-id 快照还是 write-id 快照。 */
    public boolean hive2() { return version == HiveVersion.HIVE2; }

    // ==================== 连接 ====================

    private HiveMetaStoreClient client() {
        HiveMetaStoreClient c = client;
        if (c != null) return c;
        synchronized (this) {
            if (client == null) {
                conf.set("hive.metastore.uris", uris);
                try {
                    client = user == null
                            ? new HiveMetaStoreClient(conf)
                            : UserGroupInformation.createRemoteUser(user).doAs(
                                    (PrivilegedExceptionAction<HiveMetaStoreClient>)
                                            () -> new HiveMetaStoreClient(conf));
                } catch (Exception e) {
                    throw new IllegalStateException("连接 Hive MetaStore 失败: " + uris
                            + (user == null ? "" : "（Hadoop 访问身份: " + user + "）"), e);
                }
            }
            return client;
        }
    }

    @Override
    public synchronized void close() {
        HiveMetaStoreClient c = client;
        client = null;
        if (c != null) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }

    // ==================== 库 / 表 清单 ====================
    // 以下方法均 synchronized：HiveMetaStoreClient 非线程安全，而并行扫描的多个线程会同时探表。

    public synchronized List<String> databases() {
        try {
            List<String> dbs = new ArrayList<>(client().getAllDatabases());
            Collections.sort(dbs);
            return dbs;
        } catch (Exception e) {
            throw new RuntimeException("列出 Hive 数据库失败 (" + uris + "): " + rootCause(e), e);
        }
    }

    public synchronized boolean databaseExists(String database) {
        if (database == null) return false;
        try {
            client().getDatabase(database);
            return true;
        } catch (NoSuchElementException e) {
            return false;
        } catch (Exception e) {
            // 老版本 HMS 抛 InvalidObjectException / MetaException 表示不存在
            if (isNotFound(e)) return false;
            throw new RuntimeException("探查 Hive 数据库失败: " + database + ": " + rootCause(e), e);
        }
    }

    public synchronized Optional<Database> rawDatabase(String database) {
        try {
            return Optional.of(client().getDatabase(database));
        } catch (NoSuchElementException e) {
            return Optional.empty();
        } catch (Exception e) {
            if (isNotFound(e)) return Optional.empty();
            throw new RuntimeException("探查 Hive 数据库失败: " + database + ": " + rootCause(e), e);
        }
    }

    public synchronized List<String> tables(String database) {
        try {
            List<String> tables = new ArrayList<>(client().getAllTables(database));
            tables.removeIf(t -> t == null || t.isEmpty());
            Collections.sort(tables);
            return tables;
        } catch (Exception e) {
            throw new RuntimeException("列出 Hive 表失败: " + database + ": " + rootCause(e), e);
        }
    }

    // ==================== 表元数据 ====================

    /** 表元数据（读路径，TTL 内复用快照）。表不存在返回 empty。 */
    public synchronized Optional<HiveTableMeta> table(String database, String table) {
        String key = database + "." + table;
        CachedTable cached = tableCache.get(key);
        if (cached != null && System.nanoTime() - cached.fetchedNanos() < TABLE_CACHE_TTL_NANOS) {
            return Optional.of(cached.meta());
        }
        return loadTable(database, table, key);
    }

    /**
     * 跳过缓存直接回源 HMS，并把结果回写缓存 —— <b>写 / DDL 路径专用</b>（建表、删表、写会话建立）。
     * 这些操作必须看到当前真实的表结构：旧快照会让写入按旧列校验页（缺列报错）或用旧 location
     * 落文件 / 搬分区。
     */
    public synchronized Optional<HiveTableMeta> tableFresh(String database, String table) {
        return loadTable(database, table, database + "." + table);
    }

    private Optional<HiveTableMeta> loadTable(String database, String table, String key) {
        Table t;
        try {
            t = client().getTable(database, table);
        } catch (NoSuchElementException e) {
            tableCache.remove(key);
            return Optional.empty();
        } catch (Exception e) {
            if (isNotFound(e)) {
                tableCache.remove(key);
                return Optional.empty();
            }
            throw new RuntimeException("探查 Hive 表失败: " + key + ": " + rootCause(e), e);
        }
        HiveTableMeta meta = translate(database, table, t);
        tableCache.put(key, new CachedTable(meta, System.nanoTime()));
        return Optional.of(meta);
    }

    public boolean tableExists(String database, String table) {
        return table(database, table).isPresent();
    }

    /** 失效单表缓存（本进程内 DDL 后调用；跨进程靠 TTL / 写路径回源兜底）。 */
    public synchronized void invalidateTable(String database, String table) {
        tableCache.remove(database + "." + table);
    }

    /** 清空表元数据缓存（Hive 侧 DDL 变更后由调用方手动触发）。 */
    public void invalidateCache() {
        tableCache.clear();
    }

    // ==================== ACID 写事务快照 ====================

    /**
     * 取该表的<b>有效写事务快照</b>（HMS {@code getValidWriteIds}）—— ACID 读取据此过滤
     * base / delta 目录：比快照水位高（未提交的事务）或落在 invalid 列表（已回滚）里的写 id
     * 一律不可见。
     *
     * <p>快照是<b>查询级</b>的（每次规划重新取），刻意不挂进按 {@code db.table} 缓存的
     * {@link HiveTableMeta} —— 表结构在查询期内不变，写事务快照会变。</p>
     *
     * <p>拿不到（HMS 版本过老 / 表非事务表）返回 {@code null}，由调用方按「不过滤」处理 ——
     * 宁可能多读几个已提交的 delta，也不要把可见数据当成不可见而丢行。</p>
     */
    public synchronized ValidReaderWriteIdList validWriteIds(String database, String table) {
        String full = database + "." + table;
        try {
            List<TableValidWriteIds> res = client().getValidWriteIds(List.of(full), null);
            if (res == null || res.isEmpty()) return null;
            TableValidWriteIds t = res.get(0);
            long[] invalid = t.getInvalidWriteIds() == null ? new long[0]
                    : t.getInvalidWriteIds().stream().mapToLong(Long::longValue).toArray();
            byte[] aborted = t.isSetAbortedBits() ? t.getAbortedBits() : null;
            BitSet abortedBits = aborted == null ? new BitSet() : BitSet.valueOf(aborted);
            long minOpen = t.isSetMinOpenWriteId() ? t.getMinOpenWriteId() : Long.MAX_VALUE;
            return new ValidReaderWriteIdList(t.getFullTableName() == null ? full : t.getFullTableName(),
                    invalid, abortedBits, t.getWriteIdHighWaterMark(), minOpen);
        } catch (Exception e) {
            throw new RuntimeException("获取 Hive ACID 写事务快照失败: " + full
                    + "（ACID 读取需要 Hive 3.x 及以上 HMS）", e);
        }
    }

    /**
     * 取<b>事务级读快照</b>（Hive2 用）—— 走 HMS 的 {@code get_open_txns_info}
     * （{@link HiveMetaStoreClient#showTxns()}），自行拼成 {@link ValidReadTxnList}。
     *
     * <p><b>为什么不用 {@code client.getValidTxns()}</b>：它内部走 {@code TxnUtils
     * .createValidReadTxnList}，要读 {@code GetOpenTxnsResponse.getAbortedBits()} —— 那是 Hive3
     * 才有的字段，连 Hive2 HMS 时该响应对应字段缺失、反序列化为 null，构造 bitset 时 NPE。
     * {@code get_open_txns_info} 返回的 {@code GetOpenTxnsInfoResponse} 是 Hive2/Hive3 共有的
     * Thrift 结构，故直接用它。</p>
     *
     * <p>拼装口径与 Hive 的 reader 一致：<b>未提交</b>（{@code OPEN}）与<b>已回滚</b>（{@code ABORTED}）
     * 的事务 id 进 {@code exceptions}（= 不可见集），{@code COMMITTED} 的跳过；{@code exceptions} 按
     * 升序（{@link ValidReadTxnList} 内部走 {@code binarySearch}，乱序必错）；{@code abortedBits}
     * 标记哪些 exception 是回滚的；{@code minOpen} 取最小的 OPEN 事务 id。</p>
     *
     * <p>拿不到（HMS 过老 / 表非事务表）返回 {@code null}，由调用方按「不过滤」处理。</p>
     */
    public synchronized ValidTxnList validTxns() {
        try {
            GetOpenTxnsInfoResponse resp = client().showTxns();
            return buildTxnSnapshot(resp.getTxn_high_water_mark(), resp.getOpen_txns());
        } catch (Exception e) {
            throw new RuntimeException("获取 Hive2 ACID 事务读快照失败"
                    + "（需 HMS 支持 get_open_txns_info）", e);
        }
    }

    /**
     * 由 HMS 的 {@code get_open_txns_info} 结果拼装 {@link ValidReadTxnList}（纯函数，供探针直测）。
     *
     * @param hwm   事务高水位（{@code >hwm} 的一律不可见）
     * @param infos 打开事务列表（各带状态）；{@code null} 视为空
     */
    static ValidReadTxnList buildTxnSnapshot(long hwm, List<TxnInfo> infos) {
        List<Long> invalid = new ArrayList<>();
        long minOpen = Long.MAX_VALUE;
        if (infos != null) {
            for (TxnInfo ti : infos) {
                TxnState st = ti.getState();
                if (st == TxnState.COMMITTED) continue;   // 已提交 → 可见
                invalid.add(ti.getId());                  // OPEN / ABORTED → 不可见
                if (st == TxnState.OPEN && ti.getId() < minOpen) minOpen = ti.getId();
            }
        }
        invalid.sort(Long::compareTo);   // binarySearch 要求升序
        long[] exceptions = new long[invalid.size()];
        for (int i = 0; i < exceptions.length; i++) {
            exceptions[i] = invalid.get(i);
        }
        BitSet abortedBits = new BitSet(exceptions.length);
        if (infos != null) {
            for (TxnInfo ti : infos) {
                if (ti.getState() == TxnState.ABORTED) {
                    int idx = Arrays.binarySearch(exceptions, ti.getId());
                    if (idx >= 0) abortedBits.set(idx);
                }
            }
        }
        return new ValidReadTxnList(exceptions, abortedBits, hwm, minOpen);
    }

    // ==================== ACID 写事务 ====================
    //
    // 与上面的「读快照」对称：写侧也按版本分流 —— Hive3 有独立的 writeId（openTxn +
    // allocateTableWriteId），Hive2 没有 writeId 概念，事务 id 本身就是目录 / 行标识里的 id。

    /**
     * 打开一个写事务（HMS {@code open_txns}），返回事务 id。
     *
     * <p>用批量版 RPC（{@code open_txns}）而非已废弃的单事务接口 —— Hive 2.3+ / 3.x 都支持，
     * 老于 2.3 的 HMS 会直接报 unknown method。</p>
     */
    public synchronized long openTxn(String user) {
        try {
            return client().openTxn(user);
        } catch (Exception e) {
            throw new RuntimeException("打开 Hive ACID 写事务失败（需 HMS 支持 open_txns）", e);
        }
    }

    /**
     * 在事务下为该表取<b>写 id</b>。
     *
     * <p>Hive3 走 HMS {@code allocate_write_id}；<b>Hive2 没有 writeId 概念</b>，事务 id 即目录 /
     * 行标识里的 id，直接返回 {@code txnId}。调用方拿到的这个 id 统称 {@code acidId}
     * （见 {@link HiveAcidLayout#deltaDirName(long)}）。</p>
     */
    public synchronized long allocateWriteId(long txnId, String database, String table) {
        if (hive2()) return txnId;
        try {
            return client().allocateTableWriteId(txnId, database, table);
        } catch (Exception e) {
            throw new RuntimeException("获取 Hive ACID 写 id 失败: " + database + "." + table
                    + "（需要 Hive 3.x 及以上 HMS；hive2 catalog 不做这一步）", e);
        }
    }

    /** 提交写事务 —— 提交后本次写出的 delta 目录才对读者可见。 */
    public synchronized void commitTxn(long txnId) {
        try {
            client().commitTxn(txnId);
        } catch (Exception e) {
            throw new RuntimeException("提交 Hive ACID 写事务失败（事务 " + txnId + "）", e);
        }
    }

    /** 回滚写事务（失败 / 取消时清理）。事务已不存在（超时被 HMS 回收）视为成功。 */
    public synchronized void rollbackTxn(long txnId) {
        try {
            client().rollbackTxn(txnId);
        } catch (org.apache.hadoop.hive.metastore.api.NoSuchTxnException e) {
            // 事务早已超时 / 被回滚 → 目标状态已达成
        } catch (Exception e) {
            throw new RuntimeException("回滚 Hive ACID 写事务失败（事务 " + txnId + "）", e);
        }
    }

    /**
     * 事务保活（HMS {@code heartbeat}）。长写（大 INSERT）超过 {@code hive.txn.timeout}
     * 会被 HMS 判为超时并回滚，写侧须定时心跳。
     *
     * <p>心跳失败一律吞掉：真正的失败由随后的 {@code commitTxn} 响亮报出，心跳线程不该把
     * 写线程打断在 HDFS 上。</p>
     */
    public synchronized void heartbeatTxn(long txnId) {
        try {
            client().heartbeat(txnId, 0L);
        } catch (Exception ignored) {
            // 见 javadoc：commit 阶段兜底报错
        }
    }

    private HiveTableMeta translate(String database, String table, Table t) {
        StorageDescriptor sd = t.getSd();
        String inputFormat = sd == null ? null : sd.getInputFormat();
        SerDeInfo serde = sd == null ? null : sd.getSerdeInfo();
        String serdeLib = serde == null ? null : serde.getSerializationLib();
        Map<String, String> serdeParams = serde == null || serde.getParameters() == null
                ? Map.of() : serde.getParameters();
        Map<String, String> params = t.getParameters() == null ? Map.of() : t.getParameters();

        List<Column> dataCols = toColumns(sd == null ? List.of() : sd.getCols());
        List<Column> partCols = toColumns(t.getPartitionKeys() == null
                ? List.of() : t.getPartitionKeys());

        HiveStorageFormat format = HiveStorageFormat.detect(inputFormat, serdeLib);

        // SerDe 分隔参数（LazySimpleSerDe 默认 \001 / \ / \N）
        char delimiter = textDelimiter(serdeParams, '\001');
        char escape = parseDelimiter(serdeParams.get("escape.delim"), '\\');
        String nullString = firstNonNull(serdeParams.get("serialization.null.format"), "\\N");
        if (format == HiveStorageFormat.TEXT) {
            // 文本表切错分隔符会「静默串列」—— 把生效值与 SerDe 原文亮出来（每表一次）
            System.err.printf("[Hive] TEXT 表 %s.%s：分隔符=%s 转义=%s NULL=%s（SerDe 原文 field.delim=%s"
                            + " serialization.format=%s）%n",
                    database, table, codePoint(delimiter), codePoint(escape), printable(nullString),
                    printable(serdeParams.get("field.delim")),
                    printable(serdeParams.get("serialization.format")));
        }

        boolean transactional = "true".equalsIgnoreCase(params.getOrDefault("transactional", "false"));

        // 分桶元数据（CLUSTERED BY ... INTO N BUCKETS / SORTED BY）：供桶裁剪用。
        List<String> bucketCols = sd == null || sd.getBucketCols() == null
                ? List.of() : List.copyOf(sd.getBucketCols());
        int numBuckets = sd == null ? 0 : sd.getNumBuckets();
        List<String> sortCols = new ArrayList<>();
        if (sd != null && sd.getSortCols() != null) {
            for (org.apache.hadoop.hive.metastore.api.Order o : sd.getSortCols()) {
                if (o != null && o.getCol() != null) sortCols.add(o.getCol());
            }
        }

        String location = sd == null ? null : sd.getLocation();
        return new HiveTableMeta(catalogName, database, table, location, format,
                inputFormat, serdeLib, transactional, new LinkedHashMap<>(params),
                dataCols, partCols, bucketCols, numBuckets, sortCols, delimiter, escape, nullString);
    }

    private static List<Column> toColumns(List<FieldSchema> schemas) {
        List<Column> columns = new ArrayList<>(schemas.size());
        for (FieldSchema fs : schemas) {
            String comment = fs.getComment();
            columns.add(new Column(fs.getName(), fs.getType(),
                    comment == null || comment.isEmpty() ? null : comment, null, true));
        }
        return columns;
    }

    // ==================== 分区 ====================

    /** 列分区（上限 {@code max}；HMS 的 short 参数上限 32767）。 */
    public synchronized List<Partition> partitions(String database, String table, int max) {
        short limit = (short) Math.min(max <= 0 ? Short.MAX_VALUE : max, Short.MAX_VALUE);
        try {
            List<Partition> parts = client().listPartitions(database, table, limit);
            return parts == null ? List.of() : parts;
        } catch (Exception e) {
            throw new RuntimeException("列出 Hive 分区失败: " + database + "." + table, e);
        }
    }

    // ==================== 写元数据（CTAS / INSERT 用） ====================
    //
    // 以下方法均 synchronized：与读方法共用同一个非线程安全的 HiveMetaStoreClient。

    /**
     * 建表（CTAS）：写入 HMS 并<b>失效该表的元数据缓存</b> —— 建表后紧随的写路径会
     * {@code table(db, t)} 取分区列 / location，缓存不失效会拿到「表不存在」的旧结论。
     */
    public synchronized void createTable(Table table) {
        try {
            client().createTable(table);
            tableCache.remove(table.getDbName() + "." + table.getTableName());
        } catch (Exception e) {
            throw new RuntimeException("创建 Hive 表失败: "
                    + table.getDbName() + "." + table.getTableName(), e);
        }
    }

    /** 删表（CTAS 失败清理 / DROP TABLE）：连同 HDFS 数据一起删，表不存在时静默。 */
    public synchronized void dropTable(String database, String table) {
        try {
            client().dropTable(database, table, /*deleteData*/ true, /*ignoreUnknown*/ true);
            tableCache.remove(database + "." + table);
        } catch (Exception e) {
            throw new RuntimeException("删除 Hive 表失败: " + database + "." + table, e);
        }
    }

    /**
     * 登记分区（已存在则改 location）—— 幂等：多个 Worker 可能同时为同一分区值登记，
     * 谁先谁后都收敛到同一个 location（分区目录路径是确定性的 {@code <location>/k=v}）。
     */
    public synchronized void upsertPartition(Partition partition) {
        try {
            client().add_partition(partition);
        } catch (Exception e) {
            if (!isAlreadyExists(e)) {
                throw new RuntimeException("登记 Hive 分区失败: "
                        + partition.getDbName() + "." + partition.getTableName()
                        + " " + partition.getValues(), e);
            }
            try {
                client().alter_partition(partition.getDbName(), partition.getTableName(), partition);
            } catch (Exception e2) {
                throw new RuntimeException("更新 Hive 分区失败: "
                        + partition.getDbName() + "." + partition.getTableName()
                        + " " + partition.getValues(), e2);
            }
        }
    }

    /** 删分区（动态分区 OVERWRITE 时旧分区被整目录替换，元数据要跟着换）。 */
    public synchronized void dropPartition(String database, String table, List<String> values) {
        try {
            client().dropPartition(database, table, values, /*deleteData*/ true);
        } catch (Exception e) {
            if (isNotFound(e)) return;
            throw new RuntimeException("删除 Hive 分区失败: " + database + "." + table + " " + values, e);
        }
    }

    // ==================== 列统计 ====================

    /** 列统计（Hive {@code ANALYZE TABLE ... COMPUTE STATISTICS} 落库的 NDV / 空值 / 最值）。 */
    public synchronized List<ColumnStats> columnStats(String database, String table,
                                                      List<String> columnNames) {
        if (columnNames == null || columnNames.isEmpty()) return List.of();
        List<ColumnStatisticsObj> objs;
        try {
            objs = client().getTableColumnStatistics(database, table, columnNames);
        } catch (Exception e) {
            // 未做过 ANALYZE / 老版本不支持 → 空表（不报错，统计只是优化输入）
            return List.of();
        }
        if (objs == null) return List.of();
        List<ColumnStats> out = new ArrayList<>(objs.size());
        for (ColumnStatisticsObj obj : objs) {
            out.add(toColumnStats(obj));
        }
        return out;
    }

    private static ColumnStats toColumnStats(ColumnStatisticsObj obj) {
        ColumnStatisticsData d = obj.getStatsData();
        long nulls = -1, ndv = -1;
        String min = null, max = null;
        Long avgLen = null;
        if (d != null) {
            if (d.isSetLongStats()) {
                var s = d.getLongStats();
                nulls = s.getNumNulls();
                ndv = s.getNumDVs();
                min = String.valueOf(s.getLowValue());
                max = String.valueOf(s.getHighValue());
            } else if (d.isSetDoubleStats()) {
                var s = d.getDoubleStats();
                nulls = s.getNumNulls();
                ndv = s.getNumDVs();
                min = String.valueOf(s.getLowValue());
                max = String.valueOf(s.getHighValue());
            } else if (d.isSetStringStats()) {
                var s = d.getStringStats();
                nulls = s.getNumNulls();
                ndv = s.getNumDVs();
                avgLen = s.isSetAvgColLen() ? (long) s.getAvgColLen() : null;
            } else if (d.isSetDecimalStats()) {
                var s = d.getDecimalStats();
                nulls = s.getNumNulls();
                ndv = s.getNumDVs();
                min = s.isSetLowValue() ? s.getLowValue().toString() : null;
                max = s.isSetHighValue() ? s.getHighValue().toString() : null;
            } else if (d.isSetDateStats()) {
                var s = d.getDateStats();
                nulls = s.getNumNulls();
                ndv = s.getNumDVs();
                min = s.isSetLowValue() ? s.getLowValue().toString() : null;
                max = s.isSetHighValue() ? s.getHighValue().toString() : null;
            }
        }
        return new ColumnStats(obj.getColName(), nulls, ndv,
                Optional.ofNullable(min), Optional.ofNullable(max), Optional.ofNullable(avgLen));
    }

    // ==================== 工具 ====================

    /**
     * 文本表的字段分隔符：优先 {@code field.delim}（<b>字面字符</b>语义），没有才退回
     * {@code serialization.format}（<b>十进制字符码</b>语义）。
     *
     * <p><b>两个键的语义不同，不能合并解析</b>：Hive 给只有 {@code serialization.format} 的文本表
     * 写的常见值就是 {@code "1"}，含义是 {@code U+0001}；按字面读成字符 {@code '1'} 会让整行在
     * 「数字 1」上乱切（线上 {@code staging_es_to_skb_info} 的串列事故），而 {@code field.delim="1"}
     * 又确实可以表示分隔符就是字符 {@code 1} —— 所以按键区分，不做猜测。</p>
     */
    static char textDelimiter(Map<String, String> serdeParams, char fallback) {
        String fieldDelim = serdeParams.get("field.delim");
        if (fieldDelim != null && !fieldDelim.isEmpty()) {
            return parseDelimiter(fieldDelim, fallback);
        }
        return parseSerializationFormat(serdeParams.get("serialization.format"), fallback);
    }

    /** {@code serialization.format}：全数字时是十进制字符码（Hive 写 {@code "1"} = {@code U+0001}）。 */
    static char parseSerializationFormat(String raw, char fallback) {
        if (raw == null || raw.isEmpty()) return fallback;
        String s = raw.trim();
        if (allDigits(s) && s.length() <= 5) {
            return parseCode(s, fallback);
        }
        return parseDelimiter(s, fallback);
    }

    /**
     * 字面语义的分隔符解析（{@code field.delim} / {@code escape.delim}）—— HMS 里的写法有：
     * 真字符（{@code U+0001}）、反斜杠 u 转义（形如反斜杠+u 加 4 位十六进制）、
     * {@code \t} / {@code \n} / {@code \r} / {@code \\} / {@code \xNN}、
     * 反斜杠+十进制/八进制的 {@code \001}。单个数字按字面字符处理（见 {@link #textDelimiter}）。
     */
    static char parseDelimiter(String raw, char fallback) {
        if (raw == null || raw.isEmpty()) return fallback;
        String s = raw.trim();
        if (s.isEmpty()) return fallback;
        if (s.length() >= 6 && s.startsWith("\\u")) {
            try {
                return (char) Integer.parseInt(s.substring(2, 6), 16);
            } catch (NumberFormatException ignored) {
                // 落到下面的分支
            }
        }
        if (s.length() >= 2 && s.charAt(0) == '\\') {
            switch (s.charAt(1)) {
                case 't': return '\t';
                case 'n': return '\n';
                case 'r': return '\r';
                case '\\': return '\\';
                case 'x':
                    if (s.length() >= 4) {
                        try {
                            return (char) Integer.parseInt(s.substring(2, 4), 16);
                        } catch (NumberFormatException ignored) {
                            // 落到下面
                        }
                    }
                    break;
                default:
                    break;
            }
            if (allDigits(s.substring(1))) {
                return parseCode(s.substring(1), fallback);
            }
        }
        return raw.charAt(0);
    }

    /** 字符 → 日志可读形式（控制字符打成 {@code U+0001}）。 */
    private static String codePoint(char c) {
        return Character.isISOControl(c) ? String.format("U+%04X", (int) c) : "'" + c + "'";
    }

    /** 字符串 → 日志可读形式（控制字符逐个转义，空/null 打 {@code <无>}）。 */
    private static String printable(String s) {
        if (s == null || s.isEmpty()) return "<无>";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            sb.append(Character.isISOControl(c) ? String.format("\\u%04X", (int) c) : String.valueOf(c));
        }
        return sb.toString();
    }

    private static char parseCode(String digits, char fallback) {
        try {
            int code = Integer.parseInt(digits);
            if (code >= 0 && code <= Character.MAX_VALUE) return (char) code;
        } catch (NumberFormatException ignored) {
            // 忽略，退回默认
        }
        return fallback;
    }

    private static boolean allDigits(String s) {
        if (s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) return false;
        }
        return true;
    }

    private static String firstNonNull(String a, String b) {
        return (a == null || a.isEmpty()) ? b : a;
    }

    /** 把 HMS 的「对象已存在」异常族归一（add_partition 撞已登记分区）。 */
    private static boolean isAlreadyExists(Exception e) {
        String name = e.getClass().getSimpleName().toLowerCase(Locale.ROOT);
        if (name.contains("alreadyexists")) return true;
        String msg = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
        return msg.contains("already exists");
    }

    /** 把 HMS 的「对象不存在」异常族归一（不同版本抛的类型不一样）。 */
    /**
     * 把根因写进异常消息 —— HMS 的 Thrift 异常常是「外层一句话 + 内层真正原因」，
     * 只报外层时 WebUI 上只剩「探查 Hive 表失败」这种无信息量的提示。
     */
    private static String rootCause(Exception e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = root.getMessage();
        return msg == null || msg.isBlank()
                ? root.getClass().getSimpleName()
                : root.getClass().getSimpleName() + ": " + msg;
    }

    private static boolean isNotFound(Exception e) {
        String name = e.getClass().getSimpleName().toLowerCase(Locale.ROOT);
        if (name.contains("nosuch") || name.contains("invalid")) return true;
        String msg = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
        return msg.contains("does not exist") || msg.contains("not found")
                || msg.contains("no such");
    }
}
