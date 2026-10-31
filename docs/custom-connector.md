# 自定义连接器开发指引

本文面向**第三方开发者**：你要让 Dojao 读写一个内置连接器（MySQL / TiDB / Doris / PostgreSQL / SQL Server / Oracle / Hive / ClickHouse / Excel）覆盖不到的数据源，
并且**不改动 Dojao 任何一行代码**。

读完本文你会知道：

- 一个连接器要放进哪、怎么被引擎发现；
- 必须实现哪些接口、哪些可以不实现；
- 怎么把表切成并行切片（含"切片信息放不进 SQL 谓词"时怎么办）；
- 三个**会静默出错**的坑（谓词下推、切片覆盖、列裁剪），以及怎么避开。

配套可直接抄的完整示例：[`examples/dojao-connector-example/`](../examples/dojao-connector-example) ——
一个把本地目录当数据库读的 `csvdir` 连接器（约 400 行，无第三方依赖）。

---

## 1. 五分钟上手

### 1.1 三件事

**① 依赖 `dojao-spi`（`provided`）**

```xml
<dependency>
    <groupId>com.dojao</groupId>
    <artifactId>dojao-spi</artifactId>
    <version>0.1.0</version>
    <scope>provided</scope>
</dependency>
```

必须是 `provided`：引擎运行时已经自带 `dojao-spi`，把自己那份打进 jar 会让插件里的接口和引擎里的
不是同一个 `Class` 对象（`@Override` 变成"实现了另一个接口"）。插件类加载器对
`com.dojao.spi.*` / `com.dojao.common.*` 一律走**父加载器优先**，所以就算误打进去也以引擎版本为准，
但那只会在日志里留下一条加载失败记录，不如一开始就做对。

顺带一提：`dojao-spi` 传递依赖 `dojao-common`（`Page`）与 `arrow-vector`，都是 `provided` 语义，
你不需要额外声明。（本示例 pom 里额外 pin 了 Jackson 版本，那只是为了在离线仓库里能解析到存在的版本。）

**② 实现 `ConnectorFactory` + `DojaoConnector`**

```java
public final class MyConnectorFactory implements ConnectorFactory {
    @Override public String type() { return "mydb"; }          // CREATE CATALOG ... USING mydb
    @Override public DojaoConnector create(String catalogName, Map<String, String> properties) {
        return new MyConnector(catalogName, properties);
    }
}
```

`ConnectorFactory` 只有 `type()` / `create` 是必须的，其余可选覆写都会让人用得更顺：

| 方法 | 作用 | 不覆写的后果 |
|---|---|---|
| `displayName()` | `SHOW CATALOGS` / Web UI 里给人看的名字 | 直接显示 `type()` |
| `describeProperties()` | 属性说明（键 → 一句话）：哪些必填、长什么样 | Web UI 建 catalog 表单没有提示；报错信息列不出可用属性 |
| `validate(catalog, props)` | 落库前的**静态**准入检查，缺属性就抛 `IllegalArgumentException` | 属性写错要等到第一次查询才暴露 |
| `apiVersion()` | 声明编译时针对的 SPI 版本 | 默认取当前版本；插件比引擎新时会被拒绝加载（fail-fast 而不是运行时炸） |

**③ 声明 `META-INF/services`**

```
src/main/resources/META-INF/services/com.dojao.spi.service.ConnectorFactory
```

内容一行一个实现类全限定名（`#` 开头是注释）：

```
com.example.mydb.MyConnectorFactory
```

### 1.2 打包与部署

```bash
mvn -f examples/dojao-connector-example/pom.xml package
cp target/dojao-connector-csvdir-0.1.0.jar <dojao部署目录>/lib/connectors/
```

重启 Master 与 Worker，然后：

```sql
CREATE CATALOG demo USING csvdir WITH ('catalog.csvdir.dir' = '/data/csv');
SHOW CATALOGS;
SELECT * FROM demo.sales.orders LIMIT 10;
```

> **属性键命名规范**：catalog 级别的属性一律写成 `catalog.<连接器类型>.<属性名>`，例如
> `catalog.mysql.jdbcUrl`、`catalog.hive.metastore.uris`，你的 `csvdir` 连接器就是
> `catalog.csvdir.dir` / `catalog.csvdir.delimiter`。好处是键自解释、跨连接器不冲突。
> SPI 提供 `com.dojao.spi.util.CatalogProperties.normalize(type, props)` 做归一化 ——
> 在 `validate` / `create` 入口调一次，带前缀的键会**降级成裸键**（`dir`），
> 于是连接器内部照旧按裸键读；同时**旧写法（`'dir' = ...`）继续可用**。
> 详细契约见 `ConnectorFactory#type()` 与 `CatalogProperties` 的 javadoc。

**插件目录怎么定**（优先级从高到低，见 `ConnectorRegistry#resolvePluginDir`）：

1. `-Dsystem.connector.plugins.dir=<目录>`；
2. `-Ddojao.home=<部署根>` → `<部署根>/lib/connectors`（启动脚本已经设了这个属性）；
3. 当前工作目录下的 `./lib/connectors`。

目录不存在 = 没有插件，不算错误。**每个 jar 一个类加载器**：哪个 jar 坏了会指名道姓地报出来
（`连接器加载失败 [my-connector-1.0.jar]: ...`），两个插件各自带的依赖版本也不会互相污染。

> 插件 jar **只需自带引擎 `lib/` 里没有的依赖**（例如某个第三方 JDBC 驱动）。插件类加载器是
> **子优先 + 父回退**：先看插件自己的 jar，找不到的类会回落到服务端 classpath（`lib/`）。
> 引擎自带的驱动 / Hadoop / ORC / `dojao-connector-common` 等都在 `lib/`，无须重复打包；
> 若想钉死自己的依赖版本（子优先），再把它们 shade 进 jar。

---

## 2. `DojaoConnector` 要实现的四类东西

```
DojaoConnector
├── 元数据探查（继承自 DataSourceMetaService）  catalog/schema/table/column 的存在性、清单、结构 + 统计
├── 数据读取（核心）                            planSplits() → openSplit() → ScanReader.next()
├── 数据写入（可选）                            write() / createTable() / dropTable()
└── 优化钩子（可选）                            pruneColumns() / pushdownPredicate() / sourceFields()
                                              + 能力声明 supportsPredicatePushdown()（见 §4.1）
```

**最小可用连接器只需要**：元数据那几个方法（至少要 `tableExists` / `getTableColumns`），加上
`openSplit()`。谓词下推是**可选**的 —— 不做过滤就把 `supportsPredicatePushdown()` 留成默认
`false`，引擎会自己过滤（见 §4.1）；**做了过滤才声明 `true`**。

### 2.1 元数据：引擎拿它做什么

| 方法 | 引擎用在哪 | 返回错的后果 |
|---|---|---|
| `tableExists` / `getTableColumns` | SQL 语义校验、列裁剪、CTAS 类型推导 | 报"表/列不存在"，或建出错的表 |
| `listSchemas` / `listTables` | `SHOW SCHEMAS` / `SHOW TABLES` | 只影响这两个语句 |
| `sourceFields(...)` | CTAS 建目标表时确定列类型 | 退化为 `getColumnTypes()` + 宽安全默认（不截断，但列可能偏宽） |
| `getTableStats` | **并行度估算**（按字节数决定切几片） | 返回 `0` 会被当成空表；**未知请返回 `-1`** |
| `estimateRows(catalog, schema, table, filter)` | 监控里 Scan 进度条的**分母** | 默认忽略 filter 退化为整表行数：进度条偏慢但语义正确 |

`getTableStats` 的契约（`DataSourceMetaService` 里写死了，务必遵守）：

- `rowCount` / `byteSize` 是**估算值**，引擎不当精确值用；
- **探查不出来就返回 `-1`**，不要返回 `0`（`0` = 空表，会让引擎按最小并行度跑）；
- 别为了精确去扫全表 —— 用廉价元数据（Hive 读 ORC footer、MySQL 查 `information_schema.TABLES`）。

### 2.2 读取：唯一的入口是 `openSplit`

引擎**只调** `openSplit(request, split)`，拿到一个 `ScanReader`，反复 `next()` 直到 `Page.eof()`，
最后 `close()`。

```java
@Override
public ScanReader openSplit(ReadRequest request, Split split) {
    List<String> files = split.hasPayload()
            ? MySplitPayload.decode(split.payload())          // 私有定位信息：本切片读哪几个文件
            : allFilesOf(request.schema(), request.table());   // 整表（无分片）
    return new MyScanReader(files, request);
}
```

**为什么游标要放进 `ScanReader` 而不是连接器对象**：`read(ReadRequest)` 的"连续调用接着上次读"语义
把"读到哪儿"挂在连接器实例上，一个实例同时只能服务一个切片；跨 JVM 并行扫描时引擎就得为每个分区
复制连接器实例。`ScanReader` 让每个切片自带状态，**连接器实例因此是无状态、可多线程共享的**。

> `read(ReadRequest)` 现在有默认实现（直接抛 `UnsupportedOperationException`）：**引擎不调用它**，
> 它只是给"只实现了 `read`"的既有连接器留的桥 —— `openSplit` 的默认实现会转发到 `read`。
> 覆写了 `openSplit` 的连接器可以完全不管 `read`。

同理，** `copy()` 也不需要覆写**：只有当你的游标状态挂在连接器实例上（即你仍走 `read` 那条路）时，
才必须返回一个真正独立的副本。默认实现返回 `this` —— 这是有意的安全兜底：不覆写 = 放弃多实例并行，
慢但不会读错。

---

## 3. 并行切片（Split / ScanSlice）

### 3.1 两种切片形态

| 形态 | 切片长什么样 | 谁这么干 |
|---|---|---|
| **谓词型** | 每片只是同一个表上的不同 `WHERE`（`id >= 100 AND id < 200`） | MySQL（主键范围）、PostgreSQL（ctid 范围） |
| **载荷型** | 整表请求 + 连接器私有字节（`"读哪几个文件/stripe"`） | Hive（HDFS 文件）、本示例（CSV part 文件） |

SPI 把切片表达成两段：

```java
record ScanSlice(ReadRequest request, Split split)     // 公共那半 + 私有那半
record Split(byte[] payload)                           // payload == null ⇒ Split.WHOLE（整表不分片）
```

引擎**只搬运**载荷，从不解析它 —— 所以载荷格式是你自己的事（版本、路径、序列化方式随你定）。

### 3.2 规划入口：`planSplits(request, parallelism)`

```java
@Override
public List<ScanSlice> planSplits(ReadRequest request, int parallelism) {
    if (parallelism <= 1 || request.limit() > 0 || request.offset() > 0) {
        return List.of();     // 分页请求与切片互斥
    }
    List<String> files = allFilesOf(request.schema(), request.table());
    if (files.isEmpty()) return List.of();          // 空 = 切不了，上层退化为单切片整表扫描
    // ...把 files 分成至多 parallelism 组，每组一个 ScanSlice
}
```

**契约（硬性，违反了不会报错，只会静默出错）**：

> 返回的切片必须**互不重叠**，且**合起来恰好覆盖整表**。
> 丢一半 = 静默少数据；重叠 = 静默重复行。

引擎侧的两条保证，你不必担心：

- 切片数**不会**被 Worker 数截断 —— 切片多于读线程时，同线程顺序处理剩余切片，
  **绝不按实例数丢弃切片**（见 `ParallelTableScanOperator`）；
- 返回空列表是合法且常见的（小表、无主键、分页查询），上层会退化成"单切片整表扫描"。

### 3.3 谓词型连接器：只覆写 `planParallelScan` 即可

如果你的切片能用 SQL 谓词表达，覆写更底层的 `planParallelScan(request, parallelism)`
返回 `List<ReadRequest>` 就行 —— `planSplits` 的默认实现会把每个请求包成 `ScanSlice.whole(...)`，
你完全不用感知 `Split`。基类还提供了 `splitRangePartitions(...)` 帮你把 `[min, max]` 均分成 N 段。

---

## 4. 四个会静默出错的坑

### 4.1 ⚠️ 谓词下推：能力必须显式声明（`supportsPredicatePushdown`）

规划器只会把可下推的 WHERE 谓词交给**声明了自己能过滤**的连接器。判定标准是连接器自报的能力位：

```java
/**
 * 读取侧是否会按 request.filterExpression() 过滤行。
 * true  → 引擎下推谓词，并且不再在本地重复过滤
 * false → 引擎不下推，在扫描之上保留本地 Filter（结果一样，只是过滤在引擎侧做）→ 默认值
 */
@Override
public boolean supportsPredicatePushdown() {
    return true;   // 只有"确实会过滤"才可以返回 true
}
```

**为什么必须是显式声明而不是"默默遵守"。** 引擎下推之后会把本地的 `Filter` 算子消掉
（`Planner` 各下推点），因为「下推成功 = 过滤已发生」。若连接器拿到 `filterExpression` 却不理它，
查询**不会报错**，只会**静默多返回行** —— 没有异常、没有日志，只有对不上的行数。

> 默认值是 `false`（保守）：漏声明的代价只是少一次下推优化（引擎自己包一层 `Filter`，结果正确）；
> 而默认 `true` 的漏声明就是静默算错。两种错法不对称，所以选保守。

返回 `true` 时，有两条兑现路径，二选一：

1. **翻译成数据源自己的 SQL**（推荐，内置 MySQL / PostgreSQL 就是这么干的）——
   `pushdownPredicate` 把谓词合入 `filterExpression`，`buildReadSql` 再拼进 `SELECT ... WHERE ...`；
2. **在连接器里本地求值** —— 示例连接器走的这条路（`SimpleFilter`），
   并且**遇到不会解析的形态直接抛异常**，而不是放行所有行。

```java
@Override
public ReadRequest pushdownPredicate(ReadRequest request, String predicate) {
    // 默认实现已把谓词以 AND 合入 filterExpression；只有需要方言翻译时才覆写
    return DojaoConnector.super.pushdownPredicate(request, predicate);
}
```

> **如果暂时不做过滤（只按切片信息读文件，如 Hive 连接器）**：保持默认的 `false` 即可 ——
> 引擎会把谓词留在本地 `Filter`，结果正确。**不要**为了"看起来支持"而返回 `true`。
> 引擎也只会下推"数据源认识"的谓词：含 Dojao 注册函数（如 `cosine_similarity`）的谓词留在本地，
> 但**列比较、AND 组合、IS NULL 这类都会推下来**，别指望它自动跳过。

### 4.2 ⚠️ 切片覆盖

见 §3.2 的契约。写完 `planSplits` 后，务必用一个小表做对拍：
`SELECT COUNT(*)` 的结果必须与"单切片整表扫描"一致（引擎日志里会打印
`Planned N parallel slices`，把并行度设成 1 再跑一次即可对比）。

### 4.3 ⚠️ 列裁剪：`columns` 为空 ≠ 没有列

`request.columns()` 为 `empty` 表示**读所有列**，不是"一列都不要"。这是最容易踩的反直觉点：
把空清单当成"零列"，查询会照常成功，只是**一行都读不出来**。

> 引擎内部还有一个投影哨兵 `"*"`（`SELECT *` 的语义）。它**不该**被当成列名去表头/元数据里匹配 ——
> 遇到 `"*"` 一律按「读所有列」处理（示例里就是 `cols.contains("*") ? List.of() : cols` 一行）。
> 当前引擎多数路径会在更早处把 `*` 展开成真实列名，传到连接器手上的可能性不大，但**按名字匹配
> 一旦命中就是静默少列**，防御成本只有一行，不值得省。

### 4.4 ⚠️ Arrow 页面：别忘了 `setRowCount`

`Page` 的行数取自 `VectorSchemaRoot.getRowCount()`：

```java
VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
// ... setSafe(...) 逐格写...
root.setRowCount(rows);                 // 不设 → 行数为 0 → 整页被当空页丢弃
return Page.fromRoot(root);
```

另外 `Page.sizeInBytes` 来自各向量 `getBufferSize()`，而 **Arrow 的 `valueCount` 不设为 0 时
`getBufferSize()` 返回 0** —— 用 `setSafe()` 写数据（它会推进 `valueCount`）或显式
`setValueCount(n)`，否则监控里的"输入字节"会恒等于 0 B。

---

## 5. 写方向（可选）

| 能力 | 覆写 | 说明 |
|---|---|---|
| INSERT INTO / OVERWRITE | `write(WriteRequest)` | 必须实现；`OverwriteEnable` 另见 SPI |
| CTAS 目标表 | `createTable(...)` + `dropTable(...)` | 默认抛 `UnsupportedOperationException`；`createTable` 返回 `false` 表示"表已存在且 `ifNotExists`，调用方须跳过写入" |
| 字符串列 DDL | `varcharMapping()` | 不覆写 = 全 `TEXT` 兜底 |

写入在分布式下**数据永不回 Master**：每个分区 Worker 就地写，Master 只收 N 个完成事件求和。
所以 `write` 会被并行调用，实现里不要指望全局单线程语义。

---

## 6. 调试与自检清单

**日志**：`ConnectorRegistry` 把加载失败记成数据、由 `MetaStore` 在启动时打成一行

```
连接器加载失败 [my-connector-1.0.jar]: jar 里没有 META-INF/services/com.dojao.spi.service.ConnectorFactory 声明
```

找不到类型时的报错也会带上插件目录位置与已注册类型清单（`MetaStore#createFromStorage`）。

**上线前自检**：

- [ ] 同样一条 `SELECT COUNT(*)` 在并行度 1 与 N 下结果一致（切片覆盖）；
- [ ] `SELECT *`、`SELECT a, b` 都返回预期列（列裁剪 + `"*"` 哨兵）；
- [ ] 带 `WHERE` 的查询结果与不带 `WHERE` 再本地过滤一致（谓词下推契约，§4.1）；
- [ ] 建表后立刻查询能读到数据（元数据可见性）；
- [ ] 查询跑完没有 `Memory was leaked by query`（页面与 reader 都被 `close`）。

**常见症状 → 可能原因**：

| 症状 | 先查 |
|---|---|
| 结果的 WHERE 没生效（行数偏多） | §4.1 `supportsPredicatePushdown()` 声明了 `true` 却没过滤；若你返回 `false`，那就是引擎侧 bug |
| 行数 ≈ 1/2、1/N（并行度相关） | §4.2 切片没覆盖全表 |
| 少了某些列 | §4.3 `columns` / `"*"` 处理 |
| 查询成功但一行都没有 | §4.4 忘了 `setRowCount` |
| `NoSuchMethodError` / `LinkageError` | 插件把 `dojao-spi` / `dojao-common` 打进了 jar |

---

## 7. 相关源码

| 文件 | 说明 |
|---|---|
| `dojao-spi/src/main/java/com/dojao/spi/service/DojaoConnector.java` | 连接器契约（本指南的主角） |
| `dojao-spi/src/main/java/com/dojao/spi/service/ConnectorFactory.java` | 工厂契约与可选钩子 |
| `dojao-spi/src/main/java/com/dojao/spi/service/DataSourceMetaService.java` | 元数据探查契约（catalog/schema/table/column + 统计） |
| `dojao-spi/src/main/java/com/dojao/spi/bean/{ReadRequest,Split,ScanSlice}.java` | 请求、切片、切片+请求 |
| `dojao-spi/src/main/java/com/dojao/spi/service/ScanReader.java` | 切片级读取游标 |
| `dojao-spi/src/main/java/com/dojao/spi/service/ConnectorRegistry.java` | 插件加载（扫描、类加载器、失败记录） |
| `connector/dojao-connector-hive/` | 载荷型切片的真实范例（HDFS 文件分组） |
| `connector/dojao-connector-mysql/` `postgres/` | 谓词型切片的真实范例（范围分区） |
| `examples/dojao-connector-example/` | 本指南的完整示例（CSV 目录） |
