# 自定义连接器示例：CSV 目录（`csvdir`）

一个**约 400 行、零第三方依赖**的完整自定义连接器：把本地目录当成数据库读。

```
<dir>/<schema>/<table>/part-000.csv
                           /part-001.csv
```

- schema = `<dir>` 的一级子目录；table = schema 下的子目录（含 `*.csv` 即为表）；
- 列 = part 文件首行表头，类型一律 `STRING`（示例从简）；
- 一个 part 文件（或一组文件）= 一个切片 → 并行扫描天然按文件切开。

开发指引见 [`docs/custom-connector.md`](../../docs/custom-connector.md)，本目录是它的配套可运行代码。

## 它演示了什么

| 要点 | 看哪里 |
|---|---|
| 唯一必须实现的契约：`type()` + `create()` + `META-INF/services` 登记 | `CsvDirConnectorFactory`、`src/main/resources/META-INF/services/` |
| 「让人用得更顺」的可选钩子：`displayName` / `describeProperties` / `validate` | `CsvDirConnectorFactory` |
| 元数据探查（引擎靠它做语义校验、列裁剪、CTAS 类型推导） | `CsvDirConnector#listSchemas/listTables/getTableColumns/getTableStats` |
| **切片用不透明载荷表达**（"读哪几个文件"不是 SQL 谓词，塞不进 `filterExpression`） | `planSplits` + `Split.of(...)` + `CsvSplitPayload` |
| **游标状态放在 `ScanReader` 里**（连接器实例因此无状态、可多线程共享，不必覆写 `copy()`） | `CsvDirConnector.CsvScanReader` |
| **声明并兑现谓词下推能力**（`supportsPredicatePushdown()` 返回 `true`，引擎才会下推并消掉本地 Filter；做不到就返回默认 `false`） | `CsvDirConnector#supportsPredicatePushdown` + `SimpleFilter`（不认识的形态直接抛错，绝不静默放行） |
| `columns` 空 = 全列 / `"*"` 哨兵也是全列 | `CsvScanReader` 构造函数 |
| 造页收尾：`root.setRowCount(rows)`（不设 → 整页被当空页丢掉） | `CsvScanReader#readBatch` |

刻意**不做**的（真实连接器按需补齐）：写入 / 建表 / 谓词翻译成数据源 SQL / 列级统计 / 类型推断 /
字段内的引号与转义。

## 构建与部署

本模块**刻意不是** Dojao 主仓库的子模块（没有 `<parent>`）：它只依赖 `dojao-spi`（`provided`），
与 Dojao 的构建、版本、模块划分无关 —— 第三方抱着这个目录就能抄着写。

```bash
# 1. 先让 dojao-spi 进本地仓库（主仓库里执行一次即可）
mvn -pl dojao-spi -am install -DskipTests

# 2. 构建本示例
mvn -f examples/dojao-connector-example/pom.xml package

# 3. 丢进部署目录（插件目录优先级见 docs/custom-connector.md §1.2）
cp target/dojao-connector-csvdir-0.1.0.jar <dojao部署目录>/lib/connectors/
```

> `dojao-spi` 必须是 `provided`。插件类加载器对 `com.dojao.spi.*` / `com.dojao.common.*`
> 走父加载器优先，误打进去虽以引擎版本为准，但会在日志里留下一条加载失败记录 —— 不如一开始就做对。

重启 Master 与 Worker 后：

```sql
CREATE CATALOG demo USING csvdir WITH ('catalog.csvdir.dir' = '/data/csv');
SHOW CATALOGS;
SELECT * FROM demo.sales.orders LIMIT 10;
```

## 常见问题

| 症状 | 先查 |
|---|---|
| 结果的 WHERE 没生效（行数偏多） | 声明了 `supportsPredicatePushdown() == true` 却没真过滤（`SimpleFilter`）；做不到就把能力位返回 `false` |
| 行数 ≈ 1/N | `planSplits` 的切片有没有**恰好覆盖**整表（示例用连续分组 `[g*n/G, (g+1)*n/G)`） |
| 查询成功但一行都没有 | `columns` 空清单被当成"零列"；或忘了 `root.setRowCount(rows)` |
| `SHOW CATALOGS` 里看不到 `csvdir` | jar 里有没有 `META-INF/services/com.dojao.spi.service.ConnectorFactory`；插件目录对不对 |
| `NoSuchMethodError` / `LinkageError` | `dojao-spi` 是不是被打进了 jar（应为 `provided`） |
