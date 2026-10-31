# Dojao

**打破数据库之间的隔阂 —— 面向 AI 时代的数据科学计算引擎**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21-red.svg)](https://jdk.java.net/21/)
[![Python](https://img.shields.io/badge/Python-3.11+-green.svg)](https://www.python.org/)

---

## 简介

Dojao 不是一个数据库，也不只是一个查询引擎。它是一个**面向数据科学家和分析师的分布式计算引擎**：
一条 SQL 里同时完成 **跨源联邦查询**、**Python 向量化计算** 与 **AI 调用**，**不需要搬迁数据**。

设计初衷只有一句话：**让分析师用一条 SQL 打通散落在 MySQL / TiDB / Doris / PostgreSQL / SQL Server / Oracle /
Hive / ClickHouse / Excel 里的数据，让数据科学家不写一行 JVM 语言就能把 Python 统计量 / 小模型直接当 SQL 函数用。**

配套：**单进程可嵌入**，也能**独立组集群**（Master / Worker，Arrow Flight 传输）；
内存不足时**自动溢写磁盘**；**算子自观测**，内置 Web 控制台看每一次查询的进度、内存与溢写。

---

## 为什么是 Dojao（核心差异化）

1. **确定性优先于峰值性能** —— 大表 Join 场景下，「能跑完」比「跑得快」重要。
2. **磁盘辅助是一等公民** —— 统一的溢写管线（P1 攒 + 溢写 / P1.5 归并 / P2 输出），
   GroupBy / Window / SortMergeJoin / HashJoin 四个算子共享；**溢写是「减量」的**：
   窗口 `rn=1` 溢写时即按组裁到 N 行，高基数 GROUP BY 用 K 路归并（峰值内存与 key 总数无关）。
3. **跨源向量计算** —— 向量列（如 pgvector）映射为 Arrow `FixedSizeList<Float32>`，
   `cosine_similarity / l2_distance / inner_product` 一条 SQL 完成，数据不动。
4. **Python UDF + Arrow 零拷贝** —— 数据科学家用 NumPy / Polars 直接向量化执行，无跨语言逐行序列化；
   协议（DFP）**语言中立**，任何会说 Arrow 的语言都能作为函数运行时。
5. **AI 原生集成**（规划中）—— AIFunction 注册进表达式体系，Agent 通过元数据发现并调用。
6. **单进程可嵌入 + 可集群部署** —— 既是嵌入式引擎，也能独立集群。
7. **算子自观测** —— `AbstractMeasurableOperator` + 零注入统计，为 Web 控制台与可解释排障提供数据。

---

## 设计理念

### 分区即并行
Hash 分区算子恒定并行，**无串行回退路径**。全链路并行：Scan → Partition → Join → Sort 一条线，端到端无瓶颈。

### 数据不动，索引动
Arrow 列存储在内存中**不动**，只有行号在 Map / Sort 中重排；溢写时一次性重排输出。传输与溢写路径**天然零拷贝**。

### 算子统一管线
溢写完全复用 `AbstractSpillableOperator`：新增算子只需实现 Slot 接口 + P2 输出逻辑，即自动获得溢写能力。

### 内存模型：堆外跑数据，堆内管索引，预算只写一个总量，全节点共享一个根
- **数据在堆外**（Arrow `RootAllocator`），**索引在堆内**，二者是**同一笔预算**（`query.slot.memory`）；
- 用户只声明一个键 `system.<role>.memory`（节点内存总量），
  heap / direct / `query.slot.memory` / `query.direct.limit` **全部派生**、能算能打印；
- 全节点一个共享根分配器 `NodeMemory`：算子 / 连接器 / transport 统一记账，超限只让**查询失败**，不让**节点被内核杀掉**；
- **读端背压**把「内存不够」变成「读得慢一点」，让溢写追上来而非 OOM。

> 详细推导（为什么 slot.memory 是一笔混合账、为什么堆不用大、`query.direct.limit` 如何封顶）
> 见 **[设计说明书](dojao_design_manual.html)**。

---

## 核心特性

| 特性 | 说明 |
|------|------|
| **联邦查询** | 一条 SQL 跨 MySQL / TiDB / Doris / PostgreSQL / SQL Server / Oracle / Hive / ClickHouse / Excel 读取，三段名 `catalog.schema.table` |
| **Python UDF / UDAF** | Arrow 批处理 + Arrow Flight（gRPC）通信；装饰器注册；协议语言中立（见 `UDF_PROTOCOL.md`） |
| **磁盘溢写** | 窗口 / 排序 / 聚合 / 排序归并 Join 自动溢写；**减量式**溢写，多盘轮转，盘满保护 |
| **向量计算** | pgvector 等向量列映射 `FixedSizeList<Float32>`，内置 `cosine_similarity` / `l2_distance` / `inner_product` |
| **列裁剪 + 谓词下推** | 三层列裁剪（保险丝 / 需求传播 / analyzer 绑定）；连接器自报谓词下推能力，杜绝静默多读 |
| **动态资源管理** | 查询级资源上限 + 准入排队 + 动态并行度（按数据量估算切片数）+ 资源回收 |
| **分布式执行** | Exchange 算子（Arrow Flight）+ Fragment 计划 + Coordinator 调度；数据永不回 Master |
| **轻量部署** | Java 21 + 虚拟线程，单 JAR 启动；单进程可嵌入 |
| **Web 控制台** | 查询监控 / 任务看板 / 内存与磁盘溢写观测 / 血缘 DAG / SQL 自动补全 / 函数与 Catalog 管理 |
| **客户端** | 自带 CLI 与 JDBC 驱动；支持查询取消 |
| **AI Agent**（骨架） | 自然语言问数 + UDF 自动成为 AI Tool；`ai.*` 配置命名空间已预留 |

---

## 架构总览（模块）

| 模块 | 职责 |
|---|---|
| `dojao-grammar` | ANTLR 语法、语义分析（`SemanticAnalyzer`）、逻辑计划、优化器（列裁剪 / 谓词下推） |
| `dojao-core` | 算子、表达式引擎、标量/聚合函数、执行、内存模型、溢写管线 |
| `dojao-spi` | 连接器 SPI（`ConnectorFactory` / `DojaoConnector` / `ScanReader` / `Split`）+ 函数 SPI |
| `dojao-common` | `Page` 与 Arrow 公共类型 |
| `dojao-metastore` | SQLite 元数据（Catalog 注册 + 持久化函数） |
| `dojao-server` | Master / Worker / Arrow Flight 传输 / Exchange / 作业管理与调度 |
| `connector/*` | 内置连接器：`mysql` / `tidb` / `doris` / `postgresql` / `sqlserver` / `oracle` / `hive`（Hive2·Hive3）/ `clickhouse` / `excel` |
| `dojao-ui` | Web 控制台（静态资源 + 监控 API） |
| `dojao-cli` | 命令行客户端 |
| `dojao-jdbc` | JDBC 驱动 |
| `dojao-ai` | AI Agent（骨架，待扩展） |
| `dojao-official-website` | 官网（Vue 3 + Vite 静态站点；构建时自动同步根目录博客与设计说明书） |
| `dojao-dist` | 打包与发行（内置 `bin/`、`conf/dojao.conf`、`lib/connectors/`） |
| `python/` | Python sidecar 参考实现（`dojao_udf`） |

---

## 快速开始

### 构建

```bash
git clone https://gitee.com/scottjia/dojao.git
cd dojao
./mvnw clean package -DskipTests
```

### 启动（单机 / 分机通用）

构建产物是一份自包含发行包 `dojao-dist/target/dojao-1.0-SNAPSHOT.tar.gz`，解压即用，
内含 `bin/`（启动脚本）、`conf/dojao.conf`（一份文件描述整个集群）、`lib/`（引擎 + 内置连接器）、`python/`（sidecar 参考实现）。
默认 `system.role = both`（同机起 Master + Worker）。

```bash
tar -xzf dojao-dist/target/dojao-1.0-SNAPSHOT.tar.gz
cd dojao-1.0-SNAPSHOT/dojao
./bin/dojao                 # 按 conf 里的 system.role 启动；也可 ./bin/dojao master|worker|both
```

分机部署（A = Master，B / C = Worker）：用 `<role>.<key>` 前缀在同一份配置里按生效角色取值，
例如 B / C 机写 `worker.system.master.host = <A 机 IP>`；把 B / C 的主机名写进 Master 的
`conf/workers.conf`，再在 Master 上执行 `./bin/dojao-cluster.sh` 一键启动（各机角色仍由
`conf/dojao.conf` 的 `system.role` 决定）。完整步骤见 [`docs/deploy-linux.md`](docs/deploy-linux.md)。

### 连接

```bash
# CLI（Arrow Flight，默认端口 6530）
./bin/dojao-cli
# Web 控制台（默认端口 2380）
open http://localhost:2380
```

### 第一条联邦查询

```sql
-- 注册数据源（一次）
CREATE CATALOG hive    USING hive       WITH ('catalog.hive.metastore.uris' = 'thrift://localhost:9083');
CREATE CATALOG mydb    USING mysql      WITH ('catalog.mysql.jdbcUrl' = 'jdbc:mysql://localhost:3306/dga',
                                              'catalog.mysql.user' = 'root', 'catalog.mysql.password' = '***');

-- Hive 埋点明细 ⋈ MySQL 用户维表，只读真正用到的列
SELECT u.city, COUNT(*) AS c, ROUND(SUM(b.amount), 2) AS gmv
FROM hive.dojao_test.ods_user_behavior b
JOIN mydb.dga.users u ON b.user_id = u.user_id
WHERE b.dt BETWEEN '2026-09-01' AND '2026-09-30' AND b.event_type = 'pay'
GROUP BY u.city
ORDER BY gmv DESC LIMIT 20;
```

---

## 联邦查询与连接器

内置连接器覆盖主流数据源，能力对齐（元数据探查 + 并行切片 + 列裁剪 + 可选谓词下推）：

**关系型 / OLTP（谓词型切片：主键或行范围，天然可并行）**

| 连接器 | `type()` | 说明 |
|---|---|---|
| MySQL | `mysql` | 主键范围并行切片；支持写入 / CTAS / OVERWRITE |
| TiDB | `tidb` | MySQL 线路协议与 `information_schema`；`EXPLAIN estRows` 行数估算；支持写入 / CTAS / OVERWRITE |
| PostgreSQL | `postgresql` | ctid 范围切片；**pgvector 向量列 → `FixedSizeList<Float32>`** |
| SQL Server | `sqlserver` | 主键 / 数值列范围 / OFFSET-FETCH 三级切片；`tinyint`（0..255）读成 `SMALLINT`；支持写入 / CTAS / OVERWRITE |
| Oracle | `oracle` | 主键 / 整数 NUMBER 列范围 / OFFSET-FETCH 三级切片；双引号标识符（字典名大小写不敏感解析）；无精度 `NUMBER` CAST 成 `NUMBER(38,10)`、`FLOAT` 提升为 `BINARY_DOUBLE`；支持写入 / CTAS / OVERWRITE |

**分析型 / OLAP（载荷型切片：文件 / stripe / 分片）**

| 连接器 | `type()` | 说明 |
|---|---|---|
| Hive | `hive`（Hive2 / Hive3） | ORC / Parquet / Text / ACID；HDFS 文件分组切片 |
| ClickHouse | `clickhouse` | 列存 OLAP；MergeTree 建表与原子覆盖写 |
| Doris | `doris` | MySQL 协议接入 FE（9030）；`DISTRIBUTED BY` 建表、`REPLACE WITH TABLE` 原子覆盖 |

**本地文件（单分区读取，零第三方依赖）**

| 连接器 | `type()` | 说明 |
|---|---|---|
| Excel | `excel` | 目录下每个 `.xlsx` / `.xlsm` 是一个 schema、每张工作表是一个表；**恒定单分区**（ZIP + 一条 XML 流，切分只会重复解压）；共享字符串 / 富文本、日期格式与 1904 日期系统、`header=false` |

**后续迭代（非关系型 / 非 JDBC 式数据源）**

| 连接器 | 说明 |
|---|---|
| Kafka | 消息流接入（topic / partition 即切片） |
| Elasticsearch | 搜索引擎 / 倒排索引 |
| MongoDB | 文档型数据库 |
| Iceberg | 表格式；元数据驱动的分区裁剪 |

> 连接器 SPI **不绑定 JDBC** —— 契约只有 `planSplits → openSplit → ScanReader`（见开发指引），
> 所以消息流 / 搜索 / 文档型数据源与关系型走**同一套**接入方式；示例连接器 `csvdir`（把本地目录当数据库读）
> 已经证明引擎不关心数据源底层是什么。

- **对接长尾数据源不改 Dojao 一行代码**：依赖 `dojao-spi`（`provided`）实现接口，打成 jar 丢进
  部署目录的 `lib/connectors/`，每个 jar 独立类加载器。
- 开发指引：[`docs/custom-connector.md`](docs/custom-connector.md)；
  可运行示例（约 400 行、零第三方依赖）：[`examples/dojao-connector-example/`](examples/dojao-connector-example)（把本地目录当数据库读的 `csvdir`）。

> **连接器契约里最该记住的一条**：`columns` 为空 = 读**所有**列（不是零列）；
> 谓词下推能力必须**显式声明**，宁可少一次优化，也不静默多读一行。详见开发指引 §4。

---

## Python UDF（DFP 协议）

数据科学家把 Python 统计量 / 小模型注册成 SQL 函数，**不写一行 JVM 语言、不打 jar**：

```sql
CREATE FUNCTION stddev_py WITH (
    type        = 'python',
    code_path   = '/opt/dojao/python/dojao_udf/examples/my_fns.py',
    class_name  = 'my_fns.stddev');

SELECT product_id, stddev_py(amount) AS sd
FROM hive.dojao_test.ods_user_behavior
WHERE dt BETWEEN '2026-09-01' AND '2026-09-30'
GROUP BY product_id;
```

**协议是语言中立的**：函数逻辑跑在独立 **sidecar** 进程里，通过 **Arrow Flight**（数据面 `doExchange` /
控制面 `doAction`）与引擎通信，类型系统直接用 **Arrow schema**，不引入任何语言专属类型名。
Python 只是**官方参考实现**，Rust / Go / R / Julia / Node / C++ … 均可自行实现，**引擎零改动**。

- 权威协议规范：[`UDF_PROTOCOL.md`](UDF_PROTOCOL.md)
- Python sidecar：[`python/README.md`](python/README.md)
- 隔离性：sidecar 崩了只崩那一个进程，可限资源、超时、重启 —— 这是进程内 Java UDF 给不了的。

---

## 内存与溢写模型（一页速览）

```properties
# 只需声明一个总量，其余全派生
system.master.memory = 2g      # 堆 1g + 堆外 1g
system.worker.memory = 5g      # 堆 2.5g + 堆外 2.5g
query.spill.ratio    = 0.8     # 算子内存达 slot.memory × ratio 触发溢写
query.spill.dir      = /data1/dojao-spill,/data2/dojao-spill   # ★别放 /tmp（tmpfs = 内存盘）
```

- **不要配大堆**：堆内只装索引 / 批量暂存 / 控制面，但高基数分组会把它吃到 GB 级 ——
  堆大不会更快，只会 Full GC 更久、更容易占满物理内存不还。
- **溢写目录别指向 `/tmp`**：很多 Linux 上 `/tmp` 是 tmpfs，会变成「一边溢写一边吃内存」。
- 盘使用率上限 `query.spill.disk.maxUsage`，写满换盘，全部候选盘超限则报错而非静默写爆节点。
- 无 LIMIT 的全局 `ORDER BY` 默认**拒绝**（`query.sort.topSpillPolicy = error`），提示你加 LIMIT。

> 完整调参直觉与「6g 堆为何把 4C8G 机器拉爆」的真实事故复盘，见 **[设计说明书](dojao_design_manual.html)**。

---

## 分布式执行

- **Exchange 算子**：基于 **Arrow Flight** 的 Worker↔Worker 数据交换，写入侧并行、数据永不回 Master。
- **Fragment 计划**：逻辑计划切成分片，Coordinator 按资源调度到 Worker。
- **动态并行度**：按 `getTableStats` 估算的字节数决定切片数，`query.parallelism.bytesPerSlot` 可调。
- **闸门与背压**：`query.memory.highWatermark / lowWatermark` 在堆外用量越线时对读端做**有界等待**，把 OOM 变成减速。
- **可观测**：查询快照（`QuerySnapshot` / `OperatorSnapshot`）、内存与溢写遥测、查询取消。

---

## 客户端与工具

| 入口 | 说明 |
|---|---|
| **Web 控制台** | 端口 `system.ui.port`（默认 2380）：SQL 编辑器（自动补全 / 高亮）、任务看板与分页历史、查询监控（进度 / 内存 / 磁盘 / 溢写）、血缘 DAG、函数与 Catalog 管理、深浅主题 |
| **CLI** | `dojao-cli`，多语句、结果格式化 |
| **JDBC** | `dojao-jdbc`，标准 `Connection` / `Statement` / `PreparedStatement` / `ResultSet`，可接 BI 工具 |

---

## 与 Trino / Spark 的定位

**Dojao 的护城河是设计上的取舍，不是性能上的堆料。** 它主动站在 Trino / Spark **最难受的区间**
（小内存 + 跨源 + 溢写必发 + 不可失败）当正解：

- Trino 的溢写官方标注为 **legacy**，被迫溢写时可能 **"orders of magnitude"** 变慢、且**不保证成功**；
- Spark 的 shuffle spill 成熟稳健，但**保量落盘**（不改变数据规模）；
- Dojao 的溢写是 **减量** 的 —— 6.8KW 行窗口去重会先在溢写期塌缩成「每分区 1 行」，**内存线对它不是悬崖**。

**边界也很清楚**：真要 20+ 节点、纯内存 PB 级、拼绝对吞吐，Trino / Spark 更合适。
本仓提供三引擎对等压测方法（同硬件、同数据、同 SQL）与「溢写线」判据：
[`docs/benchmark-dojao-trino-spark.md`](docs/benchmark-dojao-trino-spark.md)。

---

## 扩展：自定义连接器

内置连接器只覆盖常用数据源；对接长尾数据源**不需要改 Dojao 任何一行代码**——
依赖 `dojao-spi`（`provided`）实现接口，打成 jar 丢进部署目录的 `lib/connectors/` 即可。

- 开发指引：[`docs/custom-connector.md`](docs/custom-connector.md)
- 可运行示例（约 400 行、零第三方依赖）：[`examples/dojao-connector-example/`](examples/dojao-connector-example)

---

## 文档索引

| 文档 | 内容 |
|---|---|
| [`dojao_design_manual.html`](dojao_design_manual.html) | **设计说明书**：内存模型推导、窗口过滤下推两级优化、HashJoin 字节预算、高基数 GROUP BY K 路归并 |
| [`UDF_PROTOCOL.md`](UDF_PROTOCOL.md) | **DFP 外挂函数协议**（语言中立，权威规范） |
| [`docs/custom-connector.md`](docs/custom-connector.md) | 自定义连接器开发指引 |
| [`docs/deploy-linux.md`](docs/deploy-linux.md) | Linux 部署与集群化 |
| [`docs/benchmark-dojao-trino-spark.md`](docs/benchmark-dojao-trino-spark.md) | 三引擎对等压测方法、SQL 阶梯、定位边界 |
| [`blog0`](blog0_design_rationale.html) | **设计初衷与技术选型**：为什么造 Dojao、为什么是 Java 21、Apache Arrow 与 Arrow Flight 的选型理由，以及这些选择付出的代价 |
| [`blog1`](blog1_dojao_shuffle_model.html) – [`blog17`](blog17_column_pruning.html)（含 [`blog2_1`](blog2_1_union_distinct_grouping_sets.html)） | 系列实现博文：Shuffle 模型、GroupBy、窗口、SortMergeJoin、HashJoin、分布式任务、查询管理、跨源 join、列裁剪 … |
| [`blog18`](blog18_dojao_ai_architecture.html) | **Dojao AI 架构与定位**：分层（通用 ReAct 内核 / 能力绑定）、8 个工具、能力面与边界、系统提示词工程、会话与技能、逐用户隔离 |
| [`blog18_1`](blog18_1_ai_optimization.html) | **Dojao AI 优化**：混合图表（逐序列图形 + 主副双 Y 轴）、服务端结果集缓存 + 引用（`render_chart(data="rs3"/"last")` 免重算免回传） |
| [`blog19`](blog19_flight15_cancel_lost_complete.html) | **线上 5/6 卡死复盘**：已经 ack 的 COMPLETE 为什么还是丢了 —— Arrow Flight 15 的 `completed()` 异步半关闭、`close()` 触发 `ClientCall.cancel`、探针 S8 与收尾修复 |
| [`blog20`](blog20_grouping_sets_single_scan.html) | **多维聚合只扫一次表**：ROLLUP / CUBE / GROUPING SETS 旧路径按维度组合展开成 N 条腿、各扫一遍表；新路径用 `ExpandOperator` 一行变 N 行 + `__grouping_id`，一次聚合（`read()` 369 → 123） |

---

## License

[Apache License 2.0](LICENSE)
