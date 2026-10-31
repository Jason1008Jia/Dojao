# 部署 Dojao 到 Linux

本文覆盖：获取发行包 → 上传解压 → 配置 → 建 catalog → systemd 托管 → 远程连接与验证。

## 1. 获取发行包

发行包是自包含的 `dojao-1.0.0.tar.gz`，解压即用（包内已含全部运行时依赖，所以体积偏大）。

到 [GitHub Releases](https://github.com/Jason1008Jia/Dojao/releases) 下载 `dojao-1.0.0.tar.gz`，
拿到后直接跳到第 2 节。

## 2. 上传解压

```bash
scp <本地的 dojao-1.0.0.tar.gz> root@<linux-host>:/opt/
ssh root@<linux-host>
cd /opt && tar -xzf dojao-1.0.0.tar.gz
chmod +x dojao/bin/dojao dojao/bin/dojao-cli
```

目录树：

```
dojao/
  bin/
    dojao          # 集群启动（Master 与/或 Worker）
    dojao-cli      # 命令行客户端
  conf/
    dojao.conf     # 引擎配置（角色 / 端口 / 内存）
    workers.conf   # 集群 Worker 机清单（dojao-cluster.sh 用）
  systemd/
    dojao.service  # systemd 单元模板
  lib/
    *.jar          # 引擎 + 共享库（JDBC 驱动 / Hadoop / ORC 等）
    connectors/    # 连接器插件目录：内置 mysql/postgres/hive/clickhouse/excel/sqlserver + 第三方
  data/            # 运行时自动创建（SQLite 元数据库）
```

本机需有 **JDK 21+**。`bin/dojao` 优先用 `$JAVA_HOME/bin/java`，否则找 PATH 里的 `java`。

## 3. 配置 `conf/dojao.conf`

加载优先级：`-D 系统属性` > 本文件 > 内置默认。**文件在 JVM 启动时读一次，改完必须重启进程。**

### 单机（Master + Worker 同机，建议先跑通这个）

**一行都不用改。** 默认就是：

```properties
system.role = both                # 同机拉起 Master + Worker 两个 JVM
system.master.host = 0.0.0.0      # 绑全网卡，别机才连得进
system.master.port = 6530
system.worker.host = 127.0.0.1    # ★只在单机（both）成立；分机必须改成各机自己的地址
system.ui.port = 2380
system.metastore.sqlite.path = data/dojao-metastore.db
```

### 分机（A = Master，B / C = Worker，共用同一份 conf）

靠**角色前缀** `<role>.<key>` 描述异构集群，不用维护多份文件：

```properties
# ---- A 机 ----
system.role = master
system.worker.host = <A 的内网 IP>        # A 机自己也要跑 Worker 时填本机地址

# ---- B / C 机 ----
system.role = worker
system.worker.host = <本机的内网 IP>      # ★必须改！否则别的机器把它解析成自己的回环
worker.system.master.host = <A 的内网 IP>
```

**★`system.worker.host` 是「对外通告地址」，不是绑卡地址**（Flight 服务器恒绑 `0.0.0.0:port`）。
它注册进 Master，并随每次派发下发给所有 Worker 当作「怎么找这个 Worker」。

留着默认 `127.0.0.1` 的后果是**静默的**：每台机器都把自己网络里的「<workerId>」解析成
`127.0.0.1:4009`，于是
- Master 派发任务 → 落到**自己这台机**的 Worker（或直接 connection refused）；
- Worker 之间送 shuffle 数据 / 通知失败 → 送到**自己**的 Flight 端口，真正的目标端一个页都收不到；
- 表现是查询卡住：终端 Consumer 等 `senders` 收齐（如 `完成=5/8`），超时后被 Master 看门狗判
  「挂起，疑似 result 回传链路故障」——**报错信息完全指错方向**。
  看门狗超时由 `query.result.watchdogMs` 控制（默认 10 分钟）；判据是「数据页 / 完成 / Worker 存活
  心跳」三者中最新的一个是否超过该值——Worker 只要还在正常收页或归并就不会被误判。

启用 systemd 时，该键写在 `conf/dojao.conf` 里即可（不必再写单元文件）。

**一键启动**：在 A 机把 B / C 的主机名写进 `conf/workers.conf`（一行一个，本机不写）：

```text
aliyun001
aliyun002
```

然后在 A 机执行 `./bin/dojao-cluster.sh`：先起本机，再按清单 ssh 到各机，在各自的安装目录执行
`nohup ./bin/dojao >> logs/dojao.out 2>&1 &`。脚本不传角色参数，每台机器读自己
`conf/dojao.conf` 的 `system.role`。前提是 A 到各机免密 ssh、各机安装路径一致。
`./bin/dojao-cluster.sh stop` 可反序停止。

### 按机器规模调的几个键

| 键 | 默认 | 说明 |
|---|---|---|
| `system.master.memory` / `system.worker.memory` | `2g` / `5g` | **节点内存总量**；堆 / 堆外 / slot / direct.limit 全按派生链自动算出（推荐只写这个） |
| `query.memory.heapRatio` | `0.5` | 节点内存的堆内占比：堆 = 总量×ratio，堆外 = 总量×(1−ratio) |
| `system.master.heap` / `system.master.direct` | 派生 | （精调）显式覆盖 Master 的 -Xmx / MaxDirectMemorySize |
| `system.worker.heap` / `system.worker.direct` | 派生 | （精调）显式覆盖 Worker 的 -Xmx / MaxDirectMemorySize |
| `query.memory.max` | = 本节点 Arrow 预算 | （精调）**单查询在每台 Worker 上的内存上限（堆 + 堆外）**；不配则用本节点 Arrow 预算（堆外 − 传输预留） |
| `query.memory.maxFragments` | `2 × worker.parallelism` | （精调）单 Worker 并发片段上限，参与 fragmentDirect 摊薄 |
| `query.slot.memory` | 派生 | （精调）单个可溢写算子的内存预算，超 `slot × query.spill.ratio` 溢写 |
| `query.memory.highWatermark` / `lowWatermark` | `0.85` / `0.75` | 堆外用量背压水位：越过高水位则读取端放缓，回落到低水位放行 |
| `system.master.parallelism` | `4` | 每 query 并行分区数 |
| `system.worker.parallelism` | `8` | 单 Worker 任务池容量（随包 `conf/dojao.conf` 设为 `6`） |
| `query.parallelism.bytesPerSlot` | `1G` | 每 scan 分区承载字节数（决定单表并行度） |

> 派生链（`ResourceConfig`，**Master 启动时算好随片段下发**）：
> 1. `system.<role>.memory × heapRatio → 堆(-Xmx)`，`× (1−heapRatio) → 堆外(-XX:MaxDirectMemorySize)`；
> 2. `Arrow 预算 = 堆外 − min(堆外 × 20%, 1g)`（给 Netty / gRPC / Flight 传输缓冲预留）；
> 3. `perQueryDirect = min(query.memory.max × (1−heapRatio), Arrow 预算)`；不配 `query.memory.max` 时直接用 Arrow 预算；
> 4. `fragmentDirect = perQueryDirect / maxConcurrentFragments`（默认 `2 × system.worker.parallelism`）；
> 5. `query.slot.memory = fragmentDirect / (3 × query.spill.ratio)`，下限 `32M`。
>
> `3` = 三卡槽流水线（1 份在累加、1 份在写盘、1 份空闲）；单槽溢写阈值 = `slot × ratio`，
> 所以三槽峰值 `3 × slot × ratio = fragmentDirect` —— 它恰好等于一个片段的堆外份额。
> ⚠ 该值在 **Master 进程**里计算（启动时）并随片段下发，因此被 **Master 的** `system.master.memory`
> 封顶：随包默认 `master.memory = 2g` ⇒ Arrow 预算 819M ⇒ `slot` 落在 32M 下限——即使 Worker 配了 5g。
> 想放大单算子预算，要么调大 `system.master.memory`，要么按 Master 的配置显式写 `query.slot.memory`。
> Arrow 向量全走堆外，**堆与堆外都必须封顶**，否则小内存机上会被内核 OOM-killer 杀进程（无 Java 异常）。

### HDFS 侧不用做任何事

Windows 上那套 `hadoop.home.dir` + `bin/winutils.exe` 兜底是 **`isWindows()` 守卫**的，Linux 上不会触发；
反过来，机器上装了 Hadoop 并设了 `HADOOP_HOME`，会自动把 `hadoop.home.dir` 指过去（正好是对的）。
那批公网保活参数（`ipc.ping.interval` 等）在内网也无害，保持默认即可。

## 4. 建 Hive catalog

catalog 注册信息落在 SQLite（`system.metastore.sqlite.path`），**重启不丢**。两种方式：

**(a) 直接复用开发机上的 `data/dojao-metastore.db`** —— 连 catalog 注册 + 持久化函数一起带过来。
里面的 `catalog.hive.metastore.uris` / `catalog.hive.fs.defaultFS` 在集群里依然有效，拿来即用。
注意两边 `system.metastore.sqlite.path` 的相对基准都是「安装根目录」（启动脚本会 `cd` 过去）。

**(b) 到 Linux 上重建**（更干净）：

```sql
create catalog hive using hive3 with (
  catalog.hive.metastore.uris = 'thrift://<hms-host>:10000',
  catalog.hive.fs.defaultFS   = 'hdfs://<namenode-host>:9000',
  catalog.hive.hadoop.user.name = 'hdfs'
);
```

- `hive2` / `hive3` 两个工厂都在，能力校验已关，单套客户端通吃。
- 属性键遵循 catalog 命名规范 `catalog.<连接器类型>.<属性名>`（Hive 系共用 `catalog.hive.`），
  旧写法（`hive.metastore.uris` 等）依然可用。
- **`catalog.hive.hadoop.user.name` 最值得先确认**：它会被设成 `HADOOP_USER_NAME`，决定读 HDFS 的身份。
  集群侧一般是 `hdfs` 或 `hive`，写错了是 `Permission denied`。
- 连接器：内置 mysql/postgres/hive/clickhouse/excel/sqlserver 与三方连接器**一律**以插件 jar 放在 `lib/connectors/`；
  三方连接器把 SPI 实现打成 jar（依赖 lib/ 里没有的驱动时需自带）丢进去即可（见该目录 README）。

## 5. systemd 托管

单元模板随包发布在 `systemd/dojao.service`，安装步骤写在文件头。照抄一遍：

```bash
sudo useradd -r -s /sbin/nologin dojao
sudo chown -R dojao:dojao /opt/dojao

sudo cp /opt/dojao/systemd/dojao.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dojao
```

**装完必改两处**（都在 `/etc/systemd/system/dojao.service`）：

1. `Environment=JAVA_HOME=/usr/lib/jvm/jdk-21` → 指向本机实际 JDK 21+（systemd 环境极简，这里不给脚本就找不到 java）。
2. 若解压目录不是 `/opt/dojao`，同步改 `ExecStart` 与 `WorkingDirectory`。

几个设计点：

- `Type=simple`：`bin/dojao` 是前台 `exec java`，不 fork，**不能用 `Type=forking`**。
- 进程拓扑：`master` / `worker` 角色下主进程就是 `DojaoMaster` / `DojaoWorker`（脚本直接 `exec`，
  不再常驻启动器）；`both` 角色下主进程是 `ClusterMain`，由它同机拉起并看护两个子 JVM。
- `KillMode=control-group`：保证 `systemctl stop` 时（`both` 的）子进程一并回收。
- `Restart=on-failure` + `SIGTERM`：正常 stop 不触发重启；崩溃才拉起。
- 想临时覆盖端口 / JVM 参数，**别改分发包里的文件**，用 `/etc/default/dojao`：

  ```bash
  # /etc/default/dojao
  DOJAO_JVM_OPTS="-Dsystem.ui.port=2381"
  ```

  `-D` 优先级最高，会压过 `conf/dojao.conf`。

不想用 systemd 就直接：

```bash
cd /opt/dojao
nohup ./bin/dojao > dojao.log 2>&1 &
./bin/dojao master        # 也可临时指定角色（覆盖 conf 里的 system.role）
```

## 6. 从别的机器连接（`bin/dojao-cli`）

### 端口与放行

| 端口 | 用途 |
|---|---|
| **6530** | Master Flight —— Worker 注册 + 客户端提交 SQL 的**唯一入口** |
| **2380** | Web 控制台（监控看板 + SQL 控制台） |
| 4009+ | Worker Flight —— 同机 `both` 时走回环，无需对外开放；分机部署才需要在 Worker 之间互通 |

跨机访问只需在防火墙 / 安全组放开 **6530** 与 **2380**。

### CLI 用法

```bash
# 交互式 REPL
./bin/dojao-cli --host <master-ip> -p 6530

# 一次性执行
./bin/dojao-cli --host <master-ip> "show catalogs"

# 批量执行文件
./bin/dojao-cli --host <master-ip> -f script.sql

# 结果落盘（结果写 stdout，统计与错误写 stderr，所以重定向是干净的）
./bin/dojao-cli --host <master-ip> -F csv -f query.sql > out.csv
```

选项：

| 选项 | 说明 |
|---|---|
| `--host <host>` | Master 主机（默认 `127.0.0.1`） |
| `-p, --port <port>` | Master 端口（默认 `6530`） |
| `-f, --file <file>` | 执行 SQL 文件，按顶层分号切分逐条执行，失败即停 |
| `-F, --format <fmt>` | 输出格式：`table`（默认）/ `csv` / `json` |
| `-q, --quiet` | 不打印启动横幅 |
| `-?, --help` | 帮助 |

交互式元命令：`\h` 帮助、`\q` 退出、`\f [fmt]` 查看/切换格式、`\timing` 开关行数+耗时统计。

几个行为要知道：

- **启动即握手探活**（对 Master 做一次 `listActions` 往返），连不上直接报错并以 **EXIT=1** 退出 ——
  脚本里可以直接用它判断集群是否可用。
- 多行 SQL 以分号结尾后执行（引擎侧不收尾分号，CLI 已替你剥掉）。
- 历史记录存在 `~/.dojao_history`，下次启动可 ↑ 回顾。
- **交互式 REPL 需要真实终端**：管道 / 重定向 / IDE 输出面板下会退化为逐行读输入（仍可用，但没有行编辑与历史）。
- 表名形如 `hive.<schema>.<table>`（联邦查询），例如 `select * from hive.dojao_test.ods_test limit 10`。

### Web 控制台

浏览器打开 `http://<master-ip>:2380`，可看查询历史、算子 DAG、集群状态，并直接在页面上跑 SQL。

## 7. 验证

```sql
select * from hive.<db>.<table> limit 500;
```

**预期从公网上的 44 s 掉到 1 s 量级**（62 MB 走机架内网 ~0.5 s + ORC 解码）。

分发包里保留了慢步骤日志（阈值 300 ms），正好当 A/B：

```
[ORC] 慢步骤 43948 ms：reader.rows（建读取器）（hdfs://.../000000_0）
[Hive] 慢步骤 44108 ms：打开数据文件（读 footer / schema）
```

**本地读时这些行应该基本不再出现**；`reader.rows` 那 44 s 会变成几十毫秒。
一条都不打 + 结果正确，就说明这一步的目的达到了。

## 8. 排查

| 现象 | 先看什么 |
|---|---|
| 起不来 / 找不到 java | systemd 单元里的 `Environment=JAVA_HOME=...`；或裸跑一次 `./bin/dojao` 看真实报错 |
| 配置没生效 | 启动日志第一行 `[ResourceConfig] 角色=.. 配置=<路径> 文件键=N 项` —— 「配置」必须是你的路径，`文件键=0 项` 说明根本没读到 |
| `Failed to reload catalog: xxx` | SQLite 里那行 catalog 的属性有问题（多为历史数据），见下 |
| HDFS `Permission denied` | catalog 属性 `hadoop.user.name`（→ `HADOOP_USER_NAME`）与集群侧身份不一致 |
| `data/` 写不进去 | 安装目录属主。systemd 以 `User=dojao` 跑，需 `chown -R dojao:dojao /opt/dojao` |
| 客户端连不上 | 6530 是否被防火墙挡住；`system.master.host` 必须是 `0.0.0.0` 而不是 `127.0.0.1` |
| 读表还是慢 | 看 `[慢步骤]` 日志落在哪一段：`reader.rows` 慢 = 数据面慢（链路/磁盘）；`createReader` 慢 = 元数据面慢（HMS/footer）。若在集群内仍慢，先确认这台机器到 DataNode 的链路与磁盘 |

改完 `conf/dojao.conf` 记得 `systemctl restart dojao` —— 配置只在进程启动时读一次。
