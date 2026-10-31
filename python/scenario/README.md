# dojao 电商场景造数

一个**两批数据互相对得上**的电商场景：MySQL 存业务数据（用户 / 商品 / 订单），
Hive 存用户行为日志（启动 / 浏览 / 加购 / 购买）。购买事件不是「编」的，而是从
MySQL 里**真实存在的已支付订单明细**推出来的，所以能做跨源对账。

```
                       ┌─────────────── 同一批纯函数（model.py）───────────────┐
                       ↓                                                      ↓
   MySQL  dojao 库                                            Hive  dojao_test.ods_user_behavior
   category / users / spu / sku / orders / order_item          30 个 dt 分区 · ORC + SNAPPY
   1.1M 用户 · 9,600 SKU · 232W 订单 · 518W 明细               9,126 万行 · 每天 226W-400W
                       └──────── order_id / sku_id / amount 分毫不差 ─────────┘
```

## 1. 规模与分布（口径 = 需求）

| 项目 | 数值 | 说明 |
| --- | --- | --- |
| 用户 | 1,100,000 | 口径 100W–120W |
| SPU / SKU | 2,400 / 9,600 | 口径 SPU 2000+、SKU 1W 左右，每 SPU 4 个 SKU |
| 一级分类 | 12 | 手机 水果 蔬菜 医药 母婴产品 男装 女装 户外运动 美妆 家居 电脑 百货 |
| 性别 | 男 45.1% : 女 54.9% | 口径 0.45 : 0.55 |
| 年龄 | 20–40 岁占 60.0% | 其余 40% 随机落在 18–19 与 41–60 岁 |
| 订单 / 明细 | 约 232W / 518W | 订单数由 Hive 侧反推（见 §4） |
| 订单状态 | 待付款 6% · 已付款 30% · 已发货 20% · 已完成 38% · 已取消 4% · 已退款 2% | 已支付合计 88%，只有它们产出购买事件 |
| 行为日志 | 30 个分区 · 9,126 万行 · 每天 226W–400W | 按 dt 分区，ORC + SNAPPY，约 28 字节/行 |
| 事件比例 | 启动 50% · 浏览 30% · 加购 15% · 购买 5% | 即 5 : 3 : 1.5 : 0.5 |

## 2. 怎么跑

在集群节点上（MySQL 与 HDFS 都走内网）：

```bash
./run.sh check                   # 先看依赖齐不齐（不装任何东西）
./run.sh mysql                   # ① 业务数据 → aliyun002:3306/dojao
./run.sh hive                    # ② 行为日志 → dojao_test.ods_user_behavior
```

`run.sh` **不强制装依赖**（缺了会告诉你装法然后退出）。机器上已经装好 python3 + 那三个包时，
直接 `python3 gen_hive.py` 跑也一样。

单独用某个脚本时：

```bash
python3 gen_mysql.py --dry-run   # 只打印规划，不连库
python3 gen_mysql.py             # 建表 + 灌数；六张表已有数据会拒绝
python3 gen_mysql.py --reset     # 先 DROP 这六张表再重建（会清空数据！）

python3 gen_hive.py --days=2     # 只造头 2 天（冒烟用，最快验证链路）
python3 gen_hive.py --clean      # 先删掉 HDFS 上的表目录，从零开始（会清空数据！）
python3 gen_hive.py --no-upload  # 只写本地 ORC，不碰 HDFS（命令自己敲）
```

规模、分布、连接串等**全部参数都在 `config.py`**，改常量即可，不必动代码。

### 前置条件

* Python 3 + `pymysql` / `numpy` / `pyarrow`
* MySQL：`aliyun002:3306`，账号见 `config.py`（默认 root/123456），库 `dojao`
* HDFS：能连上 NameNode（`config.py` 的 `HDFS_NAMENODE_HOST/PORT`）
* **不需要 `hive` 客户端**——建表 / 注册分区由 Hive 侧自己执行，脚本结尾会把 DDL 原样打出来
* Hive 库 `dojao_test` 需已存在（`create database if not exists dojao_test;`）

依赖已经装好就直接跑。真缺了（`./run.sh check` 会报），Debian/Ubuntu 的系统 pip 会被
**PEP 668 `externally-managed-environment`** 挡住，三选一：

```bash
sudo apt install -y python3-numpy python3-pyarrow python3-pymysql   # 用发行版包
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt  # 建虚拟环境（run.sh 会自动用它）
python3 -m pip install --break-system-packages -r requirements.txt  # 强行装（不推荐）
```

`./run.sh install` 会先试系统 pip，被挡住就自动改用 `.venv`。

### 数据是怎么落到 HDFS 的

**首选用 pyarrow 的 `HadoopFileSystem` 直接写 `hdfs://`**（每个分区一个目录，`HIVE_FILES_PER_DAY`
个 ORC 文件），不落本地中转、不调 `hdfs` 命令。它需要节点的 `HADOOP_HOME` / `libhdfs.so` /
`CLASSPATH` 配好；起不来时会打印原因，然后**自动退回**老路子：

```
本地 ORC（/tmp/dojao-scenario/dt=YYYY-MM-DD/000000_0）
  → hdfs dfs -mkdir -p /user/hive/warehouse/dojao_test.db/ods_user_behavior/dt=YYYY-MM-DD
  → hdfs dfs -put -f ...
```

两条路都**只负责把文件放进 HDFS**。全部跑完后脚本会打印交给 Hive 侧的两条命令：

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS dojao_test.ods_user_behavior (...)   -- 结尾原样打印
MSCK REPAIR TABLE dojao_test.ods_user_behavior;
```

## 3. 表结构

### MySQL `dojao`

```
category(category_id, category_name, sort_order)
users   (user_id, username, gender, age, birthday, city, province, phone, email,
         member_level, register_time, status)
spu     (spu_id, spu_name, title, category_id, brand, price_min, price_max,
         create_time, status)
sku     (sku_id, spu_id, sku_name, spec, price, cost_price, stock, sold_count, status)
orders  (order_id, order_no, user_id, order_time, pay_time, total_amount,
         discount_amount, pay_amount, order_status, pay_method, device_type,
         province, city)
order_item(item_id, order_id, sku_id, spu_id, category_id, quantity,
         unit_price, amount, discount_amount)
```

**用户 ↔ 商品是多对多**，由 `orders`（谁下的单）+ `order_item`（单里买了哪些 SKU）承载。
索引在数据灌完之后才建（边插边维护索引慢得多）。

### Hive `dojao_test.ods_user_behavior`（分区列 `dt`）

| 列 | 类型 | 说明 |
| --- | --- | --- |
| event_id | bigint | 全局唯一递增 |
| event_type | string | app_launch / page_view / add_cart / purchase |
| event_time | string | `yyyy-MM-dd HH:mm:ss`，分区内近似有序（利于压缩） |
| user_id | bigint | → `users.user_id` |
| session_id | bigint | 会话 id（天, 用户, 半小时桶 编码），30 分钟内的同一用户算同一次会话 |
| device_type / os / app_version / channel / network | string | 端侧信息 |
| province / city | string | → `users.province` / `users.city` |
| page_type | string | launch / item / home / search / cart / order / mine |
| referrer | string | 来源页，app 启动为 NULL |
| spu_id / sku_id / category_id | bigint / bigint / int | 商品相关事件才有值 |
| keyword | string | 仅 `page_type=search` |
| stay_seconds | int | 仅 `page_view` |
| quantity / amount | bigint / double | 加购与购买才有值 |
| order_id | bigint | **仅 purchase**，→ `orders.order_id` |
| launch_type | string | cold / hot，仅 `app_launch` |

## 4. 两批数据为什么对得上

`model.py` 里所有字段都是**订单序号 i 的纯函数**（splitmix64，无 `random` 模块、不存状态）：

```
gen_mysql.py ：for i in range(订单数)   → order_*(i) 推字段 → INSERT 进 MySQL
gen_hive.py  ：for i in 某天的订单区间  → order_*(i) 推字段 → 写成 purchase 事件
```

同一个 i 必然得到同一行，所以 Hive 的 `order_id / sku_id / amount` 一定能在 MySQL 里查到，
金额分毫不差。

**订单总数是反推出来的，不是拍脑袋配的**（`common.plan()`）：

```
订单总数 = Σ(每天行数 × 5%) ÷ (88% 已支付 × 2.23 件/单) ≈ 2,325,315
```

这样「购买事件占每天 5%」才是真对得上的 —— 而不是两边各配一个数。实测每次跑完会打印比例。

> ⚠️ 维表里凡是 `order_*()` 会用到的派生量（爆款榜、品类价、规格系数）**一律由 id 的纯函数算出来**，
> 绝不依赖「读回来的表里多了/少了一列」。否则 gen_mysql 与 gen_hive 会算出不同的爆款榜，
> Hive 的 sku_id 立刻跟 MySQL 的 order_item 对不上。

## 5. 接到 dojao 上

```sql
CREATE CATALOG aliyun002_mysql USING mysql WITH (
  'catalog.mysql.jdbcUrl'='jdbc:mysql://aliyun002:3306/dojao',
  'catalog.mysql.username'='root', 'catalog.mysql.password'='123456');

CREATE CATALOG aliyun002_hive  USING hive3 WITH (
  'catalog.hive.metastore.uris'='thrift://aliyun002:9083',
  'catalog.hive.hadoop.user.name'='<你的 HDFS 用户>');
```

### 对账样例

```sql
-- Hive 侧：某天金额最大的 5 单
select order_id, count(*) as items, round(sum(amount), 2) as amt
  from ods_user_behavior
 where dt = '2026-09-21' and event_type = 'purchase'
 group by order_id order by amt desc limit 5;

-- MySQL 侧：拿上面那 5 个 order_id 查明细（金额 / 件数必须一致）
select order_id, count(*) as items, round(sum(amount), 2) as amt
  from order_item where order_id in (...) group by order_id;

-- 漏斗
select page_type, count(*) from ods_user_behavior
 where dt = '2026-09-21' group by page_type order by 2 desc;

-- 行为日志 × 业务维表（同一批城市口径）
select b.city, count(*) as orders, round(sum(b.amount), 2) as gmv
  from ods_user_behavior b
 where b.dt between '2026-09-01' and '2026-09-21' and b.event_type = 'purchase'
 group by b.city order by gmv desc limit 10;
```

## 6. 文件

| 文件 | 作用 |
| --- | --- |
| `config.py` | 全部可调参数：连接串、规模、分布、分区天数、事件比例、维度字典 |
| `common.py` | 可复现伪随机（splitmix64）+ 权重查表 + 分区分天规划 + MySQL 连接 |
| `model.py` | 维表生成 + 订单模型（纯函数，两个生成器共用这一份算术） |
| `gen_mysql.py` | 建表 + 批量灌 MySQL |
| `gen_hive.py` | 逐天生成 ORC + 直写 HDFS（不可用时退回 `hdfs dfs -put`），打印 Hive 侧 DDL |
| `run.sh` | 一键跑 |

分布、关联、跨源一致性都用离线探针在本地验过（不连库、不连 HDFS）：
每天 50/30/15/5 比例精确命中、购买事件逐条回放 MySQL 订单模型金额一致、
空值掩码与 `page_type` 严格对应、ORC 回读行列数一致。
