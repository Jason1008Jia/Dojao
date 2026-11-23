# 内存模型与溢写卡槽：待解决问题

> 状态：**P2 已落地（2026-11-22：动态卡槽 2/3）；其余问题待排期**。本文只记录问题、现状推导与候选方向，
> 不代表已确定的设计 —— 已落地项在对应小节标注实现与验收。
> 记录日期：2026-11-12（P2 更新：2026-11-22）

## 1. 结论摘要

| # | 问题 | 性质 | 建议优先级 |
|---|---|---|---|
| P1 | `query.slot.memory` 名字有歧义，实为**单算子总预算** | 命名 | 高（纯改名，风险低） |
| P2 | 三卡槽「三份平分」看似公平，实际常低估可用内存、加剧溢写 | 设计 | **已落地（2026-11-22）**：单桶单槽预算 < 32M 自动降 2 槽、单槽预算 ×1.5，峰值不变 |
| P3 | 阈值的 `÷ numPartitions` 按**配置**分区数摊薄，数据倾斜时内存大量闲置 | 设计 | 中 |
| P4 | `maxConcurrentFragments = 2 × worker.parallelism` 是保守假设，可能过度压低每片段份额 | 参数 | 低（可配置覆盖） |
| P5 | 链式片段（join+窗口）里多个溢写算子各拿一份完整预算，实际超订 ~2× | 缺陷/口径 | 中 |

---

## 2. 现行推导链（代码事实）

```
system.<role>.memory = 10g
  ├─ × heapRatio(0.5)            → 堆 5g
  └─ × (1 − heapRatio)           → 堆外 5g
      └─ − min(堆外 × 0.2, 1g)   → Arrow 预算 4g        （传输层预留封顶 1g）

fragmentDirect = Arrow 预算 / maxConcurrentFragments     = 4g / 12 ≈ 341MB
                  （query.memory.max 若显式配置则先取 min(qmax×(1−ratio), Arrow预算)）

query.slot.memory = fragmentDirect / (3 × spill.ratio)   = 341 / 2.4 ≈ 142MB
                  （下限 32M；未配任何节点内存时退回内置默认 2G）

单分区单槽阈值 = slot.memory × spill.ratio / numPartitions
                = 142 × 0.8 / P
                P=4 → 28.4MB      P=6 → 19.0MB
```

自洽性校验：`P 个分区 × 3 槽 × 阈值 = 3 × 142 × 0.8 = 341MB = fragmentDirect`。
动态降为 2 槽时阈值 ×1.5，`P × 2 × (1.5 × 阈值)` 仍是同一数值（见 P2）。

代码锚点：

- `dojao-core/.../config/ResourceConfig.java`
  - `maxConcurrentFragments()` :635
  - `fragmentDirectBytes()` :646
  - `slotMemoryBytes()` :668
  - `queryDirectLimit()` :751
- `dojao-server/.../master/MasterServer.java:159` — `ctxBuilder.memoryLimit(ResourceConfig.slotMemoryBytes())`
- `dojao-core/.../operator/AbstractSpillableOperator.java` — `slotBytesThreshold = memoryLimit × spillRatio / owned`
  （`owned` = 本片段实际拥有的桶数，`setBudgetPartitions` 在 open 前重算）
- `dojao-core/.../operator/AbstractSpillableOperator.java:71` — `SLOT_COUNT = 3`（默认/设计峰值口径；
  实例实际槽数 `slotCount` 可为 2，见 P2）
- `dojao-core/.../operator/HashJoinOperator.java` — `setBuildBudgetPartitions(owned)`：
  单桶 build 预算 = `memoryLimit / owned`（广播 join 保持 `/ numPartitions`）
- `dojao-core/.../operator/JoinHashTable.java` — `estimateMemoryBytes()` 用 `getBufferSizeFor(行数)`
  统计 key/build 向量（`getBufferSize()` 按 `valueCount` 换算，而本表从不设 valueCount ⇒ 原先恒报 0）
- `dojao-server/.../worker/FragmentExecutor.java` — `localJoin(...)` 按 build 侧 `ownedCount()` 下发
- `conf/dojao.conf` — 派生链注释

---

## 3. P1：`query.slot.memory` → `query.operator.memory`

### 问题

`slot.memory` 字面意思是「一个槽的内存」，实际是**一个算子对象的 memoryLimit**（算子内部再按分区、按 0.8 切给 3 个槽）。已造成一次真实的排障误解。

真正叫「槽」的是下游的 `slotBytesThreshold`（单分区单槽触发量），它才是名副其实的。

### 改名范围

| 现名 | 新名 | 说明 |
|---|---|---|
| `query.slot.memory` | `query.operator.memory` | 配置键 |
| `ResourceConfig.slotMemoryBytes()` | `operatorMemoryBytes()` | 方法 |
| `DEFAULT_SLOT_MEMORY_BYTES` | `DEFAULT_OPERATOR_MEMORY_BYTES` | 常量 |
| `slotBytesThreshold` / `SLOT_PIPELINE` / `SLOT_COUNT` | **不变** | 名副其实 |

### 兼容要求

配置键是用户可见的，存量部署可能已写旧键。**必须保留旧键兜底**，两者都写时新键优先，并在读到旧键时打一次 `WARN`。否则静默 fallback 到派生值，阈值突变且极难排查。

### 待改文件

- `dojao-core`：`ResourceConfig.java`（常量、方法、约 10 处注释）、`JoinDecider.java`（注释）
- `dojao-server`：`MasterServer.java:159-160`
- 配置：`conf/dojao.conf`、`dojao-dist/src/dist/conf/dojao.conf`
- 文档：`README.md`、`docs/deploy-linux.md`、`docs/benchmark-dojao-trino-spark.md`
- 探针：`NodeMemoryProbeTest`（旧键兼容断言）、`JoinDeciderTest`、`JoinSelectionProbeTest` / `ChainJoinSelectionProbeTest`（注释）
- 镜像目录 `D:\project\dojao`：同步上述 + `official-website/content/docs/` + 设计手册
- **改动历史博客（blog0~blog16）**：不改

### 附带议题

`query.direct.limit` 同样有误导性（它是**单算子**的 Arrow 硬上限，不是 query 级）。可一并改为 `query.operator.directLimit`，但属于独立决定。

---

## 4. P2：三卡槽「三份平分」是否合理 —— 已落地：动态卡槽 2/3（2026-11-22）

### 已落地方案（先看这里）

实测溢写占空比只有 4%~15%（攒批 4.4~5.7s vs 排序+写盘 0.2~0.7s），快盘上第三槽永远等不到"两写并存"，
却按 `3 × 单槽预算` 记峰值。现在的规则：

- 判据：`单桶单槽预算 = memoryLimit × spillRatio ÷ owned`；低于 `query.spill.slot.minBytes`（默认 32M）⇒ 2 槽，否则 3 槽；
  配 0 关闭自适应。
- 重分配：2 槽时单槽预算 ×`SLOT_COUNT / MIN_SLOT_COUNT = 1.5`，保证 `槽数 × 单槽预算` 不变 ⇒
  设计峰值仍是 `3 × memoryLimit × spillRatio`，溢写触发次数/文件数约 -33%。
- 语义修正：`PartitionState.awaitIdleSlot()` 条件由 `>= slotCount - 1` 改为 `>= slotCount`（全部槽都在写才等）。
  旧条件在两槽时等于每次提交后都等自己刚提交的溢写（异步退化成同步），而拉取线程跨分区共享，
  会把整条算子（如 5 个分区的窗口）一起停摆。
- 验收：两槽/三槽（`-Dquery.spill.slot.minBytes=0`）路径均全绿；`ProbeRunner` 23 探针 0 FAIL。
  详见设计手册第一篇第 3 节与 `spill-and-disk` 记忆。

### 原问题记录（历史）

### 现象

`P=6` 时单分区单槽阈值只有 **19MB**，看起来「每个算子每个分区 19MB 就开始溢写」，直觉上内存没被用满。

需要分清两件事：

1. 阈值 ≠ 工作集。越过阈值的数据仍会被处理，只是走磁盘。阈值是**溢写粒度**，不是容量上限。
2. 单分区常驻峰值 = `3 × 阈值`（一个累积 + 两个在途溢写）。P=6 时为 57MB；全部分区同时活跃时总量才等于算子预算 142MB。

所以「利用率低」只在两种情况下成立：

- **分区倾斜**：只有一个分区活跃时，它最多占 3 × 19 ≈ 57MB，其余份额闲置（见 P3）。
- **在跑片段数少于 maxFragments**：每片段份额按最坏并发算（见 P4）。

### 原提案（用户提出，待评估）

> 三卡槽改为**双卡槽**：槽 0 占该分区内存 70%，备份槽占 30%。

优点：

- 槽 0 变大 → 中等数据量不再溢写，减少无谓落盘；
- 去掉「为了溢写而溢写」的观感。

必须回答的问题：

1. **异步溢写是否还够用**：三槽的本质是 `累积 + 在途溢写 ×2`。双槽时只剩一个在途，若槽 1 填满而槽 0 还没写完，只能阻塞等待（背压）或重新启用溢写。需要实测「槽 0 写盘耗时 vs 槽 1 填满耗时」。
2. **不变量如何重写**：现行 `P × 3 × 阈值 = fragmentDirect`、`query.direct.limit = slot × 3 × ratio × 2.5` 都以 `SLOT_PIPELINE = 3` 为因子。双槽后峰值变成 `P × (0.7 + 0.3) × 分区预算 = 算子预算`，但 `buildSortedSpillRoot` 的瞬时拷贝和 Arrow 变长列扩容仍需余量系数。
3. **倾斜时 70% 归谁**：70/30 是按分区静态切，倾斜分区仍然只能拿到自己的 70%。
4. **溢写文件变大**：单次溢写 70% 而非 33%，文件数变少、单文件变大，有利于归并轮数，但峰值写带宽要求更高。

---

## 5. P3：桶间预算不均 —— 真实桶数摊薄已修；跨桶借用推迟到下一版本

**已修部分**：`slotBytesThreshold` 不再按构造时的 `numPartitions` 摊薄，而是由调度层按片段
**实际拥有的桶数**（`ExchangeInStep.owned()` → `setBudgetPartitions(owned)`）重算 ——
只拥 1 个桶的片段拿到完整额度，不再被布局分区数白白除小。

**同一口径已推广到 HashJoin（2026-11-22）**：内存驻留的 `HashJoinOperator` 原先固定按
`memoryLimit ÷ numPartitions` 给单桶 build 预算，多 Worker 时本机只 own P/W 个桶 ⇒ 实际只能用
约 1/W 的预算。现由 `FragmentExecutor.localJoin` 在 open 前调用
`setBuildBudgetPartitions(owned)`（build 侧），单桶预算 = `memoryLimit ÷ owned`，总量仍封顶
`memoryLimit`；只影响 HASH 路径。**广播 join 例外**（`BROADCAST` 跳过）：其 `numPartitions` =
Worker 数、每台 own 1 个桶但桶里是完整右表副本，`memoryLimit` 已按 ×该桶数 下发，保持原口径。
单机路径（全部桶在本地）默认值不变。

同时修正了闸门的**估算口径**：`JoinHashTable.estimateMemoryBytes()` 原用 `getBufferSize()` 统计向量，
而 Arrow 按 `valueCount` 换算、本表从不设 valueCount（避免 `setValueCount` 的 fillHoles 压平直接拷贝的
变长偏移）⇒ build 侧全部数据恒报 0（实测 40000 行 × 200B 实占 8.2MB 只报 0.28MB），预算再大也拦不住。
现改用只读的 `getBufferSizeFor(行数)`；验收 `HashJoinBuildBudgetProbeTest` 4/0（默认 3MB 预算在第 2 页
4.12MB 失败、`setBuildBudgetPartitions(1)` 12MB 通过且行数守恒）、`JoinHashTableRowIndexProbeTest` 15/0。

**未修部分**：同一算子内**桶与桶之间**仍是静态平分 —— 热桶用满自份额度后频繁溢写，
冷桶 / 空桶的额度闲置（全算子明明还有内存）。

候选方向（均未验证）：

- **按活跃桶数动态摊薄**：`阈值 = 算子剩余预算 × ratio / max(1, 活跃桶数)`，桶激活/完成时重算。
  需保证所有活跃桶的阈值之和不超过算子预算。
- **槽位借用 / 跨桶共享预算**：空闲桶的额度可被热桶临时占用，需要算子级全局额度池 + 滞回。
  **2026-11-22 决定：本版本不做，下一版本设计**；它与"动态卡槽数"正交，落地前需要
  `fill / spill / await` 占空比遥测 —— 正确判据是"谁在等、谁在闲"，而不是单槽目标字节。
- 两者都需要避免抖动（反复扩缩阈值导致溢写/回拉抖动）。

---

## 6. P4：`maxFragments = 2 × worker.parallelism`

因子 2 的来源：Worker 有两个任务池——主池（固定 `worker.parallelism` 个消费者）+ 叶子池（弹性 `SynchronousQueue`，按需扩容）。相关代码与死锁背景见 `WorkerManager.leafExecutor` 注释。

问题：叶子池实际无上界，`2 ×` 只是估计。若不变量按「最坏并发」算，每片段份额被压低；若按实际在跑片段数算，需要运行时反馈。

建议先做**观测**，再决定是否：

- 实测单个 Worker 上同时活跃的片段峰值（心跳/监控已有 `activeMain` / `activeLeaf`）；
- 必要时用 `query.memory.maxFragments` 显式覆盖；
- 更激进：像 `NodeMemory.throttle()` 一样，按实时占用动态调整有效阈值（需迟滞，防止振荡）。

---

## 7. P5：链式片段多算子超订

`MasterServer` 给整个 `PlanningContext` 只设一个 `memoryLimit`，`FragmentSpec` 的每个 step（join / aggregate / window）都用同一个值构造算子。链式片段里若有 2 个溢写算子，合计可用内存是 `2 × slot.memory`。

- 现行兜底：`query.direct.limit`（单算子分配器硬上限）+ `NodeMemory.throttle()` 背压。
- 需要决定：是**按算子数再除一次**（严格），还是承认超订并靠全局背压（当前行为，需明确写进文档）。

---

## 8. 下一步（按顺序）

1. **先测**，不要先改：跑一个溢写量大的查询，采集
   - `NodeMemory` 实际峰值 / Arrow 预算（利用率）
   - 溢写文件数与总字节（`SpillTelemetry`）
   - 单个 Worker 上活跃片段峰值（主池 + 叶子池）
   - 分区级倾斜度（各 `PartitionState` 行数/字节）
   依据：`NodeMemoryProbeTest`、`SpillTelemetryProbeTest`、`SpillDiskProbeTest`、`WorkerManager` 心跳指标。
2. 依据实测结果选择方向（P2 已于 2026-11-22 落地：动态卡槽 2/3）：
   - 溢写文件仍过多但内存有富余 → 调大 `query.operator.memory`，或调大 `query.spill.slot.minBytes`
     （慢盘保守保留三槽）；
   - 桶间倾斜严重 → 下一版本做 P3 跨桶共享预算。
3. 任何改动都必须同步更新：
   - `ResourceConfig.queryDirectLimit()` 的 `SLOT_PIPELINE` 因子
   - `docs/deploy-linux.md` 与 `conf/dojao.conf` 的派生链
   - 探针断言（阈值、峰值、文件数）

---

## 9. 不要破坏的不变量

1. `Σ(各分区 × 槽数 × 阈值) ≤ 算子预算 ≤ fragmentDirect`（除非明确接受超订）。
2. `query.direct.limit` 必须覆盖「设计峰值 × 瞬时拷贝余量」，否则触发的是 Arrow OOM 而不是溢写（线上表现：`row_number() over(partition by …)` 直接把 Worker 撑挂）。
3. 阈值下调只增加溢写次数，不改变正确性；阈值上调若算错不变量，会从「慢」变成「崩」。
