# 内存模型与溢写卡槽：待解决问题

> 状态：**未实施，待排期**。本文只记录问题、现状推导与候选方向，不代表已确定的设计。
> 记录日期：2026-11-12

## 1. 结论摘要

| # | 问题 | 性质 | 建议优先级 |
|---|---|---|---|
| P1 | `query.slot.memory` 名字有歧义，实为**单算子总预算** | 命名 | 高（纯改名，风险低） |
| P2 | 三卡槽「三份平分」看似公平，实际常低估可用内存、加剧溢写 | 设计 | 中（需实测支撑） |
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

代码锚点：

- `dojao-core/.../config/ResourceConfig.java`
  - `maxConcurrentFragments()` :635
  - `fragmentDirectBytes()` :646
  - `slotMemoryBytes()` :668
  - `queryDirectLimit()` :751
- `dojao-server/.../master/MasterServer.java:159` — `ctxBuilder.memoryLimit(ResourceConfig.slotMemoryBytes())`
- `dojao-core/.../operator/AbstractSpillableOperator.java:199` — `slotBytesThreshold = memoryLimit × spillRatio / numPartitions`
- `dojao-core/.../operator/AbstractSpillableOperator.java:71` — `SLOT_COUNT = 3`
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

## 4. P2：三卡槽「三份平分」是否合理

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

## 5. P3：按「配置分区数」摊薄 → 倾斜时闲置

`slotBytesThreshold` 用的是构造时确定的 `numPartitions`，与实际活跃分区数无关。

候选方向（均未验证）：

- **按活跃分区数动态摊薄**：`阈值 = 算子剩余预算 × ratio / max(1, 活跃分区数)`，分区激活/完成时重算。需保证所有活跃分区的阈值之和不超过算子预算。
- **槽位借用**：空闲分区的额度可被活跃分区临时占用，需要一个算子级的全局额度池。
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
2. 依据实测结果选择方向：
   - 利用率高、倾斜小 → 维持三卡槽，只做 P1 改名；
   - 溢写文件过多但内存有富余 → 优先评估双卡槽 70/30 或整体调大 `query.operator.memory`；
   - 倾斜严重 → 优先做 P3 动态摊薄。
3. 任何改动都必须同步更新：
   - `ResourceConfig.queryDirectLimit()` 的 `SLOT_PIPELINE` 因子
   - `docs/deploy-linux.md` 与 `conf/dojao.conf` 的派生链
   - 探针断言（阈值、峰值、文件数）

---

## 9. 不要破坏的不变量

1. `Σ(各分区 × 槽数 × 阈值) ≤ 算子预算 ≤ fragmentDirect`（除非明确接受超订）。
2. `query.direct.limit` 必须覆盖「设计峰值 × 瞬时拷贝余量」，否则触发的是 Arrow OOM 而不是溢写（线上表现：`row_number() over(partition by …)` 直接把 Worker 撑挂）。
3. 阈值下调只增加溢写次数，不改变正确性；阈值上调若算错不变量，会从「慢」变成「崩」。
