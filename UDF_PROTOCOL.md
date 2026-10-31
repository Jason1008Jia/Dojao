# Dojao Function Protocol (DFP)

> 语言无关的外挂函数协议 —— 让**任何会说 Arrow 的语言**都能作为 Dojao 的函数运行时。

## 0. 定位（为什么是这个协议）

Dojao 引擎本身只认识四类函数 SPI（标量 / 聚合 / 表 / 窗口）。**具体用什么语言实现这些函数，引擎完全不感知**：
用户函数跑在独立的 **sidecar（外挂进程）** 里，通过本协议与引擎通信。

因此：

- **Python 是官方参考实现，不是唯一合法实现。** 协议对语言中立，Rust / Go / R / Julia / Node / C++ … 都可自行实现。
- 别人接入只需实现本协议（收发控制包络 + 读写 Arrow 批次），**引擎零改动**。
- 函数逻辑跑在独立进程：崩了只崩那一个 sidecar，可限资源、超时、重启 —— 这是进程内 Java UDF 给不了的隔离性。

权威定义：`dojao-core/src/main/proto/udf.proto`。本文档说明其语义与用法。

## 1. 协议版本

| 版本 | 含义 |
|---|---|
| 1 | 初版（历史）：`function_name=module.ClassName`，函数签名塞在 `UdfMeta.message` 的 JSON 里 |
| **2** | **当前**：`function_ref` 为后端自定义的不透明引用；结构化 `FunctionSignature` 用 Arrow schema 承载类型；`ping` 增加 `runtime_name` / `protocol_version` |

引擎与 sidecar 在启动握手（`ping`）时**协商版本**：不一致即拒绝启动（`PROTOCOL_MISMATCH`）。
实现新语言时，按本版本实现，并在 `ping` 上报 `protocol_version=2`。

## 2. 传输总览

数据面与控制面都走 **Arrow Flight**（gRPC + Arrow IPC），类型系统用 Arrow 本身（语言无关）：

| 通道 | Flight 调用 | 作用 |
|---|---|---|
| 数据面 | `doExchange(path="invoke")` | 一次函数调用（一批进、一批出） |
| 控制面 | `doAction("ping")` | 探活 + 能力 + 版本协商 |
| 控制面 | `doAction("check_function")` | 冒烟校验 + 回传结构化签名 |

## 3. 数据面：`doExchange(path="invoke")`

**一请求一批，一响应一批**（"响应返回 = 该批完成"）：

```
请求流（客户端→sidecar）：
  batch #1 = 原生 Arrow RecordBatch（输入数据）
  application_metadata = UdfControl 的序列化字节
  （无数据的 op 也发一个空 batch；写完即 completed()）

响应流（sidecar→客户端）：
  batch #1 = 原生 Arrow RecordBatch（结果）
  application_metadata = UdfMeta 的序列化字节（空字节 = OK）
```

规则：

- **Arrow 原生 batch 直接走流**，不要把 IPC 再塞进 `bytes` 字段做二次序列化。
- 控制字段（`UdfControl` / `UdfMeta`）放**每批的 application metadata**，与数据同帧。
- `UdfMeta` 为空字节 = `OK`（`OK=0` 是 protobuf 默认值，被省略）。

## 4. 操作语义（`UdfControl.Op`）

| op | 输入 batch | `opaque_state` 入 | `opaque_state` 出 | 结果 batch | 说明 |
|---|---|---|---|---|---|
| `EVAL` | 参数列 | — | — | 结果列 | 标量，1→1，**必须矢量化**（输出与输入等长） |
| `CREATE_STATE` | 空 | — | 新空状态 | 空 | 增量聚合第 1 步 |
| `ACCUMULATE` | 输入列 | 当前状态 | 新状态 | 空 | 增量聚合：把一批并入状态 |
| `MERGE` | 空 | target 状态 | 合并后状态 | 空 | `source_state` 为待并入的 source |
| `EVALUATE` | 空 | 最终状态 | — | 结果列（1 行） | 从状态求最终值 |
| `COLLECT_EVALUATE` | `[key列…, List值列…]` | — | — | N 行结果列 | 见下 |

- **`opaque_state`**：聚合状态的不透明字节，协议**原样透传**（Python 实现用 pickle，其他语言自选序列化）。引擎跨批次、跨节点搬运它。
- **`COLLECT_EVALUATE`**（collect-evaluate 型聚合）：引擎已按 key 分组，输入的 batch 每 key 一行，
  - 前 `key_column_count` 列是 GROUP BY 键；
  - 其后是 **SQL 调用实参按位对应的收集列**（数量 = 声明输入列数），均为 **Arrow List 列**，每行 = 该 key 攒齐的全部值。
  - sidecar 逐行取 `list.value(row)`（该 key 的值数组）→ 逐 key 调用户 `evaluate` → 返回 N 行结果列。
  - 这是**最稳的 UDAF 形态**：状态全程由引擎侧 Arrow 承载（可测可溢写），无跨栈状态往返，各语言零切组代码。
  - **按位、不按名**：`FunctionSignature.input_columns` 里声明的名字只是**参数位标签**（元数据），
    sidecar **不得**按名去 batch 找列 —— 值列完全由 SQL 实参按位决定（如声明 `("num",)` 的函数被
    `stddev(discount_amount)` 调用时，值列即 `discount_amount`）。引擎侧 `AggregationSpec.collectInputColumns`
    持久化这份按位映射，并经分布式 `AggregationDto` / `control.proto` 透传到 Worker。

## 5. 控制面：`doAction("ping")`

返回 `Result` 载荷 = `UdfPing`：

```proto
message UdfPing {
  string sidecar_version = 1;   // 运行时构建标识，如 "dojao-udf-0.2.0"
  string runtime_name = 2;      // "python" / "rust" / "go" / ...
  bool runtime_ready = 3;       // 运行时是否就绪（依赖齐全、可服务）
  uint32 protocol_version = 4;  // 实现的协议版本；引擎据此校验兼容
  string message = 5;           // 环境信息 / 失败原因
}
```

引擎在 spawn 后轮询 `ping`：`runtime_ready=true` 且进程存活 = 握手成功；随后校验 `protocol_version`。
`runtime_ready=false`（如缺依赖）时 `message` 应给出可诊断原因。

## 6. 控制面：`doAction("check_function")`

- **body** = `function_ref` 的 UTF-8 字节。
- **返回** `Result` 载荷 = `UdfMeta`（`status` + `signature`）。

冒烟校验：sidecar 解析 `function_ref`、确认函数契约完整（如 Python 装饰器四阶段齐全）。
失败 → `UdfMeta.status=ERROR` + `message` 携带 traceback（人类可读），引擎 **fail-fast，坏函数不落库**。

成功 → `UdfMeta.signature`：

```proto
message FunctionSignature {
  FunctionKind kind = 1;              // SCALAR / AGGREGATE / COLLECT / (TABLE / WINDOW 预留)
  repeated string input_columns = 2;  // 声明输入列名（仅参数位标签；SQL 实参按位对应，不按名匹配）；标量可空
  bytes output_schema = 3;            // 输出 Arrow schema 的 IPC 序列化字节
  ...
}
```

## 7. 类型系统：用 Arrow，不用语言类型名

- **输出类型** = `FunctionSignature.output_schema`，内容是 **Arrow schema 的 IPC 封装消息字节**
  （等价于 pyarrow 的 `Schema.serialize()` 产物）。引擎反序列化后取首字段类型。
- 为什么这样：如果协议里用"语言类型名字符串"（如 `"double"`），每种语言都得实现同一张映射表且要统一词汇表 —— 不中立。
  用 Arrow schema 承载类型，则**每种语言只需把自己声明的类型映射到 Arrow**（各语言本就有 Arrow 库）。
- 输入类型：由引擎在调用时随 batch 的 Arrow schema 给出；sidecar 按 batch 的列类型处理即可。
- 空值（null）：按 Arrow 的 null 语义（validity bitmap）处理，不要用哨兵值。

## 8. 函数引用：`UdfControl.function_ref` 是"不透明定位串"

协议**不解释** `function_ref` 的内容 —— 它的语法由各语言的 SDK 自定义：

| 语言 | 建议语法 |
|---|---|
| Python（官方） | `module.ClassName`（如 `my_fns.IsHigh`） |
| Rust | `libmy.so#my_fn` / `crate::module::fn` |
| Go | `pkg.Symbol` |
| Node | `@scope/mod#fn` |

引擎侧原样透传；sidecar 按本语言规则解析并加载。

## 9. 失败语义（区分"用户 bug"与"系统故障"）

| 现象 | 信号 |
|---|---|
| 用户函数抛异常 | `UdfMeta.status=ERROR` + `message`(traceback)，**连接不断** |
| 传输中断 / 进程崩溃 | Flight `UNAVAILABLE` |
| 调用超时（看门狗） | Flight `TIMED_OUT` |
| 函数代码有误（CREATE 时） | `check_function` 返回 `UdfMeta.status=ERROR` |

引擎据此归类为 `SidecarException.Kind`（USER_CODE_ERROR / CRASHED / WATCHDOG_TIMEOUT / …）。

## 10. 代码分发（`code_path`）

函数代码/模型通过 `CREATE FUNCTION ... code_path='...'` 交给后端：路径可为 `file://` / `shared://` /
`http(s)://` / `hdfs://` / `s3://`。引擎解析为本地路径后，由各语言后端注入各自的"模块搜索路径"
（Python 注入 `PYTHONPATH`）。**这是语言无关的"给一个 artifact 路径"机制。**

`UdfControl.Op.SHIP_CODE` 为**预留**：未来做就地下发（源码 / 字节码 / wasm）之用，当前未启用。

## 11. 如何接入一门新语言（对接清单）

1. **实现 sidecar**：一个进程，提供 `doAction("ping")`、`doAction("check_function")`、
   `doExchange(path="invoke")`，按第 3–9 节读写 Arrow 批次 + 控制包络。
2. **实现 Java 后端**：实现 `com.dojao.spi.function.FunctionBackend`（`type()` 返回你的语言名），
   在其中构造一个 `com.dojao.server.function.SidecarPool`，传入你的
   `SidecarPool.Runtime(name, command, env)`（启动命令 + 环境）—— **进程编排 / 探活 / 版本协商 / 池化全部复用，引擎零改动**。
3. **注册**：`BackendRegistry.register(yourBackend)`（或经 ServiceLoader 放上 classpath）。
4. **可选**：给该语言写一个薄 SDK（读写 Arrow + 收发控制包络 + 声明函数契约），降低用户门槛。

**参考实现**：`python/dojao_udf/`（`sidecar.py` = 协议的服务端；`base.py` = 函数契约；`examples/` = 示例）。

## 12. 版本历史

- **v2**（当前）：`function_ref` 不透明引用；`FunctionSignature`（Arrow schema 承载类型）；
  `ping` 增加 `runtime_name` / `protocol_version`；`SidecarPool` 语言无关化。
- **v1**：Python 专属语义（`function_name=module.ClassName`；签名走 `message` 的 JSON）。已废弃。
