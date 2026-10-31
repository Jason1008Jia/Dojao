# dojao-udf —— Python UDF 开发包

Dojao 外挂函数协议（**DFP v2**）的**官方 Python 参考实现**。协议语言中立：任何支持 Arrow 的语言
（Rust / Go / Node / Julia / C++ …）都可实现同一协议接入，引擎零改动。协议权威定义见
[`../UDF_PROTOCOL.md`](../UDF_PROTOCOL.md)，消息定义见 `dojao-core/src/main/proto/udf.proto`。

Java 引擎通过 **Arrow Flight**（数据面 `doExchange`、控制面 `doAction("ping"/"check_function")`）+
原生 Arrow 批量往返 —— **无痕**：`type='python'` 注册的函数，SQL 用法与 `type='java'` 完全一致。

> 本文档讲 **Python 侧**（写函数 + 跑 sidecar）。要接入别的语言，看 `../UDF_PROTOCOL.md` §11 对接清单。

## 目录

```
python/
├── dojao_udf/
│   ├── base.py          # ScalarFunction / UDAF 基类（数据科学家的契约）
│   ├── sidecar.py       # Arrow Flight 服务端（Java 引擎的桥对端）
│   ├── udf_pb2.py       # 消息桩（已随包提交，运行期直接 import，无需生成）
│   └── examples/
│       └── my_fns.py    # 示例：IsHigh（标量）/ WeightedAvg（聚合）
├── requirements.txt
├── generate_pb.sh       # 构建期：从 udf.proto 重新生成消息桩 udf_pb2.py（改了 proto 才用）
└── README.md
```

## 安装依赖

sidecar 的**运行期依赖**（须装进 sidecar 使用的那个解释器）：
`pyarrow`、`numpy`、`protobuf`（`pandas` 按需 —— 用户函数用了才装）。

```bash
pip install -r requirements.txt     # pyarrow / numpy / pandas / protobuf
```

> `grpcio-tools` 只在**构建期**用来从 `udf.proto` 重新生成 `udf_pb2.py`，运行期**不需要**。
> `udf_pb2.py`（消息桩）**已随包提交**，目标机解压即用 —— **无需**跑 `generate_pb.sh`。
> 传输由 `pyarrow` 自带的 Flight 承载，不再需要手写 gRPC 服务桩。

仅当改了 `dojao-core/src/main/proto/udf.proto`，才需重新生成消息桩：

```bash
pip install grpcio-tools
bash generate_pb.sh          # 重新生成 dojao_udf/udf_pb2.py
```

## 部署到目标机（随分发包）

sidecar 由引擎按需拉起，**用哪个解释器**决定用哪套依赖：

1. **解释器选择**：`-Dsystem.python.home=<环境根>`（多套 Python / conda 时指到具体那一个，
   引擎取 `<home>/bin/python`）；留空 = PATH 上的 `python3`（Linux）/ `python`（Windows）。
2. **依赖必须装进那个解释器**。推荐独立 venv（不污染系统 Python，也绕开 PEP 668 的
   “externally-managed-environment” 拒绝安装）：

   ```bash
   python3 -m venv /opt/dojao-pyenv
   /opt/dojao-pyenv/bin/pip install -r <DOJAO_HOME>/python/requirements.txt
   # Master 与每个 Worker 启动都加： -Dsystem.python.home=/opt/dojao-pyenv
   ```

   若坚持装进系统 Python（Debian/Ubuntu 较新版会拒装）：加 `--break-system-packages`。
3. **验证**（无输出即 OK，说明该解释器依赖齐全）：

   ```bash
   /opt/dojao-pyenv/bin/python -c "import pyarrow, numpy, google.protobuf"
   ```

> **常见报错**：`STARTUP_FAILED … ModuleNotFoundError: No module named 'google'`（或 `pyarrow`）
> = 该解释器没装上面的运行期依赖。按本节装齐，或用 `-Dsystem.python.home` 指到装好的环境。
> 引擎已把这类启动失败翻译成带「装依赖 / 换解释器」提示的错误，看到该提示照做即可。

## 运行 sidecar（每 worker 节点一个进程）

```bash
python -m dojao_udf.sidecar --port 50051
```

Java 端 `SidecarPool` 会自动 spawn 进程池并 `doAction("ping")` 探活（握手时协商 `protocol_version`，
不一致即拒绝启动），无需手动起。启动命令 / 环境（`PYTHONPATH` 等）由 `PythonBackend` 通过
`SidecarPool.Runtime(name, command, env)` 描述符注入 —— Python 与其他语言共用同一套进程编排。

## 写一个函数

```python
import pyarrow as pa
import pyarrow.compute as pc
from dojao_udf import ScalarFunction, UDAF

class IsHigh(ScalarFunction):                      # 标量：一批进一批出
    def evaluate(self, batch):
        cond = pc.greater_equal(batch.column("c0"), 5)
        return pa.record_batch([pc.cast(cond, pa.int64())], names=["result"])

class WeightedAvg(UDAF):                           # 聚合：每 key 一状态
    def create_state(self): return [0.0, 0.0]
    def accumulate(self, state, batch):
        state[0] += float(pc.sum(pc.multiply(batch.column("c0"), batch.column("c1"))).as_py())
        state[1] += float(pc.sum(batch.column("c1")).as_py())
    def merge(self, t, s): t[0] += s[0]; t[1] += s[1]
    def evaluate(self, state): return state[0] / state[1] if state[1] else None
```

注册（与 java 同构）：

```sql
CREATE FUNCTION is_high WITH (type='python',
    code_path='shared://udf/my_fns.py', class_name='my_fns.IsHigh')
  COMMENT 'rating>=5 判定';
```

## 失败语义

- 每批一个 `doExchange` 请求-响应：响应返回 = 该批执行完毕；
- 用户代码异常 → `UdfMeta.status=ERROR` + message 携带 Python traceback（Java 端原样抛错，连接不断）；
- 进程崩溃 / 无 python 环境 / 无 runtime / 超时 / 协议版本不符 → `SidecarPool` 分类为
  `SidecarException.Kind`（`CRASHED` / `NO_RUNTIME_ENV` / `WATCHDOG_TIMEOUT` / `PROTOCOL_MISMATCH` …）。

完整失败分类表见 [`../UDF_PROTOCOL.md`](../UDF_PROTOCOL.md) §9。
