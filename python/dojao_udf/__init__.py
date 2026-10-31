"""dojao_udf —— 与 Java SPI 对称的 Python 函数开发包。

数据科学家实现：
- ScalarFunction.evaluate(batch)       标量：一批进，一批出（矢量化）
- AggregateFunction.create_state/accumulate/merge/evaluate   聚合：每 key 一状态，逐批累加
- CollectAggregateFunction.evaluate(*arrays)  收集-求值型聚合：框架按 key 攒值，
  Phase 2 逐 key 把声明的输入列以 pyarrow.Array 传入，只写最后的计算（主力推广形态）

sidecar.py 把这些函数接入 Arrow Flight 服务（udf.proto 控制包络），Java 引擎按批往返 —— 无痕。
"""

from .base import (
    ScalarFunction,
    AggregateFunction,
    CollectAggregateFunction,
    scalar_function,
    aggregate_function,
    collect_aggregate,
    create_state,
    accumulate,
    merge,
    evaluate,
)

__all__ = [
    "ScalarFunction",
    "AggregateFunction",
    "CollectAggregateFunction",
    "scalar_function",
    "aggregate_function",
    "collect_aggregate",
    "create_state",
    "accumulate",
    "merge",
    "evaluate",
]
__version__ = "0.1.0"
