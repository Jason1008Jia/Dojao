"""sidecar.py —— dojao-udf Arrow Flight 服务端（每 worker 节点一个进程）。

这是 Dojao Function Protocol (DFP v2) 的 Python 参考实现。其他语言只需按
PROTOCOL.md（协议规格）实现同样的控制面 + 数据面，即可作为 Dojao 的函数运行时。

职责（对应设计文档 §7.7 失败模型）：
- doExchange(path="invoke")：请求流 1 条（原生 Arrow batch + metadata=UdfControl），
  调用户函数后回 1 条（结果 batch + metadata=UdfMeta）—— 响应返回 = 该批完成
- 用户代码异常 → UdfMeta.status=ERROR + traceback（第④层业务感知，不断连）
- doAction("ping") → UdfPing 载荷（能力 / 版本探活）
- doAction("check_function") → UdfMeta{status, signature}（冒烟校验 + 结构化函数签名）

运行（见 python/README.md）：
    <python> -m pip install -r requirements.txt   # pyarrow / numpy / protobuf（udf_pb2.py 已随包，无需生成）
    python -m dojao_udf.sidecar --port 50051
"""

import argparse
import importlib
import os
import pickle
import sys
import threading
import traceback

import numpy as np  # pyarrow 硬依赖（sidecar 批量标量函数可能返回 numpy 数组）
import pyarrow as pa
import pyarrow.flight as flight

from . import base, udf_pb2

# 协议版本（与 udf.proto 头注释一致；Java 端在此版本区间内才接受本 sidecar）。
PROTOCOL_VERSION = 2
# 运行时名 + 构建标识（ping 上报，供引擎能力/版本判定）。
RUNTIME_NAME = "python"
SIDECAR_VERSION = "dojao-udf-0.2.0"


class FunctionHub:
    """函数目录：按 class_name（module.ClassName）解析 + 缓存实例。"""

    def __init__(self):
        self._instances = {}

    def resolve(self, class_name, expect):
        fn = self._instances.get(class_name)
        if fn is None:
            if "." not in class_name:
                raise ValueError(f"class_name 需为 module.ClassName，实际: {class_name}")
            mod_name, cls_name = class_name.rsplit(".", 1)
            attr = getattr(importlib.import_module(mod_name), cls_name)
            if isinstance(attr, expect):
                fn = attr          # 装饰器产出（@scalar_function/@aggregate_function）→ 已是实例
            elif isinstance(attr, type) and issubclass(attr, expect):
                fn = attr()        # 继承基类 → 实例化
            else:
                raise TypeError(
                    f"{class_name} 未继承 {expect.__name__}，也非其装饰器产出实例")
            self._instances[class_name] = fn
        return fn


class UdfFlightServer(flight.FlightServerBase):
    """Flight 服务端：控制面（ping/ship_code）走 do_action，数据面（单批调用）走 do_exchange。"""

    def __init__(self, hub, **kwargs):
        super().__init__(**kwargs)
        self._hub = hub

    # ---- 能力探活（§7.7 第③层控制面）：doAction("ping") ----
    def list_actions(self, context):
        return [flight.ActionType("ping", "健康探活 + python 能力上报"),
                flight.ActionType("check_function", "CREATE 冒烟校验：导入模块 + 解析类属性，校验契约完整性"),
                flight.ActionType("ship_code", "v2 控制面下发函数代码")]

    def do_action(self, context, action):
        if action.type == "ping":
            try:
                import pyarrow
                yield flight.Result(udf_pb2.UdfPing(
                    sidecar_version=SIDECAR_VERSION,
                    runtime_name=RUNTIME_NAME,
                    runtime_ready=True,
                    protocol_version=PROTOCOL_VERSION,
                    message="pyarrow " + pyarrow.__version__).SerializeToString())
            except Exception as e:  # noqa: BLE001
                yield flight.Result(udf_pb2.UdfPing(
                    sidecar_version=SIDECAR_VERSION,
                    runtime_name=RUNTIME_NAME,
                    runtime_ready=False,
                    protocol_version=PROTOCOL_VERSION,
                    message=str(e)).SerializeToString())
            return
        if action.type == "check_function":
            body = action.body.to_pybytes() if action.body is not None else b""
            yield flight.Result(self._check_function(body.decode("utf-8")))
            return
        if action.type == "ship_code":
            raise NotImplementedError("ship_code 留待 v2")
        raise NotImplementedError(f"未知 action: {action.type}")

    def _check_function(self, function_ref):
        """CREATE 冒烟校验：导入模块 + 解析类属性，回传结构化函数签名。

        装饰器校验（@aggregate_function 完整性：漏 @accumulate / 四阶段全缺等）在
        import 时抛 TypeError，这里以 UdfMeta(ERROR) 返回 → Java 端 fail-fast，
        坏函数不落库。双分支同 FunctionHub.resolve（装饰器实例 / 基类子类）。

        function_ref 是 Python SDK 的约定：module.ClassName（协议不解释其内容）。
        成功时回传 UdfMeta.signature（kind + 输入列 + 输出 Arrow schema），
        Java 端据此分派注册表，不再解析任何语言特定的 JSON 元数据。
        """
        try:
            if "." not in function_ref:
                raise ValueError(f"function_ref 需为 module.ClassName，实际: {function_ref}")
            mod_name, cls_name = function_ref.rsplit(".", 1)
            mod = importlib.import_module(mod_name)   # 装饰器校验在 import 时抛
            attr = getattr(mod, cls_name)
            contracts = (base.ScalarFunction, base.AggregateFunction,
                         base.CollectAggregateFunction)

            def _is(kind):
                return (isinstance(attr, kind)
                        or (isinstance(attr, type) and issubclass(attr, kind)))

            # 契约识别（装饰器产出实例 / 基类子类）→ 结构化 FunctionSignature。
            # Java 端 loadScalar/loadAggregate 按 kind 判定契约：非本契约抛
            # IllegalArgumentException 跳过，与 JavaBackend 的契约试错语义对称
            # （Python 函数不会再被误注册成聚合/标量双重身份）。
            if _is(base.CollectAggregateFunction):
                fn = attr() if isinstance(attr, type) else attr
                sig = udf_pb2.FunctionSignature(
                    kind=udf_pb2.FunctionSignature.COLLECT,
                    input_columns=list(fn.inputs),
                    output_schema=_schema_bytes(fn.output))
            elif _is(base.ScalarFunction):
                sig = udf_pb2.FunctionSignature(kind=udf_pb2.FunctionSignature.SCALAR)
            elif _is(base.AggregateFunction):
                sig = udf_pb2.FunctionSignature(kind=udf_pb2.FunctionSignature.AGGREGATE)
            else:
                raise TypeError(
                    f"{function_ref} 未继承 {'/'.join(c.__name__ for c in contracts)}，"
                    "也非其装饰器产出实例")
            return udf_pb2.UdfMeta(status=udf_pb2.UdfMeta.OK, signature=sig).SerializeToString()
        except Exception:
            return udf_pb2.UdfMeta(
                status=udf_pb2.UdfMeta.ERROR,
                message=traceback.format_exc()).SerializeToString()

    # ---- 单批调用（§7.7 批次完成语义：响应返回 = 该批完成） ----
    def do_exchange(self, context, descriptor, reader, writer):
        # pyarrow.flight: descriptor.path 元素是 bytes（b"invoke"），与 str 比较恒不等
        if list(descriptor.path) != [b"invoke"]:
            raise NotImplementedError(f"未知 exchange 路径: {descriptor.path}")
        try:
            chunk = reader.read_chunk()  # FlightStreamChunk(data, app_metadata)
        except StopIteration:
            return
        control = udf_pb2.UdfControl()
        control.ParseFromString(chunk.app_metadata.to_pybytes() if chunk.app_metadata is not None else b"")
        resp_batch, resp_meta = self._dispatch(control, chunk.data)
        # pyarrow 25：exchange 响应 writer 需先 begin(schema) 才能 write_with_metadata
        writer.begin(resp_batch.schema)
        writer.write_with_metadata(resp_batch, resp_meta)
        # Java 端只发 1 条后 completed()；把剩余请求读净，让请求流正常收尾
        for _ in reader:
            pass

    def _dispatch(self, control, batch):
        op, fn_name = control.op, control.function_ref
        try:
            if op == udf_pb2.UdfControl.EVAL:
                fn = self._hub.resolve(fn_name, base.ScalarFunction)
                # n=输入行数：批量路径（n>1）下数组/等长列表当结果列、标量广播成常量列；
                # 逐行路径（n=1）行为与旧版完全一致（1 行 result 列）。
                return _as_batch(fn.evaluate(batch), batch.num_rows), _ok()
            if op == udf_pb2.UdfControl.CREATE_STATE:
                fn = self._hub.resolve(fn_name, base.AggregateFunction)
                return _empty(), _ok_state(_dump_state(fn.create_state(), _SEEN_NONE))
            if op == udf_pb2.UdfControl.ACCUMULATE:
                fn = self._hub.resolve(fn_name, base.AggregateFunction)
                state, seen = _load_state(control.opaque_state)
                if batch.num_rows > 0:
                    if _all_null(batch):
                        # 全 NULL 批：不进入用户 accumulate，只记「本组至今只见过 NULL」
                        seen = max(seen, _SEEN_NULL)
                    else:
                        fn.accumulate(state, batch)
                        seen = _SEEN_DATA
                return _empty(), _ok_state(_dump_state(state, seen))
            if op == udf_pb2.UdfControl.MERGE:
                fn = self._hub.resolve(fn_name, base.AggregateFunction)
                target, t_seen = _load_state(control.opaque_state)
                source, s_seen = _load_state(control.source_state)
                fn.merge(target, source)
                return _empty(), _ok_state(_dump_state(target, max(t_seen, s_seen)))
            if op == udf_pb2.UdfControl.EVALUATE:
                fn = self._hub.resolve(fn_name, base.AggregateFunction)
                state, seen = _load_state(control.opaque_state)
                if seen == _SEEN_NULL:
                    # 整组数据全为 NULL → 结果 NULL，不进入用户 evaluate
                    return _scalar(None), _ok()
                return _scalar(fn.evaluate(state)), _ok()
            if op == udf_pb2.UdfControl.COLLECT_EVALUATE:
                fn = self._hub.resolve(fn_name, base.CollectAggregateFunction)
                # 分组 batch（每 key 一行，值列 = List）：逐行取 list.value(row) → 逐 key 调 evaluate
                return _collect_evaluate(fn, batch, control.key_column_count), _ok()
            raise ValueError(f"未知 op: {op}")
        except Exception:
            # 第④层业务感知：用户代码异常 → ERROR + traceback，不断连
            return _empty(), udf_pb2.UdfMeta(
                status=udf_pb2.UdfMeta.ERROR, message=traceback.format_exc()).SerializeToString()


# ---- 响应 batch / metadata 构造 ----

def _ok():
    return udf_pb2.UdfMeta(status=udf_pb2.UdfMeta.OK).SerializeToString()


def _ok_state(state):
    return udf_pb2.UdfMeta(status=udf_pb2.UdfMeta.OK, opaque_state=state).SerializeToString()


def _empty():
    """无结果 op（CREATE_STATE/ACCUMULATE/MERGE）与错误响应的空 batch。"""
    return pa.record_batch([], schema=pa.schema([]))


# 装饰器 output 类型别名 → pyarrow 类型名（用户可见的友好别名；线上是 Arrow schema）。
_OUTPUT_TYPE_ALIASES = {
    "double": "float64", "float": "float64", "float64": "float64",
    "float32": "float32",
    "int": "int64", "int64": "int64", "long": "int64", "int32": "int32",
    "string": "utf8", "str": "utf8", "utf8": "utf8",
    "bool": "bool", "boolean": "bool",
    "date32": "date32", "date64": "date64", "timestamp": "timestamp[ms]",
}


def _output_arrow_type(alias):
    """装饰器 output 别名 → pyarrow 类型（未知别名抛错，fail-fast）。"""
    name = _OUTPUT_TYPE_ALIASES.get(str(alias).lower(), str(alias))
    try:
        return pa.type_for_alias(name)
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"不支持的输出类型别名: {alias}") from e


def _schema_bytes(alias):
    """输出类型别名 → Arrow schema 的 IPC 序列化字节（协议里语言无关的类型承载）。"""
    return pa.schema([pa.field("result", _output_arrow_type(alias))]).serialize().to_pybytes()


def _scalar(value):
    return pa.record_batch([pa.array([value])], names=["result"])


def _as_batch(out, n=None):
    """把 EVAL 返回值规整成整批 RecordBatch。

    - RecordBatch / Table → 直接复用（批量路径 Java 端校验行数 == 输入行数）；
    - 批量路径（n 给定且 n>1）：pa.Array / numpy 数组 / 长度==n 的 list/tuple → 整列结果；
    - 其余（含逐行路径 n=1 / n=None）：标量 → 1 行（逐行）或广播成 n 行常量列（批量）。

    注意：批量模式下返回<b>标量</b>会被广播成常量列 —— 若函数用旧逐行写法只取
    batch.column(0)[0] 之类首行值，会静默产生错误结果。Python 函数必须矢量化
    （处理整批，输出与输入等长），这本来就是 base.py 声明的契约。
    """
    if isinstance(out, pa.RecordBatch):
        return out
    if isinstance(out, pa.Table):
        batches = out.to_batches()
        if batches:
            return batches[0]
        return _empty()
    batch_mode = n is not None and n != 1
    if batch_mode and isinstance(out, pa.Array):
        if len(out) != n:
            raise ValueError(f"批量标量函数返回数组长度 {len(out)} != 输入行数 {n}")
        return pa.record_batch([out], names=["result"])
    if batch_mode and isinstance(out, np.ndarray):
        arr = pa.array(out)
        if len(arr) != n:
            raise ValueError(f"批量标量函数返回数组长度 {len(arr)} != 输入行数 {n}")
        return pa.record_batch([arr], names=["result"])
    if batch_mode and isinstance(out, (list, tuple)) and len(out) == n:
        # 等长 list/tuple 按结果列处理；否则按整体标量走广播分支
        return pa.record_batch([pa.array(out)], names=["result"])
    # 标量 → 1 行（逐行路径，与旧 _scalar 行为一致）/ 广播成 n 行常量列（批量路径）
    if isinstance(out, pa.Scalar):
        out = out.as_py()
    rows = n if n is not None else 1
    return pa.record_batch([pa.array([out] * rows)], names=["result"])


# ---- 全 NULL 组拦截 ----
#
# 一个组的输入数据全部为 NULL 时，聚合结果必须是 NULL。用户函数通常直接对值做
# float(pc.stddev(...).as_py()) / float(pc.sum(...).as_py()) 之类操作，空数据下
# compute 返回 null → float(None) 抛 TypeError → 查询 FAILED。因此 sidecar 在调用
# 用户代码前拦截：collect 型逐 key 判断值数组是否全 NULL；四阶段聚合用状态外层
# 包装 (state, seen) 记录本组数据可见性（0=未见行 / 1=只见过 NULL / 2=见过非 NULL），
# EVALUATE 时 seen==1 直接返回 NULL，不再进入用户 evaluate。状态字节对 Java 端仍
# 是不透明字节，格式仅供 sidecar 内部使用。

_SEEN_NONE, _SEEN_NULL, _SEEN_DATA = 0, 1, 2


def _dump_state(state, seen):
    """四阶段聚合状态打包：用户状态 + 本组数据可见性。"""
    return pickle.dumps((state, seen))


def _load_state(raw):
    """反序列化 (state, seen)；只处理本 sidecar 自己写出的格式。"""
    return pickle.loads(raw)


def _all_null(batch):
    """整批所有输入列是否全为 NULL（零行由调用方先行判掉）。"""
    return all(batch.column(i).null_count == batch.num_rows
               for i in range(batch.num_columns))


def _all_null_values(arrays):
    """一个组的各参数列值数组是否全为 NULL（零元素的值数组也视为 NULL）。"""
    return bool(arrays) and all(a.null_count == len(a) for a in arrays)


def _collect_evaluate(fn, batch, key_count):
    """collect-evaluate 型求值（方案A：框架已按 key 分组，外挂只计算）。

    batch = [key_cols..., list<value>...]，每 key 一行；值列 = Arrow List 列，每行 =
    该 key 攒齐的全部值。值列由 SQL 实参按位决定（列名无关，不按名查找）。
    逐行取该行子数组（offsets 定界 → child 切片）→ 每 key 值数组，
    调 fn.evaluate(*arrays) 一次一 key（O 组数，无切组扫描）→ 收集结果成 N 行结果列。

    全 NULL 组直接产出 NULL，不进入用户函数。
    """
    n = batch.num_rows
    if n == 0:
        return _empty()
    value_cols = [batch.column(i) for i in range(key_count, batch.num_columns)]
    results = []
    for r in range(n):
        arrays = [_list_value(col, r) for col in value_cols]
        if _all_null_values(arrays):
            results.append(None)
            continue
        results.append(fn.evaluate(*arrays))
    result = pa.array(results)
    if result.type == pa.null():
        # 整列都是 NULL 时 pa.array 推导出 NullType；按声明输出类型落空值，保住下游列类型
        result = result.cast(_output_arrow_type(getattr(fn, "output", "double")))
    return pa.record_batch([result], names=["result"])


def _list_value(col, r):
    """取 List 列第 r 行的值子数组（该 key 攒齐的全部值）：offsets[r], offsets[r+1] 定界 → child 切片，零拷贝。

    pyarrow ListArray 没有 .value(r)；正确姿势是读 offsets 取子数组边界，再对 .values（child 数组）
    做 slice —— 返回的仍是 pyarrow.Array，用户函数可继续用 pc.stddev 等矢量化 compute。
    """
    offsets = col.offsets
    start = offsets[r].as_py()
    end = offsets[r + 1].as_py()
    return col.values.slice(start, end - start)


def _parent_watchdog(stdin):
    """父 JVM 看门狗：父进程（Master/Worker JVM）一死，OS 即关闭它持有的 stdin 管道写端 →
    read 返回 EOF → 本进程退出。防止父 JVM 硬崩（原生 OOM / 任务管理器强杀，shutdown hook 不执行）
    时本进程变孤儿进程堆积 —— 曾因 119 个孤儿 sidecar 占满 Windows 提交额（页面文件剩 34M）
    导致新 JVM 启动时 metaspace mmap 失败、Master 原生 OOM 起不来。

    Java 端 SidecarPool 用默认 ProcessBuilder 起进程（stdin 为管道，从不写入、从不在
    存活期关闭）；这里阻塞读 stdin，EOF 即自杀（os._exit 硬退，父已死无需清理）。手动
    `python -m dojao_udf.sidecar` 调试时 stdin 是终端 → 阻塞不退出，行为不变。
    """
    try:
        while True:
            chunk = stdin.read(4096)
            if not chunk:
                break
    except Exception:
        pass
    try:
        os._exit(0)
    except Exception:
        pass


def serve(port, module_paths):
    hub = FunctionHub()
    for m in module_paths:
        importlib.import_module(m)  # 预热导入（可选，class_name 自解析时不需要）
    server = UdfFlightServer(hub, location=f"grpc://0.0.0.0:{port}")
    print(f"[dojao-udf sidecar] listening on grpc://0.0.0.0:{port}", flush=True)
    # 父 JVM 看门狗（daemon）：父死 → stdin EOF → 自杀，杜绝孤儿堆积。
    threading.Thread(target=_parent_watchdog,
                     args=(sys.stdin.buffer,), daemon=True).start()
    server.serve()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="dojao-udf sidecar")
    p.add_argument("--port", type=int, default=50051)
    p.add_argument("--module", action="append", default=[],
                   help="预热导入的用户函数模块（可多个；class_name 自解析时可省略）")
    args = p.parse_args()
    serve(args.port, args.module)
