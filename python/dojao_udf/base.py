"""dojao_udf 基类 —— 与 Java SPI（com.dojao.spi.function）一一对称。

矢量化约定：sidecar 传给函数的输入是 pyarrow.RecordBatch（一批行），
输出也是 RecordBatch / 数组 —— 与 Java 端"按批往返"（Arrow Flight）对应。
"""

import inspect

from abc import ABC, abstractmethod


class ScalarFunction(ABC):
    """标量函数：输入一批行，输出一批行（1→1，矢量化）。

    evaluate(batch) → RecordBatch（列名任意，Java 端取第一列第一行）
    """

    # 函数名（可省略；注册以 SQL 函数名为准）
    name: str = ""

    @abstractmethod
    def evaluate(self, batch):
        """batch: pyarrow.RecordBatch（参数列 c0..cn-1）→ RecordBatch / 数组（输出列）"""
        raise NotImplementedError


class AggregateFunction(ABC):
    """聚合函数：每 key 一状态，逐批累加（对齐 Java AggregateFunction 契约）。

    Java 端把状态序列化为不透明字节（opaque_state）往返透传，用户无需关心。

    全 NULL 组：sidecar 不会调用 accumulate（全 NULL 批直接跳过），也不会调用
    evaluate，直接返回 NULL —— 用户函数无需处理「整组值全为 NULL」的空数据。
    """

    name: str = ""

    @abstractmethod
    def create_state(self):
        """新建空状态（任意可 pickle 对象，sidecar 用 pickle 序列化）"""
        raise NotImplementedError

    @abstractmethod
    def accumulate(self, state, batch):
        """把一批输入并入 state（原地修改），batch: pyarrow.RecordBatch"""
        raise NotImplementedError

    @abstractmethod
    def merge(self, target, source):
        """把 source 状态并入 target（原地修改）"""
        raise NotImplementedError

    @abstractmethod
    def evaluate(self, state):
        """从 state 求最终值（标量）"""
        raise NotImplementedError


class CollectAggregateFunction(ABC):
    """收集-求值型聚合函数：框架按 key 攒齐声明的输入列并<b>按 key 分组</b>，Phase 2 逐 key
    把值以 pyarrow 传入 evaluate —— 只写最后的计算，分组/攒值/状态由框架代劳（外挂计算器）。

    与 AggregateFunction（四阶段增量）的本质差异：
      - 增量：状态是外部语言对象，跨技术栈 pickle 往返（内存黑盒 + O(N²) 传输）
      - collect：状态全程 Java 侧 Arrow 列（可测可溢写），零跨栈状态往返；切组在引擎侧
        完成（交付 = 每 key 一行的 [key..., list<value>...]），外部只在 Phase 2 逐 key
        拿到该 key 的全部值（pyarrow.Array）做一次性计算 —— 各语言无需自写切组方案

    evaluate(*arrays) → 标量
      - arrays: 每个 SQL 实参列在该 key 下的 pyarrow.Array（顺序 = SQL 实参按位 = inputs 参数位）
      - 返回:   Python 标量（Java 侧按 output 声明的类型落结果列）

    示例（标准差，攒值版——框架代劳收集全部值）：
        @collect_aggregate(("num",), "double")
        def stddev(num):
            import statistics
            return statistics.pstdev(num.to_pylist())
    """

    # 函数名（可省略；注册以 SQL 函数名为准）
    name: str = ""
    # 声明的输入列名 —— 仅是参数位标签（不要求与 SQL 列同名）；真正收集哪列由 SQL 实参按位决定，
    # 如 @collect_aggregate(("num",)) 的函数被 stddev(discount_amount) 调用时收集的是 discount_amount。
    inputs: tuple = ()
    # 返回类型声明（"double"/"bigint"/"double[]"…，Java 侧据此建结果列）
    output: str = "double"

    @abstractmethod
    def evaluate(self, *arrays):
        """arrays: 每个声明输入列在该 key 下的 pyarrow.Array → Python 标量

        全 NULL 组（该 key 的全部值都为 NULL）由 sidecar 直接返回 NULL，
        不会调用本方法 —— 用户函数无需处理空数据。
        """
        raise NotImplementedError


# =====================================================================
# 装饰器写法（推荐）：标量 @scalar_function / 聚合 @aggregate_function
#
# 与上面的基类继承写法等价、可共存；装饰器在背后把用户对象适配成
# ScalarFunction / AggregateFunction 实例，引擎侧契约不变。
#
#   标量（普通函数 + 装饰器）：
#       @scalar_function
#       def is_high(batch):
#           return ...
#
#   聚合（类 + 方法级阶段标记，方法名任意）：
#       @aggregate_function
#       class WeightedAvg:
#           @create_state
#           def fresh(self): return [0.0, 0.0]
#           @accumulate
#           def add(self, state, batch): ...
#           @merge
#           def combine(self, target, source): ...
#           @evaluate
#           def result(self, state): ...
# =====================================================================


class _ScalarFunction(ScalarFunction):
    """@scalar_function 的包装：把普通函数适配成 ScalarFunction 契约。"""

    def __init__(self, fn, name):
        self._fn = fn
        self.name = name

    def evaluate(self, batch):
        return self._fn(batch)


class _AggregateFunction(AggregateFunction):
    """@aggregate_function 的包装：把类上 @create_state/@accumulate/@merge/@evaluate
    标记的方法适配成 AggregateFunction 契约（方法名任意，阶段由装饰器声明）。"""

    def __init__(self, inst, stages, name):
        self._inst = inst
        self._stages = stages
        self.name = name

    def create_state(self):
        return self._stages["create_state"](self._inst)

    def accumulate(self, state, batch):
        self._stages["accumulate"](self._inst, state, batch)

    def merge(self, target, source):
        self._stages["merge"](self._inst, target, source)

    def evaluate(self, state):
        return self._stages["evaluate"](self._inst, state)


class _CollectAggregateFunction(CollectAggregateFunction):
    """@collect_aggregate 的包装：把普通函数适配成 CollectAggregateFunction 契约。"""

    def __init__(self, fn, inputs, output, name):
        self._fn = fn
        self.name = name
        self.inputs = tuple(inputs)
        self.output = output

    def evaluate(self, *arrays):
        return self._fn(*arrays)


def collect_aggregate(inputs, output="double", *, name=None):
    """收集-求值型聚合装饰器：作用于普通函数（收 pyarrow.Array → Python 标量）。

    框架按 key 攒齐 SQL 实参对应的列（按位对应 inputs 参数位）并<b>按 key 分组</b>（引擎侧切组，
    交付 = 每 key 一行的 [key..., list<value>...]），Phase 2 逐 key 把值以 pyarrow.Array 传入函数，只做最后的计算
    （分组/攒值/状态框架代劳）。这就是「最稳定」的 UDAF 形态：
    状态全程 Java 侧 Arrow 列（可测可溢写），无跨技术栈状态往返，各语言零切组代码。

    用法：
        @collect_aggregate(("num",), "double")
        def stddev(num):
            return statistics.pstdev(num.to_pylist())

        @collect_aggregate(("col1", "col2"), "double", name="xx_model")
        def xx_model(col1, col2):
            ...

    inputs 可传字符串（单列便捷）：@collect_aggregate("num", "double")
    """
    if isinstance(inputs, str):
        inputs = (inputs,)

    def deco(fn):
        if isinstance(fn, type):
            raise TypeError(
                "@collect_aggregate 应用于普通函数（只写 evaluate，收 pyarrow 数组）；"
                "四阶段增量聚合请用 @aggregate_function")
        return _CollectAggregateFunction(fn, inputs, output, name or getattr(fn, "__name__", ""))

    return deco


def _stage(tag):
    """方法级阶段标记：把方法标成聚合四阶段之一。"""

    def deco(fn):
        fn._dojao_stage = tag
        return fn

    return deco


create_state = _stage("create_state")
accumulate = _stage("accumulate")
merge = _stage("merge")
evaluate = _stage("evaluate")


# 聚合四阶段（装饰器完整性校验）
_REQUIRED_STAGES = {"create_state", "accumulate", "merge", "evaluate"}
# 各阶段方法（含 self）应接受的位置参数个数；宽松校验，带 *args 时跳过
_STAGE_ARITY = {"create_state": 1, "accumulate": 3, "merge": 3, "evaluate": 2}


def _check_stage_arity(cls_name, stage, fn):
    """宽松校验阶段方法参数个数（含 self）。签名不可检或有 *args 时跳过。"""
    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        return
    if any(p.kind == p.VAR_POSITIONAL for p in sig.parameters.values()):
        return  # *args 形态，不强行约束
    params = [p for p in sig.parameters.values()
              if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    expect = _STAGE_ARITY[stage]
    if len(params) != expect:
        raise TypeError(
            f"{cls_name}.{stage}() 参数个数不对：含 self 应 {expect} 个位置参数，实际 {len(params)} 个")


def scalar_function(arg=None, *, name=None):
    """标量函数装饰器：作用于普通函数（输入一批 pyarrow.RecordBatch，输出结果）。

    用法：@scalar_function / @scalar_function(name="is_high")
    """

    def deco(fn):
        if isinstance(fn, type):
            raise TypeError(
                "@scalar_function 应用于普通函数；多方法（聚合）请用 @aggregate_function")
        return _ScalarFunction(fn, name or getattr(fn, "__name__", ""))

    if arg is not None and not isinstance(arg, str):
        return deco(arg)
    if isinstance(arg, str):
        name = arg
    return deco


def aggregate_function(arg=None, *, name=None):
    """聚合函数装饰器：作用于类，收集 @create_state/@accumulate/@merge/@evaluate
    标记的四阶段方法（方法名任意）。

    用法：@aggregate_function / @aggregate_function(name="weighted_avg")

    完整性校验（import 时 fail-fast，不等求值）：
    - 目标必须是类（标量请用 @scalar_function）；
    - 四阶段 @create_state/@accumulate/@merge/@evaluate 缺一不可（漏标直接报错，列出缺的）；
    - 同一阶段不能重复标记；阶段方法参数个数做宽松校验（有 *args 跳过）；
    - 支持继承：父类已标记的阶段方法会被子类收集（子类可覆盖同名阶段）。
    """

    def deco(cls):
        if not isinstance(cls, type):
            raise TypeError(
                "@aggregate_function 应用于类；单函数（标量）请用 @scalar_function")
        stages = {}
        for klass in cls.__mro__:                 # 本类优先，父类阶段方法兜底
            if klass is object:
                continue
            local = {}
            for attr in vars(klass).values():
                stage = getattr(attr, "_dojao_stage", None)
                if stage is None:
                    continue
                if stage in local:
                    raise TypeError(
                        f"{cls.__name__} 阶段 {stage} 被重复标记："
                        f"{local[stage].__qualname__} 与 {attr.__qualname__}")
                _check_stage_arity(cls.__name__, stage, attr)
                local[stage] = attr
            stages.update(local)
        missing = _REQUIRED_STAGES - set(stages)
        if missing:
            raise TypeError(
                f"{cls.__name__} 缺少聚合阶段: {sorted(missing)}。"
                "在每个阶段方法上补对应的 @create_state/@accumulate/@merge/@evaluate"
                "（方法名任意），四阶段缺一不可")
        return _AggregateFunction(cls(), stages, name or cls.__name__)

    if arg is not None and not isinstance(arg, str):
        return deco(arg)
    if isinstance(arg, str):
        name = arg
    return deco
