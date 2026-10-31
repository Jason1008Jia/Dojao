"""示例：与 Java SPI 对称的 Python 函数（注册 type='python'）。

两种写法等价、可共存（引擎契约不变）：
- 装饰器写法（推荐）：@scalar_function 标普通函数；@aggregate_function 标类，
  四个阶段用 @create_state/@accumulate/@merge/@evaluate 标记到方法上（方法名任意）。
- 继承写法（兼容）：继承 ScalarFunction / AggregateFunction 基类。

注册（v1 依赖共享 FS，code_path 指向本文件）：
    CREATE FUNCTION is_high WITH (type='python',
        code_path='shared://udf/my_fns.py', class_name='my_fns.IsHigh')
    CREATE FUNCTION weighted_avg WITH (type='python',
        code_path='shared://udf/my_fns.py', class_name='my_fns.WeightedAvg')
    CREATE FUNCTION stddev_incr WITH (type='python',
        code_path='shared://udf/my_fns.py', class_name='my_fns.Stddev')
"""

import pyarrow as pa
import pyarrow.compute as pc

from dojao_udf import (
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


# =====================================================================
# 装饰器写法（推荐）
# =====================================================================

@scalar_function
def is_high_deco(batch):
    """标量：rating>=5 → 1/0。对应 SQL: SELECT is_high(rating)"""
    cond = pc.greater_equal(batch.column("c0"), 5)
    return pa.record_batch([pc.cast(cond, pa.int64())], names=["result"])


@collect_aggregate(("num",), "double")
def stddev(num):
    """收集-求值型聚合：按 key 攒齐 num 列全部值后算总体标准差（主力推广形态）。

    对应 SQL: SELECT stddev(num) FROM t GROUP BY ... —— 框架按 key 攒值并分组（Java 侧
    Arrow 列，可测可溢写），Phase 2 把每个 key 的 num 列全部值以 pyarrow.Array 传入
    本函数（引擎已按 key 分组，外挂只计算）—— 只写最后的计算，无跨栈状态往返。
    """
    return float(pc.stddev(num).as_py()) if len(num) > 0 else None


@collect_aggregate(("rating",), "double")
def rating_stddev(rating):
    """收集-求值型聚合：按 key 攒齐 rating 列全部值后算总体标准差（dga 演示列）。

    对应 SQL: SELECT rating_stddev(rating) FROM mysql.dga.user_review GROUP BY product_id
    —— 首版声明输入列 = 实际收集列（rating 必须真实存在于表），分布式下 key 哈希到唯一
    分区、每分区独立攒值并分组（Java 侧）→ 攒齐 COLLECT_EVALUATE 调本函数（引擎已分组，逐 key 算）。
    """
    return float(pc.stddev(rating).as_py()) if len(rating) > 0 else None


@aggregate_function
class WeightedAvgDeco:
    """聚合：加权平均。方法名任意，阶段由装饰器声明。对应 SQL: SELECT weighted_avg(score, weight) GROUP BY ..."""

    @create_state
    def fresh(self):
        return [0.0, 0.0]

    @accumulate
    def add(self, state, batch):
        scores = batch.column("c0")
        weights = batch.column("c1")
        state[0] += float(pc.sum(pc.multiply(scores, weights)).as_py())
        state[1] += float(pc.sum(weights).as_py())

    @merge
    def combine(self, target, source):
        target[0] += source[0]
        target[1] += source[1]

    @evaluate
    def result(self, state):
        return state[0] / state[1] if state[1] else None


# =====================================================================
# 继承写法（兼容）
# =====================================================================


class IsHigh(ScalarFunction):
    """标量：rating>=5 → 1/0。对应 SQL: SELECT is_high(rating)"""

    def evaluate(self, batch):
        cond = pc.greater_equal(batch.column("c0"), 5)
        return pa.record_batch([pc.cast(cond, pa.int64())], names=["result"])


class WeightedAvg(AggregateFunction):
    """聚合：加权平均。对应 SQL: SELECT weighted_avg(score, weight) GROUP BY ..."""

    def create_state(self):
        return [0.0, 0.0]

    def accumulate(self, state, batch):
        scores = batch.column("c0")
        weights = batch.column("c1")
        state[0] += float(pc.sum(pc.multiply(scores, weights)).as_py())
        state[1] += float(pc.sum(weights).as_py())

    def merge(self, target, source):
        target[0] += source[0]
        target[1] += source[1]

    def evaluate(self, state):
        return state[0] / state[1] if state[1] else None


class Stddev(AggregateFunction):
    """聚合：总体标准差（四阶段增量写法，Welford 算法，数值稳定）。

    对应 SQL: SELECT stddev_incr(x) FROM t GROUP BY ...

    与 collect 写法的 ``stddev``（引擎攒齐整组值后一次性计算）相比，本写法逐批增量累加，
    状态 = ``[n, mean, m2]``（计数 / 均值 / 平方差和），可 pickle 跨技术栈往返。
    采用 Welford 合并公式，避免 ``sum(x^2) - sum(x)^2/n`` 在大均值上的灾难性抵消。

    evaluate 返回<b>总体</b>标准差（ddof=0，与本文件 ``stddev`` / pyarrow ``pc.stddev``
    默认口径一致）；全 NULL 返回 None，n=1 返回 0.0。
    """

    def create_state(self):
        return [0, 0.0, 0.0]          # [n, mean, m2]

    def accumulate(self, state, batch):
        col = pc.cast(batch.column("c0"), pa.float64())
        n = int(pc.count(col).as_py() or 0)          # 非 NULL 个数
        if n == 0:
            return
        mean = float(pc.mean(col).as_py())
        diff = pc.subtract(col, pa.scalar(mean, pa.float64()))
        m2 = float(pc.sum(pc.multiply(diff, diff)).as_py() or 0.0)
        self._merge_into(state, n, mean, m2)

    def merge(self, target, source):
        self._merge_into(target, source[0], source[1], source[2])

    def evaluate(self, state):
        n, _, m2 = state
        return (m2 / n) ** 0.5 if n > 0 else None

    @staticmethod
    def _merge_into(target, n2, mean2, m2_2):
        """把 (n2, mean2, m2_2) 这组统计量并入 target（Welford 并行合并公式）。"""
        n1, mean1, m2_1 = target
        if n2 == 0:
            return
        if n1 == 0:
            target[:] = [n2, mean2, m2_2]
            return
        n = n1 + n2
        delta = mean2 - mean1
        target[:] = [n, mean1 + delta * n2 / n, m2_1 + m2_2 + delta * delta * n1 * n2 / n]
