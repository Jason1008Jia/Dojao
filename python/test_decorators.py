"""test_decorators.py —— dojao_udf 装饰器（@scalar_function / @aggregate_function）完整性校验测试。

覆盖：四阶段缺标（漏 @accumulate / 只加类装饰器）、同阶段重复标记、参数个数错误、
装饰器误用（函数/类用反）、合法聚合/标量走通、继承写法兼容、MRO 继承阶段方法。

运行：cd python && python test_decorators.py    （仅依赖标准库，不依赖 pyarrow）
"""

import sys

from dojao_udf import (
    ScalarFunction,
    AggregateFunction,
    scalar_function,
    aggregate_function,
    create_state,
    accumulate,
    merge,
    evaluate,
)

_passed = 0
TOTAL = 10


def _ok(name):
    global _passed
    _passed += 1
    print(f"  PASS  {name}")


def _expect_error(fn, name, fragment):
    """执行 fn 应抛 TypeError 且信息含 fragment。"""
    global _passed
    try:
        fn()
    except TypeError as e:
        if fragment in str(e):
            _passed += 1
            print(f"  PASS  {name}  ->  {e}")
            return
        raise AssertionError(f"{name}: 拦截了但信息不含 '{fragment}': {e}")
    raise AssertionError(f"{name}: 未拦截（应抛 TypeError 含 '{fragment}')")


# 1) 合法聚合装饰器：包装器走通四阶段
@aggregate_function
class GoodAgg:
    @create_state
    def fresh(self):
        return [0.0, 0.0]

    @accumulate
    def add(self, state, batch):
        state[0] += 1.0

    @merge
    def combine(self, target, source):
        target[0] += source[0]

    @evaluate
    def result(self, state):
        return state[0]


assert isinstance(GoodAgg, AggregateFunction), "装饰器应产出 AggregateFunction 实例"
s = GoodAgg.create_state()
GoodAgg.accumulate(s, None)
GoodAgg.accumulate(s, None)
s2 = GoodAgg.create_state()
GoodAgg.accumulate(s2, None)
GoodAgg.merge(s, s2)
assert GoodAgg.evaluate(s) == 3.0, GoodAgg.evaluate(s)
_ok("合法聚合：包装器走通 create/accumulate/merge/evaluate")


# 2) 漏 @accumulate → import 时报缺哪个阶段
def miss_accum():
    @aggregate_function
    class MissAccum:
        @create_state
        def fresh(self):
            return [0.0, 0.0]

        @merge
        def combine(self, t, s):
            pass

        @evaluate
        def result(self, s):
            return 0


_expect_error(miss_accum, "漏 @accumulate", "accumulate")


# 3) 只加类装饰器，方法都没标 → 四阶段全缺
def only_class_deco():
    @aggregate_function
    class OnlyClass:
        def create_state(self):
            return [0.0]

        def accumulate(self, s, b):
            pass


_expect_error(only_class_deco, "只加类装饰器（无方法标记）", "accumulate")


# 4) 同一阶段重复标记 → 拦截
def dup_stage():
    @aggregate_function
    class Dup:
        @create_state
        def a(self):
            return 0

        @create_state
        def b(self):
            return 1

        @accumulate
        def add(self, s, b):
            pass

        @merge
        def combine(self, t, s):
            pass

        @evaluate
        def result(self, s):
            return 0


_expect_error(dup_stage, "同阶段重复标记", "重复标记")


# 5) 阶段方法参数个数错误（accumulate 漏 batch）→ 拦截
def bad_arity():
    @aggregate_function
    class BadArity:
        @create_state
        def fresh(self):
            return [0.0]

        @accumulate
        def add(self, state):
            pass  # 漏了 batch

        @merge
        def combine(self, t, s):
            pass

        @evaluate
        def result(self, s):
            return 0


_expect_error(bad_arity, "accumulate 参数个数不对", "参数个数")


# 6) @aggregate_function 用在函数上 → 拦截
def agg_on_fn():
    @aggregate_function
    def f(batch):
        return batch


_expect_error(agg_on_fn, "@aggregate_function 用在函数上", "应用于类")


# 7) 标量装饰器：合法 + 误用（标在类上）
@scalar_function
def doubler(batch):
    return batch


assert isinstance(doubler, ScalarFunction), "装饰器应产出 ScalarFunction 实例"
_ok("合法标量：普通函数 + @scalar_function")


def scalar_on_class():
    @scalar_function
    class C:
        def evaluate(self, b):
            return b


_expect_error(scalar_on_class, "@scalar_function 用在类上", "普通函数")


# 8) 继承写法兼容
class InheritAgg(AggregateFunction):
    def create_state(self):
        return [0.0]

    def accumulate(self, s, b):
        s[0] += 1.0

    def merge(self, t, s):
        t[0] += s[0]

    def evaluate(self, s):
        return s[0]


inst = InheritAgg()
assert isinstance(inst, AggregateFunction)
_ok("继承写法兼容（基类 AbstractMethod 强制实现四阶段）")


# 9) MRO：子类复用父类已标记的阶段方法（子类可覆盖）
class _Base:
    @create_state
    def fresh(self):
        return [0.0]

    @accumulate
    def add(self, s, b):
        s[0] += 1.0


@aggregate_function
class SubAgg(_Base):
    @merge
    def combine(self, t, s):
        t[0] += s[0]

    @evaluate
    def result(self, s):
        return s[0]


assert isinstance(SubAgg, AggregateFunction)
s = SubAgg.create_state()
SubAgg.accumulate(s, None)
assert SubAgg.evaluate(s) == 1.0, SubAgg.evaluate(s)
_ok("MRO：父类阶段方法被子类收集")

print(f"\n{_passed}/{TOTAL} 项通过")
sys.exit(0 if _passed == TOTAL else 1)
