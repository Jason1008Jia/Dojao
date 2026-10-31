# -*- coding: utf-8 -*-
"""公共工具：可复现伪随机、权重查表、分区分天规划、MySQL 连接。

★ 造数铁律：所有值都由「行号 / 订单号」通过**纯函数**推出来，不用 random 模块、不存状态。
   同一个输入永远得到同一行。这样有两个好处：
     1. 重跑覆盖写入的结果完全一致（写坏半个文件重来即可，不用清库）；
     2. MySQL 与 Hive 两个生成器能**共用同一套算术**（见 model.py）——
        Hive 里的 order_id / sku_id / amount 因此一定是 MySQL 里真实存在的那些。
"""

from datetime import timedelta

import config as cfg

MASK64 = (1 << 64) - 1


# ============================================================================
# 一、可复现伪随机（splitmix64）
# ============================================================================

def mix(x):
    """整数 → 稳定的 64 位无符号伪随机数（splitmix64）。

    返回值是**无符号**的（已经 & MASK64），所以下面 `% n` 取模永远是非负的。
    写 C/Java 版本时这里最容易翻车：有符号 long 取模会得到负数，拿去做数组下标直接越界。
    """
    z = (x + 0x9E3779B97F4A7C15) & MASK64
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK64
    return (z ^ (z >> 31)) & MASK64


def u01(x):
    """整数 → [0,1) 均匀浮点（取高 53 位，double 的有效位）。"""
    return (mix(x) >> 11) * (1.0 / (1 << 53))


def pick(x, n):
    """整数 → [0, n) 均匀下标。"""
    if n <= 0:
        raise ValueError("pick(x, n) 的 n 必须 > 0")
    return mix(x) % n


def pick_weighted(x, weights):
    """整数 → 按权重命中的下标（weights 是任意正整数序列）。"""
    total = sum(weights)
    r = mix(x) % total
    acc = 0
    for i, w in enumerate(weights):
        acc += w
        if r < acc:
            return i
    return len(weights) - 1     # 理论上到不了，兜底


def pick_weighted_value(x, pairs):
    """整数 → 从 ((值, 权重), ...) 里按权重取一个值。"""
    i = pick_weighted(x, [w for _, w in pairs])
    return pairs[i][0]


# ============================================================================
# 一·补充：向量化版本（百万行级的维表用，逐行调 Python 太慢）
# ============================================================================

def mix_np(x):
    """splitmix64 的 numpy 向量版 —— 与上面的 mix() 逐位等价（uint64 自然回绕）。"""
    import numpy as np
    z = x.astype(np.uint64) + np.uint64(0x9E3779B97F4A7C15)
    z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return z ^ (z >> np.uint64(31))


def u01_np(h):
    """向量版 u01：uint64 数组 → [0,1) 浮点数组。"""
    import numpy as np
    return (h >> np.uint64(11)).astype(np.float64) * (1.0 / (1 << 53))


def weighted_pick_np(h, weights):
    """向量版权重取下标：h 是 uint64 数组 → [0, len(weights)) 的 int64 数组。"""
    import numpy as np
    cdf = np.cumsum(np.asarray(weights, dtype=np.int64))
    r = (h % np.uint64(int(cdf[-1]))).astype(np.int64)
    return np.searchsorted(cdf, r, side="right")


def weighted_lut(pairs, size=1000):
    """((值, 权重), ...) → 定长查表：一次取模拿到**值**，热路径零循环。

    size 建议 1000（千分比精度），各权重之和会被按比例缩放铺满整张表。
    """
    total = sum(w for _, w in pairs)
    lut = []
    for i, (val, w) in enumerate(pairs):
        n = size - len(lut) if i == len(pairs) - 1 else round(size * w / total)
        n = min(n, size - len(lut))
        lut.extend([val] * n)
    return lut


def build_sec_of_day(hour_weights, size=86400):
    """均匀分位 0..size-1 → 按 24 小时作息加权后的「一天内秒数」，**严格非递减**。

    用它而不是均匀随机：文件里的 event_time 会近似有序，SNAPPY 才压得动
    （乱序的时间串基本压不动，文件会大好几倍）。最后一小时吃掉取整余数，保证正好铺满。
    """
    total = sum(hour_weights)
    lut = []
    for h in range(24):
        n = (size - len(lut)) if h == 23 else round(size * hour_weights[h] / total)
        n = min(n, size - len(lut))
        for i in range(n):
            lut.append(h * 3600 + int(3600 * i / n) if n else 0)
    return lut


# 一天 24 小时的相对活跃度（凌晨低谷、晚高峰 19-22 点）
HOUR_WEIGHTS = (2, 1, 1, 1, 1, 2, 4, 8, 14, 18, 20, 22,
                22, 20, 22, 26, 30, 34, 40, 46, 44, 36, 20, 8)

# 会话窗口：30 分钟内的同一用户事件算同一次会话
SESSION_WINDOW_SEC = 1800

_CITIES = list(cfg.CITIES)
_CITY_LUT = weighted_lut(list(zip(range(len(_CITIES)), cfg.CITY_WEIGHTS)))
_CATEGORY_LUT = weighted_lut(list(zip(range(len(cfg.CATEGORIES)), cfg.CATEGORY_WEIGHTS)))


# ============================================================================
# 二、分区分天 / 订单规模规划
# ============================================================================

class Plan(object):
    """一次造数的全局规划（两个生成器共用同一份，保证分区名与订单规模一致）。"""

    __slots__ = ("days", "first_day", "last_day", "day_rows",
                 "order_count", "day_orders", "order_cdf",
                 "target_purchase", "paid_rate", "avg_items")

    def __init__(self, **kw):
        for k in self.__slots__:
            setattr(self, k, kw[k])

    def order_day(self, i):
        """订单序号 i（0-based）落在第几个分区 —— 二分查找累计订单数。"""
        lo, hi = 0, self.days - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if i < self.order_cdf[mid + 1]:
                hi = mid
            else:
                lo = mid + 1
        return lo

    def order_range(self, d):
        """第 d 天的订单序号区间 [start, end)。"""
        return self.order_cdf[d], self.order_cdf[d + 1]


_PLAN = None


def plan():
    """构建（并缓存）全局规划。

    订单总数是**反推**出来的，不是拍脑袋配的：
        每天要有 rows[d] × 5% 个购买事件；
        每个已支付订单贡献 (件数) 个购买事件（平均 2.23 件）；
        已支付订单占 88%。
      ⇒ 订单总数 = Σ(rows[d] × 5%) / (88% × 2.23)
    这样购买事件占每天 5% 才是真对得上的（实测比例跑完会打印）。
    """
    global _PLAN
    if _PLAN is not None:
        return _PLAN

    days = cfg.HIVE_DAYS
    last_day = cfg.HIVE_LAST_DAY
    first_day = last_day - timedelta(days=days - 1)

    # ---------- ① 每天行数：增长 × 周末 × 抖动，夹到 [MIN, MAX] ----------
    day_rows = []
    for d in range(days):
        # weekday(): 周一=0 ... 周六=5 周日=6
        base = cfg.HIVE_BASE_ROWS * (1.0 + cfg.HIVE_GROWTH_PER_DAY * d)
        if (first_day + timedelta(days=d)).weekday() >= 5:
            base *= cfg.HIVE_WEEKEND_BOOST
        base *= 0.92 + 0.16 * u01(0xA5A5_0000 + d)      # ±8% 天级抖动
        n = int(round(base))
        n = max(cfg.HIVE_MIN_ROWS, min(cfg.HIVE_MAX_ROWS, n))
        day_rows.append(n)

    total_rows = sum(day_rows)

    # ---------- ② 反推订单总数 ----------
    paid_rate = sum(w for s, w in cfg.ORDER_STATUS_WEIGHTS.items()
                    if s in cfg.PAID_STATUSES) / 1000.0
    items_w = cfg.ITEMS_WEIGHTS
    avg_items = sum((j + 1) * w for j, w in enumerate(items_w)) / float(sum(items_w))

    target_purchase = int(total_rows * cfg.PURCHASE_RATIO)
    order_count = int(round(target_purchase / (paid_rate * avg_items)))

    # ---------- ③ 订单按天摊（与行为日志同一条曲线 → 购买事件自然落在对应分区） ----------
    day_orders = [int(round(order_count * r / total_rows)) for r in day_rows]
    day_orders[-1] += order_count - sum(day_orders)
    if min(day_orders) < 0:
        raise RuntimeError("订单按天摊出了负数，检查 HIVE_* 规模配置")

    order_cdf = [0]
    for n in day_orders:
        order_cdf.append(order_cdf[-1] + n)

    _PLAN = Plan(
        days=days,
        first_day=first_day,
        last_day=last_day,
        day_rows=day_rows,
        order_count=order_count,
        day_orders=day_orders,
        order_cdf=order_cdf,
        target_purchase=target_purchase,
        paid_rate=paid_rate,
        avg_items=avg_items,
    )
    return _PLAN


def date_of(d):
    """第 d 个分区（0-based）对应的日期。"""
    return plan().first_day + timedelta(days=d)


# ============================================================================
# 三、维度（品类 / 城市）—— 两个生成器共用同一份，保证两边对得上
# ============================================================================

def category_of(x):
    """按品类热度取一个品类下标（0-based，指向 cfg.CATEGORIES）。"""
    return _CATEGORY_LUT[pick(x, len(_CATEGORY_LUT))]


def city_of(x):
    """按城市权重取一个城市下标（0-based，指向 cfg.CITIES）。"""
    return _CITY_LUT[pick(x, len(_CITY_LUT))]


# ============================================================================
# 四、MySQL 连接
# ============================================================================

def mysql_connect(db=None, autocommit=False, local_infile=False):
    """建一个 MySQL 连接。db=None 时不指定库（用于 CREATE DATABASE）。"""
    import pymysql
    return pymysql.connect(
        host=cfg.MYSQL_HOST,
        port=cfg.MYSQL_PORT,
        user=cfg.MYSQL_USER,
        password=cfg.MYSQL_PASSWORD,
        database=db,
        charset=cfg.MYSQL_CHARSET,
        autocommit=autocommit,
        local_infile=local_infile,
    )


_HMS = None


def hms(sec):
    """一天内秒数 → 'HH:mm:ss'。

    86400 个串建一次全局复用 —— 造几千万行时间戳时，`%` 拼串比 strftime 快一个数量级。
    """
    global _HMS
    if _HMS is None:
        _HMS = ["%02d:%02d:%02d" % (s // 3600, (s % 3600) // 60, s % 60)
                for s in range(86400)]
    return _HMS[sec]


def fmt_int(n):
    """1234567 → '1,234,567'（打印用）。"""
    return "{:,}".format(n)
