# -*- coding: utf-8 -*-
"""维度 + 订单的**确定性模型** —— gen_mysql.py 与 gen_hive.py 共用这一份算术。

为什么要有这个文件：Hive 里的 purchase 事件必须带**真实存在**的 order_id / sku_id / amount。
做法不是「Hive 那边再随机编一套长得像的」，而是两个生成器调同一批纯函数：

    gen_mysql.py ：for i in range(订单数)  用 order_*(i) 推字段 → INSERT 进 MySQL
    gen_hive.py  ：for i in 某天的订单区间  用 order_*(i) 推字段 → 写成 purchase 事件

同一个 i 必然得到同一行，所以 Hive 的 order_id 一定能在 MySQL 里查到，金额也分毫无差。

维表（users / spu / sku）同理：gen_mysql 用 build_dims() 造并写库，gen_hive 用 load_dims()
从库里读回来。**凡是 order_*() 会用到的维表派生量（爆款榜、品类价、规格系数），
一律由 id 的纯函数算出来，绝不依赖「读回来的表里多了/少了一列」** ——
否则两条路径会算出不同的爆款榜，Hive 的 sku_id 立刻跟 MySQL 的 order_item 对不上。
"""

import numpy as np

import common as cm
import config as cfg

# ============================================================================
# 各品类的价格区间（元）与规格选项 —— SKU 单价从这里派生，金额全靠它
# ============================================================================

PRICE_RANGE = {
    1: (899.0, 8999.0),      # 手机
    2: (9.0, 199.0),         # 水果
    3: (3.0, 60.0),          # 蔬菜
    4: (12.0, 260.0),        # 医药
    5: (39.0, 1299.0),       # 母婴产品
    6: (79.0, 899.0),        # 男装
    7: (89.0, 999.0),        # 女装
    8: (59.0, 1599.0),       # 户外运动
    9: (39.0, 799.0),        # 美妆
    10: (29.0, 699.0),       # 家居
    11: (299.0, 9999.0),     # 电脑
    12: (5.0, 89.0),         # 百货
}

SPEC_OPTIONS = {
    1: ("128G", "256G", "512G", "1T"),
    2: ("1斤", "2斤", "5斤"),
    3: ("500g", "1kg", "2kg"),
    4: ("10片", "30片", "60片"),
    5: ("S码", "M码", "L码"),
    6: ("S", "M", "L", "XL"),
    7: ("S", "M", "L", "XL"),
    8: ("37码", "39码", "41码", "43码"),
    9: ("10ml", "30ml", "50ml"),
    10: ("单件", "套装"),
    11: ("8G", "16G", "32G"),
    12: ("单包", "3连包", "6连包"),
}

SPEC_COLORS = ("黑色", "白色", "蓝色", "红色", "绿色", "灰色", "金色", "粉色")

# 通用品牌名（与品类词根拼成商品名）
BRANDS = ("优选", "海蓝", "南山", "天禾", "康美", "卡思", "洛可", "星野", "臻品", "悦享")

TITLE_SUFFIX = ("官方旗舰", "正品直营", "新款上市", "爆款热卖", "限时特惠", "经典款", "轻奢款")


# ============================================================================
# 一、由 id 直接推出的派生量（两条路径必须一致的地方）
# ============================================================================

def _sku_score_np(ids):
    """SKU 的「销量分」—— 同一个 SKU 在任何路径下都必须得到同一个值。"""
    return cm.mix_np(ids * np.uint64(0x2545F491) + np.uint64(307)) >> np.uint64(20)


def _sku_score(sku_id):
    return cm.mix(sku_id * 0x2545F491 + 307) >> 20


# ============================================================================
# 二、维表生成（gen_mysql 用）
# ============================================================================

def build_dims():
    """一次造齐 users / spu / sku（数值字段）并完成索引化。文本列由 gen_mysql 现拼。"""
    global DIMS_SPU
    spu = _build_spus()
    DIMS_SPU = spu                     # _build_skus 要用它的 cat_id / base_price
    try:
        d = {"user": _build_users(), "spu": spu, "sku": _build_skus()}
    finally:
        DIMS_SPU = None
    return d


def _build_users():
    """性别比例 男:女 = 0.45:0.55；年龄**恰好 60%** 落在 20-40 岁，其余随机落在 18-19 与 41-60。"""
    n = cfg.USER_COUNT
    uid = np.arange(1, n + 1, dtype=np.uint64)
    h1 = cm.mix_np(uid * np.uint64(0x9E3779B9) + np.uint64(11))
    h2 = cm.mix_np(uid * np.uint64(0x85EBCA6B) + np.uint64(23))
    h3 = cm.mix_np(uid * np.uint64(0xC2B2AE35) + np.uint64(37))

    main_age = (20 + (h2 % np.uint64(21))).astype(np.int8)     # 主年龄段 20..40
    off = 18 + (h2 % np.uint64(43))                            # 18..60 均匀
    # ★ 落在 20-40 的挪到 41-60：否则「其余 40%」里还有 20-40，主年龄段会变成 79.5% 而不是 60%
    other_age = np.where((off >= 20) & (off <= 40), 41 + (off % np.uint64(20)), off).astype(np.int8)

    return {
        "count": n,
        "gender": np.where(cm.u01_np(h1) < 0.45, 1, 2).astype(np.int8),
        "age": np.where(cm.u01_np(h3) < 0.60, main_age, other_age).astype(np.int8),
        "city": cm.weighted_pick_np(h1, cfg.CITY_WEIGHTS).astype(np.int16),
        "level": (cm.weighted_pick_np(h2, cfg.MEMBER_LEVEL_WEIGHTS) + 1).astype(np.int8),
        "reg_day": (h3 % np.uint64(1095)).astype(np.int16),       # 近 3 年内注册
        "reg_sec": (cm.weighted_pick_np(h1, cm.HOUR_WEIGHTS).astype(np.int32) * 3600
                    + (h2 % np.uint64(3600)).astype(np.int32)),
    }


def _build_spus():
    n = cfg.SPU_COUNT
    sid = np.arange(1, n + 1, dtype=np.uint64)
    h1 = cm.mix_np(sid * np.uint64(0x27D4EB2F) + np.uint64(101))
    h2 = cm.mix_np(sid * np.uint64(0x165667B1) + np.uint64(211))

    cat_idx = cm.weighted_pick_np(h1, cfg.CATEGORY_WEIGHTS)      # 0-based，指向 cfg.CATEGORIES
    brand_idx = np.array([cm.pick(int(s) * 331 + 7, len(BRANDS)) for s in sid], dtype=np.int16)
    noun_idx = np.array([cm.pick(int(s) * 337 + 9, len(cfg.CATEGORIES[int(c)][2:]))
                         for c, s in zip(cat_idx, sid)], dtype=np.int16)

    lo = np.array([PRICE_RANGE[cfg.CATEGORIES[int(c)][0]][0] for c in cat_idx], dtype=np.float64)
    hi = np.array([PRICE_RANGE[cfg.CATEGORIES[int(c)][0]][1] for c in cat_idx], dtype=np.float64)

    return {
        "count": n,
        "cat_idx": cat_idx.astype(np.int16),
        "cat_id": (cat_idx + 1).astype(np.int32),                # CATEGORIES 的 id 就是 1..12
        "brand_idx": brand_idx,
        "noun_idx": noun_idx,
        "base_price": np.round(lo + cm.u01_np(h2) * (hi - lo), 2),
    }


def _build_skus():
    spus = DIMS_SPU
    per = cfg.SKU_PER_SPU
    n = spus["count"] * per

    sku_id = np.arange(1, n + 1, dtype=np.uint64)
    spu_of = ((sku_id.astype(np.int64) - 1) // per + 1)          # 1-based spu_id
    spec_idx = (sku_id.astype(np.int64) - 1) % per
    h = cm.mix_np(sku_id * np.uint64(0x2545F491) + np.uint64(307))
    score = _sku_score_np(sku_id)

    # 同 SPU 内不同规格价格有差异（0.80 ~ 1.25），最高规格最贵
    price = np.round(spus["base_price"][spu_of - 1] * (0.80 + 0.15 * spec_idx), 2)

    return {
        "count": n,
        "spu_of": spu_of,
        "cat_id": spus["cat_id"][spu_of - 1].astype(np.int32),
        "spec_idx": spec_idx.astype(np.int16),
        "color_idx": (h % np.uint64(len(SPEC_COLORS))).astype(np.int16),
        "price": price,
        "cost_price": np.round(price * 0.62, 2),
        "stock": (h % np.uint64(5000) + 10).astype(np.int32),
        "sold_count": (score % np.uint64(100000)).astype(np.int32),
    }


# ============================================================================
# 三、维表登记 / 读回
# ============================================================================

DIMS = None
DIMS_SPU = None       # _build_skus 需要 SPU 的品类与基准价，构造期临时挂一下


def set_dims(d):
    """登记维表（gen_mysql 造完调一次）。"""
    _install(d)
    return DIMS


def load_dims(conn):
    """从 MySQL 读回维表（gen_hive 用）—— 读回来的就是 gen_mysql 写进去的。

    只读、纯 JDBC：**绝不经 Dojao 的 MetaStore**（那边 registerConnector 是无条件落库的，
    一个造数工具不该去动元数据）。
    """
    d = {"user": {}, "spu": {}, "sku": {}}

    with conn.cursor() as cur:
        cur.execute("select user_id, gender, age, city from users order by user_id")
        rows = cur.fetchall()
    d["user"] = {
        "count": len(rows),
        "gender": np.array([r[1] for r in rows], dtype=np.int8),
        "age": np.array([r[2] for r in rows], dtype=np.int8),
        "city": np.array([_city_index(r[3]) for r in rows], dtype=np.int16),
    }

    with conn.cursor() as cur:
        cur.execute("select sku_id, spu_id, price from sku order by sku_id")
        srows = cur.fetchall()
        cur.execute("select spu_id, category_id from spu order by spu_id")
        prow = cur.fetchall()

    spu_cat = {int(r[0]): int(r[1]) for r in prow}
    d["spu"] = {
        "count": len(prow),
        "cat_id": np.array([r[1] for r in prow], dtype=np.int32),
    }
    d["sku"] = {
        "count": len(srows),
        "spu_of": np.array([r[1] for r in srows], dtype=np.int64),
        "cat_id": np.array([spu_cat.get(int(r[1]), 0) for r in srows], dtype=np.int32),
        "price": np.array([float(r[2]) for r in srows], dtype=np.float64),
    }
    _install(d)
    return DIMS


def _install(d):
    """索引化成热路径要用的数组（1-based：下标 = id）。"""
    global DIMS, DIMS_SPU
    DIMS_SPU = d["spu"]
    sku = d["sku"]
    n_sku = sku["count"]
    cat_id = np.asarray(sku["cat_id"], dtype=np.int32)

    d["sku_price"] = np.concatenate(([0.0], np.asarray(sku["price"], dtype=np.float64)))
    d["sku_count"] = n_sku

    # 品类 → SKU 列表（订单里 80% 命中买家偏好品类）
    by_cat = [[] for _ in range(len(cfg.CATEGORIES) + 1)]
    for k in range(n_sku):
        cid = int(cat_id[k])
        if 1 <= cid <= len(cfg.CATEGORIES):
            by_cat[cid].append(k + 1)
    d["skus_by_cat"] = [np.array(v, dtype=np.int64) if v else np.array([1], dtype=np.int64)
                        for v in by_cat]

    # 爆款榜：按「由 sku_id 纯函数推出的销量分」降序取头部 10%。
    # ★ 必须用纯函数而不是表里的 sold_count 列 —— gen_hive 读回来的表里没有这一列，
    #   两边排序不一致的话，order_items() 推出来的 sku_id 就会跟 MySQL 里写的对不上。
    score = _sku_score_np(np.arange(1, n_sku + 1, dtype=np.uint64))
    d["hot_skus"] = (np.argsort(score, kind="stable")[::-1][:max(1, n_sku // 10)] + 1
                     ).astype(np.int64)

    DIMS = d
    DIMS_SPU = None


def dims():
    if DIMS is None:
        raise RuntimeError("维表还没登记：gen_mysql 先调 set_dims()，gen_hive 先调 load_dims()")
    return DIMS


def _city_index(city_name):
    for i, (name, _prov) in enumerate(cfg.CITIES):
        if name == city_name:
            return i
    return 0


# ============================================================================
# 四、订单模型（纯函数：订单序号 i → 这一单的全部字段）
# ============================================================================

_STATUS_LUT = cm.weighted_lut(sorted(cfg.ORDER_STATUS_WEIGHTS.items()))
_ITEMS_LUT = cm.weighted_lut([(j + 1, w) for j, w in enumerate(cfg.ITEMS_WEIGHTS)])
_QTY_LUT = cm.weighted_lut([(j + 1, w) for j, w in enumerate(cfg.QUANTITY_WEIGHTS)])
_PAY_LUT = cm.weighted_lut([(j + 1, w) for j, w in enumerate(cfg.PAY_METHOD_WEIGHTS)])
_DEV_LUT = cm.weighted_lut([(j + 1, w) for j, w in enumerate(cfg.DEVICE_WEIGHTS)])
_DISCOUNT_LUT = cm.weighted_lut(sorted(cfg.DISCOUNT_WEIGHTS.items()))

_SEC_OF_DAY = None


def _sec_of_day():
    global _SEC_OF_DAY
    if _SEC_OF_DAY is None:
        _SEC_OF_DAY = cm.build_sec_of_day(cm.HOUR_WEIGHTS)
    return _SEC_OF_DAY


def order_user(i):
    """订单 i 的买家 user_id —— 三段式重尾：40% 的订单来自头部 0.2% 的用户。

    不要用纯幂律：α 稍大就会让「第一名用户吃掉 38%」，是病态分布，join 出来一眼假。
    """
    u = cm.u01(i * 31 + 5)
    r = cm.mix(i * 31 + 5)
    n = cfg.USER_COUNT
    head = max(1, n // 500)          # 0.2%
    mid = max(1, n * 18 // 1000)     # 1.8%
    if u < 0.40:
        return 1 + r % head
    if u < 0.70:
        return 1 + head + r % mid
    return 1 + head + mid + (r % max(1, n - head - mid))


def order_status(i):
    """1 待付款 / 2 已付款 / 3 已发货 / 4 已完成 / 5 已取消 / 6 已退款。"""
    return _STATUS_LUT[cm.pick(i * 7 + 1, len(_STATUS_LUT))]


def order_is_paid(i):
    """只有已支付（含发货/完成）的订单才产出 purchase 事件。"""
    return order_status(i) in cfg.PAID_STATUSES


def order_num_items(i):
    return _ITEMS_LUT[cm.pick(i * 11 + 2, len(_ITEMS_LUT))]


def order_sec(i):
    """下单时间在当天第几秒（按 24 小时作息加权，非均匀）。"""
    return _sec_of_day()[cm.pick(i * 13 + 3, 86400)]


def order_pay_delay(i):
    """下单 → 支付的延迟秒数（3 分钟 ~ 2 小时）。"""
    return 180 + cm.pick(i * 17 + 4, 7200)


def order_pay_method(i):
    return _PAY_LUT[cm.pick(i * 19 + 5, len(_PAY_LUT))]


def order_device(i):
    return _DEV_LUT[cm.pick(i * 23 + 6, len(_DEV_LUT))]


def order_discount(i):
    return _DISCOUNT_LUT[cm.pick(i * 29 + 7, len(_DISCOUNT_LUT))]


def order_items(i):
    """订单 i 的明细：[{sku_id, quantity, unit_price, amount}, ...]。

    商品选择：80% 落在「买家偏好品类」（user_id % 品类数），12% 取全站爆款，8% 全站均匀。
    没有这一条，按品类聚合就是一团噪声。
    """
    d = dims()
    user = order_user(i)
    pref = user % len(cfg.CATEGORIES) + 1          # 1-based category id
    out = []
    for j in range(order_num_items(i)):
        x = i * 131 + j * 17 + 3
        r = cm.u01(x)
        if r < 0.80:
            pool = d["skus_by_cat"][pref]
            sku = int(pool[cm.pick(x + 1, len(pool))])
        elif r < 0.92:
            hot = d["hot_skus"]
            sku = int(hot[cm.pick(x + 1, len(hot))])
        else:
            sku = 1 + cm.pick(x + 1, d["sku_count"])
        qty = _QTY_LUT[cm.pick(x + 2, len(_QTY_LUT))]
        unit = float(d["sku_price"][sku])
        out.append({
            "sku_id": sku,
            "quantity": qty,
            "unit_price": round(unit, 2),
            "amount": round(unit * qty, 2),
        })
    return out
