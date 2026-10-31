# -*- coding: utf-8 -*-
"""② 用户行为日志 → Hive（dojao_test.ods_user_behavior）。

一张宽表、按 dt 分区、ORC + SNAPPY，一个月 30 个分区，每天 200W-400W 行。
事件比例 app启动 : 浏览 : 购物车 : 购买 = 5 : 3 : 1.5 : 0.5。

★ 购买事件不是「编」出来的
    它由 gen_mysql 写进 MySQL 的**已支付订单明细**推出来（同一个 order_* 纯函数），
    所以 Hive 里的 order_id / sku_id / amount 与 MySQL 的 orders / order_item
    **严格对得上**，可以做「Hive 聚合 vs JOIN MySQL 明细」的跨源对账。
    其余 95% 的事件才走哈希流（与 MySQL 无关，只是好看的行为日志）。

★ 只写 HDFS，不连 Hive
    首选用 pyarrow 的 HadoopFileSystem **直接写 hdfs://**；它起不来（没 libhdfs /
    CLASSPATH）就退回「先写本地 LOCAL_TMP_DIR，再 hdfs dfs -put」。
    两条路都不需要 hive 客户端 —— 建表 / MSCK REPAIR 由 Hive 侧的人执行，本程序
    在结尾把 DDL 原样打出来。

用法（在集群节点上跑）：

    python3 gen_hive.py                    # 逐天生成 → 直写 HDFS，结尾打印 Hive DDL
    python3 gen_hive.py --days=2           # 只造头 2 天（冒烟）
    python3 gen_hive.py --dry-run          # 只打印规划，不读库不写文件
    python3 gen_hive.py --clean            # 先删掉 HDFS 上的表目录，从零开始
    python3 gen_hive.py --no-upload        # 只在本地产出 ORC，不碰 HDFS
"""

import os
import shutil
import subprocess
import sys
import time
from datetime import timedelta

import numpy as np
import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.orc as pa_orc

import common as cm
import config as cfg
import model

# ============================================================================
# 列定义：顺序 = Arrow 表字段顺序 = Hive 表列顺序（三处必须一致）
# ============================================================================

COLUMNS = (
    ("event_id",     "bigint", "事件 id（全局唯一递增）"),
    ("event_type",   "string", "app_launch / page_view / add_cart / purchase"),
    ("event_time",   "string", "yyyy-MM-dd HH:mm:ss（分区内近似有序，利于压缩）"),
    ("user_id",      "bigint", "→ MySQL users.user_id"),
    ("session_id",   "bigint", "会话 id：(天, 用户, 半小时桶) 编码"),
    ("device_type",  "string", "android / ios / web / pc"),
    ("os",           "string", "操作系统及版本"),
    ("app_version",  "string", "客户端版本"),
    ("channel",      "string", "安装渠道"),
    ("network",      "string", "wifi / 4g / 5g"),
    ("province",     "string", "→ MySQL users.province"),
    ("city",         "string", "→ MySQL users.city"),
    ("page_type",    "string", "launch / item / home / search / cart / order / mine"),
    ("referrer",     "string", "来源页，app 启动为 NULL"),
    ("spu_id",       "bigint", "→ MySQL spu.spu_id（商品相关事件才有）"),
    ("sku_id",       "bigint", "→ MySQL sku.sku_id（商品相关事件才有）"),
    ("category_id",  "int",    "→ MySQL category.category_id"),
    ("keyword",      "string", "搜索词，仅 page_type=search 有值"),
    ("stay_seconds", "int",    "停留秒数，仅 page_view 有值"),
    ("quantity",     "bigint", "件数，加购/购买才有值"),
    ("amount",       "double", "金额，加购/购买才有值"),
    ("order_id",     "bigint", "→ MySQL orders.order_id，仅 purchase 有值"),
    ("launch_type",  "string", "cold / hot，仅 app_launch 有值"),
)

# ============================================================================
# 取值字典（Hive 侧维度用 config 里那一份，保证与 MySQL 的省份/城市对得上）
# ============================================================================

DEVICES = tuple(v for v, _ in cfg.HIVE_DEVICE_WEIGHTS)
DEVICE_W = tuple(w for _, w in cfg.HIVE_DEVICE_WEIGHTS)
OSES = tuple(v for v, _ in cfg.OS_WEIGHTS)
OS_W = tuple(w for _, w in cfg.OS_WEIGHTS)
NETWORKS = tuple(v for v, _ in cfg.NETWORK_WEIGHTS)
NETWORK_W = tuple(w for _, w in cfg.NETWORK_WEIGHTS)
LAUNCH_TYPES = tuple(v for v, _ in cfg.LAUNCH_TYPE_WEIGHTS)
LAUNCH_W = tuple(w for _, w in cfg.LAUNCH_TYPE_WEIGHTS)
CHANNELS = tuple(v for v, _ in cfg.CHANNEL_WEIGHTS)
CHANNEL_W = tuple(w for _, w in cfg.CHANNEL_WEIGHTS)
VERSIONS = tuple(v for v, _ in cfg.APP_VERSION_WEIGHTS)
VERSION_W = tuple(w for _, w in cfg.APP_VERSION_WEIGHTS)
REFERRERS = tuple(v for v, _ in cfg.REFERRER_WEIGHTS)
REFERRER_W = tuple(w for _, w in cfg.REFERRER_WEIGHTS)
STAYS = np.array([v for v, _ in cfg.STAY_SECONDS_WEIGHTS], dtype=np.int32)
STAY_W = tuple(w for _, w in cfg.STAY_SECONDS_WEIGHTS)
CATEGORY_NAMES = tuple(c[1] for c in cfg.CATEGORIES)
CATEGORY_W = tuple(cfg.CATEGORY_WEIGHTS)

# 页面类型：0=launch（启动无页面） 1..6 = cfg.PAGE_TYPE_WEIGHTS 的顺序
PAGE_TYPES = ("launch",) + tuple(v for v, _ in cfg.PAGE_TYPE_WEIGHTS)
PAGE_W = tuple(w for _, w in cfg.PAGE_TYPE_WEIGHTS)
P_LAUNCH, P_ITEM, P_HOME, P_SEARCH, P_CART, P_ORDER, P_MINE = range(7)

_HASH_SEEDS = ((0x9E3779B1, 11), (0x85EBCA77, 23), (0xC2B2AE3D, 37),
               (0x27D4EB2F, 53), (0x165667B1, 71), (0x2545F491, 97))

# 城市 / 省份（与 MySQL users 表同一份 cfg.CITIES，两边 join 得上）
CITY_NAMES = tuple(c[0] for c in cfg.CITIES)
PROVINCES = tuple(dict.fromkeys(c[1] for c in cfg.CITIES))
_PROV_OF = {p: i for i, p in enumerate(PROVINCES)}
PROV_IDX = np.array([_PROV_OF[c[1]] for c in cfg.CITIES], dtype=np.int64)

_SEC_LUT = None
_TIME_LUT = {}

# HDFS 通道（运行时探测填入；None = 不可用）
_HDFS_FS = None     # pyarrow HadoopFileSystem —— 直写 hdfs://
_HDFS = None        # hdfs 命令行前缀 —— 退回「本地写 + put」


# ============================================================================
# 主流程
# ============================================================================

def main(argv):
    opts = _parse_args(argv)
    if opts is None:
        return 2

    p = cm.plan()
    days = min(p.days, opts["days"])
    total = sum(p.day_rows[:days])

    print("=" * 78)
    print("dojao 场景 · 用户行为日志 → Hive %s.%s" % (cfg.HIVE_DB, cfg.HIVE_TABLE))
    print("=" * 78)
    print("  分区        : %d 天 · %s ~ %s（每天 %s ~ %s 行）"
          % (days, p.first_day, p.first_day + timedelta(days=days - 1),
             cm.fmt_int(min(p.day_rows[:days])), cm.fmt_int(max(p.day_rows[:days]))))
    print("  合计行数    : %s" % cm.fmt_int(total))
    print("  事件比例    : 启动 %.1f%% · 浏览 %.1f%% · 加购 %.1f%% · 购买 %.1f%%"
          % (50.0, 30.0, 15.0, 5.0))
    print("  购买事件    : 由 MySQL 已支付订单明细推出（order_id / sku_id / amount 严格对得上）")
    print("  存储        : ORC + SNAPPY，每天 %d 个文件 → %s"
          % (cfg.HIVE_FILES_PER_DAY, cfg.HDFS_TABLE_DIR))
    print("  本地中转    : %s（仅在直连 HDFS 不可用时用）" % cfg.LOCAL_TMP_DIR)
    if opts["dry_run"]:
        print("\n--dry-run：只做规划，不读库、不写文件、不碰 HDFS。")
        print("\n" + _ddl())
        return 0

    # ---------- 先探 HDFS 通道：不通就立刻说清楚，别等跑一半才炸 ----------
    direct = False                                   # True = pyarrow 直写 hdfs://
    if opts["no_upload"]:
        print("\n--no-upload：只在本地产出 ORC，不碰 HDFS。")
    else:
        direct = _resolve_hdfs_fs() is not None
        if not direct:
            if not _resolve_hdfs():
                print("\n!! 连不上 HDFS：pyarrow 的 HadoopFileSystem 起不来，也没找到 hdfs 命令行。")
                print("   确认 HADOOP_HOME / libhdfs / CLASSPATH（见 config.py），"
                      "或在 HDFS_CLI_CANDIDATES 里写全 hdfs 的路径。")
                print("   只想先在本地产出 ORC 就加 --no-upload。")
                return 3
            print("\nhdfs  : %s（回退：先写本地，再 put）" % " ".join(_HDFS))

    # ---------- 读回维表（纯 JDBC 只读，绝不碰 Dojao 的 MetaStore）----------
    t0 = time.time()
    conn = cm.mysql_connect(cfg.MYSQL_DB)
    try:
        model.load_dims(conn)
    finally:
        conn.close()
    dims = model.dims()
    pools = _cat_pools(dims)
    print("\n维表已从 MySQL 读回（users %s / spu %s / sku %s），耗时 %.1f 秒。"
          % (cm.fmt_int(dims["user"]["count"]), cm.fmt_int(dims["spu"]["count"]),
             cm.fmt_int(dims["sku_count"]), time.time() - t0))

    if opts["clean"] and not opts["no_upload"]:
        print("--clean：先删掉 HDFS 上的 %s" % cfg.HDFS_TABLE_DIR)
        _remote_rm(cfg.HDFS_TABLE_DIR)

    if os.path.isdir(cfg.LOCAL_TMP_DIR):
        shutil.rmtree(cfg.LOCAL_TMP_DIR)
    os.makedirs(cfg.LOCAL_TMP_DIR)

    n_ev = [0, 0, 0, 0]      # 启动 / 浏览 / 加购 / 购买 实测计数
    base_id = 0
    t0 = time.time()
    for d in range(days):
        date = p.first_day + timedelta(days=d)
        tbl, cnt = build_day(d, p, dims, pools, base_id)
        base_id += p.day_rows[d]
        n_ev = [a + b for a, b in zip(n_ev, cnt)]

        out_dir = os.path.join(cfg.LOCAL_TMP_DIR, "dt=%s" % date)
        os.makedirs(out_dir, exist_ok=True)
        dest = "%s/dt=%s" % (cfg.HDFS_TABLE_DIR, date)

        if direct:
            files = _write_orc_hdfs(tbl, dest)          # 直写 hdfs://，不落本地
        else:
            files = _write_orc(tbl, out_dir)            # 本地产出
        del tbl

        if not opts["no_upload"] and not direct:
            _hdfs("-mkdir", "-p", dest)
            for f in files:
                _hdfs("-put", "-f", f, dest)

        print("    dt=%s  %s 行（启动 %s · 浏览 %s · 加购 %s · 购买 %s）  %d 文件  %.1f 秒"
              % (date, cm.fmt_int(p.day_rows[d]), cm.fmt_int(cnt[0]), cm.fmt_int(cnt[1]),
                 cm.fmt_int(cnt[2]), cm.fmt_int(cnt[3]), len(files), time.time() - t0))
        if not opts["no_upload"] and not direct:
            shutil.rmtree(out_dir)                      # 传完就删本地中转

    tot = float(sum(n_ev)) or 1.0
    print("\n完成，耗时 %.1f 秒。实测事件比例：启动 %.2f%% · 浏览 %.2f%% · 加购 %.2f%% · 购买 %.2f%%"
          % (time.time() - t0, 100 * n_ev[0] / tot, 100 * n_ev[1] / tot,
             100 * n_ev[2] / tot, 100 * n_ev[3] / tot))
    if not opts["no_upload"]:
        _print_handoff()
    _print_check()
    return 0


def _parse_args(argv):
    o = {"days": 10 ** 9, "dry_run": False, "clean": False, "no_upload": False}
    for a in argv:
        if a == "--dry-run":
            o["dry_run"] = True
        elif a == "--clean":
            o["clean"] = True
        elif a == "--no-upload":
            o["no_upload"] = True
        elif a.startswith("--days="):
            try:
                o["days"] = int(a.split("=", 1)[1])
            except ValueError:
                print("--days 需要一个整数")
                return None
        else:
            print("未知参数: %s" % a)
            print("可用：--days=N --dry-run --clean --no-upload")
            return None
    return o


# ============================================================================
# 一天的数据（核心）
# ============================================================================

def build_day(d, p, dims, pools, base_id):
    """造第 d 个分区的整张 Arrow 表，返回 (table, (启动, 浏览, 加购, 购买) 计数)。"""
    date = p.first_day + timedelta(days=d)
    n = p.day_rows[d]

    # ---------- ① 购买事件：从 MySQL 已支付订单明细推出来 ----------
    lo, hi = p.order_range(d)
    sku_spu = dims["sku"]["spu_of"]
    sku_cat = dims["sku"]["cat_id"]
    secs, uids, oids, skus, qty, amt = [], [], [], [], [], []
    for i in range(lo, hi):
        if not model.order_is_paid(i):
            continue
        s = model.order_sec(i)
        u = model.order_user(i)
        for it in model.order_items(i):
            k = it["sku_id"]
            secs.append(s)
            uids.append(u)
            oids.append(i + 1)
            skus.append(k)
            qty.append(it["quantity"])
            amt.append(it["amount"])
    n_p = len(secs)
    if n_p >= n:
        raise RuntimeError("第 %d 天购买事件 %d 条已超过当天行数 %d，检查 HIVE_* 规模配置"
                           % (d, n_p, n))
    m = n - n_p                                   # 其余事件的条数

    # 三段：启动 5 / 浏览 3 / 加购 1.5（占总量的 95%）
    n_l = int(round(m * 5.0 / 9.5))
    n_v = int(round(m * 3.0 / 9.5))
    n_c = m - n_l - n_v

    # ---------- ② 哈希流（一次算齐，后面所有列都从它派生）----------
    h = _hashes(n, d)
    h1, h2, h3, h4, h5, h6 = h

    # ---------- ③ 用户：三段重尾（40% 的订单来自头部 0.2% 的用户）----------
    nu = cfg.USER_COUNT
    head, mid = max(1, nu // 500), max(1, nu * 18 // 1000)
    tail = max(1, nu - head - mid)
    u01 = cm.u01_np(h1)
    user = np.where(u01 < 0.40, 1 + (h2 % np.uint64(head)),
                    np.where(u01 < 0.70, 1 + head + (h2 % np.uint64(mid)),
                             1 + head + mid + (h2 % np.uint64(tail)))).astype(np.int64)
    if n_p:
        user[m:] = np.fromiter(uids, dtype=np.int64, count=n_p)

    # ---------- ④ 事件时间：按作息加权的秒数；购买用真实下单秒 ----------
    lut = np.asarray(_sec_lut(), dtype=np.int32)
    sec = lut[(h3 % np.uint64(lut.size)).astype(np.int64)]
    if n_p:
        sec[m:] = np.fromiter(secs, dtype=np.int32, count=n_p)

    # ---------- ⑤ 事件类型 / 页面类型 ----------
    is_p = np.zeros(n, dtype=bool)
    is_l = np.zeros(n, dtype=bool)
    is_v = np.zeros(n, dtype=bool)
    is_c = np.zeros(n, dtype=bool)
    is_l[:n_l] = True
    is_v[n_l:n_l + n_v] = True
    is_c[n_l + n_v:m] = True
    is_p[m:] = True

    ptype = np.zeros(n, dtype=np.int64)
    ptype[:n_l] = P_LAUNCH
    ptype[n_l:n_l + n_v] = 1 + cm.weighted_pick_np(h4[n_l:n_l + n_v], PAGE_W)
    ptype[is_c] = P_CART
    ptype[is_p] = P_ORDER

    # ---------- ⑥ 商品：80% 买家偏好品类 / 12% 全站爆款 / 8% 全站均匀 ----------
    sku = _pick_skus(user, h5, h6, dims, pools)
    if n_p:
        sku[m:] = np.fromiter(skus, dtype=np.int64, count=n_p)
    spu = sku_spu[sku - 1]
    cid = sku_cat[sku - 1]

    qty_all = cm.weighted_pick_np(h4, cfg.CART_QTY_WEIGHTS) + 1
    if n_p:
        qty_all[m:] = np.fromiter(qty, dtype=np.int64, count=n_p)
    amount = np.round(dims["sku_price"][sku] * qty_all, 2)
    if n_p:
        amount[m:] = np.fromiter(amt, dtype=np.float64, count=n_p)

    order_id = np.zeros(n, dtype=np.int64)
    if n_p:
        order_id[m:] = np.fromiter(oids, dtype=np.int64, count=n_p)

    # ---------- ⑦ 其余噪声列 ----------
    city_idx = dims["user"]["city"][user - 1].astype(np.int64)
    city = _lut_col(CITY_NAMES, city_idx)
    province = _lut_col(PROVINCES, PROV_IDX[city_idx])
    device = _lut_col(DEVICES, cm.weighted_pick_np(h1, DEVICE_W))
    os_name = _lut_col(OSES, cm.weighted_pick_np(h2, OS_W))
    version = _lut_col(VERSIONS, cm.weighted_pick_np(h3, VERSION_W))
    channel = _lut_col(CHANNELS, cm.weighted_pick_np(h4, CHANNEL_W))
    network = _lut_col(NETWORKS, cm.weighted_pick_np(h5, NETWORK_W))

    referrer = _lut_col(REFERRERS, cm.weighted_pick_np(h6, REFERRER_W), mask=is_l)
    launch_type = _lut_col(LAUNCH_TYPES, cm.weighted_pick_np(h6, LAUNCH_W), mask=~is_l)
    keyword = _lut_col(cfg.KEYWORDS, (h5 % np.uint64(len(cfg.KEYWORDS))).astype(np.int32),
                       mask=(ptype != P_SEARCH))
    stay = _lut_col_arr(STAYS, cm.weighted_pick_np(h2, STAY_W), mask=~is_v)

    # ---------- ⑧ 时间排序（近似有序 → SNAPPY 压得动）----------
    order = np.argsort(sec, kind="stable")
    sec = sec[order]
    evt = _str_col([cfg.EV_LAUNCH, cfg.EV_VIEW, cfg.EV_CART, cfg.EV_PURCHASE],
                   np.where(is_l, 0, np.where(is_v, 1, np.where(is_c, 2, 3)))[order])
    event_id = np.arange(base_id, base_id + n, dtype=np.int64)
    session = ((d * np.uint64(nu) + user[order].astype(np.uint64)) * np.uint64(48)
               + (sec // np.int32(1800)).astype(np.uint64)).astype(np.int64)

    prod_mask = (is_p | is_c | (is_v & (ptype == P_ITEM)))[order]
    qty_mask = (is_p | is_c)[order]

    table = pa.table({
        "event_id": pa.array(event_id),
        "event_type": evt,
        "event_time": _time_col(date, sec),
        "user_id": pa.array(user[order]),
        "session_id": pa.array(session),
        "device_type": _take(device, order),
        "os": _take(os_name, order),
        "app_version": _take(version, order),
        "channel": _take(channel, order),
        "network": _take(network, order),
        "province": _take(province, order),
        "city": _take(city, order),
        "page_type": _take(_lut_col(PAGE_TYPES, ptype), order),
        "referrer": _take(referrer, order),
        "spu_id": pa.array(spu[order], mask=~prod_mask),
        "sku_id": pa.array(sku[order], mask=~prod_mask),
        "category_id": pa.array(cid[order], mask=~prod_mask),
        "keyword": _take(keyword, order),
        "stay_seconds": _take(stay, order),
        "quantity": pa.array(qty_all[order], mask=~qty_mask),
        "amount": pa.array(amount[order], mask=~qty_mask),
        "order_id": pa.array(order_id[order], mask=~is_p[order]),
        "launch_type": _take(launch_type, order),
    })
    cnt = (int(is_l.sum()), int(is_v.sum()), int(is_c.sum()), n_p)
    _check_schema(table)
    return table, cnt


# ============================================================================
# 小工具
# ============================================================================

# Hive 列类型 → Arrow 类型。ORC 文件里存的就是 Arrow 这边推出的物理类型，
# 而 Dojao 的 Hive 连接器会拿**建表语句里的类型**跟文件类型严格比对，对不上就报
# `Hive 列类型与数据文件不一致` 直接失败 —— 所以两边必须一字不差。
_HIVE_TO_ARROW = {
    "bigint": pa.int64(),
    "int": pa.int32(),
    "double": pa.float64(),
    "string": pa.string(),
}


def _check_schema(table):
    """写照之前先自检：Arrow 实际类型 vs `COLUMNS` 里声明的 Hive 类型。

    踩过的坑：`quantity` 声明成 `int`（32 位），但 numpy 那边算出的是 int64，
    ORC 文件里就成了 `Int(64)` → 查询时报 `Hive 列类型与数据文件不一致`。
    在这里拦住，比等到查询时再炸早得多。
    """
    for name, hive_t, _c in COLUMNS:
        got = table.schema.field(name).type
        want = _HIVE_TO_ARROW[hive_t]
        if got != want and not (hive_t == "string" and pa.types.is_string(got)):
            raise RuntimeError(
                "列 %s 的实际类型与建表声明不一致：DDL 声明 %s（%s），实际写出 %s。"
                "请改 COLUMNS 里的声明，或在 build_day 里显式转换该列。"
                % (name, hive_t, want, got))

def _hashes(n, day):
    """n 行 × 6 条独立哈希流（splitmix64 的 numpy 版）。"""
    idx = np.arange(n, dtype=np.uint64)
    return [cm.mix_np(idx * np.uint64(a) + np.uint64(day * 1000003 + b))
            for a, b in _HASH_SEEDS]


def _sec_lut():
    """当天第几秒的查表（按 24 小时作息加权，严格非递减）。"""
    global _SEC_LUT
    if _SEC_LUT is None:
        _SEC_LUT = cm.build_sec_of_day(cm.HOUR_WEIGHTS)
    return _SEC_LUT


def _time_col(date, sec):
    """秒数 → 'yyyy-MM-dd HH:mm:ss'（每天一张 86400 项的字典，take 一次拿到全列）。"""
    key = date.toordinal()
    base = _TIME_LUT.get(key)
    if base is None:
        pre = date.isoformat()
        base = pa.array(["%s %s" % (pre, cm.hms(s)) for s in range(86400)])
        _TIME_LUT.clear()
        _TIME_LUT[key] = base
    return base.take(pa.array(sec.astype(np.int32)))


def _cat_pools(d):
    """12 个品类的 SKU 列表摊平成一个数组 + 每类的起始偏移（向量化挑品类内 SKU 用）。"""
    pools = [np.asarray(d["skus_by_cat"][c + 1], dtype=np.int64) for c in range(len(cfg.CATEGORIES))]
    off = np.zeros(len(pools), dtype=np.int64)
    acc = 0
    for i, v in enumerate(pools):
        off[i] = acc
        acc += v.size
    return {"flat": np.concatenate(pools), "off": off,
            "len": np.array([v.size for v in pools], dtype=np.int64)}


def _pick_skus(user, h5, h6, dims, pools):
    """80% 买家偏好品类（user_id % 12）· 12% 全站爆款 · 8% 全站均匀。"""
    r = cm.u01_np(h6)
    r2 = (h5 % np.uint64(1 << 31)).astype(np.int64)
    pref = (user % len(cfg.CATEGORIES)).astype(np.int64)            # 0-based
    pos = pools["off"][pref] + (r2 % pools["len"][pref])
    in_cat = pools["flat"][pos]
    hot = dims["hot_skus"]
    hit_hot = np.asarray(hot, dtype=np.int64)[r2 % np.asarray(hot).size]
    uniform = 1 + (r2 % dims["sku_count"])
    return np.where(r < 0.80, in_cat, np.where(r < 0.92, hit_hot, uniform)).astype(np.int64)


def _lut_col(values, idx, mask=None):
    """小字典 → 字符串列：先把字典编成 Arrow 数组，再 take（C++ 的，比 Python 逐行快几个量级）。"""
    i = pa.array(np.asarray(idx, dtype=np.int32),
                 mask=None if mask is None else np.asarray(mask))
    return pa.array(list(values)).take(i)


def _lut_col_arr(values, idx, mask=None):
    i = pa.array(np.asarray(idx, dtype=np.int32),
                 mask=None if mask is None else np.asarray(mask))
    return pa.array(np.asarray(values)).take(i)


def _str_col(values, idx):
    return pa.array(list(values)).take(pa.array(np.asarray(idx, dtype=np.int32)))


def _take(col, order):
    return col.take(pa.array(np.asarray(order, dtype=np.int32)))


def _write_orc(table, out_dir):
    """按 HIVE_FILES_PER_DAY 切片写 ORC（每片一个文件 = Hive 并行扫描的天然切片）。"""
    n = table.num_rows
    k = max(1, cfg.HIVE_FILES_PER_DAY)
    step = (n + k - 1) // k
    files = []
    for f in range(k):
        lo = f * step
        if lo >= n:
            break
        path = os.path.join(out_dir, "%06d_0" % f)
        pa_orc.write_table(table.slice(lo, min(step, n - lo)), path, compression="SNAPPY")
        files.append(path)
    return files


def _write_orc_hdfs(table, remote_dir):
    """把 ORC 切片**直接**写到 hdfs://（pyarrow HadoopFileSystem，不落本地中转）。"""
    n = table.num_rows
    k = max(1, cfg.HIVE_FILES_PER_DAY)
    step = (n + k - 1) // k
    fs = _HDFS_FS
    try:
        fs.create_dir(remote_dir, recursive=True)
    except Exception:
        pass                                    # 已存在
    files = []
    for f in range(k):
        lo = f * step
        if lo >= n:
            break
        path = "%s/%06d_0" % (remote_dir.rstrip("/"), f)
        with fs.open_output_stream(path) as fh:
            pa_orc.write_table(table.slice(lo, min(step, n - lo)), fh, compression="SNAPPY")
        files.append(path)
    return files


# ============================================================================
# HDFS 通道（不依赖 hive 客户端）
# ============================================================================

def _which(candidates):
    """在候选里找一个**真的存在且可执行**的客户端（先当命令名在 PATH 里找，再当绝对路径）。"""
    for c in candidates:
        if not c:
            continue
        if os.sep in c or (os.altsep and os.altsep in c):
            if os.path.isfile(c) and os.access(c, os.X_OK):
                return c
        else:
            p = shutil.which(c)
            if p:
                return p
    return None


def _resolve_hdfs_fs():
    """用 pyarrow 的 HadoopFileSystem 直连 HDFS —— 不落本地、不调 hdfs 命令。

    起不来（没有 libhdfs / CLASSPATH）就打印原因并返回 None，由调用方退回命令行 put。
    """
    global _HDFS_FS
    if _HDFS_FS is not None:
        return _HDFS_FS
    if os.path.isdir(cfg.HADOOP_HOME):
        os.environ.setdefault("HADOOP_HOME", cfg.HADOOP_HOME)
    if cfg.HDFS_USER:
        os.environ.setdefault("HADOOP_USER_NAME", cfg.HDFS_USER)
    try:
        fs = pa_fs.HadoopFileSystem(cfg.HDFS_NAMENODE_HOST, cfg.HDFS_NAMENODE_PORT)
        fs.get_file_info("/")                   # 真正连一次，别等写的时候才炸
        _HDFS_FS = fs
    except Exception as e:
        print("\n(pyarrow 直连 HDFS 不可用：%s)" % e)
        print("(确认 HADOOP_HOME / libhdfs.so / CLASSPATH；本次退回 hdfs 命令行的 put 方式)")
        _HDFS_FS = None
    return _HDFS_FS


def _resolve_hdfs():
    """探测 hdfs 命令行。返回是否找到（结果填进模块级 _HDFS）。"""
    global _HDFS
    if _HDFS is None:
        p = _which(cfg.HDFS_CLI_CANDIDATES)
        if p:
            _HDFS = [p, "dfs"]
    return _HDFS is not None


def _run(*cmd):
    try:
        p = subprocess.run(list(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except OSError as e:
        return 127, "无法执行 %s：%s" % (cmd[0], e)
    return p.returncode, p.stdout.decode("utf-8", "replace")


def _must(*cmd):
    rc, out = _run(*cmd)
    if rc != 0:
        raise RuntimeError("命令失败（exit=%d）：%s\n%s" % (rc, " ".join(cmd), out.strip()))
    return out


def _hdfs(*args):
    return _must(*(_HDFS + list(args)))


def _remote_rm(path):
    """删掉 HDFS 上的一整个目录（--clean 用）；不存在就算了。"""
    if _HDFS_FS is not None:
        try:
            _HDFS_FS.delete_dir(path)
        except Exception as e:
            print("   （%s 删不掉或本来就不在：%s）" % (path, e))
    elif _HDFS is not None:
        _hdfs("-rm", "-r", "-f", path)


def _print_handoff():
    """把 Hive 侧要做的事原样打出来 —— 本程序到此为止，不碰 hive 客户端。"""
    print("\n" + "=" * 78)
    print("HDFS 数据已就绪。下面两条请在 Hive 侧执行（本程序不连 Hive）：")
    print("=" * 78)
    print("\n" + _ddl() + ";\n")
    print("MSCK REPAIR TABLE %s.%s;" % (cfg.HIVE_DB, cfg.HIVE_TABLE))


def _ddl():
    cols = ",\n".join("  %-14s %-7s COMMENT '%s'" % (n, t, c) for n, t, c in COLUMNS)
    return ("CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (\n%s\n)\n"
            "PARTITIONED BY (dt string COMMENT '日期分区 yyyy-MM-dd')\n"
            "STORED AS ORC\n"
            "LOCATION '%s'\n"
            "TBLPROPERTIES ('orc.compress'='SNAPPY')"
            % (cfg.HIVE_DB, cfg.HIVE_TABLE, cols, cfg.HDFS_TABLE_DIR))


def _print_check():
    print("\n对账样例（Hive 聚合 vs JOIN MySQL 明细，两边必须一致）：")
    print("  -- Hive 侧：某天金额最大的 5 单")
    print("  select order_id, count(*) items, round(sum(amount),2) amt")
    print("    from %s.%s where dt='%s' and event_type='purchase'"
          % (cfg.HIVE_DB, cfg.HIVE_TABLE, cfg.HIVE_LAST_DAY))
    print("   group by order_id order by amt desc limit 5;")
    print("  -- MySQL 侧：拿上面那 5 个 order_id 查")
    print("  select order_id, count(*) items, round(sum(amount),2) amt")
    print("    from order_item where order_id in (...) group by order_id;")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
