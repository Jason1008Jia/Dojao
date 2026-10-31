# -*- coding: utf-8 -*-
"""① 业务数据 → MySQL（aliyun002 的 dojao 库）。

六张表：category / users / spu / sku / orders / order_item。
  * 用户 100W-120W，SKU 约 1W（SPU 2000+），订单数百万、明细千万级；
  * 用户 ↔ 商品是**多对多**：orders（谁下的单）+ order_item（单里买了哪些 SKU）；
  * 所有值由行号/订单号纯函数推出（见 model.py），重跑可复现。

用法：

    python3 gen_mysql.py                 # 建表 + 灌数；表已存在且有数据会拒绝
    python3 gen_mysql.py --reset         # 先 DROP 这六张表再重建（会清空数据！）
    python3 gen_mysql.py --dry-run       # 只打印规划，不连任何服务
"""

import sys
import time
from datetime import timedelta

import common as cm
import config as cfg
import model

# ============================================================================
# 建表 DDL
# ============================================================================

DDL = [
    """
    CREATE TABLE IF NOT EXISTS category (
      category_id   int          NOT NULL COMMENT '一级分类 id',
      category_name varchar(32)  NOT NULL COMMENT '一级分类名',
      sort_order    int          NOT NULL DEFAULT 0,
      PRIMARY KEY (category_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='一级分类'
    """,
    """
    CREATE TABLE IF NOT EXISTS users (
      user_id       bigint       NOT NULL COMMENT '用户 id（连续 1..N）',
      username      varchar(64)  NOT NULL,
      gender        tinyint      NOT NULL COMMENT '1=男 2=女',
      age           tinyint      NOT NULL,
      birthday      date         DEFAULT NULL,
      city          varchar(32)  NOT NULL,
      province      varchar(32)  NOT NULL,
      phone         varchar(20)  NOT NULL,
      email         varchar(64)  NOT NULL,
      member_level  tinyint      NOT NULL COMMENT '1..5',
      register_time datetime     NOT NULL,
      status        tinyint      NOT NULL COMMENT '1=正常 0=禁用',
      PRIMARY KEY (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户'
    """,
    """
    CREATE TABLE IF NOT EXISTS spu (
      spu_id      bigint        NOT NULL COMMENT '一级商品 id',
      spu_name    varchar(128)  NOT NULL,
      title       varchar(200)  NOT NULL COMMENT '营销标题',
      category_id int           NOT NULL COMMENT '→ category.category_id',
      brand       varchar(64)   NOT NULL,
      price_min   decimal(10,2) NOT NULL,
      price_max   decimal(10,2) NOT NULL,
      create_time datetime      NOT NULL,
      status      tinyint       NOT NULL DEFAULT 1,
      PRIMARY KEY (spu_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SPU（一级商品）'
    """,
    """
    CREATE TABLE IF NOT EXISTS sku (
      sku_id     bigint        NOT NULL COMMENT 'SKU id（连续 1..N）',
      spu_id     bigint        NOT NULL COMMENT '→ spu.spu_id',
      sku_name   varchar(128)  NOT NULL,
      spec       varchar(128)  NOT NULL COMMENT '规格，如 颜色:黑色 内存:256G',
      price      decimal(10,2) NOT NULL COMMENT '售价',
      cost_price decimal(10,2) NOT NULL,
      stock      int           NOT NULL,
      sold_count int           NOT NULL,
      status     tinyint       NOT NULL DEFAULT 1,
      PRIMARY KEY (sku_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SKU（最小可售单元）'
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
      order_id        bigint        NOT NULL COMMENT '订单 id',
      order_no        varchar(32)   NOT NULL COMMENT '业务订单号',
      user_id         bigint        NOT NULL COMMENT '→ users.user_id',
      order_time      datetime      NOT NULL,
      pay_time        datetime      DEFAULT NULL COMMENT '未支付为 NULL',
      total_amount    decimal(12,2) NOT NULL COMMENT '商品总额',
      discount_amount decimal(12,2) NOT NULL,
      pay_amount      decimal(12,2) NOT NULL COMMENT '实付 = 总额 - 优惠',
      order_status    tinyint       NOT NULL COMMENT '1待付款 2已付款 3已发货 4已完成 5已取消 6已退款',
      pay_method      tinyint       NOT NULL COMMENT '1支付宝 2微信 3银行卡 4余额',
      device_type     tinyint       NOT NULL COMMENT '1android 2ios 3web 4pc',
      province        varchar(32)   NOT NULL,
      city            varchar(32)   NOT NULL,
      PRIMARY KEY (order_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单'
    """,
    """
    CREATE TABLE IF NOT EXISTS order_item (
      item_id         bigint        NOT NULL AUTO_INCREMENT,
      order_id        bigint        NOT NULL COMMENT '→ orders.order_id',
      sku_id          bigint        NOT NULL COMMENT '→ sku.sku_id',
      spu_id          bigint        NOT NULL,
      category_id     int           NOT NULL,
      quantity        int           NOT NULL,
      unit_price      decimal(10,2) NOT NULL,
      amount          decimal(12,2) NOT NULL,
      discount_amount decimal(12,2) NOT NULL,
      PRIMARY KEY (item_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单明细（订单↔商品 多对多的桥梁）'
    """,
]

# 数据灌完后再建索引：边插边维护索引慢得多
INDEXES = [
    ("users", "create index idx_users_city on users(city)"),
    ("spu", "create index idx_spu_cat on spu(category_id)"),
    ("sku", "create index idx_sku_spu on sku(spu_id)"),
    ("orders", "create index idx_orders_user on orders(user_id)"),
    ("orders", "create index idx_orders_time on orders(order_time)"),
    ("order_item", "create index idx_oi_order on order_item(order_id)"),
    ("order_item", "create index idx_oi_sku on order_item(sku_id)"),
    ("order_item", "create index idx_oi_cat on order_item(category_id)"),
]

TABLES = ("category", "users", "spu", "sku", "orders", "order_item")

_DAYS = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]


# ============================================================================
# 主流程
# ============================================================================

def main(argv):
    reset = "--reset" in argv
    dry = "--dry-run" in argv
    for a in argv:
        if a not in ("--reset", "--dry-run"):
            print("未知参数: %s（可用：--reset --dry-run）" % a)
            return 2

    p = cm.plan()
    total_rows = sum(p.day_rows)

    print("=" * 78)
    print("dojao 场景 · 业务数据 → MySQL %s:%d/%s" % (cfg.MYSQL_HOST, cfg.MYSQL_PORT, cfg.MYSQL_DB))
    print("=" * 78)
    print("  用户        : %s" % cm.fmt_int(cfg.USER_COUNT))
    print("  SPU / SKU   : %s / %s（每 SPU %d 个 SKU）"
          % (cm.fmt_int(cfg.SPU_COUNT), cm.fmt_int(cfg.SPU_COUNT * cfg.SKU_PER_SPU),
             cfg.SKU_PER_SPU))
    print("  订单 / 明细 : %s / 约 %s（已支付占 %.0f%%，平均 %.2f 件/单）"
          % (cm.fmt_int(p.order_count), cm.fmt_int(int(p.order_count * p.avg_items)),
             p.paid_rate * 100, p.avg_items))
    print("  分区窗口    : %s ~ %s（%d 天，订单按同一条曲线摊到各天）"
          % (p.first_day, p.last_day, p.days))
    print("  ← 反推依据  : Hive 每天 %s 行合计 × %.0f%% 购买占比 ÷ (%.0f%% 已支付 × %.2f 件/单)"
          % (cm.fmt_int(total_rows), cfg.PURCHASE_RATIO * 100, p.paid_rate * 100, p.avg_items))
    if dry:
        print("\n--dry-run：只做规划，不连数据库。")
        return 0

    t0 = time.time()
    conn = cm.mysql_connect()                       # 先不指定库
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET utf8mb4" % cfg.MYSQL_DB)
        conn.commit()
        conn.select_db(cfg.MYSQL_DB)

        _guard(conn, reset)
        _create_tables(conn)
        _load_dimensions(conn)
        _load_orders(conn, p)
        _create_indexes(conn)
        _summary(conn)
    finally:
        conn.close()

    print("\n完成，耗时 %.1f 秒。" % (time.time() - t0))
    print("\n在 Dojao 里注册这个库（把 host/user/password 换成你的）：")
    print("  CREATE CATALOG aliyun002_mysql USING mysql WITH (")
    print("    'catalog.mysql.jdbcUrl'='jdbc:mysql://%s:%d/%s',"
          " 'catalog.mysql.username'='%s', 'catalog.mysql.password'='%s');"
          % (cfg.MYSQL_HOST, cfg.MYSQL_PORT, cfg.MYSQL_DB, cfg.MYSQL_USER, cfg.MYSQL_PASSWORD))
    print("  show schemas from aliyun002_mysql;")
    return 0


def _guard(conn, reset):
    """表已存在且有数据时拒绝执行，除非显式 --reset —— 避免把已有数据当垃圾清掉。"""
    with conn.cursor() as cur:
        existing = []
        for t in TABLES:
            cur.execute("SHOW TABLES LIKE %s", (t,))
            if cur.fetchone():
                cur.execute("SELECT COUNT(*) FROM %s" % t)
                existing.append((t, cur.fetchone()[0]))
    if not existing:
        return
    if not reset:
        print("\n!! 目标库里这些表已经存在：")
        for t, n in existing:
            print("     %-12s %s 行" % (t, cm.fmt_int(n)))
        print("   本程序会 DROP 掉它们再重建。确认要清空就加 --reset 重跑：")
        print("     python3 gen_mysql.py --reset")
        raise SystemExit(3)
    print("\n--reset：先 DROP 掉 " + "、".join(t for t, _ in existing))
    with conn.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        for t in TABLES:
            cur.execute("DROP TABLE IF EXISTS %s" % t)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
    conn.commit()


def _create_tables(conn):
    with conn.cursor() as cur:
        for ddl in DDL:
            cur.execute(ddl)
    conn.commit()
    print("\n六张表已就绪。")


def _insert(conn, sql, rows, label, total):
    """分批 executemany —— 每批 commit 一次，中途断了也能看到进度。"""
    n, batch = 0, cfg.MYSQL_BATCH
    t0 = time.time()
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch):
            chunk = rows[i:i + batch]
            cur.executemany(sql, chunk)
            conn.commit()
            n += len(chunk)
            if n % (batch * 5) == 0 or n == total:
                print("    %-10s %s / %s  %.1f 秒"
                      % (label, cm.fmt_int(n), cm.fmt_int(total), time.time() - t0))
    return n


def _load_dimensions(conn):
    print("\n=== 维度表 ===")
    dims = model.build_dims()
    model.set_dims(dims)

    # ---------- category ----------
    cats = [(c[0], c[1], c[0] * 10) for c in cfg.CATEGORIES]
    _insert(conn, "INSERT INTO category (category_id, category_name, sort_order) VALUES (%s,%s,%s)",
            cats, "category", len(cats))

    # ---------- users ----------
    u = dims["user"]
    n = u["count"]
    ref_year = cm.plan().last_day.year
    rows = []
    for k in range(n):
        uid = k + 1
        city, prov = cfg.CITIES[int(u["city"][k])]
        h = cm.mix(uid * 99991 + 17)
        month = 1 + h % 12
        day = 1 + (h >> 8) % 28
        age = int(u["age"][k])
        reg_date = cm.plan().first_day - timedelta(days=int(u["reg_day"][k]))
        rows.append((
            uid,
            "user%07d" % uid,
            int(u["gender"][k]),
            age,
            "%04d-%02d-%02d" % (ref_year - age, month, day),
            city, prov,
            "1%010d" % ((h >> 16) % 10 ** 10),
            "user%07d@example.com" % uid,
            int(u["level"][k]),
            "%s %s" % (reg_date, cm.hms(int(u["reg_sec"][k]))),
            1 if (h % 100) < 97 else 0,
        ))
    _insert(conn, "INSERT INTO users (user_id, username, gender, age, birthday, city, province,"
                  " phone, email, member_level, register_time, status)"
                  " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows, "users", n)

    # ---------- spu ----------
    sp = dims["spu"]
    rows = []
    for k in range(sp["count"]):
        sid = k + 1
        cat = cfg.CATEGORIES[int(sp["cat_idx"][k])]
        cid = cat[0]
        noun = cat[2 + int(sp["noun_idx"][k])]
        brand = model.BRANDS[int(sp["brand_idx"][k])]
        suffix = model.TITLE_SUFFIX[cm.pick(sid * 601 + 3, len(model.TITLE_SUFFIX))]
        base = float(sp["base_price"][k])
        rows.append((
            sid, "%s%s%s" % (brand, noun, ["入门款", "标准款", "旗舰款", "Pro"][sid % 4]),
            "%s%s %s" % (brand, noun, suffix), cid, brand,
            round(base * 0.80, 2), round(base * 1.25, 2),
            "%s %s" % (cm.plan().first_day - timedelta(days=cm.pick(sid * 607 + 5, 900)),
                       cm.hms(cm.pick(sid * 613 + 7, 86400))),
            1,
        ))
    _insert(conn, "INSERT INTO spu (spu_id, spu_name, title, category_id, brand, price_min,"
                  " price_max, create_time, status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            rows, "spu", sp["count"])

    # ---------- sku ----------
    sk = dims["sku"]
    rows = []
    for k in range(sk["count"]):
        kid = k + 1
        host = int(sk["spu_of"][k]) - 1                  # 所属 SPU（-1 转 0-based）
        cat = cfg.CATEGORIES[int(sp["cat_idx"][host])]
        specs = model.SPEC_OPTIONS[cat[0]]
        si = int(sk["spec_idx"][k])
        color = model.SPEC_COLORS[int(sk["color_idx"][k])]
        spec = "颜色:%s 规格:%s" % (color, specs[si % len(specs)])
        rows.append((
            kid, host + 1,
            "%s %s %s" % (model.BRANDS[int(sp["brand_idx"][host])],
                          cat[2 + int(sp["noun_idx"][host])], specs[si % len(specs)]),
            spec,
            float(sk["price"][k]), float(sk["cost_price"][k]),
            int(sk["stock"][k]), int(sk["sold_count"][k]), 1,
        ))
    _insert(conn, "INSERT INTO sku (sku_id, spu_id, sku_name, spec, price, cost_price, stock,"
                  " sold_count, status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            rows, "sku", sk["count"])
    conn.commit()


def _load_orders(conn, p):
    print("\n=== 订单 + 明细（%s 单 / 约 %s 行）==="
          % (cm.fmt_int(p.order_count), cm.fmt_int(int(p.order_count * p.avg_items))))
    d = model.dims()
    user_city = d["user"]["city"]
    sku = d["sku"]

    order_sql = ("INSERT INTO orders (order_id, order_no, user_id, order_time, pay_time,"
                 " total_amount, discount_amount, pay_amount, order_status, pay_method,"
                 " device_type, province, city) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
    item_sql = ("INSERT INTO order_item (order_id, sku_id, spu_id, category_id, quantity,"
                " unit_price, amount, discount_amount) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")

    obatch, ibatch = [], []
    n_orders = n_items = 0
    paid_orders = 0
    t0 = time.time()
    batch = cfg.MYSQL_BATCH
    cur = conn.cursor()
    try:
        for i in range(p.order_count):
            oid = i + 1
            day = p.order_day(i)
            date = p.first_day + timedelta(days=day)
            sec = model.order_sec(i)
            uid = model.order_user(i)
            city, prov = cfg.CITIES[int(user_city[uid - 1])]
            status = model.order_status(i)
            paid = status in cfg.PAID_STATUSES
            rate = model.order_discount(i)

            items = model.order_items(i)
            total = 0.0
            for it in items:
                total += it["amount"]
            total = round(total, 2)
            disc = round(total * rate, 2)

            pay_time = None
            if paid:
                s2 = sec + model.order_pay_delay(i)
                d2 = date
                if s2 >= 86400:
                    s2 -= 86400
                    d2 = date + timedelta(days=1)
                pay_time = "%s %s" % (d2, cm.hms(s2))
                paid_orders += 1

            obatch.append((
                oid, "SO%s%09d" % (date.strftime("%Y%m%d"), oid), uid,
                "%s %s" % (date, cm.hms(sec)), pay_time,
                total, disc, round(total - disc, 2), status,
                model.order_pay_method(i), model.order_device(i), prov, city,
            ))
            for it in items:
                kid = it["sku_id"]
                ibatch.append((
                    oid, kid, int(sku["spu_of"][kid - 1]), int(sku["cat_id"][kid - 1]),
                    it["quantity"], it["unit_price"], it["amount"],
                    round(it["amount"] * rate, 2),
                ))

            if len(obatch) >= batch:
                cur.executemany(order_sql, obatch)
                cur.executemany(item_sql, ibatch)
                conn.commit()
                n_orders += len(obatch)
                n_items += len(ibatch)
                obatch, ibatch = [], []
                print("    %s / %s 单 · %s 明细  %.1f 秒"
                      % (cm.fmt_int(n_orders), cm.fmt_int(p.order_count),
                         cm.fmt_int(n_items), time.time() - t0))
        if obatch:
            cur.executemany(order_sql, obatch)
            cur.executemany(item_sql, ibatch)
            conn.commit()
            n_orders += len(obatch)
            n_items += len(ibatch)
    finally:
        cur.close()

    print("    合计 %s 单（已支付 %s）/ %s 明细  %.1f 秒"
          % (cm.fmt_int(n_orders), cm.fmt_int(paid_orders), cm.fmt_int(n_items),
             time.time() - t0))


def _create_indexes(conn):
    print("\n=== 建索引（数据灌完再建，快得多）===")
    with conn.cursor() as cur:
        for table, ddl in INDEXES:
            cur.execute(ddl)
    conn.commit()
    print("    已建 %d 个索引。" % len(INDEXES))


def _summary(conn):
    print("\n=== 落地自查 ===")
    with conn.cursor() as cur:
        for t in TABLES:
            cur.execute("SELECT COUNT(*) FROM %s" % t)
            print("    %-12s %s 行" % (t, cm.fmt_int(cur.fetchone()[0])))
        cur.execute("""SELECT c.category_name, COUNT(*) AS sku_cnt
                       FROM sku s JOIN spu p ON s.spu_id = p.spu_id
                       JOIN category c ON p.category_id = c.category_id
                       GROUP BY c.category_name ORDER BY sku_cnt DESC""")
        print("    SKU 品类分布：" + "，".join("%s %d" % (r[0], r[1]) for r in cur.fetchall()))
        cur.execute("""SELECT u.gender, COUNT(*) FROM users u GROUP BY u.gender""")
        print("    性别分布：" + "，".join(
            ("男" if r[0] == 1 else "女") + " %s" % cm.fmt_int(r[1]) for r in cur.fetchall()))
        cur.execute("SELECT COUNT(*) FROM users WHERE age BETWEEN 20 AND 40")
        n20 = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM users")
        print("    20-40 岁占比：%.1f%%" % (100.0 * n20 / cur.fetchone()[0]))
        cur.execute("SELECT COUNT(*) FROM orders WHERE order_status IN (2,3,4)")
        paid = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM orders")
        print("    已支付订单占比：%.1f%%" % (100.0 * paid / cur.fetchone()[0]))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
