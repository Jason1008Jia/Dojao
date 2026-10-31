package com.dojao.connector.hive;

/**
 * Hive 版本 —— 决定 catalog 的类型标识（{@code type='hive2'} / {@code type='hive3'}）。
 *
 * <p>Hive2 与 Hive3 的 MetaStore 后端表结构不同，故在 Dojao 侧拆成两个 catalog 类型。
 * 当前两版共用同一个 MetaStore Thrift 客户端（协议基本兼容），版本差异只在：</p>
 * <ul>
 *   <li>catalog 类型标识（落库 / 展示）；</li>
 *   <li><b>ACID 表的读快照模型</b>：Hive2 目录名里的 id 是<b>事务 id</b>（快照走
 *       {@code get_open_txns_info} → {@code ValidTxnList}），Hive3 是<b>写 id</b>
 *       （快照走 {@code getValidWriteIds} → {@code ValidReaderWriteIdList}）。
 *       两者 ORC 文件结构一致，行级合并复用同一套逻辑，只有可见性判定分派。</li>
 * </ul>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/19
 */
public enum HiveVersion {

    HIVE2("hive2", "2"),
    HIVE3("hive3", "3");

    private final String type;
    private final String shortName;

    HiveVersion(String type, String shortName) {
        this.type = type;
        this.shortName = shortName;
    }

    /** catalog 类型标识（ConnectorFactory#type 的返回值）。 */
    public String type() {
        return type;
    }

    /**
     * 属性键的<b>家族前缀</b>：{@code hive2} / {@code hive3} 共用 {@code catalog.hive.} ——
     * 两版的 catalog 属性完全相同，用户不该为了版本号把键写两遍。
     */
    public String family() {
        return "hive";
    }

    /** 简短版本号（"2" / "3"），用于日志与属性回显。 */
    public String shortName() {
        return shortName;
    }

    /** 由 catalog 类型标识反查；未知返回 null。 */
    public static HiveVersion fromType(String type) {
        if (type == null) return null;
        for (HiveVersion v : values()) {
            if (v.type.equalsIgnoreCase(type)) return v;
        }
        return null;
    }
}
