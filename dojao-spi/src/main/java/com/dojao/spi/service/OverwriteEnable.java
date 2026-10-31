package com.dojao.spi.service;

/**
 * INSERT OVERWRITE 覆盖写能力接口 —— 连接器<b>按能力声明</b>（capability interface），
 * 不是策略枚举：后续新增海量外部数据源时，谁支持覆盖谁就实现本接口，引擎用
 * {@code connector instanceof OverwriteEnable} 探测，不需要为每个数据源枚举。
 *
 * <p>四步协议（staging 中转 + 全成或全不成，数据不回 Master）：</p>
 * <ol>
 *   <li>{@link #supportsOverwrite}：权限/能力探针 —— Master 规划期真实执行建 throwaway 表 +
 *       RENAME + DROP（只碰临时对象、不触碰目标表），反映角色授权的<b>有效</b>权限；不足 → false
 *       （比查权限表可靠）；</li>
 *   <li>{@link #beginOverwrite}：建覆盖用 staging（JDBC 系 = {@code CREATE TABLE <t>__dojao_tmp_*
 *       LIKE 目标} 克隆结构 / Hive 系 = 表目录下临时目录），返回 staging 标识 —— 由 Worker 侧
 *       分布式写灌数据；</li>
 *   <li>{@link #commitOverwrite}：收齐全部 N 分区写入后一次性把 staging 切换成目标表
 *       （JDBC 系原子 RENAME 无空窗期 / Hive 系删旧 + mv），仅全部成功时被调一次；</li>
 *   <li>{@link #abortOverwrite}：失败/取消时删 staging，目标表从未被触碰。</li>
 * </ol>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/5
 */
public interface OverwriteEnable {

    /**
     * 权限/能力探针：目标账户能否对目标表做覆盖（能建 throwaway 表 + RENAME + DROP）。
     * 用真实执行而非查权限表，反映角色授权的<b>有效</b>权限；不足 → false，Master 报「暂不支持」。
     */
    boolean supportsOverwrite(String catalog, String schema, String table);

    /**
     * 建覆盖用的 staging（克隆目标表结构 / 建临时目录），返回 staging 标识（表名/目录）。
     * staging 由 Worker 侧分布式写灌数据。
     *
     * @throws IllegalStateException 建 staging 失败（数据源异常）
     */
    String beginOverwrite(String catalog, String schema, String table);

    /**
     * 收齐全部 N 分区写入后，一次性把 staging 切换成目标表（JDBC 系原子 RENAME / Hive 系删旧 + mv）。
     * 仅在全部成功时被调用一次（全成或全不成）。
     *
     * @throws IllegalStateException 切换失败
     */
    void commitOverwrite(String catalog, String schema, String table, String staging);

    /**
     * 覆盖失败/取消时清理 staging（JDBC 系 {@code DROP TABLE IF EXISTS}），原表保持原样。
     */
    void abortOverwrite(String catalog, String schema, String table, String staging);
}
