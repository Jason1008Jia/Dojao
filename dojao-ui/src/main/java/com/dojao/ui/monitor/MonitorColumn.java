package com.dojao.ui.monitor;

/**
 * 表的一列（首页目录树展开到表级别后展示的列信息）。
 *
 * <p>字段与连接器 SPI 的列描述一一对应，但刻意不依赖 {@code dojao-spi}
 * （契约留 ui、实现进 server 的分层约定）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/18
 */
public record MonitorColumn(
        String name,
        String dataType,
        String comment,
        boolean nullable) {
}
