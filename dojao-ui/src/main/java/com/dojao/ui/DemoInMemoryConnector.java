package com.dojao.ui;

import com.dojao.common.data.Page;
import com.dojao.spi.bean.Catalog;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.ColumnStats;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.Schema;
import com.dojao.spi.bean.Table;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.bean.WriteRequest;
import com.dojao.spi.service.DojaoConnector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * <b>独立预览模式的演示连接器</b> —— 完全自包含的内存实现，不连任何真实数据源。
 *
 * <p>元数据（catalog / schema / table / column）来自构造时传入的静态定义；{@link #read} 恒 EOF、
 * {@link #write} no-op。目的是让 {@link DojaoUiApp} 的「独立预览」（{@code /api/explain} 的 DAG
 * 渲染）在<b>没有本机 MySQL / Hive</b> 的机器上也能演示，同时让 dojao-ui 的主代码<b>不再依赖任何
 * 具体连接器实现</b>（连接器已全部插件化，见 {@code lib/connectors/}）。</p>
 *
 * <p>历史：早先预览模式直接 {@code new MysqlConnector(...)}（真连本机 MySQL），并用
 * {@code DemoHiveConnector} 包装它再多挂一张假表 —— 既要求本机有库，又把 UI 焊死在连接器类上。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/5
 */
public final class DemoInMemoryConnector implements DojaoConnector {

    /** 一张演示表：原始 schema / 表名 + 列定义（列名保序、原样保留，供 SQL 解析）。 */
    public record DemoTable(String schema, String table, List<Column> columns) {}

    private final String catalogName;
    private final String catalogType;
    private final List<DemoTable> tables;
    /** {@code lower(schema).lower(table)} → 表定义（大小写不敏感查找）。 */
    private final Map<String, DemoTable> byKey = new LinkedHashMap<>();

    public DemoInMemoryConnector(String catalogName, String catalogType, List<DemoTable> tables) {
        this.catalogName = catalogName;
        this.catalogType = catalogType;
        this.tables = List.copyOf(tables);
        for (DemoTable t : this.tables) {
            byKey.put(key(t.schema(), t.table()), t);
        }
    }

    private static String key(String schema, String table) {
        return schema.toLowerCase(Locale.ROOT) + "." + table.toLowerCase(Locale.ROOT);
    }

    private DemoTable find(String schema, String table) {
        if (schema == null || table == null) return null;
        return byKey.get(key(schema, table));
    }

    // ============ Catalog ============

    @Override public boolean catalogExists(String catalog) {
        return catalogName.equalsIgnoreCase(catalog);
    }

    @Override public Optional<Catalog> getCatalog(String catalog) {
        return catalogExists(catalog)
                ? Optional.of(new Catalog(catalogName, catalogType, Map.of(), null, null))
                : Optional.empty();
    }

    @Override public List<String> listCatalogs() {
        return List.of(catalogName);
    }

    // ============ Schema ============

    @Override public boolean schemaExists(String catalog, String schema) {
        if (!catalogExists(catalog) || schema == null) return false;
        return tables.stream().anyMatch(t -> t.schema().equalsIgnoreCase(schema));
    }

    @Override public Optional<Schema> getSchema(String catalog, String schema) {
        return schemaExists(catalog, schema)
                ? Optional.of(new Schema(catalogName, schema, Map.of()))
                : Optional.empty();
    }

    @Override public List<String> listSchemas(String catalog) {
        if (!catalogExists(catalog)) return List.of();
        List<String> out = new ArrayList<>();
        for (DemoTable t : tables) {
            if (out.stream().noneMatch(s -> s.equalsIgnoreCase(t.schema()))) out.add(t.schema());
        }
        return out;
    }

    // ============ Table ============

    @Override public boolean tableExists(String catalog, String schema, String table) {
        return catalogExists(catalog) && find(schema, table) != null;
    }

    @Override public Optional<Table> getTable(String catalog, String schema, String table) {
        DemoTable t = catalogExists(catalog) ? find(schema, table) : null;
        return t == null
                ? Optional.empty()
                : Optional.of(new Table(catalogName, schema, table, t.columns(), Map.of(), "虚拟演示表"));
    }

    @Override public List<Column> getTableColumns(String catalog, String schema, String table) {
        DemoTable t = catalogExists(catalog) ? find(schema, table) : null;
        return t == null ? List.of() : t.columns();
    }

    @Override public List<String> listTables(String catalog, String schema) {
        if (!catalogExists(catalog) || schema == null) return List.of();
        List<String> out = new ArrayList<>();
        for (DemoTable t : tables) {
            if (t.schema().equalsIgnoreCase(schema)) out.add(t.table());
        }
        return out;
    }

    // ============ Column ============

    @Override public boolean columnExists(String catalog, String schema, String table, String column) {
        return getColumn(catalog, schema, table, column).isPresent();
    }

    @Override public Optional<Column> getColumn(String catalog, String schema, String table, String column) {
        DemoTable t = catalogExists(catalog) ? find(schema, table) : null;
        if (t == null || column == null) return Optional.empty();
        return t.columns().stream().filter(c -> c.name().equalsIgnoreCase(column)).findFirst();
    }

    // ============ 统计探查（演示：未知，交由引擎回退默认） ============

    @Override public TableStats getTableStats(String catalog, String schema, String table) {
        return new TableStats(catalog, schema, table, -1, -1, List.of());
    }

    @Override public List<ColumnStats> getColumnStats(String catalog, String schema, String table) {
        return List.of();
    }

    // ============ 数据面（预览不执行，恒空 / no-op） ============

    @Override public Page read(ReadRequest request) {
        return Page.eof();
    }

    @Override public void write(WriteRequest request) {
        // 演示连接器不落任何数据。
    }
}
