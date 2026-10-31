package com.dojao.grammar.analyzer.resolver;

import com.dojao.grammar.analyzer.error.SemanticError;
import com.dojao.metastore.MetaStore;

import java.util.*;

/**
 * 表解析器：通过 MetaStore 将 qualifiedName 逐级校验并解析为
 * catalog.schema.table 及列列表。
 *
 * <p>校验链路（逐级进行，一旦失败立即返回错误，不再继续）：
 * <ol>
 *   <li>catalog 是否存在（MetaStore.isCatalogRegistered → 查外接存储）</li>
 *   <li>schema 是否存在（MetaStore.schemaExists → 查对应连接器）</li>
 *   <li>table 是否存在（MetaStore.tableExists → 查对应连接器）</li>
 *   <li>获取表的列列表（MetaStore.getColumns）</li>
 * </ol>
 *
 * @author jiaxin
 * @version 2.0
 * @date 2026/6/28
 */
public class TableResolver {

    public record ResolveResult(
        Optional<ResolvedName> resolved,
        List<SemanticError> errors
    ) {}

    public record ResolvedName(
        String catalog,
        String schema,
        String table,
        List<String> columns
    ) {}

    private final MetaStore metaStore;
    private final String currentCatalog;
    private final String currentSchema;

    public TableResolver(MetaStore metaStore, String currentCatalog, String currentSchema) {
        this.metaStore = metaStore;
        this.currentCatalog = currentCatalog;
        this.currentSchema = currentSchema;
    }

    /**
     * 解析 qualifiedName（1/2/3 段），逐级校验。
     */
    public ResolveResult resolve(List<String> parts) {
        return switch (parts.size()) {
            case 1 -> resolveSingle(parts.get(0));
            case 2 -> resolveTwo(parts.get(0), parts.get(1));
            case 3 -> resolveThree(parts.get(0), parts.get(1), parts.get(2));
            default -> new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Invalid table name: " + String.join(".", parts))));
        };
    }

    private ResolveResult resolveSingle(String table) {
        if (currentCatalog == null || currentSchema == null) {
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Cannot resolve table '" + table
                        + "' -- no current catalog/schema set. Use USE catalog.schema first.")));
        }
        return resolveThree(currentCatalog, currentSchema, table);
    }

    private ResolveResult resolveTwo(String schema, String table) {
        if (currentCatalog == null) {
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Cannot resolve '" + schema + "." + table
                        + "' -- no current catalog set. Use USE catalog.schema first.")));
        }
        return resolveThree(currentCatalog, schema, table);
    }

    private ResolveResult resolveThree(String catalog, String schema, String table) {
        // --- 第 1 级：校验 catalog（从 Dojao 元数据存储查） ---
        if (!metaStore.isCatalogRegistered(catalog)) {
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Catalog '" + catalog + "' does not exist")));
        }

        // 获取对应 Connector
        var connector = metaStore.getConnector(catalog).orElse(null);
        if (connector == null) {
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Connector for catalog '" + catalog + "' is not loaded")));
        }

        // --- 第 2 级：校验 schema（Connector 直连检测） ---
        try {
            if (!connector.schemaExists(catalog, schema)) {
                return new ResolveResult(Optional.empty(),
                    List.of(new SemanticError(
                        "Schema '" + catalog + "." + schema + "' not found")));
            }
        } catch (RuntimeException e) {
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Schema check failed for '" + catalog + "." + schema
                        + "': " + unwrapMessage(e))));
        }

        // --- 第 3 级：校验 table（Connector 直连检测，透传原始错误） ---
        try {
            if (!connector.tableExists(catalog, schema, table)) {
                return new ResolveResult(Optional.empty(),
                    List.of(new SemanticError(
                        "Table '" + catalog + "." + schema + "." + table
                            + "' not found")));
            }
        } catch (RuntimeException e) {
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Table check failed for '" + catalog + "." + schema + "." + table
                        + "': " + unwrapMessage(e))));
        }

        List<String> columns = metaStore.getColumns(catalog, schema, table)
            .stream()
            .map(c -> c.name())
            .toList();

        return new ResolveResult(
            Optional.of(new ResolvedName(catalog, schema, table, columns)),
            List.of()
        );
    }

    /** 从 RuntimeException 中提取原始底层错误消息。 */
    private static String unwrapMessage(RuntimeException e) {
        Throwable cause = e.getCause();
        if (cause != null && cause.getMessage() != null) {
            return cause.getMessage();
        }
        return e.getMessage() != null ? e.getMessage() : "unknown error";
    }
}
