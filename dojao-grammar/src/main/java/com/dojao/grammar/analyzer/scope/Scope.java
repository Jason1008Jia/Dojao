package com.dojao.grammar.analyzer.scope;

import com.dojao.grammar.analyzer.error.SemanticError;
import com.dojao.grammar.analyzer.expression.Expression;
import com.dojao.grammar.analyzer.plan.LogicalPlan;

import java.util.*;

/**
 * 查询作用域：跟踪 FROM 子句中的关系及其列信息，并提供列引用解析。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27 22:27
 */
public class Scope {

    /**
     * 作用域中可见的关系信息。
     *
     * @param subquery 派生表（FROM 子查询）的内层逻辑计划；实体表为空。
     */
    public record RelationInfo(
        String catalog,
        String schema,
        String tableName,
        String alias,
        List<String> columns,
        Optional<LogicalPlan> subquery
    ) {
        /** Convenience constructor for backward compatibility (实体表无子查询)。 */
        public RelationInfo(String catalog, String schema, String tableName,
                             String alias, List<String> columns) {
            this(catalog, schema, tableName, alias, columns, Optional.empty());
        }
    }

    /** 按别名查找（主索引），按表名查找（别名不存在时的回退）。 */
    private final Map<String, RelationInfo> byAlias = new LinkedHashMap<>();
    private final Map<String, RelationInfo> byTableName = new LinkedHashMap<>();

    /** 作用域中是否有注册过任何关系。 */
    public boolean isEmpty() {
        return byAlias.isEmpty();
    }

    /**
     * 向当前作用域注册一个关系。
     */
    public void addRelation(RelationInfo info) {
        byAlias.put(info.alias().toLowerCase(), info);
        byTableName.put(info.tableName().toLowerCase(), info);
    }

    /**
     * 列解析结果。
     */
    public record ResolveResult(
        Optional<Expression.ColumnRef> columnRef,
        List<SemanticError> errors
    ) {}

    /**
     * 根据标识符段解析列引用。
     * <ul>
     *   <li>1 段：非限定列名 -- 在所有关系中搜索</li>
     *   <li>2 段：qualifier.columnName -- qualifier 可以是别名或表名</li>
     *   <li>3 段：schema.table.column</li>
     *   <li>4 段：catalog.schema.table.column</li>
     * </ul>
     */
    public ResolveResult resolveColumn(List<String> parts) {
        return switch (parts.size()) {
            case 1 -> resolveUnqualified(parts.get(0));
            case 2 -> resolveQualified(parts.get(0), parts.get(1));
            case 3, 4 -> resolveQualifiedWithParts(parts);
            default -> new ResolveResult(
                Optional.empty(),
                List.of(new SemanticError("Invalid column reference: "
                    + String.join(".", parts)))
            );
        };
    }

    /**
     * 非限定列名：在所有关系中搜索。
     */
    private ResolveResult resolveUnqualified(String colName) {
        List<RelationInfo> matched = new ArrayList<>();
        for (RelationInfo rel : byAlias.values()) {
            if (rel.columns().contains(colName)) {
                matched.add(rel);
            }
        }

        if (matched.isEmpty()) {
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Column '" + colName + "' is not found in any table in scope")));
        }
        if (matched.size() > 1) {
            List<String> tableNames = matched.stream()
                .map(RelationInfo::alias)
                .toList();
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Column '" + colName + "' is ambiguous -- found in tables: "
                        + tableNames)));
        }

        RelationInfo rel = matched.get(0);
        return new ResolveResult(
            Optional.of(new Expression.ColumnRef(rel.alias(), colName)),
            List.of()
        );
    }

    /**
     * 两段式：qualifier.columnName。
     */
    private ResolveResult resolveQualified(String qualifier, String colName) {
        RelationInfo rel = byAlias.get(qualifier.toLowerCase());
        if (rel == null) {
            rel = byTableName.get(qualifier.toLowerCase());
        }

        if (rel == null) {
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Table or alias '" + qualifier + "' is not found in scope")));
        }

        if (!rel.columns().contains(colName)) {
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Column '" + colName + "' not found in table '" + qualifier
                        + "' (columns: " + rel.columns() + ")")));
        }

        // 归一化为关系规范别名（与 resolveUnqualified / resolveQualifiedWithParts 对齐）：
        // 消费方（列裁剪/谓词下推）按 rel.alias().orElse(rel.table()) 匹配，用户写的 qualifier 可能是表名。
        return new ResolveResult(
            Optional.of(new Expression.ColumnRef(rel.alias(), colName)),
            List.of()
        );
    }

    /**
     * 三段式（schema.table.column）或四段式（catalog.schema.table.column）。
     * 在作用域中按表名匹配（匹配最后两个段或最后一个段）。
     */
    private ResolveResult resolveQualifiedWithParts(List<String> parts) {
        String colName = parts.get(parts.size() - 1);
        String twoParts = parts.get(parts.size() - 3) + "."
            + parts.get(parts.size() - 2);
        String onePart = parts.get(parts.size() - 2);

        RelationInfo matched = null;
        for (RelationInfo rel : byAlias.values()) {
            String fullName = rel.catalog() + "." + rel.schema() + "." + rel.tableName();
            String schemaTable = rel.schema() + "." + rel.tableName();
            if (fullName.equals(String.join(".", parts.subList(0, parts.size() - 1)))
                || schemaTable.equals(twoParts)
                || rel.tableName().equals(onePart)) {
                matched = rel;
                break;
            }
        }

        if (matched == null) {
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Cannot resolve column reference '"
                        + String.join(".", parts) + "' -- table not found in scope")));
        }

        if (!matched.columns().contains(colName)) {
            return new ResolveResult(Optional.empty(),
                List.of(new SemanticError(
                    "Column '" + colName + "' not found in table '"
                        + matched.tableName() + "'")));
        }

        return new ResolveResult(
            Optional.of(new Expression.ColumnRef(matched.alias(), colName)),
            List.of()
        );
    }
}
