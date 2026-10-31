package com.dojao.grammar.analyzer.plan;

import com.dojao.grammar.analyzer.expression.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 将 LogicalPlan 序列化为 JSON 字符串（无外部依赖，纯手动构建）。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/28
 */
public final class PlanJsonSerializer {

    private PlanJsonSerializer() {}

    public static String toJson(LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        writePlan(sb, plan, 0);
        return sb.toString();
    }

    private static void writePlan(StringBuilder sb, LogicalPlan plan, int indent) {
        switch (plan) {
            case null -> sb.append("null");
            case LogicalPlan.Select p -> writeSelect(sb, p, indent);
            case LogicalPlan.Insert p -> writeInsert(sb, p, indent);
            case LogicalPlan.Use p -> writeUse(sb, p, indent);
            case LogicalPlan.CreateCatalog p -> writeCatalogDdl(sb, "CreateCatalog", indent)
                    .field("name", p.name()).field("connectorType", p.connectorType())
                    .mapField("properties", p.properties()).field("ifNotExists", p.ifNotExists()).end(indent);
            case LogicalPlan.DropCatalog p -> writeCatalogDdl(sb, "DropCatalog", indent)
                    .field("name", p.name()).field("ifExists", p.ifExists()).end(indent);
            case LogicalPlan.CreateSchema p -> writeSchemaDdl(sb, "CreateSchema", indent)
                    .listField("qualifiedName", p.qualifiedName()).mapField("properties", p.properties())
                    .field("ifNotExists", p.ifNotExists()).end(indent);
            case LogicalPlan.DropSchema p -> writeSchemaDdl(sb, "DropSchema", indent)
                    .listField("qualifiedName", p.qualifiedName()).field("ifExists", p.ifExists())
                    .field("cascade", p.cascade()).end(indent);
            case LogicalPlan.CreateTable p -> writeCreateTable(sb, p, indent);
            case LogicalPlan.CreateTableAsSelect p -> writeCtaspDdl(sb, "CreateTableAsSelect", indent)
                    .listField("qualifiedName", p.qualifiedName())
                    .optListField("columnAliases", p.columnAliases()).mapField("properties", p.properties())
                    .optField("comment", p.comment()).planField("query", p.query(), indent).end(indent);
            case LogicalPlan.DropTable p -> writeTableDdl(sb, "DropTable", indent)
                    .listField("qualifiedName", p.qualifiedName()).field("ifExists", p.ifExists()).end(indent);
            case LogicalPlan.TruncateTable p -> writeTableDdl(sb, "TruncateTable", indent)
                    .listField("qualifiedName", p.qualifiedName()).end(indent);
            case LogicalPlan.AlterTable p -> writeAlterTable(sb, p, indent);
            case LogicalPlan.CreateView p -> writeCtaspDdl(sb, "CreateView", indent)
                    .listField("qualifiedName", p.qualifiedName()).field("orReplace", p.orReplace())
                    .planField("query", p.query(), indent).end(indent);
            case LogicalPlan.DropView p -> writeTableDdl(sb, "DropView", indent)
                    .listField("qualifiedName", p.qualifiedName()).field("ifExists", p.ifExists()).end(indent);
            case LogicalPlan.CreateFunction p -> writeCtaspDdl(sb, "CreateFunction", indent)
                    .listField("qualifiedName", p.qualifiedName()).field("temporary", p.temporary())
                    .mapField("properties", p.properties()).optField("comment", p.comment())
                    .field("ifNotExists", p.ifNotExists()).end(indent);
            case LogicalPlan.DropFunction p -> writeTableDdl(sb, "DropFunction", indent)
                    .listField("qualifiedName", p.qualifiedName()).field("temporary", p.temporary())
                    .field("ifExists", p.ifExists()).end(indent);
            case LogicalPlan.ShowCatalogs p -> writePlain(sb, "ShowCatalogs", indent);
            case LogicalPlan.ShowSchemas p -> writeShowSchemas(sb, p, indent);
            case LogicalPlan.ShowTables p -> writeShowTables(sb, p, indent);
            case LogicalPlan.ShowColumns p -> writeNsDdl(sb, "ShowColumns", indent)
                    .listField("qualifiedName", p.qualifiedName()).end(indent);
            case LogicalPlan.ShowCreateTable p -> writeNsDdl(sb, "ShowCreateTable", indent)
                    .listField("qualifiedName", p.qualifiedName()).end(indent);
            case LogicalPlan.Describe p -> writeNsDdl(sb, "Describe", indent)
                    .listField("qualifiedName", p.qualifiedName()).end(indent);
            case LogicalPlan.Explain p -> writeExplain(sb, p, indent);
            case LogicalPlan.With p -> writeWith(sb, p, indent);
            case LogicalPlan.SetOperation p -> writeSetOp(sb, p, indent);
            case LogicalPlan.Values p -> writeValues(sb, p, indent);
            case LogicalPlan.Unsupported p -> writeNsDdl(sb, "Unsupported", indent)
                    .field("statementType", p.statementType()).end(indent);
            default -> {
            }
        }

    }

    private static void writeSelect(StringBuilder sb, LogicalPlan.Select p, int indent) {
        open(sb, "Select", indent);
        indent(sb, indent + 1).append("\"selectItems\": [");
        List<LogicalPlan.SelectItem> items = p.selectItems();
        if (items != null) {
            for (int i = 0; i < items.size(); i++) {
                if (i > 0) sb.append(", ");
                writeSelectItem(sb, items.get(i));
            }
        }
        sb.append("],\n");
        indent(sb, indent + 1).append("\"from\": [\n");
        List<LogicalPlan.ResolvedRelation> relations = p.fromRelations();
        if (relations != null) {
            for (int i = 0; i < relations.size(); i++) {
                if (i > 0) sb.append(",\n");
                writeRelation(sb, relations.get(i), indent + 2);
            }
        }
        sb.append("\n");
        indent(sb, indent + 1).append("],\n");
        indent(sb, indent + 1).append("\"joins\": [\n");
        List<LogicalPlan.JoinInfo> joins = p.joins();
        if (joins != null) {
            for (int i = 0; i < joins.size(); i++) {
                if (i > 0) sb.append(",\n");
                writeJoinInfo(sb, joins.get(i), indent + 2);
            }
        }
        sb.append("\n");
        indent(sb, indent + 1).append("],\n");
        indent(sb, indent + 1).append("\"where\": ");
        optExpr(sb, p.where());
        sb.append(",\n");
        indent(sb, indent + 1).append("\"groupBy\": ");
        exprList(sb, p.groupBy());
        sb.append(",\n");
        indent(sb, indent + 1).append("\"having\": ");
        optExpr(sb, p.having());
        sb.append("\n");
        close(sb, indent);
    }

    private static void writeJoinInfo(StringBuilder sb, LogicalPlan.JoinInfo j, int indent) {
        indent(sb, indent).append("{\n");
        indent(sb, indent + 1).append("\"type\": \"").append(j.joinType().name()).append("\",\n");
        indent(sb, indent + 1).append("\"left\": \"").append(j.leftAlias()).append("\",\n");
        indent(sb, indent + 1).append("\"right\": \"").append(j.rightAlias()).append("\",\n");
        indent(sb, indent + 1).append("\"condition\": ");
        writeExpr(sb, j.condition());
        sb.append("\n");
        indent(sb, indent).append("}");
    }

    private static void writeSelectItem(StringBuilder sb, LogicalPlan.SelectItem item) {
        sb.append("{\"expression\": ");
        writeExpr(sb, item.expression());
        sb.append(", \"alias\": ");
        optStr(sb, item.alias());
        sb.append("}");
    }

    private static void writeRelation(StringBuilder sb, LogicalPlan.ResolvedRelation r, int indent) {
        indent(sb, indent).append("{\n");
        indent(sb, indent + 1).append("\"catalog\": ").append(jsonStr(r.catalog())).append(",\n");
        indent(sb, indent + 1).append("\"schema\": ").append(jsonStr(r.schema())).append(",\n");
        indent(sb, indent + 1).append("\"table\": ").append(jsonStr(r.table())).append(",\n");
        indent(sb, indent + 1).append("\"alias\": ");
        optStr(sb, r.alias());
        sb.append(",\n");
        indent(sb, indent + 1).append("\"columns\": ");
        listStr(sb, r.columns());
        sb.append(",\n");
        indent(sb, indent + 1).append("\"filter\": ");
        optExpr(sb, r.filter());
        sb.append("\n");
        indent(sb, indent).append("}");
    }

    private static void writeInsert(StringBuilder sb, LogicalPlan.Insert p, int indent) {
        open(sb, "Insert", indent);
        indent(sb, indent + 1).append("\"catalog\": ").append(jsonStr(p.catalog())).append(",\n");
        indent(sb, indent + 1).append("\"schema\": ").append(jsonStr(p.schema())).append(",\n");
        indent(sb, indent + 1).append("\"table\": ").append(jsonStr(p.table())).append(",\n");
        indent(sb, indent + 1).append("\"columnAliases\": ");
        optListStr(sb, p.columnAliases());
        sb.append(",\n");
        indent(sb, indent + 1).append("\"query\": ");
        writePlan(sb, p.query(), indent + 1);
        sb.append("\n");
        close(sb, indent);
    }

    private static void writeUse(StringBuilder sb, LogicalPlan.Use p, int indent) {
        open(sb, "Use", indent);
        indent(sb, indent + 1).append("\"catalog\": ").append(jsonStr(p.catalog())).append(",\n");
        indent(sb, indent + 1).append("\"schema\": ");
        optStr(sb, p.schema());
        sb.append("\n");
        close(sb, indent);
    }

    private static DdlWriter writeCatalogDdl(StringBuilder sb, String type, int indent) {
        return new DdlWriter(sb, type, indent);
    }

    private static DdlWriter writeSchemaDdl(StringBuilder sb, String type, int indent) {
        return new DdlWriter(sb, type, indent);
    }

    private static DdlWriter writeCtaspDdl(StringBuilder sb, String type, int indent) {
        return new DdlWriter(sb, type, indent);
    }

    private static DdlWriter writeTableDdl(StringBuilder sb, String type, int indent) {
        return new DdlWriter(sb, type, indent);
    }

    private static DdlWriter writeNsDdl(StringBuilder sb, String type, int indent) {
        return new DdlWriter(sb, type, indent);
    }

    private static void writeCreateTable(StringBuilder sb, LogicalPlan.CreateTable p, int indent) {
        DdlWriter w = new DdlWriter(sb, "CreateTable", indent);
        w.listField("qualifiedName", p.qualifiedName());
        sb.append(",\n");
        indent(sb, indent + 1).append("\"columns\": [\n");
        List<LogicalPlan.ColumnDef> cols = p.columns();
        if (cols != null) {
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) sb.append(",\n");
                writeColumnDef(sb, cols.get(i), indent + 2);
            }
        }
        sb.append("\n");
        indent(sb, indent + 1).append("]");
        w.mapField("properties", p.properties()).optField("comment", p.comment())
            .field("ifNotExists", p.ifNotExists()).end(indent);
    }

    private static void writeColumnDef(StringBuilder sb, LogicalPlan.ColumnDef c, int indent) {
        indent(sb, indent).append("{\n");
        indent(sb, indent + 1).append("\"name\": ").append(jsonStr(c.name())).append(",\n");
        indent(sb, indent + 1).append("\"dataType\": ").append(jsonStr(c.dataType())).append(",\n");
        indent(sb, indent + 1).append("\"comment\": ");
        optStr(sb, c.comment());
        sb.append(",\n");
        indent(sb, indent + 1).append("\"defaultValue\": ");
        optStr(sb, c.defaultValue());
        sb.append(",\n");
        indent(sb, indent + 1).append("\"nullable\": ").append(c.nullable()).append("\n");
        indent(sb, indent).append("}");
    }

    private static void writeAlterTable(StringBuilder sb, LogicalPlan.AlterTable p, int indent) {
        open(sb, "AlterTable", indent);
        indent(sb, indent + 1).append("\"qualifiedName\": ");
        listStr(sb, p.qualifiedName());
        sb.append(",\n");
        indent(sb, indent + 1).append("\"action\": ");
        LogicalPlan.AlterAction a = p.action();
        if (a instanceof LogicalPlan.AlterAction.RenameTo rt) {
            sb.append("{\"type\":\"RenameTo\",\"newName\":").append(jsonStr(rt.newName())).append("}");
        } else if (a instanceof LogicalPlan.AlterAction.AddColumn ac) {
            sb.append("{\"type\":\"AddColumn\",\"column\":");
            writeColumnDef(sb, ac.column(), indent + 1);
            sb.append("}");
        } else if (a instanceof LogicalPlan.AlterAction.DropColumn dc) {
            sb.append("{\"type\":\"DropColumn\",\"columnName\":").append(jsonStr(dc.columnName())).append("}");
        } else if (a instanceof LogicalPlan.AlterAction.RenameColumn rc) {
            sb.append("{\"type\":\"RenameColumn\",\"oldName\":").append(jsonStr(rc.oldName()))
              .append(",\"newName\":").append(jsonStr(rc.newName())).append("}");
        }
        sb.append("\n");
        close(sb, indent);
    }

    private static void writePlain(StringBuilder sb, String type, int indent) {
        indent(sb, indent).append("{\"type\":\"").append(type).append("\"}");
    }

    private static void writeExplain(StringBuilder sb, LogicalPlan.Explain p, int indent) {
        open(sb, "Explain", indent);
        indent(sb, indent + 1).append("\"analyze\": ").append(p.analyze()).append(",\n");
        indent(sb, indent + 1).append("\"verbose\": ").append(p.verbose()).append(",\n");
        indent(sb, indent + 1).append("\"statement\": ");
        writePlan(sb, p.statement(), indent + 1);
        sb.append("\n");
        close(sb, indent);
    }

    private static void writeShowSchemas(StringBuilder sb, LogicalPlan.ShowSchemas p, int indent) {
        open(sb, "ShowSchemas", indent);
        indent(sb, indent + 1).append("\"catalog\": ");
        optStr(sb, p.catalog());
        sb.append("\n");
        close(sb, indent);
    }

    private static void writeShowTables(StringBuilder sb, LogicalPlan.ShowTables p, int indent) {
        open(sb, "ShowTables", indent);
        indent(sb, indent + 1).append("\"qualifiedName\": ");
        optListStr(sb, p.qualifiedName());
        sb.append(",\n");
        indent(sb, indent + 1).append("\"likePattern\": ");
        optStr(sb, p.likePattern());
        sb.append("\n");
        close(sb, indent);
    }

    private static void writeWith(StringBuilder sb, LogicalPlan.With p, int indent) {
        open(sb, "With", indent);
        indent(sb, indent + 1).append("\"ctes\": [\n");
        List<LogicalPlan.CteDef> ctes = p.ctes();
        if (ctes != null) {
            for (int i = 0; i < ctes.size(); i++) {
                if (i > 0) sb.append(",\n");
                writeCteDef(sb, ctes.get(i), indent + 2);
            }
        }
        sb.append("\n");
        indent(sb, indent + 1).append("],\n");
        indent(sb, indent + 1).append("\"query\": ");
        writePlan(sb, p.query(), indent + 1);
        sb.append("\n");
        close(sb, indent);
    }

    private static void writeCteDef(StringBuilder sb, LogicalPlan.CteDef cte, int indent) {
        indent(sb, indent).append("{\n");
        indent(sb, indent + 1).append("\"name\": ").append(jsonStr(cte.name())).append(",\n");
        indent(sb, indent + 1).append("\"columns\": ");
        listStr(sb, cte.columns());
        sb.append(",\n");
        indent(sb, indent + 1).append("\"query\": ");
        writePlan(sb, cte.query(), indent + 1);
        sb.append("\n");
        indent(sb, indent).append("}");
    }

    private static void writeSetOp(StringBuilder sb, LogicalPlan.SetOperation p, int indent) {
        open(sb, "SetOperation", indent);
        indent(sb, indent + 1).append("\"opType\":\"").append(p.opType().name()).append("\",\n");
        indent(sb, indent + 1).append("\"distinct\":").append(p.distinct()).append(",\n");
        indent(sb, indent + 1).append("\"left\":");
        writePlan(sb, p.left(), indent + 1);
        sb.append(",\n");
        indent(sb, indent + 1).append("\"right\":");
        writePlan(sb, p.right(), indent + 1);
        sb.append("\n");
        close(sb, indent);
    }

    private static void writeValues(StringBuilder sb, LogicalPlan.Values p, int indent) {
        open(sb, "Values", indent);
        indent(sb, indent + 1).append("\"rows\":[\n");
        List<List<Expression>> rows = p.rows();
        if (rows != null) {
            for (int i = 0; i < rows.size(); i++) {
                if (i > 0) sb.append(",\n");
                indent(sb, indent + 2).append("[");
                List<Expression> row = rows.get(i);
                for (int j = 0; j < row.size(); j++) {
                    if (j > 0) sb.append(", ");
                    writeExpr(sb, row.get(j));
                }
                sb.append("]");
            }
        }
        sb.append("\n");
        indent(sb, indent + 1).append("]\n");
        close(sb, indent);
    }

    private static void writeExpr(StringBuilder sb, Expression expr) {
        if (expr == null) { sb.append("null"); return; }
        if (expr instanceof Expression.OpaqueExpr e) {
            sb.append("{\"type\":\"OpaqueExpr\",\"text\":").append(jsonStr(e.originalText())).append("}");
        } else if (expr instanceof Expression.ColumnRef e) {
            sb.append("{\"type\":\"ColumnRef\",\"qualifier\":");
            sb.append(e.qualifier() != null ? jsonStr(e.qualifier()) : "null");
            sb.append(",\"columnName\":").append(jsonStr(e.columnName())).append("}");
        } else if (expr instanceof Expression.WindowFunction wf) {
            writeWindowFunction(sb, wf);
        }
    }

    private static void writeWindowFunction(StringBuilder sb, Expression.WindowFunction wf) {
        sb.append("{\"type\":\"WindowFunction\",\"functionName\":").append(jsonStr(wf.functionName()));
        sb.append(",\"args\":");
        listStr(sb, wf.args());
        sb.append(",\"partitionBy\":");
        listStr(sb, wf.partitionBy());
        sb.append(",\"orderBy\":");
        listStr(sb, wf.orderBy());
        sb.append(",\"windowFrame\":");
        optStr(sb, wf.windowFrame());
        sb.append("}");
    }

    private static void optExpr(StringBuilder sb, Optional<Expression> opt) {
        if (opt != null && opt.isPresent()) writeExpr(sb, opt.get());
        else sb.append("null");
    }

    private static void exprList(StringBuilder sb, List<Expression> list) {
        sb.append("[");
        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(", ");
                writeExpr(sb, list.get(i));
            }
        }
        sb.append("]");
    }

    private static String jsonStr(String s) {
        if (s == null) return "null";
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        return sb.append("\"").toString();
    }

    private static void optStr(StringBuilder sb, Optional<String> opt) {
        if (opt != null && opt.isPresent()) sb.append(jsonStr(opt.get()));
        else sb.append("null");
    }

    private static void listStr(StringBuilder sb, List<String> list) {
        sb.append("[");
        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(jsonStr(list.get(i)));
            }
        }
        sb.append("]");
    }

    private static void optListStr(StringBuilder sb, Optional<List<String>> opt) {
        if (opt != null && opt.isPresent()) listStr(sb, opt.get());
        else sb.append("null");
    }

    private static StringBuilder indent(StringBuilder sb, int level) {
        for (int i = 0; i < level; i++) sb.append("  ");
        return sb;
    }

    private static void open(StringBuilder sb, String type, int indent) {
        indent(sb, indent).append("{\n");
        indent(sb, indent + 1).append("\"type\":\"").append(type).append("\",\n");
    }

    private static void close(StringBuilder sb, int indent) {
        indent(sb, indent).append("}");
    }

    private static class DdlWriter {
        private final StringBuilder sb;
        private boolean first = true;

        DdlWriter(StringBuilder sb, String type, int indent) {
            this.sb = sb;
            open(sb, type, indent);
        }

        DdlWriter field(String name, Object value) {
            comma(); sb.append("\n");
            indent(sb.append("\"").append(name).append("\":"), 0);
            if (value instanceof Boolean b) sb.append(b);
            else if (value instanceof String s) sb.append(jsonStr(s));
            else sb.append(value);
            return this;
        }

        DdlWriter mapField(String name, Map<String, String> map) {
            comma(); sb.append("\n");
            indent(sb, 0);
            sb.append("\"").append(name).append("\":{");
            boolean fe = true;
            if (map != null) {
                for (Map.Entry<String, String> e : map.entrySet()) {
                    if (!fe) sb.append(", ");
                    sb.append(jsonStr(e.getKey())).append(":").append(jsonStr(e.getValue()));
                    fe = false;
                }
            }
            sb.append("}");
            return this;
        }

        DdlWriter listField(String name, List<String> list) {
            comma(); sb.append("\n");
            indent(sb, 0);
            sb.append("\"").append(name).append("\":");
            listStr(sb, list);
            return this;
        }

        DdlWriter optField(String name, Optional<String> opt) {
            comma(); sb.append("\n");
            indent(sb, 0);
            sb.append("\"").append(name).append("\":");
            optStr(sb, opt);
            return this;
        }

        DdlWriter optListField(String name, Optional<List<String>> opt) {
            comma(); sb.append("\n");
            indent(sb, 0);
            sb.append("\"").append(name).append("\":");
            optListStr(sb, opt);
            return this;
        }

        DdlWriter planField(String name, LogicalPlan plan, int indent) {
            comma(); sb.append("\n");
            indent(sb, 0);
            sb.append("\"").append(name).append("\":");
            writePlan(sb, plan, indent);
            return this;
        }

        void end(int indent) {
            sb.append("\n");
            close(sb, indent);
        }

        private void comma() {
            if (!first) sb.append(",");
            first = false;
        }
    }
}
