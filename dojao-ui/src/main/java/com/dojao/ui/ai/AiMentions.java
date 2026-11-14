package com.dojao.ui.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * AI 对话的「@ 引用」上下文块。
 *
 * <p>前端在输入框里通过 {@code @} 选择 catalog / 函数（后续可扩展表等），随本轮请求以
 * {@code [{type,name,detail}]} 数组提交；这里把引用转成一段模型可读的上下文，拼进本轮提问
 * （与附件块同构，见 {@link FileParser#promptBlock}）。目的：用户已经明确了检索范围（某个 catalog
 * 或某个函数），模型不必再全量搜索元数据、也不容易用同名对象替代。</p>
 *
 * <p>只做校验 / 去重 / 截断，<b>不访问元数据服务</b> —— 函数签名、catalog 的 schema 列表等说明
 * 由前端随请求带上（前端已从 {@code /api/functions}、{@code /api/metadata/...} 取到）。</p>
 *
 * @author jiaxin
 * @date 2026/11/14
 */
public final class AiMentions {

    /** 单轮最多保留的引用数（避免被构造的超长数组撑爆提示词）。 */
    public static final int MAX_MENTIONS = 20;
    /** 单条说明的最大字符数。 */
    public static final int MAX_DETAIL_CHARS = 800;
    /** 引用名最大长度。 */
    public static final int MAX_NAME_CHARS = 200;

    private static final Set<String> TYPES = Set.of("catalog", "table", "function");

    /** 一条 @ 引用：{@code type} ∈ {catalog, table, function}。 */
    public record Mention(String type, String name, String detail) {
    }

    private AiMentions() {
    }

    /** 归一化后写回轮次 JSON（去重 / 截断后的形状，历史轮次据此重建上下文块）。 */
    public static ArrayNode toJson(List<Mention> mentions) {
        ArrayNode arr = JsonNodeFactory.instance.arrayNode();
        if (mentions != null) {
            for (Mention m : mentions) {
                arr.addObject()
                        .put("type", m.type())
                        .put("name", m.name())
                        .put("detail", m.detail() == null ? "" : m.detail());
            }
        }
        return arr;
    }

    /**
     * 解析请求体里的 {@code mentions} 数组（形状不对 / 空 → 空列表，绝不抛错）。
     * 非法条目跳过；同 {@code type+name} 只保留第一条；最多 {@link #MAX_MENTIONS} 条。
     */
    public static List<Mention> fromJson(JsonNode arr) {
        if (arr == null || !arr.isArray()) {
            return List.of();
        }
        List<Mention> out = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (JsonNode n : arr) {
            if (out.size() >= MAX_MENTIONS) {
                break;
            }
            String type = n.path("type").asText("").trim().toLowerCase();
            String name = n.path("name").asText("").trim();
            if (!TYPES.contains(type) || name.isEmpty()) {
                continue;
            }
            if (name.length() > MAX_NAME_CHARS) {
                name = name.substring(0, MAX_NAME_CHARS);
            }
            String detail = n.path("detail").asText("").trim();
            if (detail.length() > MAX_DETAIL_CHARS) {
                detail = detail.substring(0, MAX_DETAIL_CHARS) + "…";
            }
            if (!seen.add(type + "\u0001" + name)) {
                continue;
            }
            out.add(new Mention(type, name, detail));
        }
        return out;
    }

    /**
     * 把引用拼成提示词块（粘在本轮提问后面）；无引用 → 空串。
     * 文案明确「优先在该范围内理解」并提示可继续用元数据工具核实，避免模型仅凭名字臆测结构。
     */
    public static String promptBlock(List<Mention> mentions) {
        if (mentions == null || mentions.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("\n\n【@ 引用】用户用 @ 指定了以下对象，请优先在这些范围内理解问题，不要用同名但不同来源的对象替代；")
                .append("需要更多细节时仍可调用 list_tables / describe_table / describe_function 核实：\n");
        for (Mention m : mentions) {
            sb.append("- ").append(label(m.type())).append("「").append(m.name()).append("」");
            if (m.detail() != null && !m.detail().isBlank()) {
                sb.append("：").append(m.detail());
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static String label(String type) {
        return switch (type) {
            case "catalog" -> "目录";
            case "table" -> "表";
            case "function" -> "函数";
            default -> type;
        };
    }
}
