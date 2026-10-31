package com.dojao.core.function.table;

import com.dojao.common.data.Page;
import com.dojao.common.memory.NodeMemory;
import com.dojao.core.config.ResourceConfig;
import com.dojao.core.operator.AbstractMeasurableOperator;
import com.dojao.core.operator.Operator;
import com.dojao.core.operator.expr.ExprEvaluator;
import com.dojao.core.operator.expr.ExprNode;
import com.dojao.core.operator.expr.ExprParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 行展开算子 —— {@code FROM t, UNNEST(json_array_expr) [WITH ORDINALITY] [AS u(item[, ord)]]}。
 *
 * <p>逐页读取上游，按行求值 {@code json_array_expr} 得到 JSON 文本，再把数组<b>展开成多行</b>：
 * 上游列原样透传，追加元素列（Utf8）与可选的序号列（BigInt，1 起、按输入行重置）。</p>
 *
 * <p>展开口径：</p>
 * <ul>
 *   <li>上游值为 SQL NULL、空串，或 JSON {@code null} → 该输入行产出 <b>0 行</b>（与 SQL UNNEST 一致）；</li>
 *   <li>JSON 数组 → 每个元素一行；元素是标量时输出其文本（字符串不带引号），
 *       对象/数组输出 JSON 文本；JSON {@code null} 元素输出 NULL；</li>
 *   <li>非法 JSON 或非数组的 JSON 值（对象/标量）→ <b>立即报错</b>（不静默丢行，避免「结果莫名少行」）。</li>
 * </ul>
 */
public final class UnnestOperator extends AbstractMeasurableOperator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String expressionText;
    private final String elementColumn;
    /** WITH ORDINALITY 的序号列名；未使用时为 null。 */
    private final String ordinalityColumn;

    private Operator upstream;
    private BufferAllocator allocator;
    private ExprEvaluator evaluator;
    private final Field elementField;
    private final Field ordinalityField;

    public UnnestOperator(Operator upstream, String expressionText,
                          String elementColumn, String ordinalityColumn) {
        super("Unnest", "Unnest", 1);
        this.upstream = upstream;
        this.expressionText = expressionText;
        this.elementColumn = elementColumn;
        this.ordinalityColumn = ordinalityColumn;
        this.elementField = new Field(elementColumn,
                FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()), null);
        this.ordinalityField = ordinalityColumn == null ? null
                : new Field(ordinalityColumn,
                    FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true)), null);
    }

    public void setUpstream(Operator upstream) {
        this.upstream = upstream;
    }

    public String expressionText() {
        return expressionText;
    }

    /** 展开表达式引用的上游列（供物理列裁剪要求上游保留）。 */
    public java.util.Set<String> expressionColumns() {
        return new ExprEvaluator(ExprParser.parse(expressionText)).columnArgs();
    }

    public String elementColumn() {
        return elementColumn;
    }

    /** 序号列名；未使用 WITH ORDINALITY 时为 null。 */
    public String ordinalityColumn() {
        return ordinalityColumn;
    }

    @Override
    protected void doOpen() {
        upstream.open();
        allocator = NodeMemory.shared()
                .newOperatorAllocator("unnest", ResourceConfig.queryDirectLimit());
        ExprNode node = ExprParser.parse(expressionText);
        evaluator = new ExprEvaluator(node);
    }

    @Override
    protected Page doNext() {
        while (true) {
            Page input = upstream.next();
            recordInput(input);
            if (input.isEOF()) {
                return input;
            }
            int rowCount = input.getRowCount();
            if (rowCount == 0) {
                input.close();
                continue;
            }
            Page out = expand(input);
            if (out != null) {
                return out;
            }
            // 本页一行都没展开出来（全 NULL / 空数组）→ 继续拉下一页，绝不返回空页
        }
    }

    /** 展开一页；无可展开行时返回 null（调用方继续拉下一页）。 */
    private Page expand(Page input) {
        VectorSchemaRoot inRoot = input.getRoot();
        FieldVector exprVector = null;
        // 中途抛错（非法 JSON / 非数组）时这些已分配的列必须释放，交出去后置 null
        List<FieldVector> outVectors = null;
        try {
            BufferAllocator evalAllocator = inRoot.getSchema().getFields().isEmpty()
                    ? allocator : inRoot.getVector(0).getAllocator();
            exprVector = evaluator.evaluate(inRoot, evalAllocator, "__unnest_expr");

            List<Field> inFields = inRoot.getSchema().getFields();
            List<Field> outFields = new ArrayList<>(inFields.size() + 2);
            outVectors = new ArrayList<>(inFields.size() + 2);
            List<FieldVector> inVectors = new ArrayList<>(inFields.size());
            for (Field f : inFields) {
                FieldVector src = inRoot.getVector(f.getName());
                FieldVector copy = f.createVector(allocator);
                copy.setInitialCapacity(input.getRowCount());
                copy.allocateNew();
                inVectors.add(src);
                outVectors.add(copy);
                outFields.add(f);
            }
            VarCharVector elements = (VarCharVector) elementField.createVector(allocator);
            elements.setInitialCapacity(input.getRowCount());
            elements.allocateNew();
            outFields.add(elementField);
            outVectors.add(elements);

            BigIntVector ordinals = null;
            if (ordinalityField != null) {
                ordinals = (BigIntVector) ordinalityField.createVector(allocator);
                ordinals.setInitialCapacity(input.getRowCount());
                ordinals.allocateNew();
                outFields.add(ordinalityField);
                outVectors.add(ordinals);
            }

            int outRow = 0;
            for (int row = 0; row < input.getRowCount(); row++) {
                String json = readJson(exprVector, row);
                if (json == null || json.isBlank()) {
                    continue;
                }
                JsonNode node;
                try {
                    node = MAPPER.readTree(json);
                } catch (Exception e) {
                    throw new IllegalStateException("UNNEST 的输入不是合法 JSON（表达式："
                            + expressionText + "，值：" + abbreviate(json) + "）", e);
                }
                if (node == null || node.isNull()) {
                    continue;
                }
                if (!node.isArray()) {
                    throw new IllegalStateException("UNNEST 的输入必须是 JSON 数组（表达式："
                            + expressionText + "，值：" + abbreviate(json) + "）");
                }
                int ord = 1;
                for (JsonNode element : node) {
                    for (int c = 0; c < inVectors.size(); c++) {
                        FieldVector src = inVectors.get(c);
                        FieldVector dst = outVectors.get(c);
                        if (src.isNull(row)) {
                            dst.setNull(outRow);
                        } else {
                            dst.copyFromSafe(row, outRow, src);
                        }
                    }
                    String text = elementText(element);
                    if (text == null) {
                        elements.setNull(outRow);
                    } else {
                        elements.setSafe(outRow, text.getBytes(StandardCharsets.UTF_8));
                    }
                    if (ordinals != null) {
                        ordinals.setSafe(outRow, ord++);
                    }
                    outRow++;
                }
            }

            if (outRow == 0) {
                return null; // 列由 finally 关闭
            }
            for (FieldVector v : outVectors) v.setValueCount(outRow);
            Page out = Page.fromRoot(new VectorSchemaRoot(outFields, outVectors, outRow));
            outVectors = null; // 所有权交给返回页
            return out;
        } finally {
            if (outVectors != null) {
                for (FieldVector v : outVectors) {
                    try {
                        v.close();
                    } catch (RuntimeException ignored) {
                        // 释放失败不能盖住原始异常
                    }
                }
            }
            if (exprVector != null) exprVector.close();
            input.close();
        }
    }

    /** 表达式结果 → JSON 文本；SQL NULL → null。 */
    private static String readJson(FieldVector vector, int row) {
        if (vector.isNull(row)) return null;
        Object value = vector.getObject(row);
        return value == null ? null : value.toString();
    }

    /** JSON 元素 → 输出文本：标量取文本（字符串不带引号）、对象/数组取 JSON、null → NULL。 */
    private static String elementText(JsonNode element) {
        if (element == null || element.isNull()) return null;
        return element.isValueNode() ? element.asText() : element.toString();
    }

    private static String abbreviate(String s) {
        return s.length() <= 120 ? s : s.substring(0, 120) + "…";
    }

    @Override
    protected void doClose() {
        if (upstream != null) {
            upstream.close();
        }
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }
}
