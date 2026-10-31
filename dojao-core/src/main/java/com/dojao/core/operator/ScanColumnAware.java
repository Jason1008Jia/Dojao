package com.dojao.core.operator;

import java.util.List;

/**
 * 「本算子会派发哪些基表扫描、各自请求了哪些列」的<b>只读自述</b>契约。
 *
 * <p>存在的理由：分布式扫描的列集在规划期由 Planner 手抄给算子（{@link List}{@code <String>} 构造参数），
 * 而「空列集 = 连接器读全表」的约定使<b>漏传不会报错、只会静默 10× I/O</b>。历史 bug：
 * {@code Planner.tryBuildChainedWindow} 用无列集重载构造 {@link DistributedChainedOperator}，把列丢了。
 * 本契约让每个算子自己说清「我扫了哪些表、请求了哪些列」，供 {@code ColumnPruningAudit} 在规划期
 * 统一审计 —— 漏传即报错/告警，不再静默。</p>
 *
 * <p>每个 {@link AbstractDistributedOperator} 子类<b>必须</b>实现（抽象方法），所以新增路径不会「忘记自述」——
 * 这是相对「在手抄点逐个加断言」的关键差别。</p>
 *
 * @author jiaxin
 */
public interface ScanColumnAware {

    /**
     * 一条基表扫描的列集描述。
     *
     * @param relation {@code catalog.schema.table}（诊断文案用）
     * @param columns  请求的列集；{@code null}/空 的含义见 {@code derived} / {@code optional}
     * @param derived  true = 该关系的列由上游流水线喂入（派生源），不走基表扫描 —— 不参与列裁剪审计
     * @param optional true = 本算子的列集可空（如列集随 {@code ReadRequest} 携带、空 = 连接器语义上的「读全表」），
     *                 拿不到「应当裁剪」的证据 ⇒ 不作违规判定
     */
    record ScanColumn(String relation, List<String> columns, boolean derived, boolean optional) {

        /** 必须带列集的基表扫描（Planner 漏传即违规）。 */
        public static ScanColumn required(String relation, List<String> columns) {
            return new ScanColumn(relation, columns, false, false);
        }

        /** 列由上游流水线喂入的派生源（无基表扫描）。 */
        public static ScanColumn pipeline(String relation) {
            return new ScanColumn(relation, List.of(), true, true);
        }

        /** 列集可空的基表扫描（列随 {@code ReadRequest} 走，空 = 连接器约定的读全表）。 */
        public static ScanColumn optional(String relation, List<String> columns) {
            return new ScanColumn(relation, columns, false, true);
        }

        /** 是否为「要求有列却拿到空列集」的违规。 */
        public boolean violation() {
            return !derived && !optional && (columns == null || columns.isEmpty());
        }
    }

    /** 本算子派发的全部基表扫描列集（顺序 = 扫描先后；无基表扫描 → 空表）。 */
    List<ScanColumn> scanColumnSets();
}
