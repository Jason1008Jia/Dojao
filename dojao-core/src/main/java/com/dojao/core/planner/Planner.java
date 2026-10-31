package com.dojao.core.planner;

import com.dojao.core.config.ResourceConfig;
import com.dojao.core.operator.*;
import com.dojao.core.operator.expr.ExprBuiltins;
import com.dojao.core.operator.expr.ExprEvaluator;
import com.dojao.core.operator.expr.ExprNode;
import com.dojao.core.operator.expr.ExprParser;
import com.dojao.core.function.scalar.ScalarExpr;
import com.dojao.core.function.scalar.ScalarFunctionRegistry;
import com.dojao.core.function.table.TableFunctionOperator;
import com.dojao.core.function.table.TableFunctionRegistry;
import com.dojao.core.function.table.UnnestOperator;
import com.dojao.core.resource.JoinCardinality;
import com.dojao.core.resource.JoinDecider;
import com.dojao.core.runtime.master.DefaultWorkerSelector;
import com.dojao.core.runtime.master.StageScheduler;
import com.dojao.core.runtime.master.WorkerRegistration;
import com.dojao.core.runtime.master.WorkerRegistry;
import com.dojao.core.runtime.master.WorkerSelector;
import com.dojao.grammar.analyzer.expression.Expression;
import com.dojao.grammar.analyzer.plan.LogicalPlan;
import com.dojao.spi.bean.Column;
import com.dojao.spi.bean.ReadRequest;
import com.dojao.spi.bean.TableStats;
import com.dojao.spi.service.DojaoConnector;
import com.dojao.spi.service.OverwriteEnable;
import com.dojao.spi.function.AggregateFunction;
import com.dojao.spi.function.AggregateFunctionRegistry;
import com.dojao.spi.function.CollectAggregateFunction;
import com.dojao.spi.function.CollectAggregateRegistry;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.math.BigDecimal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 物理规划器 —— 把优化后的逻辑计划（{@link LogicalPlan.Select}）转换为可执行的物理算子树。
 *
 * <h3>转换规则（自上而下）</h3>
 * <ol>
 *   <li><b>FROM</b>：每个 {@link LogicalPlan.ResolvedRelation} → {@link ParallelTableScanOperator}
 *       （并行度 = 1 即等价串行表扫描），带上列裁剪（relation.columns）与谓词下推
 *       （relation.filter + 单表 WHERE）。</li>
 *   <li><b>JOIN</b>：左深树重建，ON 条件在顶层 AND 处切分为等值合取，
 *       两侧各经 {@link HashPartitionOperator} 按 join key 分区 → 恒定并行
 *       {@link HashJoinOperator}（每分区一个工作线程独立 build/probe/send）。</li>
 *   <li><b>WHERE（残留）</b>：跨表/复杂谓词 → {@link FilterOperator}（仅支持简单比较）。</li>
 *   <li><b>GROUP BY / 聚合</b>：→ {@link HashAggregationOperator}，聚合函数映射到
 *       {@link AggregationSpec}，输出列名取别名（无别名取归一化文本）。</li>
 *   <li><b>HAVING</b>：把聚合调用文本替换为输出列名 → {@link FilterOperator}。</li>
 *   <li><b>窗口函数</b>：按 (PARTITION BY, ORDER BY) 分组 →
 *       {@link WindowSortOperator} → {@link WindowFunctionOperator}。</li>
 *   <li><b>SELECT 投影</b>：{@link ProjectOperator}，按输出列名投影（SELECT * 保留 *，运行时展开全列）。</li>
 * </ol>
 *
 * <p><b>v1 限制</b>：非等值 JOIN、任意表达式（amount*2）、FROM 子查询/CTE、标量子查询、
 * GROUP BY 表达式、COUNT(DISTINCT)、ORDER BY/LIMIT/OFFSET/DISTINCT（分析器已丢弃）、
 * 普通列 AS 别名（投影保持物理列名）均不支持。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/8/2
 */
public final class Planner {

    private final PlanningContext context;

    /**
     * 顶层 LIMIT 的截断下推行数（= offset + limit；0 = 不下推）。由 {@link #withScanFetchRows}
     * 在「扫描行与最终输出一一对应」的简单单表查询上设置，规划期交给扫描算子落到数据源
     * （{@code SELECT ... LIMIT n}），使数据源读满即停，而不是靠下游关游标来掐断。
     */
    private final long scanFetchRows;

    /**
     * 被物化的「多引用 CTE」查询体 → 共享存储。键按<b>对象身份</b>比较：{@code inlineCtes} 会把
     * 同一个查询体对象塞进每个引用点，结构相等（record equals）的同形查询体不能混为一谈。
     */
    private final Map<LogicalPlan, MaterializedCteScanOperator.Store> materializedCtes =
            new IdentityHashMap<>();

    /**
     * 待生效的多维聚合展开（{@code ROLLUP/CUBE/GROUPING SETS}）。
     *
     * <p>{@link #buildGroupingSetsUnion} 选定「一趟扫描」形态后把展开描述放在这里，紧接着
     * {@code buildQuery(leg)} 在<b>入口处取走并清空</b>（{@code 取走即清空}保证腿内的派生表
     * 递归规划不会误取），聚合层据此插入展开并按 {@code (维度…, __grouping_id)} 分组。</p>
     */
    private FragmentSpec.ExpandStep pendingExpand;

    public Planner(PlanningContext context) {
        this(context, 0L);
    }

    private Planner(PlanningContext context, long scanFetchRows) {
        this.context = context;
        this.scanFetchRows = Math.max(0L, scanFetchRows);
    }

    /**
     * 派生一份以指定并行度构建物理计划的 Planner（动态资源分配：每 query 初始并行度
     * 决策后按授予值重规划）。其余上下文（连接器 / join 算法 / 内存 / 分布式开关）原样保留。
     */
    public Planner withParallelism(int parallelism) {
        PlanningContext ctx = context;
        return new Planner(new PlanningContext(ctx.connectorProvider(), ctx.joinAlgorithm(),
                ctx.memoryLimit(), parallelism, ctx.workerGateway(), ctx.workerSelector(),
                ctx.workerHealthTimeoutMs(), ctx.distributedExecution(), ctx.localScanOnly()),
                scanFetchRows);
    }

    /**
     * 派生一份强制本地扫描的 Planner（小 LIMIT 简单单表查询优化：不派发 Worker，Master 直读）。
     * 其余上下文原样保留。
     */
    public Planner withLocalScanOnly(boolean on) {
        PlanningContext ctx = context;
        return new Planner(new PlanningContext(ctx.connectorProvider(), ctx.joinAlgorithm(),
                ctx.memoryLimit(), ctx.parallelism(), ctx.workerGateway(), ctx.workerSelector(),
                ctx.workerHealthTimeoutMs(), ctx.distributedExecution(), on), scanFetchRows);
    }

    /**
     * 派生一份带「顶层 LIMIT 截断下推」的 Planner。调用方须先确认查询形态安全：单表、无 join /
     * 派生表 / 聚合 / 窗口 / ORDER BY，此时扫描行与 {@link LimitOperator} 的输出一一对应，
     * 让数据源少读不会改变结果。行数取 {@code offset + limit}（offset 由 LimitOperator 照常跳过）。
     */
    public Planner withScanFetchRows(long rows) {
        return new Planner(context, rows);
    }

    /** 逻辑计划 → 物理计划入口。 */
    public PhysicalPlan plan(LogicalPlan logicalPlan) {
        // WITH（CTE）先内联为派生表：任何语句类型里出现的 With 都在此被展开，
        // 后续物理规划只面对 Select（FROM 里 CTE 引用已替换为 subquery 槽）。
        logicalPlan = inlineCtes(logicalPlan);
        // 多引用 CTE 物化：内联后同一查询体对象出现在 N 个引用点 ⇒ 只规划/执行一次，
        // 结果落盘供各引用重放（否则源表被读 N 次，见漏斗 SQL 4 份分布式聚合）。
        if (ResourceConfig.cteMaterializeEnabled()) {
            prepareCteMaterialization(logicalPlan);
        }
        PhysicalNode root;
        boolean skipColumnPruning = false;
        // 顶层查询节点（Select / SetOperation，含 EXPLAIN 内层）：其 orderBy/limit/offset 决定
        // 是否在列裁剪之后包全局 Sort + Limit（INSERT/CTAS 内层 SELECT 的排序不接线，已知限制）。
        LogicalPlan orderSource = null;
        if (logicalPlan instanceof LogicalPlan.Select s) {
            orderSource = s;
            root = buildQuery(s, null);
        } else if (logicalPlan instanceof LogicalPlan.SetOperation so) {
            // 集合操作（UNION / UNION ALL）：两侧递归降级后经对齐型 UnionAll 拼接；
            // UNION(distinct) 额外包全列去重。INTERSECT/EXCEPT 在 buildSetOperation 明确 unsupported。
            orderSource = so;
            root = buildSetOperation(so);
        } else if (logicalPlan instanceof LogicalPlan.Explain e) {
            // EXPLAIN 的语句可能是集合运算 / INSERT / CTAS（EXPLAIN a UNION b、EXPLAIN INSERT ...）：
            // 各自走对应构建器而非一律 asSelect；写入类同顶层 Insert/CTAS 一样跳过列裁剪。
            LogicalPlan stmt = e.statement();
            if (stmt instanceof LogicalPlan.SetOperation so) {
                orderSource = so;
                root = buildSetOperation(so);
            } else if (stmt instanceof LogicalPlan.Insert ins) {
                root = buildInsert(ins);
                skipColumnPruning = true;
            } else if (stmt instanceof LogicalPlan.CreateTableAsSelect c) {
                root = buildCreateTableAsSelect(c);
                skipColumnPruning = true;
            } else {
                orderSource = asSelect(stmt);
                root = buildQuery(asSelect(stmt), null);
            }
        } else if (logicalPlan instanceof LogicalPlan.Insert ins) {
            root = buildInsert(ins);
            skipColumnPruning = true;   // 写入需要全部列，不做列裁剪
        } else if (logicalPlan instanceof LogicalPlan.CreateTableAsSelect ctas) {
            root = buildCreateTableAsSelect(ctas);
            skipColumnPruning = true;   // 写入需要全部列，不做列裁剪
        } else {
            throw unsupported("语句类型: " + logicalPlan.getClass().getSimpleName());
        }
        // 递归列裁剪：从结果列反推上游需求，全树插入 Project + 扫描推下列选择（只顶层调用一次）
        if (!skipColumnPruning) {
            root = optimizeColumns(root);
        }
        // 顶层 ORDER BY / LIMIT / OFFSET：裁剪后按最终输出列解析键并包全局 Sort + Limit
        if (orderSource instanceof LogicalPlan.Select sel) {
            root = applyOrderLimit(root, sel.orderBy(), sel.limit(), sel.offset());
        } else if (orderSource instanceof LogicalPlan.SetOperation sop) {
            root = applyOrderLimit(root, sop.orderBy(), sop.limit(), sop.offset());
        }
        // 估算行数自下而上传播（EXPLAIN 用）：叶子 / join 已有估算，透传型节点沿链继承
        root = propagateEstimates(root);
        // 列裁剪审计（保险丝）：让漏传列集的分布式扫描在规划期告警 / 报错，而非运行期静默读全表
        ColumnPruningAudit.audit(root);
        String explain = PhysicalPlanPrinter.print(root);
        return new PhysicalPlan(root, explain);
    }

    // ==================== 顶层 ORDER BY / LIMIT / OFFSET ====================

    /**
     * ORDER BY 键解析结果：列名 + 每键升/降序标志 + 表达式键的物化投影。
     *
     * <p>{@code columns} 是排序层实际使用的列名：普通列键为输出列名；表达式键（CASE WHEN / 函数 /
     * 算术等）为合成列名（{@code __order_key_N}），对应 {@code synthetic} 里的物化投影。键个数 =
     * {@code ascending} 个数 = {@code columns} 个数。{@code synthetic} 为空即纯列键（可走 top-N 下推）。</p>
     */
    private record OrderKeys(List<String> columns, List<Boolean> ascending,
                            List<ScalarProjectOperator.Projection> synthetic) {}

    /**
     * 把 ORDER BY / LIMIT / OFFSET 包到已建好的结果树顶层（SELECT 或集合运算结果之上）。
     *
     * <p>排序内核复用 {@link WindowSortOperator}：<b>空 groupKeyColumns = 全局单分组</b>（走
     * {@code RowAccumulator.addPage} 的 NULL_KEY 单分组 + 全局行号排序），免费获得溢写 + Arrow
     * 类型比较 + 分页输出，与既有全局窗口（{@code OVER (ORDER BY x)}）同一条路径。因上游可能是
     * 分布式算子 / 单机聚合这类带分区号的页生产者，先经 {@link SinglePartitionOperator} 把分区号
     * 归一到 0，防止 AbstractSpillableOperator 按 pid 分槽把全局排序切成多段。LIMIT/OFFSET 再由
     * {@link LimitOperator} 流式截断。</p>
     *
     * @return 包好 Sort/Limit 后的新根（无 ORDER BY 且无 LIMIT/OFFSET 时原样返回）
     */
    private PhysicalNode applyOrderLimit(PhysicalNode root, List<LogicalPlan.SortItem> orderBy,
                                         Optional<Long> limit, Optional<Long> offset) {
        PhysicalNode result = root;
        boolean hasLimit = limit.isPresent() || (offset.isPresent() && offset.get() > 0);
        if (orderBy.isEmpty() && !hasLimit) {
            return result;
        }
        // 排序键：SELECT 输出列（输出列名 / 别名 / 位置序号）或可求值表达式（ORDER BY CASE WHEN ...）
        // —— 后者物化为合成列再排序，均不静默忽略
        if (!orderBy.isEmpty()) {
            OrderKeys ok = resolveOrderKeys(orderBy, root.outputColumns());
            boolean hasExprKey = !ok.synthetic().isEmpty();
            // 表达式键（CASE WHEN / IF / 算术 / 布尔）不在 root 输出列里，无法在分布式切片的 Worker 端
            // 求值 —— 先在 root 之上把表达式物化为合成列（__order_key_N），全局 Sort 按合成列排序，
            // 事后剔除合成列恢复原始输出列。
            PhysicalNode sortInput = hasExprKey ? materializeOrderKeys(root, ok) : root;
            // ---- top-N 截断下推（Phase 1）：Master 排序上游若直接是分布式叶子且带有限 LIMIT ----
            // 下推 gate：① 有界 LIMIT（keepRows=offset+limit 有界才值得截断）；② 排序上游是分布式
            // 叶子（DistributedScanOperator / AbstractDistributedOperator，无 Master 中间变换）→ 叶子
            // 在各 Worker 产出缝本地排序 + 截断到 offset+limit 行，Master 只收 P×(offset+limit) 候选集，
            // 免去全量收拢单点排序。命中即把尾挂到叶子（dispatchStage/scanAndSend 读它拼片段/载荷），
            // Master 下方既有全局 Sort+Limit 照跑 —— 候选集 = 各切片 top-(offset+limit) 并集，正确性见
            // OrderByTail javadoc（rank_global(r) ≥ rank_local(r)）。未命中（无 LIMIT / 非叶子）→ 原全量路径。
            // ★表达式键禁用下推：Worker 端按列名排序，拿不到仅 Master 端才计算的合成键列。
            if (limit.isPresent() && !hasExprKey) {
                Operator preSort = root.operator();
                FragmentSpec.SortLimitStep pushdown = new FragmentSpec.SortLimitStep(
                        ok.columns(), ok.ascending(), offset.orElse(0L), limit.get(), context.memoryLimit());
                if (preSort instanceof AbstractDistributedOperator ad) {
                    ad.setTail(pushdown);
                    System.err.printf("[Planner] top-N 下推命中 %s: keys=[%s] keep=%d%n",
                            ad.getClass().getSimpleName(), String.join(", ", ok.columns()),
                            pushdown.keepRows());
                } else if (preSort instanceof DistributedScanOperator ds) {
                    ds.setTail(pushdown);
                    System.err.printf("[Planner] top-N 下推命中 DistributedScan: keys=[%s] keep=%d%n",
                            String.join(", ", ok.columns()), pushdown.keepRows());
                }
            }
            // 分区归一 → 全局单分区排序（pid 恒 0；空 groupKeyColumns = 全部行一个全局分组）
            Operator unify = new SinglePartitionOperator(sortInput.operator());
            WindowSortOperator sort = new WindowSortOperator(
                    unify, List.of(), ok.columns(), ok.ascending(), context.memoryLimit());
            // 无 LIMIT 的全量全局排序护栏：整表收拢 Master 单点排序，超内存预算即溢写 → 写满 Master 磁盘、
            // 极慢且危险。按 query.sort.topSpillPolicy（默认 error）设策略拒绝/告警；带 LIMIT 的有界排序
            // 保留默认 SPILL（且是推荐形态，后续 top-N 下推会免去 Master 全量排序）。
            if (limit.isEmpty()) {
                String topPolicy = ResourceConfig.topOrderSortSpillPolicy();
                if ("error".equals(topPolicy)) {
                    sort.setSpillPolicy(AbstractSpillableOperator.SpillPolicy.ERROR);
                } else if ("warn".equals(topPolicy)) {
                    sort.setSpillPolicy(AbstractSpillableOperator.SpillPolicy.WARN);
                }
            }
            result = new PhysicalNode(
                    "Sort[keys=" + String.join(", ", ok.columns())
                            + ", asc=" + String.join(", ", ok.ascending().stream()
                                    .map(a -> a ? "ASC" : "DESC").toList()) + "]",
                    sort, sortInput.outputColumns(), sortInput);
            // 剔除合成排序键列，恢复原始输出列（仅表达式键路径需要）
            if (hasExprKey) {
                ProjectOperator drop = new ProjectOperator(result.operator(), root.outputColumns());
                result = new PhysicalNode("Project", drop, root.outputColumns(), result);
            }
        }
        if (hasLimit) {
            Operator lim = new LimitOperator(result.operator(), offset.orElse(0L), limit);
            result = new PhysicalNode(
                    "Limit[offset=" + offset.orElse(0L) + ", limit="
                            + (limit.isPresent() ? limit.get() : "ALL") + "]",
                    lim, root.outputColumns(), result);
        }
        return result;
    }

    /**
     * 把 ORDER BY 项解析成排序列名清单：位置序号（1 基）→ 第 N 个输出列；输出列名（含别名，大小写
     * 不敏感）→ 该列；否则若能作为投影表达式求值（{@code ORDER BY CASE WHEN ... END} / 函数 / 算术）
     * → 分配合成列 {@code __order_key_N} 并附物化投影，由调用方在排序前算出该列。三者皆非 → 抛错
     * （拒绝静默丢弃）。
     *
     * @param orderBy       ORDER BY 项（SemanticAnalyzer 已捕获，表达式为原文文本）
     * @param outputColumns 最终输出列名（规划后 root.outputColumns()，已含聚合/窗口别名）
     */
    private OrderKeys resolveOrderKeys(List<LogicalPlan.SortItem> orderBy,
                                       List<String> outputColumns) {
        List<String> columns = new ArrayList<>(orderBy.size());
        List<Boolean> ascending = new ArrayList<>(orderBy.size());
        List<ScalarProjectOperator.Projection> synthetic = new ArrayList<>();
        Set<String> usedNames = new LinkedHashSet<>(outputColumns);
        int seq = 0;
        for (LogicalPlan.SortItem item : orderBy) {
            String text = item.expression().text().trim().replace("`", "");
            ascending.add(item.ascending());
            // 位置序号（ORDER BY 1）
            if (isIntegerLiteral(text)) {
                int idx;
                try {
                    idx = Integer.parseInt(text);
                } catch (NumberFormatException e) {
                    throw unsupported("ORDER BY 序号越界: " + text);
                }
                if (idx < 1 || idx > outputColumns.size()) {
                    throw unsupported("ORDER BY 序号越界: " + idx + "（输出列共 "
                            + outputColumns.size() + " 列）");
                }
                columns.add(outputColumns.get(idx - 1));
                continue;
            }
            String bare = SqlTextUtil.stripQualifier(text);
            String matched = indexOfOutputColumn(outputColumns, bare);
            if (matched != null) {
                columns.add(matched);
                continue;
            }
            // 表达式排序键：物化为合成列（不与输出列/已分配名冲突）
            if (isProjectableExpr(text)) {
                String synth;
                do {
                    synth = "__order_key_" + seq++;
                } while (usedNames.contains(synth));
                usedNames.add(synth);
                synthetic.add(new ScalarProjectOperator.Projection(text, synth));
                columns.add(synth);
                continue;
            }
            throw unsupported("ORDER BY 列 " + text + " 不在输出列中（可用列: "
                    + String.join(", ", outputColumns) + "）");
        }
        return new OrderKeys(List.copyOf(columns), List.copyOf(ascending), List.copyOf(synthetic));
    }

    /**
     * 把 ORDER BY 表达式键物化为合成列：在 {@code root} 之上包一层 {@link ScalarProjectOperator}，
     * 输出列 = 原输出列 + 各合成键列（{@code __order_key_N}）。排序层按合成列名排序，事后由调用方
     * 再包一层 {@link ProjectOperator} 剔除合成列、恢复原始输出列。
     */
    private PhysicalNode materializeOrderKeys(PhysicalNode root, OrderKeys ok) {
        for (ScalarProjectOperator.Projection p : ok.synthetic()) {
            validateProjectionExpr(root, p.exprText(), "ORDER BY 表达式列");
        }
        List<String> outCols = new ArrayList<>(root.outputColumns());
        Set<String> computed = new LinkedHashSet<>();
        for (ScalarProjectOperator.Projection p : ok.synthetic()) {
            outCols.add(p.outputName());
            computed.add(p.outputName());
        }
        ScalarProjectOperator sp = new ScalarProjectOperator(
                root.operator(), outCols, computed, ok.synthetic());
        String desc = ok.synthetic().stream()
                .map(p -> p.exprText() + " -> " + p.outputName())
                .collect(java.util.stream.Collectors.joining(", "));
        return new PhysicalNode("ScalarProject[" + desc + "]", sp, outCols, root);
    }

    /** 大小写不敏感匹配输出列名，返回输出侧的规范名（与 Arrow schema 字段名精确一致）。 */
    private static String indexOfOutputColumn(List<String> outputColumns, String bare) {
        for (String c : outputColumns) {
            if (c.equalsIgnoreCase(bare)) return c;
        }
        return null;
    }

    private static boolean isIntegerLiteral(String text) {
        if (text.isEmpty()) return false;
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (ch < '0' || ch > '9') return false;
        }
        return true;
    }

    // ==================== 集合操作（UNION / UNION ALL）====================

    /**
     * UNION / UNION ALL 降级：两侧递归构建（Select → {@code buildQuery}，嵌套集合运算 → 递归本方法），
     * 校验两侧列数一致后经 {@link UnionAllOperator} 按规范槽位拼接（运行时把每腿页字段名统一到
     * 左侧列名，缺席/异名腿走对齐拷贝，同名腿零拷贝）。{@code UNION}(distinct) 在拼接之上再包
     * 一层全列去重聚合。
     */
    private PhysicalNode buildSetOperation(LogicalPlan.SetOperation so) {
        if (so.opType() != LogicalPlan.SetOpType.UNION) {
            throw unsupported("集合操作: " + so.opType() + "（当前仅支持 UNION / UNION ALL）");
        }
        PhysicalNode left = buildSetOperand(so.left());
        PhysicalNode right = buildSetOperand(so.right());
        List<String> leftCols = left.outputColumns();
        List<String> rightCols = right.outputColumns();
        if (leftCols.size() != rightCols.size()) {
            throw unsupported("UNION 两侧输出列数不一致: " + leftCols.size() + " vs " + rightCols.size());
        }
        if (leftCols.isEmpty()) {
            throw unsupported("UNION 无输出列");
        }
        // 规范输出列 = 左侧列名（标准 SQL：结果列名取首个查询）；两侧均为恒等槽位映射
        List<String> outCols = new ArrayList<>(leftCols);
        List<Field> staticFields = nullFieldList(outCols.size());
        UnionAllOperator unionOp = new UnionAllOperator(
                List.of(left.operator(), right.operator()), outCols, staticFields,
                List.of(identitySlots(outCols.size()), identitySlots(outCols.size())));
        PhysicalNode unionNode = new PhysicalNode("UnionAll", unionOp, outCols, left, right);
        if (so.distinct()) {
            return distinctOver(unionNode);
        }
        return unionNode;
    }

    /** 构建集合运算的一个操作数（SELECT 或嵌套集合运算）。 */
    private PhysicalNode buildSetOperand(LogicalPlan operand) {
        if (operand instanceof LogicalPlan.Select s) {
            return buildQuery(s, null);
        }
        if (operand instanceof LogicalPlan.SetOperation so) {
            return buildSetOperation(so);
        }
        throw unsupported("UNION 操作数类型: "
                + (operand == null ? "null" : operand.getClass().getSimpleName()));
    }

    /**
     * 全列去重（UNION distinct）。复用聚合机制：以全部输出列作分组键、聚合列为空，即"每组一行"
     * = DISTINCT 语义（NULL 视为同一组）。聚合哈希表内存可测可溢写。
     *
     * <p><b>分布式执行（消除最后一处 Master 单机聚合）</b>：开启分布式执行且网关支持
     * Master→Worker 数据直推（{@code supportsExchangePush}）时，把去重哈希表推到 Worker ——
     * Master 端 UnionAll 对齐单流按全列哈希分区，每分区一个 AggregateConsumer（空聚合）本地
     * 去重后回传（{@link DistributedUnionDistinctOperator}）；Master 只做无状态路由转发。
     * 否则回退 Master 本地聚合（单 JVM / 伪分布式网关零行为变化）。</p>
     */
    private PhysicalNode distinctOver(PhysicalNode child) {
        List<String> cols = new ArrayList<>(child.outputColumns());
        long aggMemoryLimit = Math.min(context.memoryLimit(), ResourceConfig.masterAggregationMemoryBytes());
        // 分布式门控：上游确为 UnionAll（其 doNext 有状态，pump 须单分发线程）→ 分布式去重
        if (context.distributedExecution() && child.operator() instanceof UnionAllOperator
                && context.workerGateway() != null && context.workerGateway().supportsExchangePush()) {
            List<WorkerRegistration> healthy = WorkerRegistry.get().healthy(context.workerHealthTimeoutMs());
            if (!healthy.isEmpty()) {
                int partitions = Math.max(1, context.parallelism());
                String queryId = "query-" + Long.toHexString(System.nanoTime());
                DistributedUnionDistinctOperator op = new DistributedUnionDistinctOperator(
                        queryId, child.operator(), cols, context.memoryLimit(), partitions,
                        context.workerGateway(), healthy);
                String name = "DistributedUnionDistinct[" + cols
                        + ", workers=" + healthy.size() + ", partitions=" + partitions + "]";
                // 叶子节点：UNION distinct 需全部输出列去重，无列可裁；UnionAll 子树由算子内部持有
                return new PhysicalNode(name, op, cols, List.of());
            }
        }
        HashAggregationOperator agg = new HashAggregationOperator(
                child.operator(), cols, List.of(), aggMemoryLimit);
        return new PhysicalNode("HashAggregation[Distinct]", agg, cols, child);
    }

    /** 均匀恒等槽位映射 [0,1,...,n-1]。 */
    private static int[] identitySlots(int n) {
        int[] slots = new int[n];
        for (int i = 0; i < n; i++) slots[i] = i;
        return slots;
    }

    /** n 个 null 的兜底字段列表（静态类型统一留空，运行时学）。 */
    private static List<Field> nullFieldList(int n) {
        List<Field> fields = new ArrayList<>(n);
        for (int i = 0; i < n; i++) fields.add(null);
        return fields;
    }

    // ==================== 多维聚合（CUBE/ROLLUP/GROUPING SETS）→ UNION ALL ====================

    /**
     * 多维聚合（{@code GROUP BY CUBE/ROLLUP/GROUPING SETS(...)}）的两条规划路径：
     *
     * <ol>
     *   <li><b>一趟扫描（默认）</b>：{@link #buildGroupingSetsExpand} —— 全部维度组合共用<b>一次</b>
     *       基表扫描 / join：{@link ExpandOperator} 把每行复制成 N 份（未参与维度置 NULL，追加
     *       {@code __grouping_id}），再按 {@code (全部维度…, gid)} 做<b>一次</b>聚合。
     *       {@code CUBE(a,b)} 由「扫 3 次」变成「扫 1 次、行数 ×3」。</li>
     *   <li><b>UNION ALL 多腿（回退）</b>：逐 grouping set 一条 {@code GROUP BY 子集} 的 SELECT 腿，
     *       各腿用 {@link UnionAllOperator} 对齐拼接（缺席维度由算子运行期补 typed NULL，类型学自
     *       其它腿的首个非空页）。配置关掉、或聚合项引用维度列（展开会把维度掩成 NULL，改变聚合
     *       结果）时走这条路 —— 结果与一趟扫描逐行相同，只是多扫 N-1 次表。</li>
     * </ol>
     *
     * <p>规范列两种路径一致 = [维度列（SELECT 出现序去重）…, 聚合列（SELECT 出现序，别名或自动名）…]。</p>
     */
    private PhysicalNode buildGroupingSetsUnion(LogicalPlan.Select select,
                                                Set<String> requiredOutputColumns) {
        if (select.groupBy().size() != 1) {
            throw unsupported("CUBE/ROLLUP/GROUPING SETS 须为 GROUP BY 的唯一元素（不支持与普通键混用）");
        }
        Expression.GroupingSets gs = (Expression.GroupingSets) select.groupBy().get(0);
        if (!select.scalarSubqueries().isEmpty()) {
            throw unsupported("CUBE/ROLLUP/GROUPING SETS 含标量子查询");
        }

        // 1) 分类原 SELECT 项：维度列（裸列标识符）∪ 聚合项；窗口/星号/表达式列明确不支持
        // 分组集合成员 / SELECT 列统一按去限定符的单标识符比较（t.a 与 a 视作同维度）。
        Set<String> memberNorm = new HashSet<>();
        for (List<Expression> set : gs.sets()) {
            for (Expression m : set) memberNorm.add(plannerNorm(SqlTextUtil.stripQualifier(m.text())));
        }
        List<String> dimIds = new ArrayList<>();
        List<LogicalPlan.SelectItem> aggItems = new ArrayList<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            String text = item.expression().text().trim();
            // 聚合项按「整项聚合 <b>或</b> 聚合嵌在表达式里」判定（与 buildAggregation 的
            // itemHasAggregate 同一口径）：`round(sum(amount), 2)` 这类包裹表达式在普通
            // GROUP BY 里本来就走内层聚合 + 标量投影，多腿 UNION 也必须以整项作为腿的
            // SELECT 项，否则会被误判成「既不是维度也不是聚合」而规划失败（压测缺陷 3）。
            if (itemHasAggregate(item)) {
                aggItems.add(item);
                continue;
            }
            if (item.expression() instanceof Expression.WindowFunction
                    || !item.windowCalls().isEmpty()) {
                throw unsupported("CUBE/ROLLUP/GROUPING SETS 的 SELECT 含窗口函数: " + text);
            }
            if (isStarExpansion(text)) {
                throw unsupported("CUBE/ROLLUP/GROUPING SETS 的 SELECT 不支持 *，须显式列出维度列与聚合");
            }
            String id = SqlTextUtil.stripQualifier(text).trim();
            if (!SqlTextUtil.isSingleIdentifier(id)) {
                throw unsupported("CUBE/ROLLUP/GROUPING SETS 的 SELECT 表达式: " + text);
            }
            if (!memberNorm.contains(plannerNorm(id))) {
                throw unsupported("SELECT 列 " + id + " 既不是分组维度列也不是聚合函数");
            }
            if (!containsPlannerNorm(dimIds, id)) dimIds.add(id);
        }
        if (aggItems.isEmpty()) {
            throw unsupported("CUBE/ROLLUP/GROUPING SETS 的 SELECT 必须含 ≥1 个聚合函数（纯维度多腿 = 重复 DISTINCT）");
        }

        // 2) 规范输出列按原 SELECT 项顺序（结果列头顺序与用户 SQL 一致）；维列去重、聚合按项逐位保留。
        //    各腿输出与规范槽位的对应由 childSlots 完成（UnionAll 按槽位重排），故腿内顺序无约束。
        List<String> canonical = new ArrayList<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            String text = item.expression().text().trim();
            if (itemHasAggregate(item)) {
                canonical.add(item.alias().orElse(aggOutputName(text)));
            } else {
                String id = SqlTextUtil.stripQualifier(text).trim();
                if (!containsPlannerNorm(canonical, id)) canonical.add(id);
            }
        }
        if (canonical.isEmpty()) {
            throw unsupported("CUBE/ROLLUP/GROUPING SETS 无输出列");
        }

        // 3) 一趟扫描（展开算子）形态：聚合项只要不引用维度列就成立 —— 展开会把「本组合不参与的
        //    维度」掩成 NULL，若聚合参数是维度列本身（如 max(category_name)），掩码会改变结果，
        //    与「各腿独立计算」不等价 ⇒ 退回多腿 UNION。
        //    ★判据 / 展开范围必须用<b>全部分组集合成员</b>（groupingDims），不能用 dimIds ——
        //    dimIds 只含 SELECT 里投影出来的维度（`SELECT dt, province … CUBE(dt, province,
        //    device_type)` 的 device_type 不在其中）。只用 dimIds 展开时掩码漏掉 device_type、
        //    分组键也少一个维度 ⇒ 各腿并组、行数与聚合值双错（比多腿少 4 行）。
        List<String> groupingDims = groupingSetDims(gs);
        if (ResourceConfig.groupingSetsExpandEnabled() && !aggregatesTouchDims(select, groupingDims)) {
            PhysicalNode expanded = buildGroupingSetsExpand(select, gs, dimIds, groupingDims);
            if (expanded != null) {
                return expanded;
            }
        }

        List<PhysicalNode> legNodes = new ArrayList<>();
        List<int[]> childSlots = new ArrayList<>();
        for (List<Expression> set : gs.sets()) {
            Set<String> setNorm = new HashSet<>();
            List<Expression> legGroupBy = new ArrayList<>();
            for (Expression m : set) {
                setNorm.add(plannerNorm(SqlTextUtil.stripQualifier(m.text())));
                legGroupBy.add(m);
            }
            List<LogicalPlan.SelectItem> legItems = new ArrayList<>();
            for (String d : dimIds) {
                if (setNorm.contains(plannerNorm(d))) {
                    legItems.add(new LogicalPlan.SelectItem(new Expression.OpaqueExpr(d), Optional.empty()));
                }
            }
            legItems.addAll(aggItems);
            LogicalPlan.Select leg = new LogicalPlan.Select(legItems,
                    select.fromRelations(), select.joins(), select.where(),
                    legGroupBy, select.having(), select.scalarSubqueries());
            PhysicalNode legNode = buildQuery(leg, null);
            int[] slots = new int[legNode.outputColumns().size()];
            for (int j = 0; j < slots.length; j++) {
                int idx = indexOfPlannerNorm(canonical, legNode.outputColumns().get(j));
                if (idx < 0) {
                    throw unsupported("CUBE 腿输出列 [" + legNode.outputColumns().get(j)
                            + "] 无法映射到规范列（腿: " + legItems + "）");
                }
                slots[j] = idx;
            }
            childSlots.add(slots);
            legNodes.add(legNode);
        }

        List<Operator> ops = new ArrayList<>(legNodes.size());
        for (PhysicalNode n : legNodes) ops.add(n.operator());
        UnionAllOperator union = new UnionAllOperator(ops, canonical, nullFieldList(canonical.size()), childSlots);
        return new PhysicalNode("UnionAll[" + gs.originalText() + "]", union, canonical, legNodes);
    }

    /**
     * 一趟扫描形态：<b>一条</b> SELECT 腿（SELECT 项按原顺序，维度列去重、聚合项原位）+ 全部维度
     * 参与的分组键，配合 {@link #pendingExpand} 让聚合层插展开算子。返回的节点复用腿的执行算子，
     * 只把展示名换成 {@code Expand[...]}。
     *
     * <p><b>为什么腿的输出列就是规范列</b>：腿的 SELECT 项与用户 SQL 同序同别名，投影产出的列名
     * 与多腿路径的 {@code canonical} 逐位相同 ⇒ 不需要 UnionAll 那样的槽位重排。</p>
     */
    private PhysicalNode buildGroupingSetsExpand(LogicalPlan.Select select,
                                                 Expression.GroupingSets gs,
                                                 List<String> dimIds,
                                                 List<String> groupingDims) {
        // 展开范围 = 全部分组集合成员（含未投影的维度）—— 掩码与分组键都得覆盖它们，
        // 否则「本组合不参与的维度」不会被掩成 NULL、分组键也少维度 ⇒ 并组 / 错值。
        // 成员不是单标识符（如 CUBE(upper(x))）时无法按名掩码 ⇒ 退回多腿（调用方按 null 处理）。
        List<String> expandDims = new ArrayList<>(groupingDims.size());
        for (String d : groupingDims) if (!SqlTextUtil.isSingleIdentifier(d)) return null;
        for (List<Expression> set : gs.sets()) {
            for (Expression m : set) {
                String d = canonicalDim(dimIds, m.text());
                if (!SqlTextUtil.isSingleIdentifier(d)) return null;
                if (!containsPlannerNorm(expandDims, d)) expandDims.add(d);
            }
        }
        // 每个分组集合的成员归一到维度列名大小写（维度 / 集合成员按去限定符比较）
        List<List<String>> sets = new ArrayList<>(gs.sets().size());
        for (List<Expression> set : gs.sets()) {
            List<String> members = new ArrayList<>(set.size());
            for (Expression m : set) members.add(canonicalDim(expandDims, m.text()));
            sets.add(members);
        }

        // 单腿 SELECT 项：维度列取首次出现（与 canonical 去重口径一致），聚合项原位
        List<LogicalPlan.SelectItem> items = new ArrayList<>();
        Set<String> emitted = new LinkedHashSet<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            if (itemHasAggregate(item)) {
                items.add(item);
                continue;
            }
            String id = SqlTextUtil.stripQualifier(item.expression().text().trim()).trim();
            if (emitted.add(plannerNorm(id))) {
                items.add(new LogicalPlan.SelectItem(new Expression.OpaqueExpr(id), Optional.empty()));
            }
        }
        List<Expression> groupBy = new ArrayList<>(expandDims.size());
        for (String d : expandDims) groupBy.add(new Expression.OpaqueExpr(d));

        LogicalPlan.Select leg = new LogicalPlan.Select(items,
                select.fromRelations(), select.joins(), select.where(),
                groupBy, select.having(), select.scalarSubqueries());
        this.pendingExpand = new FragmentSpec.ExpandStep(expandDims, sets);
        PhysicalNode legNode;
        try {
            legNode = buildQuery(leg, null);
        } finally {
            this.pendingExpand = null;
        }
        return new PhysicalNode("Expand[" + gs.originalText() + "]",
                legNode.operator(), legNode.outputColumns(), legNode.children());
    }

    /** 全部分组集合成员（去限定符、按归一化去重，保留首次出现的原文写法）。 */
    private static List<String> groupingSetDims(Expression.GroupingSets gs) {
        List<String> dims = new ArrayList<>();
        for (List<Expression> set : gs.sets()) {
            for (Expression m : set) {
                String id = SqlTextUtil.stripQualifier(m.text().trim()).trim();
                if (id.isEmpty() || containsPlannerNorm(dims, id)) continue;
                dims.add(id);
            }
        }
        return dims;
    }

    /** 集合成员文本 → 维度列名（按去限定符 + 归一化匹配，取 SELECT 里的写法）。 */
    private static String canonicalDim(List<String> dimIds, String memberText) {
        String id = SqlTextUtil.stripQualifier(memberText.trim()).trim();
        String n = plannerNorm(id);
        for (String d : dimIds) if (plannerNorm(d).equals(n)) return d;
        return id;
    }

    /**
     * 聚合项 / HAVING 文本是否引用了维度列 —— 是则不能用展开形态。
     *
     * <p>展开把「本维度组合不参与的维度」掩成 NULL（{@code __grouping_id} 保证与真实 NULL 可区分），
     * 于是 {@code sum(amount)} 这类不受影响的聚合照常正确，但 {@code max(category_name)} 在粗粒度
     * 组合里会变成 {@code max(NULL)} = NULL —— 而 UNION ALL 多腿语义里那条腿是在<b>真实值</b>上
     * 求极值。两者不等价 ⇒ 命中即回退多腿（多扫 N-1 次表，但结果正确）。</p>
     *
     * <p>判定是保守的文本词边界匹配：多识别一次只会回退到多腿路径，不会算错。</p>
     */
    private static boolean aggregatesTouchDims(LogicalPlan.Select select, List<String> dimIds) {
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            if (!itemHasAggregate(item)) continue;
            if (mentionsAny(item.expression().text(), dimIds)) return true;
        }
        return select.having().map(h -> mentionsAny(h.text(), dimIds)).orElse(false);
    }

    /** 文本里是否出现任一标识符（{@code \b} 口径：{@code id} 不匹配 {@code user_id}）。 */
    private static boolean mentionsAny(String text, List<String> identifiers) {
        if (text == null) return false;
        for (String id : identifiers) {
            if (id == null || id.isEmpty()) continue;
            Pattern p = Pattern.compile("(?<![\\w$])" + Pattern.quote(id) + "(?![\\w$])",
                    Pattern.CASE_INSENSITIVE);
            if (p.matcher(text).find()) return true;
        }
        return false;
    }

    /** 文本归一化（小写+去空白）用于维度/列名匹配。 */
    private static String plannerNorm(String s) {
        return SqlTextUtil.normalize(s.trim());
    }

    private static boolean containsPlannerNorm(List<String> list, String s) {
        String n = plannerNorm(s);
        for (String e : list) if (plannerNorm(e).equals(n)) return true;
        return false;
    }

    private static int indexOfPlannerNorm(List<String> list, String s) {
        String n = plannerNorm(s);
        for (int i = 0; i < list.size(); i++) if (plannerNorm(list.get(i)).equals(n)) return i;
        return -1;
    }

    // ==================== WITH / CTE 内联 ====================

    /**
     * 递归展开 WITH（CTE）：把 FROM 里对 CTE 的引用替换为内联派生表
     * （{@link LogicalPlan.ResolvedRelation#subquery()} 槽），使物理规划只面对 Select。
     * 无 WITH / 无 CTE 引用的计划原样返回（零开销）。非递归 CTE；同名 CTE 内层覆盖外层。
     */
    private static LogicalPlan inlineCtes(LogicalPlan plan) {
        return inlineCtes(plan, Map.of());
    }

    private static LogicalPlan inlineCtes(LogicalPlan plan, Map<String, LogicalPlan.CteDef> ctes) {
        if (plan instanceof LogicalPlan.With w) {
            // 新作用域：外层 CTE 可见 + 本层 CTE 按序定义（后者可引用前者），主查询见全部
            Map<String, LogicalPlan.CteDef> scope = new HashMap<>(ctes);
            for (LogicalPlan.CteDef cte : w.ctes()) {
                LogicalPlan body = inlineCtes(cte.query(), scope);
                LogicalPlan.CteDef def = new LogicalPlan.CteDef(cte.name(), cte.columns(), body);
                scope.put(cte.name(), def);
            }
            return inlineCtes(w.query(), scope);
        }
        if (plan instanceof LogicalPlan.Select s) {
            return inlineSelect(s, ctes);
        }
        if (plan instanceof LogicalPlan.Explain e) {
            return new LogicalPlan.Explain(inlineCtes(e.statement(), ctes), e.analyze(), e.verbose());
        }
        if (plan instanceof LogicalPlan.Insert ins) {
            return new LogicalPlan.Insert(ins.catalog(), ins.schema(), ins.table(), ins.columnAliases(),
                    ins.overwrite(), inlineCtes(ins.query(), ctes));
        }
        if (plan instanceof LogicalPlan.CreateTableAsSelect ctas) {
            return new LogicalPlan.CreateTableAsSelect(ctas.qualifiedName(), ctas.columnAliases(),
                    ctas.properties(), ctas.comment(), ctas.ifNotExists(), inlineCtes(ctas.query(), ctes));
        }
        if (plan instanceof LogicalPlan.CreateView cv) {
            return new LogicalPlan.CreateView(cv.qualifiedName(), cv.orReplace(), inlineCtes(cv.query(), ctes));
        }
        if (plan instanceof LogicalPlan.SetOperation so) {
            return new LogicalPlan.SetOperation(so.opType(), so.distinct(),
                    inlineCtes(so.left(), ctes), inlineCtes(so.right(), ctes),
                    so.orderBy(), so.limit(), so.offset());
        }
        return plan;
    }

    /** 内联 Select 的 FROM 关系（CTE 引用 → 派生表），并在派生表内递归展开嵌套 WITH。 */
    private static LogicalPlan inlineSelect(LogicalPlan.Select s, Map<String, LogicalPlan.CteDef> ctes) {
        List<LogicalPlan.ResolvedRelation> rels = new ArrayList<>(s.fromRelations().size());
        boolean changed = false;
        for (LogicalPlan.ResolvedRelation rel : s.fromRelations()) {
            LogicalPlan.ResolvedRelation out = inlineRelation(rel, ctes);
            if (out != rel) changed = true;
            rels.add(out);
        }
        if (!changed) {
            return s;
        }
        return new LogicalPlan.Select(s.selectItems(), rels, s.joins(), s.where(),
                s.groupBy(), s.having(), s.scalarSubqueries(), s.orderBy(), s.limit(), s.offset(),
                s.distinct());
    }

    /**
     * 内联单个 FROM 关系：catalog/schema 均为空且表名命中 CTE → 替换为 subquery 槽（内联派生表）；
     * 否则若该关系本身是派生表，递归展开其内部的 WITH / CTE 引用。
     */
    private static LogicalPlan.ResolvedRelation inlineRelation(LogicalPlan.ResolvedRelation rel,
                                                               Map<String, LogicalPlan.CteDef> ctes) {
        if (rel.catalog().isBlank() && rel.schema().isBlank() && ctes.containsKey(rel.table())) {
            LogicalPlan.CteDef def = ctes.get(rel.table());
            return new LogicalPlan.ResolvedRelation(rel.catalog(), rel.schema(), rel.table(),
                    rel.alias(), def.columns(), rel.filter(), Optional.of(def.query()));
        }
        if (rel.subquery().isPresent()) {
            LogicalPlan inner = inlineCtes(rel.subquery().get(), ctes);
            if (inner == rel.subquery().get()) {
                return rel;   // 内层无 WITH / CTE 引用 → 原样返回，避免对象重建
            }
            return new LogicalPlan.ResolvedRelation(rel.catalog(), rel.schema(), rel.table(),
                    rel.alias(), rel.columns(), rel.filter(), Optional.of(inner));
        }
        return rel;
    }

    // ==================== 多引用 CTE 物化 ====================

    /**
     * 找出内联后<b>同一查询体对象被引用 ≥2 次</b>的 CTE，登记为待物化。
     *
     * <p>引用次数按对象身份统计（{@code IdentityHashMap}）：{@code inlineCtes} 对每个引用点
     * 都放同一个 {@code def.query()} 对象，所以「同一对象出现多次」= 同一个 CTE 被多处 FROM。
     * 统计后<b>递归全树</b>登记：即使外层查询体已被物化，它内部的多引用 CTE 仍会因为
     * 该查询体只执行一次而只执行一次 —— 登记它不增加任何执行次数（幂等存储）。</p>
     *
     * <p>只物化 {@link LogicalPlan.Select} 形态的查询体；UNION 查询体等暂按旧行为逐引用规划。</p>
     */
    private void prepareCteMaterialization(LogicalPlan plan) {
        IdentityHashMap<LogicalPlan, Integer> counts = new IdentityHashMap<>();
        countCteRefs(plan, counts);
        registerSharedBodies(plan, counts);
    }

    private static void countCteRefs(LogicalPlan plan, IdentityHashMap<LogicalPlan, Integer> counts) {
        if (plan instanceof LogicalPlan.Select s) {
            for (LogicalPlan.ResolvedRelation rel : s.fromRelations()) {
                if (rel.subquery().isPresent()) {
                    LogicalPlan body = rel.subquery().get();
                    counts.merge(body, 1, Integer::sum);
                    countCteRefs(body, counts);
                }
            }
        } else if (plan instanceof LogicalPlan.SetOperation so) {
            countCteRefs(so.left(), counts);
            countCteRefs(so.right(), counts);
        } else if (plan instanceof LogicalPlan.Explain e) {
            countCteRefs(e.statement(), counts);
        } else if (plan instanceof LogicalPlan.Insert ins) {
            countCteRefs(ins.query(), counts);
        } else if (plan instanceof LogicalPlan.CreateTableAsSelect ctas) {
            countCteRefs(ctas.query(), counts);
        }
    }

    private void registerSharedBodies(LogicalPlan plan, IdentityHashMap<LogicalPlan, Integer> counts) {
        if (plan instanceof LogicalPlan.Select s) {
            for (LogicalPlan.ResolvedRelation rel : s.fromRelations()) {
                if (rel.subquery().isEmpty()) {
                    continue;
                }
                LogicalPlan body = rel.subquery().get();
                if (counts.getOrDefault(body, 0) > 1
                        && body instanceof LogicalPlan.Select sel
                        && !materializedCtes.containsKey(body)) {
                    String name = rel.table().isBlank() ? "cte" : rel.table();
                    materializedCtes.put(body, new MaterializedCteScanOperator.Store(
                            name, () -> planMaterializedBody(sel)));
                }
                registerSharedBodies(body, counts);
            }
        } else if (plan instanceof LogicalPlan.SetOperation so) {
            registerSharedBodies(so.left(), counts);
            registerSharedBodies(so.right(), counts);
        } else if (plan instanceof LogicalPlan.Explain e) {
            registerSharedBodies(e.statement(), counts);
        } else if (plan instanceof LogicalPlan.Insert ins) {
            registerSharedBodies(ins.query(), counts);
        } else if (plan instanceof LogicalPlan.CreateTableAsSelect ctas) {
            registerSharedBodies(ctas.query(), counts);
        }
    }

    /**
     * 规划一次 CTE 查询体的物理计划（含查询体自身的 ORDER BY / LIMIT / OFFSET —— 与派生表
     * 分支同一口径；{@code buildQuery} 只负责查询主体）。结果按<b>全列</b>物化，各引用点
     * 按自身需求由列裁剪在上层插入 Project。
     */
    private MaterializedCteScanOperator.PlannedBody planMaterializedBody(LogicalPlan.Select body) {
        PhysicalNode node = buildQuery(body, null);
        node = applyOrderLimit(node, body.orderBy(), body.limit(), body.offset());
        List<String> explain = new ArrayList<>();
        for (String line : PhysicalPlanPrinter.print(node).split("\n")) {
            explain.add(line);
        }
        return new MaterializedCteScanOperator.PlannedBody(
                node.operator(), node.outputColumns(), explain);
    }

    /** 物化 CTE 引用点的扫描叶子（普通叶子：列裁剪会在上层按需插 Project）。 */
    private PhysicalNode buildMaterializedCteScan(MaterializedCteScanOperator.Store store,
                                                  LogicalPlan.ResolvedRelation rel,
                                                  Optional<String> extraPredicate) {
        PhysicalNode node = new PhysicalNode("MaterializedCte[" + rel.table() + "]",
                new MaterializedCteScanOperator(store), store.columns(), List.of());
        if (extraPredicate.isPresent()) {
            node = applyFilterIfSimple(node, extraPredicate.get(), "物化 CTE 外层 WHERE");
        }
        return node;
    }

    /** INSERT INTO ... SELECT：内层 SELECT 读链 → TableSink 写目标表。INSERT OVERWRITE 走 buildOverwriteInsert。 */
    private PhysicalNode buildInsert(LogicalPlan.Insert ins) {
        if (ins.overwrite()) {
            return buildOverwriteInsert(ins);
        }
        LogicalPlan.Select inner = asSelect(ins.query());
        PhysicalNode innerRoot = buildQuery(inner, null);
        DojaoConnector connector = context.connectorProvider().get(ins.catalog());

        // 目标列名：列别名子句（INSERT INTO t(a,b)）优先，否则目标表全列（按序）。
        // 用于写路径组 INSERT INTO t (目标列) VALUES（按位置绑定），支持 SELECT 输出列名与目标列名不一致。
        List<String> targetColumns = null;
        if (ins.columnAliases().isPresent()) {
            targetColumns = ins.columnAliases().get();
        } else {
            try {
                targetColumns = connector.getTableColumns(ins.catalog(), ins.schema(), ins.table())
                        .stream().map(Column::name).toList();
            } catch (Exception e) {
                targetColumns = null; // 元数据不可得 → 退回用 Page schema 列名
            }
        }
        // 分布式写门控：内层查询的顶层就是分布式算子（join/聚合/窗口/UNION distinct，无 Project/
        // ScalarProject/CaseProject 包装）→ 把写目标挂到该算子，N 个分区在各归属 Worker 就地写走
        // 自己那份（数据不回 Master），Master 只收 N 个 FragmentFinished 判成功 + 求和行数。
        if (innerRoot.operator() instanceof AbstractDistributedOperator dist) {
            dist.setSink(new FragmentSpec.SinkStep(ins.catalog(), ins.schema(), ins.table(), targetColumns));
            return new PhysicalNode(
                    "DistributedWrite[" + ins.catalog() + "." + ins.schema() + "." + ins.table() + "]",
                    dist, innerRoot.outputColumns(), innerRoot);
        }
        // 否则回退：Master 侧 TableSinkOperator 逐页写出（含标量投影 / 列重排 / CASE 派生等非分布式包装，
        // v1 暂不下推投影描述）
        int writeParallelism = Math.max(1, context.parallelism());
        TableSinkOperator sink = new TableSinkOperator(
                innerRoot.operator(), connector,
                ins.catalog(), ins.schema(), ins.table(), false, false, targetColumns,
                writeParallelism);
        return new PhysicalNode(
                "TableSink[INSERT " + ins.catalog() + "." + ins.schema() + "." + ins.table() + "]",
                sink, innerRoot.outputColumns(), innerRoot);
    }

    /**
     * INSERT OVERWRITE ... SELECT（Feature B）—— staging 中转 + 全成或全不成，数据不回 Master。
     *
     * <p>门控与 Feature A（INSERT INTO）完全一致：仅"内层查询顶层 = 分布式算子"（join / 聚合 /
     * 窗口 / UNION distinct 顶层，无 Project/ScalarProject/CaseProject 包装）命中；纯扫描 / 复杂
     * 投影 / VALUES 等非分布式形态本轮直接报「暂不支持」（数据永不回 Master 铁律下不做 Master 侧
     * staging 兜底）。</p>
     *
     * <p>覆盖能力按<b>能力接口</b>探测（非枚举）：目标连接器须实现 {@link OverwriteEnable}，
     * 否则规划期即报「暂不支持」—— 后续新增数据源谁支持覆盖谁实现该接口，引擎零改动。</p>
     *
     * <p>执行期编排（全部挂在 {@link AbstractDistributedOperator#setOverwriteLifecycle}，EXPLAIN
     * 不 open 算子 → 零数据源副作用）：</p>
     * <ol>
     *   <li>{@code prepare}（doOpen 派发前）：目标表存在性 → {@code supportsOverwrite} 权限探针
     *       （不足 → 明确报错，不回退静默）→ {@code beginOverwrite} 建 staging（克隆目标表结构）→
     *       {@code setSink(staging)} —— N 个分区随后全部写 staging；</li>
     *   <li>{@code success}（全部 N 分区 FragmentFinished 后恰好一次）：{@code commitOverwrite}
     *       原子切换 staging → 原表（RENAME 无空窗期）；</li>
     *   <li>{@code abort}（doClose 时未 success）：{@code abortOverwrite} 删 staging —— 原表从未被触碰。</li>
     * </ol>
     */
    private PhysicalNode buildOverwriteInsert(LogicalPlan.Insert ins) {
        LogicalPlan.Select inner = asSelect(ins.query());
        PhysicalNode innerRoot = buildQuery(inner, null);
        DojaoConnector connector = context.connectorProvider().get(ins.catalog());
        if (!(connector instanceof OverwriteEnable oe)) {
            throw unsupported("INSERT OVERWRITE 目标数据源不支持覆盖（连接器未实现 OverwriteEnable）: "
                    + ins.catalog() + "." + ins.schema() + "." + ins.table());
        }
        // 目标列名（同 INSERT INTO）：列别名子句优先，否则目标表全列
        List<String> targetColumns = null;
        if (ins.columnAliases().isPresent()) {
            targetColumns = ins.columnAliases().get();
        } else {
            try {
                targetColumns = connector.getTableColumns(ins.catalog(), ins.schema(), ins.table())
                        .stream().map(Column::name).toList();
            } catch (Exception e) {
                targetColumns = null;   // 元数据不可得 → 退回用 Page schema 列名
            }
        }
        // 非分布式内层（纯扫描 / 复杂投影 / VALUES）：Master 侧 TableSinkOperator 逐页写出，覆盖
        // 生命周期（begin → 每页写 staging → 全部写完 commit / 失败 abort）由算子编排 ——
        // 数据仍不回 Master 的分布式路径之外，这条是本轮放宽的兜底（原直接报「暂不支持」）。
        if (!(innerRoot.operator() instanceof AbstractDistributedOperator dist)) {
            int writeParallelism = Math.max(1, context.parallelism());
            TableSinkOperator sink = new TableSinkOperator(
                    innerRoot.operator(), connector,
                    ins.catalog(), ins.schema(), ins.table(), false, false, targetColumns,
                    writeParallelism);
            sink.setOverwrite(oe);
            return new PhysicalNode(
                    "TableSink[OVERWRITE " + ins.catalog() + "." + ins.schema() + "." + ins.table() + "]",
                    sink, innerRoot.outputColumns(), innerRoot);
        }
        // staging 名在 prepare（执行期）由 beginOverwrite 生成；success/abort 闭包经 holder 取用。
        // prepare 里的探针/建 staging 是真副作用，必须执行期（doOpen）跑 → EXPLAIN 保持零数据源副作用。
        AtomicReference<String> staging = new AtomicReference<>();
        // targetColumns 在 if/else 分支被重赋值 → 非 effectively-final，prepare 闭包须捕获 final 副本
        final List<String> sinkColumns = targetColumns;
        Runnable prepare = () -> {
            if (!connector.tableExists(ins.catalog(), ins.schema(), ins.table())) {
                throw new IllegalStateException("INSERT OVERWRITE 目标表不存在: "
                        + ins.catalog() + "." + ins.schema() + "." + ins.table());
            }
            if (!oe.supportsOverwrite(ins.catalog(), ins.schema(), ins.table())) {
                throw new IllegalStateException("INSERT OVERWRITE 目标数据源权限不足，无法覆盖: "
                        + ins.catalog() + "." + ins.schema() + "." + ins.table());
            }
            staging.set(oe.beginOverwrite(ins.catalog(), ins.schema(), ins.table()));
            dist.setSink(new FragmentSpec.SinkStep(ins.catalog(), ins.schema(), staging.get(), sinkColumns));
        };
        Runnable success = () ->
                oe.commitOverwrite(ins.catalog(), ins.schema(), ins.table(), staging.get());
        Runnable abort = () -> {
            String s = staging.get();
            if (s != null) oe.abortOverwrite(ins.catalog(), ins.schema(), ins.table(), s);
        };
        dist.setOverwriteLifecycle(prepare, success, abort);
        return new PhysicalNode(
                "DistributedOverwrite[" + ins.catalog() + "." + ins.schema() + "." + ins.table() + "]",
                dist, innerRoot.outputColumns(), innerRoot);
    }

    /**
     * CREATE TABLE AS SELECT：解析目标表（要求全限定名）。分布式内层（顶层 = 分布式算子）→
     * Master 先按算子终端输出页建表、再把写目标挂到算子，各分区在各归属 Worker 就地写走自己那份
     * （数据不回 Master）；非分布式内层 → 旧 Master 侧 TableSinkOperator 逐页写出兜底。
     */
    private PhysicalNode buildCreateTableAsSelect(LogicalPlan.CreateTableAsSelect ctas) {
        List<String> qname = ctas.qualifiedName();
        String catalog, schema, table;
        if (qname.size() >= 3) {
            catalog = qname.get(0);
            schema = qname.get(1);
            table = qname.get(2);
        } else {
            throw unsupported("CTAS 目标表须为全限定名 catalog.schema.table，实际: "
                    + String.join(".", qname));
        }
        LogicalPlan.Select inner = asSelect(ctas.query());
        PhysicalNode innerRoot = buildQuery(inner, null);
        DojaoConnector connector = context.connectorProvider().get(catalog);
        // 分布式写门控（同 INSERT / INSERT OVERWRITE）：内层查询顶层就是分布式算子
        // （join / 聚合 / 窗口等 FragmentSpec 派发形态）→ 数据不回 Master，Master 只建表 + 收集行数
        if (innerRoot.operator() instanceof AbstractDistributedOperator dist) {
            return buildDistributedCreateTableAsSelect(ctas, innerRoot, connector,
                    catalog, schema, table, dist);
        }
        // 写并行度 = 上游扫描并行度（硬约束：上游多少 写入时就多少）
        int writeParallelism = Math.max(1, context.parallelism());
        TableSinkOperator sink = new TableSinkOperator(
                innerRoot.operator(), connector,
                catalog, schema, table, true, ctas.ifNotExists(), null,
                writeParallelism); // CTAS：建表即按 Page 列建，无目标列名映射
        return new PhysicalNode(
                "TableSink[CTAS " + catalog + "." + schema + "." + table + "]",
                sink, innerRoot.outputColumns(), innerRoot);
    }

    /**
     * CTAS 分布式分支 —— Master 先建目标表、各分区就地写（数据永不回 Master）。
     *
     * <p>目标表列 = 分布式算子的<b>终端输出页列</b>（{@code AbstractDistributedOperator.outputFields}：
     * 源列走连接器精确 {@code sourceFields}，计算列走聚合 / 窗口类型推导），与 Worker 回写的页 schema
     * 完全一致 —— {@code SinkStep.targetColumns=null} 按页列名绑定即可写对位。</p>
     *
     * <p>建表（DDL 副作用）挂在 {@code doOpen} 的 prepare 闭包 → EXPLAIN（不 open 算子）零副作用：
     * <ol>
     *   <li>{@code prepare}：createTable 建目标表 —— 返回 false（IF NOT EXISTS 且表已存在）→
     *       {@code skipDispatch()} 跳过派发、rows=0，不触碰既有表；返回 true → setSink 目标表，N 分区随后就地写；</li>
     *   <li>{@code abort}（doClose 时未成功）：刚建的新表任一分区失败 → DROP 刚建的表（全成或全不成，
     *       不留半成品新表）；表已存在跳过时不动。</li>
     * </ol></p>
     */
    private PhysicalNode buildDistributedCreateTableAsSelect(LogicalPlan.CreateTableAsSelect ctas,
                                                             PhysicalNode innerRoot, DojaoConnector connector,
                                                             String catalog, String schema, String table,
                                                             AbstractDistributedOperator dist) {
        // 目标表列 = 算子终端输出页列（源列精确 sourceFields + 计算列类型推导）；UNION distinct 等
        // 无法静态推导的形态在此抛 UnsupportedOperationException → CTAS 报「暂不支持」。
        List<Field> fields = dist.outputFields();
        if (fields.isEmpty()) {
            throw unsupported("CTAS 无法推导目标表列（输出列为空）: " + catalog + "." + schema + "." + table);
        }
        // 列别名子句（CREATE TABLE t (a,b) AS SELECT ...）：目标列按位置重命名；页列名 ≠ 目标列名
        // → 写路径用 targetColumns（位置绑定），不再按页列名绑定
        List<String> targetColumns = null;
        if (ctas.columnAliases().isPresent()) {
            List<String> aliases = ctas.columnAliases().get();
            if (aliases.size() != fields.size()) {
                throw unsupported("CTAS 列别名数量与输出列不一致: 别名 " + aliases.size()
                        + "，输出列 " + fields.size());
            }
            List<Field> renamed = new ArrayList<>(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                Field f = fields.get(i);
                renamed.add(new Field(aliases.get(i), f.getFieldType(), f.getChildren()));
            }
            fields = renamed;
            targetColumns = new ArrayList<>(aliases);
        }
        // 建表是否由本语句实际创建（false = IF NOT EXISTS 且已存在，跳过写且失败时不删既有表）
        AtomicBoolean created = new AtomicBoolean();
        // fields/targetColumns 在列别名分支被重赋值 → 非 effectively-final，prepare 闭包须捕获 final 副本
        final List<Field> ctasFields = fields;
        final List<String> ctasTargetColumns = targetColumns;
        Runnable prepare = () -> {
            boolean c = connector.createTable(catalog, schema, table, ctasFields, ctas.ifNotExists());
            created.set(c);
            dist.setSink(new FragmentSpec.SinkStep(catalog, schema, table, ctasTargetColumns));
            if (!c) dist.skipDispatch();   // 表已存在 → 不派发、rows=0（数据不回写）
        };
        // success：N 分区都写完即成功，数据已就地落表，无额外 commit
        Runnable success = () -> { };
        Runnable abort = () -> {
            // 建表后任一分区失败/取消 → 删掉本语句刚建的表（不留半成品新表）；连接器不支持删表则吞掉
            // （不掩盖主错误）；表已存在（未建）时不动既有表
            if (created.get()) {
                try { connector.dropTable(catalog, schema, table); }
                catch (Throwable ignored) { }
            }
        };
        dist.setOverwriteLifecycle(prepare, success, abort);
        return new PhysicalNode(
                "DistributedCTAS[" + catalog + "." + schema + "." + table + "]",
                dist, innerRoot.outputColumns(), innerRoot);
    }

    private LogicalPlan.Select asSelect(LogicalPlan plan) {
        if (plan instanceof LogicalPlan.Select s) return s;
        if (plan instanceof LogicalPlan.Explain e) return asSelect(e.statement());
        throw unsupported("语句类型: " + plan.getClass().getSimpleName());
    }

    // ==================== 查询主体 ====================

    /**
     * @param requiredOutputColumns 外层查询对本查询输出列的需求（用于列裁剪）；
     *                              null 表示按 SELECT 项自行判断（顶层查询或无需求）
     */
    private PhysicalNode buildQuery(LogicalPlan.Select select, Set<String> requiredOutputColumns) {
        // 本层的多维聚合展开（由 buildGroupingSetsUnion 放好）：取走即清空，腿内派生表的递归规划
        // 不会误取；未使用者（无聚合等）自然丢弃。
        FragmentSpec.ExpandStep expand = this.pendingExpand;
        this.pendingExpand = null;
        // 多维聚合（GROUP BY CUBE/ROLLUP/GROUPING SETS）：展开为逐 grouping set 的
        // GROUP BY 子集 SELECT 腿 → 各腿经对齐型 UnionAllOperator 拼接（缺席维度补 typed NULL）。
        // 该检测必须在任何单表 / 派生扫描构建之前命中，腿 Select 的 groupBy 已不含 GroupingSets，
        // 递归 buildQuery 不会二次进入本分支。
        if (select.groupBy().stream().anyMatch(g -> g instanceof Expression.GroupingSets)) {
            return buildGroupingSetsUnion(select, requiredOutputColumns);
        }
        List<LogicalPlan.ResolvedRelation> rels = select.fromRelations();
        boolean noFrom = rels.isEmpty();

        // 标量子查询（SELECT / WHERE 中的 (SELECT ...)）：有则需去相关（LEFT JOIN 内层聚合派生表），
        // WHERE 不能下推扫描（子查询值来自派生表 join），SELECT 项 / WHERE 文本替换为派生列。
        boolean hasSubquery = !select.scalarSubqueries().isEmpty();

        PhysicalNode current;
        if (noFrom) {
            // 无 FROM 的 SELECT：单行常量查询（SELECT 1 / SELECT concat('a', NULL)，也可作 UNION 分支）。
            // 数据源 = 恰好一行的 {@link SingleRowOperator}；表达式由后续投影 / 聚合 / 窗口照常求值。
            // * 无列可展开（且不得把内部占位列泄漏给用户）→ 规划期明确拒绝。
            for (LogicalPlan.SelectItem item : select.selectItems()) {
                if (isStarExpansion(item.expression().text().trim())) {
                    throw unsupported("无 FROM 的 SELECT 不支持 *（没有可展开的列）");
                }
            }
            current = new PhysicalNode("SingleRow",
                    new SingleRowOperator(), List.of(SingleRowOperator.DUMMY_COLUMN), List.of());
        } else if (select.joins().isEmpty()) {
            if (rels.size() == 1 && !isUnnestRelation(rels.get(0))) {
                // 单表：无标量子查询时 WHERE 直接下推到扫描（MySQL 可处理更广谓词）；
                // 有子查询时延后为 Filter（在去相关 LEFT JOIN 之后应用）。
                Optional<String> wherePred = (!hasSubquery && select.where().isPresent())
                        ? select.where().map(e -> SqlTextUtil.stripQualifiers(e.text()))
                        : Optional.empty();
                // 派生表：计算外层 SELECT 引用的列 → 向下传递用于列裁剪
                Set<String> derivedRequired = null;
                if (rels.get(0).subquery().isPresent()) {
                    derivedRequired = computeRequiredFromSelect(select);
                }
                // ★分区元数据扫描：聚合输入全是分区列（min/max/count(distinct) 分区列 + count(*)）
                //   时不读数据文件，只输出「一行为一个非空分区」的合成页。判据不满足 → 原样回退。
                PhysicalNode metadata = tryBuildPartitionMetadataScan(rels.get(0), select, wherePred);
                current = metadata != null ? metadata
                        : buildScan(rels.get(0), wherePred, derivedRequired);
            } else {
                // 逗号连接 → CROSS JOIN 链（UNNEST 关系按位置做行展开，不是独立扫描）
                current = null;
                for (LogicalPlan.ResolvedRelation rel : rels) {
                    if (rel.subquery().orElse(null) instanceof LogicalPlan.Unnest un) {
                        current = buildUnnest(current, un);
                        continue;
                    }
                    PhysicalNode n = buildScan(rel, Optional.empty(), null);
                    current = current == null ? n : buildCrossJoin(current, n);
                }
            }
        } else {
            current = buildJoins(rels, select.joins());
            // 外连接同名列消歧（LEFT/RIGHT/FULL 保留右键 → 与左键同名）：必须在 WHERE 之前完成，
            // 否则 WHERE b.key IS NULL 仍会按裸列绑到左列（见 aliasOuterJoinKeyRefs）。
            if (!hasSubquery) {
                OuterJoinRefRewrite rr = aliasOuterJoinKeyRefs(current, select);
                current = rr.node();
                select = rr.select();
            }
        }

        // ==================== 标量子查询去相关 ====================
        if (hasSubquery) {
            Decorrelation dec = decorrelate(select, current.outputColumns());
            // 标量子查询去相关 ⇒ 等价于「外层关系树 ⋈ 内层聚合派生表」的左深链，把它们追加到本查询的
            // rels / joins 后交给 buildJoins 统一分发：分布式开关打开且形态允许时，内层聚合与所有 join
            // 全在 Worker 执行（派生聚合流水线 + 分布式派生 join），Master 只收最终结果 —— 不再把内层
            // 聚合结果与外层数据都拉回 Master 本地做 HashJoin。
            //   ① 相关（等值）子查询 → LEFT JOIN 派生表；② <b>非相关</b>子查询（内层全局聚合恒 1 行）
            //      → CROSS JOIN 派生表（CROSS 的分布式落点是「单值广播」：一个聚合消费者算出那 1 行，
            //      REPLICATE 到每个 join 分区，本机空键哈希 join 即交叉连接 —— 见 StageScheduler）。
            //   链序：相关在前（其 ON 引用外层 + 先前追加的派生列）、非相关在后（恒 1 行，位置无关）。
            // 外层本身就是「逗号连接」（rels 多且无显式 join）时左深链的左端不唯一 ⇒ 保持原本地路径。
            // 形态不支持 / 未开分布式时 buildJoins 自然回退本地组装（CROSS 由 CrossJoinOperator 承接，
            // 与旧「逐个子查询 buildCrossJoin」逐句等价）。
            PhysicalNode joined = null;
            boolean outerIsLeftDeep = select.joins().isEmpty() ? rels.size() == 1 : true;
            if (outerIsLeftDeep && !dec.joins().isEmpty()) {
                List<DecorrelatedJoin> ordered = new ArrayList<>(dec.joins().size());
                for (DecorrelatedJoin dj : dec.joins()) {
                    if (dj.onCondition() != null) ordered.add(dj);      // 相关（LEFT JOIN）先
                }
                for (DecorrelatedJoin dj : dec.joins()) {
                    if (dj.onCondition() == null) ordered.add(dj);      // 非相关（CROSS）后
                }
                List<LogicalPlan.ResolvedRelation> chainRels = new ArrayList<>(rels);
                List<LogicalPlan.JoinInfo> chainJoins = new ArrayList<>(
                        select.joins().size() + ordered.size());
                chainJoins.addAll(select.joins());
                String leftAlias = select.joins().isEmpty()
                        ? alias(rels.get(0))
                        : normalize(select.joins().get(select.joins().size() - 1).rightAlias());
                for (DecorrelatedJoin dj : ordered) {
                    chainRels.add(dj.relation());
                    boolean cross = dj.onCondition() == null;
                    chainJoins.add(new LogicalPlan.JoinInfo(
                            cross ? LogicalPlan.JoinType.CROSS : LogicalPlan.JoinType.LEFT,
                            leftAlias, alias(dj.relation()),
                            new Expression.OpaqueExpr(cross ? "" : dj.onCondition())));
                    leftAlias = alias(dj.relation());
                }
                joined = buildJoins(chainRels, chainJoins);
            }
            if (joined != null) {
                current = joined;
            } else {
                for (DecorrelatedJoin dj : dec.joins()) {
                    PhysicalNode derivedNode = buildScan(dj.relation(), Optional.empty(), null);
                    if (dj.onCondition() != null) {
                        // 相关子查询：等值 LEFT JOIN（外层相关键 = 内层 GROUP BY 相关键）
                        LogicalPlan.JoinInfo ji = new LogicalPlan.JoinInfo(
                                LogicalPlan.JoinType.LEFT, "l", "r",
                                new Expression.OpaqueExpr(dj.onCondition()));
                        // 传 relation（而非 null）让 HashJoin 能按「派生表声明列 + 静态类型推断」预置右侧
                        // schema：内层被过滤成空集时 build 侧收不到任何数据页，没有声明列就只剩左列 ⇒
                        // 后续 WHERE 引用 __subq_val 报「不在上游输出中」。
                        current = buildJoin(current, derivedNode, ji, dj.relation());
                    } else {
                        // 非相关子查询：内层全局聚合恒 1 行 → CROSS JOIN（每外层行都匹配）
                        current = buildCrossJoin(current, derivedNode);
                    }
                }
            }
            // WHERE（重写后引用 __subq_val 派生列）：在 join 之后应用 Filter
            if (dec.rewrittenWhere().isPresent()) {
                current = applyFilterIfSimple(current, dec.rewrittenWhere().get().text(),
                        "标量子查询去相关后的 WHERE");
            }
            // 后续聚合 / 窗口 / 投影使用重写后的 select（子查询已替换为派生列，清空避免递归）
            select = new LogicalPlan.Select(dec.rewrittenItems(), rels, select.joins(),
                    dec.rewrittenWhere(), select.groupBy(), select.having(), List.of(),
                    List.of(), Optional.empty(), Optional.empty(), select.distinct());
        } else if (select.where().isPresent() && !select.joins().isEmpty()) {
            current = applyFilterIfSimple(current, select.where().get().text(),
                    "跨表/复杂 WHERE");
        } else if (select.where().isPresent()
                && (rels.size() > 1
                    || (rels.size() == 1 && isUnnestRelation(rels.get(0))))) {
            // 逗号连接 + 跨表 WHERE（单表已下推扫描）；单 UNNEST 关系没有扫描可下推，同样在此过滤
            current = applyFilterIfSimple(current, select.where().get().text(),
                    "逗号连接的跨表 WHERE");
        } else if (select.where().isPresent() && noFrom) {
            // 无 FROM 的 WHERE（如 SELECT 1 WHERE 1 = 0）：单行之上应用过滤
            current = applyFilterIfSimple(current, select.where().get().text(),
                    "无 FROM 的 SELECT WHERE");
        }

        // 聚合（GROUP BY 或含聚合函数）
        // itemHasAggregate：连「聚合套在表达式里」（round(sum(x),2)）也算 —— 否则这条 SELECT 会被
        // 当成纯投影，聚合函数原样进表达式求值 → 规划期报「找不到列」或运行期算错。窗口函数除外
        // （sum(x) OVER (…) 文本里含 sum(x)，但那是窗口不是聚合）。
        boolean hasAgg = select.selectItems().stream().anyMatch(Planner::itemHasAggregate);
        if (!select.groupBy().isEmpty() || hasAgg) {
            current = buildAggregation(current, select, expand);
        }

        // HAVING（在聚合之后、窗口之前）
        if (select.having().isPresent()) {
            current = buildHaving(current, select);
        }

        // 窗口函数（含「窗口嵌在表达式里」：gmv / sum(gmv) OVER ()）
        List<LogicalPlan.SelectItem> windowItems = select.selectItems().stream()
                .filter(it -> it.expression() instanceof Expression.WindowFunction
                        || !it.windowCalls().isEmpty())
                .toList();
        if (!windowItems.isEmpty()) {
            // 列裁剪：窗口算子上游只传入真正需要的列，避免 select a.*,b.* 携带全量列
            current = pruneForWindows(current, windowItems, select, requiredOutputColumns);
            current = buildWindows(current, windowItems, select);
        }

        // ★SELECT DISTINCT：去重节点必须**真的接进算子链**（PhysicalNode 只是计划的展示/裁剪元数据，
        //   执行走的是算子构造期固定的 upstream 引用），故两种形态分别在投影的「下/上」构造：
        //     ① 含星号展开（`select distinct *` / `distinct *, x`）：投影输出在计划期写作 "*"（真列名要
        //        运行期 Page.select 才知道）→ 不能当分组键。好在含星号时投影覆盖**全部上游列**，
        //        故把去重键取「上游全部列」、节点建在投影**之下**（buildProject 随后以它为上游）。
        //     ② 无星号：投影输出列是离散的具体列名（可能含求值表达式如 b+1）→ 去重必须落在投影**之上**
        //        （键是求值结果，在投影之下拿不到）。
        //   ORDER BY / LIMIT / OFFSET 由顶层 applyOrderLimit 在最终节点之上施加 —— 先去重再排序，
        //   LIMIT 数的是去重后的行，语义正确。
        boolean distinctStar = select.distinct() && select.selectItems().stream()
                .anyMatch(it -> isStarExpansion(it.expression().text().trim()));
        if (distinctStar) {
            current = buildDedupeNode(current);
        }

        // 最终投影
        current = buildProject(current, select);

        if (select.distinct() && !distinctStar) {
            current = buildDedupeNode(current);
        }
        return current;
    }

    /**
     * 去重节点 —— 复用「空聚合规格」的 {@link HashAggregationOperator}（输出 = 分组键 + 空聚合列 =
     * 每组一行），与 {@code UNION DISTINCT} 的 {@code distinctOver} 同一套机制，零新算子。
     * 键 = 上游全部输出列。刻意走 Master 本地：空规格的分布式片元路径未在真集群验证过。
     */
    private PhysicalNode buildDedupeNode(PhysicalNode upstream) {
        List<String> keys = new ArrayList<>(upstream.outputColumns());
        if (keys.isEmpty() || keys.contains("*")) {
            // 无具体列可作键（理论不出现）→ 不插去重节点，保持原计划而非抛错。
            return upstream;
        }
        long distinctMemoryLimit = Math.min(context.memoryLimit(), ResourceConfig.masterAggregationMemoryBytes());
        HashAggregationOperator dedupe = new HashAggregationOperator(
                upstream.operator(), keys, List.of(), distinctMemoryLimit);
        return new PhysicalNode("HashAggregation[Distinct]", dedupe, keys, upstream);
    }

    // ==================== UNNEST 行展开 ====================

    /** 关系是否承载 UNNEST（{@code FROM t, UNNEST(json_array) ...}）。 */
    private static boolean isUnnestRelation(LogicalPlan.ResolvedRelation rel) {
        return rel.subquery().orElse(null) instanceof LogicalPlan.Unnest;
    }

    /**
     * 构建行展开节点 —— {@code FROM ..., UNNEST(json_array_expr) [WITH ORDINALITY] AS u(item[, ord])}。
     *
     * <p>上游 = 逗号分隔的左邻关系；没有左邻（{@code FROM UNNEST(...)}）时用
     * {@link SingleRowOperator} 造一行占位。输出列 = 上游列 + 元素列（+ 序号列）。
     * 展开表达式引用的上游列由列的裁剪的 Unnest 分支保证不被裁掉。</p>
     */
    private PhysicalNode buildUnnest(PhysicalNode upstream, LogicalPlan.Unnest un) {
        if (upstream == null) {
            upstream = new PhysicalNode("SingleRow", new SingleRowOperator(),
                    List.of(SingleRowOperator.DUMMY_COLUMN), List.of());
        }
        // 列名取自 DTO（不是 rel.columns()）—— 逻辑列裁剪可能已把本关系未引用的输出列裁掉，
        // 按裁剪后的列表反推会让序号列被当成元素列。
        String element = un.elementColumn();
        String ordinality = un.ordinalityColumn().orElse(null);
        // 展开列与左侧关系重名会让按名投影 / 列裁剪绑错列 → 规划期明确拒绝（提示改列别名）
        for (String c : ordinality == null ? List.of(element) : List.of(element, ordinality)) {
            for (String existing : upstream.outputColumns()) {
                if (existing.equalsIgnoreCase(c)) {
                    throw unsupported("UNNEST 输出列 '" + c + "' 与左侧关系列重名；"
                            + "请用 AS u(" + c + "_展开) 之类改列别名");
                }
            }
        }
        List<String> out = new ArrayList<>(upstream.outputColumns());
        out.add(element);
        if (ordinality != null) out.add(ordinality);
        UnnestOperator op = new UnnestOperator(upstream.operator(),
                un.expression().text(), element, ordinality);
        return new PhysicalNode("Unnest[" + un.expression().text() + "]", op, out, upstream);
    }

    // ==================== 表扫描 ====================

    /** 构建单表扫描；extraPredicate 为单表 WHERE（已去限定符）。 */
    private PhysicalNode buildScan(LogicalPlan.ResolvedRelation rel,
                                   Optional<String> extraPredicate,
                                   Set<String> requiredCols) {
        // 派生表（FROM 子查询）：递归规划内层查询，外层 WHERE 无法下推 → 转成 Filter
        if (rel.subquery().isPresent()) {
            // 多引用 CTE：查询体已由共享 Store 物化一次 → 这里只挂重放叶子
            MaterializedCteScanOperator.Store materialized = materializedCtes.get(rel.subquery().get());
            if (materialized != null) {
                return buildMaterializedCteScan(materialized, rel, extraPredicate);
            }
            // 表函数（FROM TABLE(fn(常量))）：表生成器语义，独立切片，无 LATERAL / 列引用
            if (rel.subquery().get() instanceof LogicalPlan.TableFunction tf) {
                return buildTableFunction(tf);
            }
            if (rel.subquery().get() instanceof LogicalPlan.SetOperation innerSo) {
                // 派生表 / CTE 查询体是集合运算（UNION [ALL]）：结果列名取首个操作数
                // （语义层 deriveDerivedTableColumns 同口径）；内层 ORDER BY / LIMIT 照常施加。
                PhysicalNode innerNode = buildSetOperation(innerSo);
                PhysicalNode current = applyOrderLimit(innerNode, innerSo.orderBy(),
                        innerSo.limit(), innerSo.offset());
                if (extraPredicate.isPresent()) {
                    current = applyFilterIfSimple(current, extraPredicate.get(), "派生表外层 WHERE");
                }
                return current;
            }
            if (!(rel.subquery().get() instanceof LogicalPlan.Select inner)) {
                throw unsupported("派生表仅支持 SELECT 子查询，实际: "
                        + rel.subquery().get().getClass().getSimpleName());
            }
            PhysicalNode innerNode = buildQuery(inner, requiredCols);
            // ★派生表自身的 ORDER BY / LIMIT / OFFSET 必须在这里施加：buildQuery 只负责查询主体，
            // 顶层 applyOrderLimit 只跑最外层（orderSource）——少了这一步，子查询的 LIMIT 被
            // 静默忽略（`SELECT count(*) FROM (SELECT ... LIMIT 200) t` 返回全表计数）。
            PhysicalNode current = applyOrderLimit(innerNode, inner.orderBy(),
                    inner.limit(), inner.offset());
            if (extraPredicate.isPresent()) {
                // 与残留 WHERE 同一路径（同一判据：FilterOperator 能不能编译）—— 谓词形态随算子能力扩展，
                // 这里绝不能再各留一张会漂移的硬编码白名单。
                // ★必须包在 current（含派生表自身 ORDER BY/LIMIT）之上：外层 WHERE 语义上在
                //   子查询 LIMIT 之后求值，包 innerNode 会把内层 LIMIT 整段丢掉（实测返回 6 ≠ 2）。
                current = applyFilterIfSimple(current, extraPredicate.get(), "派生表外层 WHERE");
            }
            return current;
        }

        String schema = rel.schema(), table = rel.table();
        List<String> outCols = new ArrayList<>(rel.columns());
        ReadRequest req = ReadRequest.of(rel.catalog(), schema, table);

        int parallelism = Math.max(1, context.parallelism());

        // 分布式扫描：Master 注入了 WorkerGateway 且有存活 Worker 时，把 TableScan 派发到 Worker 执行
        // （各 Worker 拉自己分区数据回传 Master；Join / 聚合等后续算子仍在 Master）。
        // 无 Worker / 未注入 → 降级本地并行扫描，行为不变。
        // localScanOnly：小 LIMIT 的简单单表查询由 Master 直读（省一次派发 + 跨进程搬数据）。
        if (context.workerGateway() != null && !context.localScanOnly()) {
            List<WorkerRegistration> healthy = WorkerRegistry.get().healthy(context.workerHealthTimeoutMs());
            if (!healthy.isEmpty()) {
                List<WorkerRegistration> selected = selectWorkers(healthy, parallelism);
                return buildDistributedScan(rel, extraPredicate, req, selected);
            }
        }

        // 统一使用 ParallelTableScanOperator：并行度 = 1 即等价旧 TableScanOperator（整表串行扫描）
        List<DojaoConnector> conns = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            conns.add(context.connectorProvider().get(rel.catalog()));
        }
        ParallelTableScanOperator scan = new ParallelTableScanOperator(conns, req, parallelism);
        scan.setFetchLimit(scanFetchRows);   // 顶层 LIMIT 下推（0 = 不下推）
        if (!rel.columns().isEmpty()) scan.setColumns(rel.columns());
        // 基表谓词（rel.filter）与单表 WHERE（extraPredicate）一并处理：
        //   连接器兑现下推契约（supportsPredicatePushdown）→ 列谓词交给数据源执行，引擎侧不再重复过滤；
        //   连接器不兑现 → 不下推，改由引擎在本节点（与数据同进程）包 Filter 执行，结果同样正确。
        // 标量函数谓词（数据库不认识 Dojao 注册函数如 cosine_similarity）永远只能本地过滤。
        List<String> localParts = new ArrayList<>(2);
        if (pushdownCapable(conns.get(0))) {
            rel.filter().ifPresent(f -> scan.pushdown(SqlTextUtil.stripQualifiers(f.text())));
            if (extraPredicate.isPresent()) {
                ScanPredicate sp = splitScanPredicate(extraPredicate.get());
                if (sp.pushdown() != null) {
                    scan.pushdown(sp.pushdown());
                }
                if (sp.residual() != null) {
                    localParts.add(sp.residual());
                }
            }
        } else {
            rel.filter().ifPresent(f -> localParts.add(SqlTextUtil.stripQualifiers(f.text())));
            extraPredicate.ifPresent(localParts::add);
        }
        String residualFilter = localParts.isEmpty() ? null : String.join(" AND ", localParts);
        // 估算行数（EXPLAIN 用）：连接器元数据 rowCount × 谓词过滤比例；探不到 → UNKNOWN（不显示）
        long estimatedRows = JoinCardinality.sideRows(JoinDecider.StatsProbe.of(conns.get(0)),
                rel.catalog(), schema, table,
                estimateFilter(relScanFilter(rel), extraPredicate.orElse(null)));
        PhysicalNode node = new PhysicalNode("ParallelTableScan[" + schema + "." + table + "]",
                scan, outCols, List.of()).withEstimate(estimatedRows);
        if (residualFilter != null) {
            // 逐项包 Filter（而非一条 AND 合取串）：FilterOperator 只编译单个简单谓词，合取串会落
            // UNKNOWN 静默放行全部行（`(a=1) AND (b=2)` 更糟 —— 拿 "a" 去比 "1) AND (b>2" 得 0 行）。
            for (String cond : SqlTextUtil.splitConjuncts(residualFilter)) {
                FilterOperator filter = new FilterOperator(node.operator(), cond);
                node = new PhysicalNode("Filter[" + cond + "]", filter, outCols, node);
            }
            // 扫描之上挂了会减行的算子（本侧 Filter）→ 截断下推的前缀不再等于「结果前缀」，
            // 撤销它（早停交给顶层 LimitOperator）。
            scan.setFetchLimit(0L);
        }
        return node;
    }

    /**
     * 分区元数据扫描 —— 聚合输入<b>全是分区列</b>时，不读数据文件。
     *
     * <p>典型形态：{@code SELECT min(dt), max(dt), count(DISTINCT dt), count(*) FROM t}。
     * 数据源连接器能按「一行为一个非空分区」输出合成页（分区值 + footer 精确行数），
     * 下游 {@link HashAggregationOperator} 照常聚合，结果与全表扫描<b>精确等价</b>。</p>
     *
     * <p><b>为什么必须精确</b>：{@code count(*)} 在规划和这里被改写成对行数列的
     * {@code sum_int64}，所以分区行数只能是 footer 精确值（HMS {@code numRows} 是估算、
     * 连接器任何一步拿不到精确值都必须返回 null 回退真实扫描）。</p>
     *
     * <p><b>接受</b>：{@code COUNT(*)}、{@code MIN/MAX(分区列)}、{@code COUNT(DISTINCT 分区列)}。
     * <b>拒绝</b>：{@code COUNT(分区列)}（会变成分区数）、{@code SUM/AVG}（需按行数加权）、
     * 表达式/派生列、GROUP BY / HAVING / DISTINCT、非分区列、连接器不支持 —— 任一不满足原样回退。</p>
     */
    private PhysicalNode tryBuildPartitionMetadataScan(LogicalPlan.ResolvedRelation rel,
                                                       LogicalPlan.Select select,
                                                       Optional<String> wherePred) {
        if (!ResourceConfig.partitionMetadataScanEnabled()) return null;
        if (rel.subquery().isPresent() || rel.filter().isPresent()) return null;
        // 只处理「无 GROUP BY / HAVING / DISTINCT 的纯聚合查询」：输出恒 1 行，与「每分区一行」
        // 上的聚合逐列等价。其它形态下游还会做投影/去重，不在 v1 覆盖范围内。
        if (!select.groupBy().isEmpty() || select.having().isPresent() || select.distinct()) return null;
        if (select.selectItems().isEmpty() || rel.catalog().isBlank()) return null;

        DojaoConnector connector;
        List<String> partCols;
        try {
            connector = context.connectorProvider().get(rel.catalog());
            if (connector == null) return null;
            partCols = connector.getPartitionColumns(rel.catalog(), rel.schema(), rel.table());
        } catch (RuntimeException e) {
            return null;                      // 连接器不可用 / 元数据探查失败 → 走真实扫描
        }
        if (partCols == null || partCols.isEmpty()) return null;
        try {
            if (!connector.supportsPartitionMetadataScan(rel.catalog(), rel.schema(), rel.table())) {
                return null;
            }
        } catch (RuntimeException e) {
            return null;
        }
        Set<String> partSet = new HashSet<>(partCols);

        // SELECT 项：每一项都必须能由「每分区一行」精确算出（否则回退，绝不部分下推）
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            Optional<SqlTextUtil.AggInfo> info = SqlTextUtil.parseAggregate(item.expression().text());
            if (info.isEmpty()) return null;            // 非整项聚合（如 round(count(*),2)）→ 回退
            SqlTextUtil.AggInfo a = info.get();
            String fn = a.function();
            if ("COUNT".equals(fn) && a.star()) continue;   // 改写成 sum_int64(行数列)
            if (!"COUNT".equals(fn) && !"MIN".equals(fn) && !"MAX".equals(fn)) {
                return null;                            // SUM/AVG/collect/UDAF 需要行数加权或整组值
            }
            String arg = a.arg() == null ? "" : a.arg().trim();
            boolean distinct = false;
            if (arg.length() > 8 && arg.regionMatches(true, 0, "distinct", 0, 8)
                    && Character.isWhitespace(arg.charAt(8))) {
                distinct = true;
                arg = arg.substring(8).trim();
            }
            // COUNT(分区列) 非去重 = 分区数 ≠ 行数 → 不可由元数据回答；
            // MIN/MAX(DISTINCT 分区列) 与不带 DISTINCT 等价，照常接受。
            if ("COUNT".equals(fn) && !distinct) return null;
            if (!SqlTextUtil.isColumnRef(arg)) return null;
            if (!partSet.contains(SqlTextUtil.stripQualifier(arg))) return null;
        }

        // WHERE：每个合取项都必须能被 FilterOperator 编译、且只引用分区列 —— 同一分区内所有行
        // 的分区值相同，故在元数据行上求值与逐行求值等价。连接器据此先剪分区（仅是加速）。
        String whereText = null;
        if (wherePred.isPresent()) {
            String cond = SqlTextUtil.stripQualifiers(wherePred.get());
            List<String> leaves = SqlTextUtil.splitConjuncts(cond);
            if (leaves.isEmpty()) return null;
            for (String leaf : leaves) {
                List<String> refs = FilterOperator.referencedColumnsOf(leaf);
                if (refs.isEmpty()) return null;
                for (String r : refs) {
                    if (!partSet.contains(SqlTextUtil.stripQualifier(r))) return null;
                }
            }
            whereText = cond;
        }

        ReadRequest request = ReadRequest.of(rel.catalog(), rel.schema(), rel.table())
                .withFilter(whereText);
        List<String> outCols = new ArrayList<>(partCols);
        outCols.add(DojaoConnector.PARTITION_ROW_COUNT_COLUMN);
        PhysicalNode node = new PhysicalNode(
                "PartitionMetadataScan[" + rel.schema() + "." + rel.table() + "]",
                new PartitionMetadataScanOperator(connector, request, outCols),
                outCols, List.of());
        // 分区谓词仍由引擎在元数据页上求值：连接器裁剪只是避免读无关分区的 footer，
        // 不作为正确性依据（两处口径一致 ⇒ 重复求值无副作用）。
        if (whereText != null) {
            node = applyFilterIfSimple(node, whereText, "分区元数据扫描 WHERE");
        }
        return node;
    }

    /** 单表 WHERE 拆分结果：{@code pushdown}（下推数据源，可 null）+ {@code residual}（残留函数 Filter，可 null）+ 其逐项清单。 */
    private record ScanPredicate(String pushdown, String residual, List<String> residualLeaves) {
    }

    /**
     * 派生表（子查询 / CTE，<b>展平不了的那种</b>）的分布式执行描述 ——
     * 「扫派生基表（已列裁剪 + 内层 WHERE）→ 按分组键分区 → 每分区本地聚合（HAVING）→
     * 按 join 键重哈希」。
     *
     * <p>两表派生 join（{@link #tryBuildDerivedDistributedJoin}）与多表链里的派生源
     * （{@link #tryBuildDistributedJoinChain}）共用这一份解析结果 —— 派生体形态判定只写一次。</p>
     *
     * @param innerRequest   派生基表扫描请求（已列裁剪 + 内层 WHERE 下推）
     * @param innerKeys      聚合分组键（= 派生基表分区键）
     * @param aggregations   聚合函数（输出列名 = 派生体输出列）
     * @param caseProjections 聚合输入的 CASE/IF 派生列投影（可空）
     * @param havingFilter   派生体<b>聚合后</b>过滤（已做聚合→输出列名替换）：派生内层 HAVING
     *                       <b>与</b>「挂在派生关系上的谓词」以 {@code AND} 相连（列全部 ∈
     *                       {@code outputColumns}），可 null
     * @param outputColumns  派生体可见输出列（分组键 + 聚合输出列，与聚合算子输出序一致）
     * @param projections    投影型派生源的逐行表达式派生列（无聚合）；空 = 聚合型派生源
     * @param window         窗口型派生源的单窗口组（无聚合）；null = 非窗口型派生源
     */
    private record DerivedPlan(ReadRequest innerRequest, List<String> innerKeys,
                               List<AggregationSpec> aggregations, List<CaseProjection> caseProjections,
                               String havingFilter, List<String> outputColumns,
                               List<StageScheduler.DerivedProjection> projections,
                               StageScheduler.DerivedWindow window) {

        /** 聚合型派生源的便捷构造（无投影列）。 */
        DerivedPlan(ReadRequest innerRequest, List<String> innerKeys,
                    List<AggregationSpec> aggregations, List<CaseProjection> caseProjections,
                    String havingFilter, List<String> outputColumns) {
            this(innerRequest, innerKeys, aggregations, caseProjections, havingFilter, outputColumns,
                    List.of(), null);
        }

        /** 投影型派生源的便捷构造（无窗口）。 */
        DerivedPlan(ReadRequest innerRequest, List<String> innerKeys,
                    List<AggregationSpec> aggregations, List<CaseProjection> caseProjections,
                    String havingFilter, List<String> outputColumns,
                    List<StageScheduler.DerivedProjection> projections) {
            this(innerRequest, innerKeys, aggregations, caseProjections, havingFilter, outputColumns,
                    projections, null);
        }

        /** true = 投影型派生源（无聚合，逐行表达式投影）。 */
        boolean isProjection() {
            return projections != null && !projections.isEmpty();
        }

        /** true = 窗口型派生源（无聚合，单窗口组窗口函数）。 */
        boolean isWindow() {
            return window != null;
        }
    }

    /** 列名转小写集合（列裁剪「需求列集」比较用）。 */
    private static Set<String> lowerSet(List<String> cols) {
        Set<String> out = new HashSet<>();
        if (cols != null) {
            for (String c : cols) {
                if (c != null) out.add(c.toLowerCase(Locale.ROOT));
            }
        }
        return out;
    }

    /** 派生内层是否存在「聚合输出列 ∈ wanted」（决定聚合列是否可裁）。 */
    private static boolean hasWantedAggregate(LogicalPlan.Select inner, Set<String> wanted) {
        for (LogicalPlan.SelectItem it : inner.selectItems()) {
            String t = it.expression().text().trim();
            if (SqlTextUtil.parseAggregate(t).isEmpty()) continue;
            String out = it.alias().orElse(aggOutputName(t));
            if (wanted.contains(out.toLowerCase(Locale.ROOT))) return true;
        }
        return false;
    }

    /** 派生内层是否存在「表达式投影输出列 ∈ wanted」（决定表达式项是否可裁）。 */
    private static boolean hasWantedProjection(LogicalPlan.Select inner, Set<String> wanted) {
        for (LogicalPlan.SelectItem it : inner.selectItems()) {
            String t = it.expression().text().trim();
            if (isStarExpansion(t) || SqlTextUtil.isColumnRef(t)) continue;
            String out = it.alias().orElse(null);
            if (out != null && wanted.contains(out.toLowerCase(Locale.ROOT))) return true;
        }
        return false;
    }

    /** 派生内层是否存在「窗口输出列 ∈ wanted」（决定窗口列是否可裁）。 */
    private static boolean hasWantedWindow(LogicalPlan.Select inner, Set<String> wanted) {
        for (LogicalPlan.SelectItem it : inner.selectItems()) {
            if (!(it.expression() instanceof Expression.WindowFunction wf)) continue;
            String out = windowOutputName(it, wf);
            if (wanted.contains(out.toLowerCase(Locale.ROOT))) return true;
        }
        return false;
    }

    /**
     * 连接器是否<b>兑现</b>谓词下推契约 —— 由连接器自报（{@link DojaoConnector#supportsPredicatePushdown()}）。
     *
     * <p>不兑现的连接器（如 Hive：只按切片读文件、不求值谓词）拿到 {@code filterExpression} 会直接忽略，
     * 所以引擎<b>不能</b>下推后就指望过滤已发生 —— 谓词必须仍由引擎执行。执行地点是<b>扫描节点</b>
     * （单机路径 = 本进程的 {@code Filter}；分布式路径 = Worker 侧 {@link ConnectorScanOperator#withEngineFilter}），
     * 换句话说「数据在哪儿算在哪儿」，绝不把整表拉回 Master 再过滤。</p>
     */
    private static boolean pushdownCapable(DojaoConnector conn) {
        return conn != null && conn.supportsPredicatePushdown();
    }

    /**
     * 分布式路径把某关系的基表过滤下推到 Worker 扫描端是否安全。
     *
     * <p>两个前提缺一不可：① 连接器兑现下推契约；② 谓词整体可在数据源求值 —— 含数据源不认识的
     * 函数谓词时本来要靠外层 {@code Filter} 承接，而 Worker 的扫描片段里没有这个算子。</p>
     *
     * <p>不成立 → 调用方须回退单机路径（本地会在扫描后包 {@code Filter}，结果正确，只是不分布式）。
     * 若照旧派发，连接器忽略过滤条件 → <b>静默多返回行</b>。</p>
     */
    private static boolean distributedFilterSupported(LogicalPlan.ResolvedRelation rel) {
        if (rel.filter().isEmpty()) {
            return true;   // 无谓词可下推，与连接器能力无关
        }
        // 谓词一律由「数据所在地」执行，连接器能力只决定<b>谁来求值</b>：
        //   连接器兑现下推 → 认识的合取项推给数据源（SQL 里过滤），引擎注册函数谓词摘出来由扫描节点
        //                    的 FilterOperator 就地求值（ConnectorScanOperator.openWithEngineFilter）；
        //   不兑现（如 Hive）→ 整串都由扫描节点的 FilterOperator 求值（FilterOperator 支持引擎标量函数，
        //                     见 Form.FUNC_COMPARE / FUNC_IS_NULL）。
        // 唯一仍须回退的情形：残留谓词 FilterOperator <b>编译不出</b>任何引用列（落 Form.UNKNOWN，
        // 会静默放行全部行 = 谓词失效）—— 那种形态宁可回退单机，由本地扫描后的 Filter 承接。
        String cond = SqlTextUtil.stripQualifiers(rel.filter().get().text());
        for (String leaf : splitScanPredicate(cond).residualLeaves()) {
            if (FilterOperator.referencedColumnsOf(leaf).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 拆分单表 WHERE（顶层 AND）为下推部分与残留函数过滤部分。
     *
     * <p>标量函数谓词（数据源不认识注册函数，如 {@code is_high(rating)=1}）留作本地 Filter；
     * 其余（列比较 / 数据源内建函数）下推给数据源执行 —— 支持 {@code func(x)=1 AND col<20}
     * 这类组合谓词（原来整体下推，数据源无法求值注册函数而报错）。</p>
     *
     * <p><b>拆解必须与 Worker 侧的引擎过滤（{@code ConnectorScanOperator.withEngineFilter}）和
     * {@link #engineFilterColumns} 完全同一套</b>：{@link SqlTextUtil#splitConjuncts}（先剥外层括号、
     * 再切顶层 AND、每段剥括号后回炉）。不能用 {@code splitTopLevelKeyword} 直切 —— 派生表展平会把
     * 内层 WHERE 包成 {@code (dt ='2026-08-25' and spu_id is not null)}，AND 落在括号内 ⇒ 切不动 ⇒
     * 整段被当成「一个谓词」⇒ 其中的<b>函数谓词</b>（如 {@code is_high(rating)=1}）漏判为「可下推」⇒
     * 函数名被原样发给数据源（报错 / 静默多返回行），且引擎侧不再补残留 Filter。</p>
     *
     * <p>拼回时给含顶层 {@code OR} 的叶子补回括号：{@code (a=1 or b=2) and c=3} 拆成两叶后若直接
     * 用 {@code AND} 拼接会丢掉括号、改变优先级。</p>
     */
    private static ScanPredicate splitScanPredicate(String cond) {
        SqlTextUtil.PredicateSplit sp = SqlTextUtil.splitByConnectorCapability(cond);
        return new ScanPredicate(sp.pushdown(), sp.residual(), sp.residualLeaves());
    }

    /**
     * 单表 WHERE 下推进 Worker 扫描（分布式 join 用）：取该 relation 的过滤器文本、去限定符。
     *
     * <p><b>返回的是完整谓词</b>（含引擎注册函数谓词），不再在这里剔除残留 —— 扫描节点
     * {@code ConnectorScanOperator.openWithEngineFilter} 会按连接器能力自行拆解：数据源认识的
     * 部分留在请求里交给数据源，引擎函数谓词摘出来由本节点的 {@code FilterOperator} 就地求值。
     * 在这里剔除等于<b>静默丢掉残留</b>（多返回行），故整串下发。</p>
     */
    private static String relScanFilter(LogicalPlan.ResolvedRelation rel) {
        if (rel.filter().isEmpty()) {
            return null;
        }
        return SqlTextUtil.stripQualifiers(rel.filter().get().text());
    }

    /**
     * 构建表函数扫描：{@code FROM TABLE(fn(常量))} → 叶子算子。
     *
     * <p>参数须为字面量（规划期经 {@link #literalValue} 解析为常量）；参数引列（LATERAL 语义）
     * 本轮不支持。输出列 = {@code AS t(a,b)} 列别名；无别名时规划期列名空（执行期算子推断
     * {@code c0..cN-1}），{@code SELECT *} 由 Page.select 在运行时按 root schema 展开。</p>
     */
    private PhysicalNode buildTableFunction(LogicalPlan.TableFunction tf) {
        String name = String.join(".", tf.functionName());
        com.dojao.spi.function.TableFunction fn = TableFunctionRegistry.resolve(name);
        Object[] args = tf.args().stream().map(Planner::literalValue).toArray(Object[]::new);
        List<String> outCols = new ArrayList<>(tf.columnAliases());
        TableFunctionOperator op = new TableFunctionOperator(fn, args, outCols);
        return new PhysicalNode("TableFunction[" + name + "]", op, outCols, List.of());
    }

    // ==================== 分布式扫描 ====================

    /**
     * 从健康节点中选出最多 {@code parallelism} 个 Worker 作为分区派发落点：
     * 用 {@link WorkerSelector} 反复挑当前最优节点，每个节点至多被选一次
     * （分区数 &gt; worker 数时由算子 round-robin 轮转）。兜底策略为
     * {@link DefaultWorkerSelector}（IDLE 优先 → CPU 低 → 内存低）。
     */
    private List<WorkerRegistration> selectWorkers(List<WorkerRegistration> healthy, int parallelism) {
        WorkerSelector selector = context.workerSelector() != null
                ? context.workerSelector() : new DefaultWorkerSelector();
        List<WorkerRegistration> pool = new ArrayList<>(healthy);
        List<WorkerRegistration> selected = new ArrayList<>();
        for (int i = 0; i < parallelism && !pool.isEmpty(); i++) {
            WorkerRegistration best = selector.select(pool).orElse(null);
            if (best == null) break;
            selected.add(best);
            pool.remove(best);
        }
        return selected.isEmpty() ? List.copyOf(healthy) : selected;
    }

    /** 构建分布式扫描算子：分区经 WorkerGateway 派发到 Worker 执行，回传 Page 到 Master。 */
    private PhysicalNode buildDistributedScan(LogicalPlan.ResolvedRelation rel,
                                              Optional<String> extraPredicate,
                                              ReadRequest req,
                                              List<WorkerRegistration> workers) {
        String schema = rel.schema(), table = rel.table();
        List<String> outCols = new ArrayList<>(rel.columns());
        int parallelism = Math.max(1, context.parallelism());
        DojaoConnector probeConn = context.connectorProvider().get(rel.catalog());
        DistributedScanOperator scan = new DistributedScanOperator(
                req, parallelism, probeConn, context.workerGateway(), rel.catalog(), workers);
        scan.setFetchLimit(scanFetchRows);   // 顶层 LIMIT 下推（0 = 不下推）
        if (!rel.columns().isEmpty()) scan.setColumns(rel.columns());
        // 谓词归属（谁执行这条 WHERE）：一律写进扫描请求的 filterExpression，由「数据所在地」执行 ——
        //   ① 连接器兑现下推契约 → 数据源自己在扫描时过滤（推给它）；
        //   ② 不兑现（如 Hive：只按切片读文件、不求值 filterExpression）→ 执行者换成引擎自己，
        //      落在扫描节点（Worker）的 FilterOperator 上，见 WorkerControlProducer.scanAndSend /
        //      FragmentExecutor.executeProducer（判定依据 = 连接器的 supportsPredicatePushdown()）。
        // ★ 分布式路径绝不在此处再包一层 Master 侧 Filter：那等于把整表拉回 Master 过滤（派发扫描
        //   的意义全丢 —— 大表直接打爆 Master 堆，曾实测 ~880MB 触发 OutOfMemoryError），而且
        //   extractBaseTable 遇 FilterOperator 即返回 null → 分布式聚合/窗口全部不成立。
        List<String> localParts = new ArrayList<>(2);
        if (pushdownCapable(probeConn)) {
            rel.filter().ifPresent(f -> scan.pushdown(SqlTextUtil.stripQualifiers(f.text())));
            if (extraPredicate.isPresent()) {
                ScanPredicate sp = splitScanPredicate(extraPredicate.get());
                if (sp.pushdown() != null) {
                    scan.pushdown(sp.pushdown());
                }
                if (sp.residual() != null) {
                    // 谓词含引擎注册函数：数据源求不了值，且不能当过滤条件交给它 → 只能本侧 Filter。
                    // 这是「引擎函数参与过滤」的固有边界，与连接器能力无关。
                    localParts.add(sp.residual());
                }
            }
        } else {
            rel.filter().ifPresent(f -> scan.pushdown(SqlTextUtil.stripQualifiers(f.text())));
            extraPredicate.ifPresent(scan::pushdown);
        }
        String name = "DistributedScan[" + schema + "." + table
                + ", workers=" + workers.size()
                + ", partitions=" + parallelism + "]";
        // 估算行数（EXPLAIN 用）：与本地扫描同口径（元数据 rowCount × 谓词过滤比例）
        long estimatedRows = JoinCardinality.sideRows(JoinDecider.StatsProbe.of(probeConn),
                rel.catalog(), schema, table,
                estimateFilter(relScanFilter(rel), extraPredicate.orElse(null)));
        PhysicalNode node = new PhysicalNode(name, scan, outCols, List.of()).withEstimate(estimatedRows);
        if (!localParts.isEmpty()) {
            String residualFilter = String.join(" AND ", localParts);
            FilterOperator filter = new FilterOperator(scan, residualFilter);
            node = new PhysicalNode("Filter[" + residualFilter + "]", filter, outCols, node);
            // 扫描之上挂了会减行的算子（本侧 Filter）→ 截断下推的前缀不再等于「结果前缀」，
            // 撤销它（早停交给顶层 LimitOperator）。见 scanRowsAreOneToOne 同一不变量的另一侧。
            scan.setFetchLimit(0L);
        }
        return node;
    }

    // ==================== JOIN ====================

    /**
     * <b>简单派生表展平</b>：{@code (SELECT col1, col2, ... FROM T WHERE P)} 在语义上等价于
     * 「基表 T + 裸列投影 + 过滤 P」，故展平成普通基表 {@link LogicalPlan.ResolvedRelation}
     * （{@code subquery} 空）后，即可整体复用 {@link #tryBuildDistributedJoin} /
     * {@link #tryBuildDistributedJoinChain} → {@code StageScheduler.planJoinChain} 的分布式 join 链
     * —— 零新算子 / 零新片段类型。CTE 经 {@code inlineCtes} 已落进 {@code subquery} 槽，同此处理。
     *
     * <p>只展平「单裸表 + 裸列投影（无别名 / 无表达式，且裸列名与关系对外声明的列名一致）
     * + 可选 WHERE」这一保守形态；{@code SELECT *} / {@code a.*} 按基表真实列展开后同裸列投影
     * （语义分析 {@code deriveDerivedTableColumns} 早已把星号展开成真实列名，故两者一致）。
     * 其余（GROUP BY / 窗口 / DISTINCT / ORDER BY+LIMIT / 列改名 / 表达式投影 /
     * 显式列清单 / 嵌套派生表 / 含 join 的派生表）一律<b>原样返回</b>（保持既有回退行为，不引入
     * 未验证的语义）。展平是语义等价变换，分布式与本地回退两条路都安全。</p>
     *
     * <p>过滤合并 = {@code base.filter AND inner.where AND 外层 rel.filter}（三段文本 AND 相连，各段加括号；
     * 限定符由下游 {@code distributedFilterSupported}/{@code relScanFilter} 的 {@code stripQualifiers} 统一剥掉）。</p>
     */
    private LogicalPlan.ResolvedRelation tryFlattenSimpleDerived(
            LogicalPlan.ResolvedRelation rel) {
        if (rel.subquery().isEmpty()) {
            return rel;
        }
        // 物化的多引用 CTE 不得展平：展平会把「只执行一次」退化成每个引用各扫一遍基表。
        if (materializedCtes.containsKey(rel.subquery().get())) {
            return rel;
        }
        LogicalPlan inner = rel.subquery().get();
        if (!(inner instanceof LogicalPlan.Select s)) {
            return rel;
        }
        if (s.fromRelations().size() != 1 || !s.joins().isEmpty()
                || !s.scalarSubqueries().isEmpty()) {
            return rel;
        }
        if (!s.groupBy().isEmpty() || s.having().isPresent()) {
            return rel;   // GROUP BY 派生表另有 tryBuildDerivedDistributedJoin
        }
        if (s.distinct()) {
            return rel;   // 展平会丢 DISTINCT 语义
        }
        if (!s.orderBy().isEmpty() || s.limit().isPresent() || s.offset().isPresent()) {
            return rel;   // 展平会丢 ORDER BY / LIMIT / OFFSET
        }
        LogicalPlan.ResolvedRelation base = s.fromRelations().get(0);
        if (base.subquery().isPresent()) {
            // 嵌套派生：内层同样能展平就先展平内层，再拿展平后的裸基表接着展平外层
            // （展平是语义等价变换，可逐层复用同一份实现；任意深度靠递归自然收敛）。
            // 内层展平不了（聚合 / 窗口 / 改名 / 表达式投影 …）→ 原样返回，保持既有回退行为。
            LogicalPlan.ResolvedRelation innerFlattened = tryFlattenSimpleDerived(base);
            if (innerFlattened.subquery().isPresent()) {
                return rel;
            }
            base = innerFlattened;
        }
        List<String> projected = new ArrayList<>(s.selectItems().size());
        for (LogicalPlan.SelectItem item : s.selectItems()) {
            String text = item.expression().text();
            // 语义分析把 select item 一律建成 OpaqueExpr（非 ColumnRef）→ 按文本判裸列。
            // isColumnRef 只认 `ident` / `alias.ident`；`*` / `a+1` / 函数调用一律 false。
            if (item.alias().isPresent()) {
                return rel;   // 列改名 → 基表无对应列，不展平
            }
            if (isStarExpansion(text)) {
                // `SELECT *` / `a.*`：展平成基表<b>全部真实列</b>（与
                // {@code SemanticAnalyzer.deriveDerivedTableColumns} 的 `*` 展开规则一致 ——
                // 所以 rel.columns() 本就是这些列名，投影清单因而与声明列逐字相等）。
                // 限定符（a.*）须指向本派生表<b>唯一的</b> FROM 关系，否则不展平。
                if (base.columns().isEmpty() || !starQualifierMatchesBase(text, base)) {
                    return rel;
                }
                projected.addAll(base.columns());
                continue;
            }
            if (!SqlTextUtil.isColumnRef(text)) {
                return rel;   // 表达式投影 → 基表无对应列，不展平
            }
            projected.add(SqlTextUtil.stripQualifier(text));
        }
        // 展平后输出列名 = 内层 select item 的裸列名，但<b>声明列以 rel.columns() 为准</b>：
        // 列裁剪（ColumnPruning）早已按外层实际引用把派生关系的列收窄成 projected 的<b>子集</b>——
        // 那是合法裁剪，展平必须原样保留（否则又退回读全列，join 列裁剪白做）。真正的非法情形是
        // <b>改名</b>：显式列清单 `with b(uid) as (select user_id …)` / 派生表列别名 ⇒ 声明列里出现
        // projected 中没有的名字，展平会丢掉这层改名（外层 `b.uid` 解析不到）⇒ 不展平。
        if (rel.columns().isEmpty()) {
            return rel;
        }
        for (String c : rel.columns()) {
            if (!containsIgnoreCase(projected, c)) {
                return rel;
            }
        }
        List<String> parts = new ArrayList<>(3);
        base.filter().ifPresent(f -> parts.add("(" + f.text() + ")"));
        s.where().ifPresent(w -> parts.add("(" + w.text() + ")"));
        rel.filter().ifPresent(f -> parts.add("(" + f.text() + ")"));
        Optional<Expression> merged = parts.isEmpty()
                ? Optional.empty()
                : Optional.of(new Expression.OpaqueExpr(String.join(" AND ", parts)));
        return new LogicalPlan.ResolvedRelation(base.catalog(), base.schema(), base.table(),
                rel.alias(), rel.columns(), merged, Optional.empty());
    }

    /**
     * 星号展开（{@code *} / {@code a.*}）的限定符是否指向本派生表唯一的 FROM 关系 ——
     * 决定该星号能否展平成基表真实列。无限定符恒真；有则须等于基表别名或表名。
     */
    private static boolean starQualifierMatchesBase(String starText,
                                                    LogicalPlan.ResolvedRelation base) {
        if (!starText.contains(".")) {
            return true;   // 裸 `*`
        }
        String qualifier = starText.substring(0, starText.lastIndexOf('.')).trim();
        String relAlias = base.alias().orElse(base.table());
        return qualifier.equalsIgnoreCase(relAlias) || qualifier.equalsIgnoreCase(base.table());
    }

    private PhysicalNode buildJoins(List<LogicalPlan.ResolvedRelation> rels,
                                    List<LogicalPlan.JoinInfo> joins) {
        // 派生表展平：`(SELECT 裸列 FROM 基表 WHERE 列谓词)` 等价于「基表 + 投影 + 过滤」——
        // 展平成普通基表后即可整体走分布式 join 链（原先 subquery 存在即回退单机 → 数据全回流 Master）。
        // CTE 经 inlineCtes 已落进 subquery 槽，与子查询派生表同一处处理。非简单派生表原样返回。
        List<LogicalPlan.ResolvedRelation> flattened = new ArrayList<>(rels.size());
        for (LogicalPlan.ResolvedRelation rel : rels) {
            flattened.add(tryFlattenSimpleDerived(rel));
        }
        rels = flattened;

        // UNNEST 关系是「对左邻结果做行展开」的算子，不是可独立扫描的 join 输入：
        // 先摘出来，join 链照常构建，最后按原始 FROM 顺序逐层套上展开。
        List<LogicalPlan.ResolvedRelation> unnestRels = new ArrayList<>();
        List<LogicalPlan.ResolvedRelation> joinRels = new ArrayList<>(rels.size());
        for (LogicalPlan.ResolvedRelation rel : rels) {
            if (isUnnestRelation(rel)) unnestRels.add(rel);
            else joinRels.add(rel);
        }
        boolean hasUnnest = !unnestRels.isEmpty();
        if (hasUnnest) {
            // join 一侧指向 UNNEST 关系无法在展开前求值 → 明确报错，不静默丢列
            for (LogicalPlan.JoinInfo ji : joins) {
                for (LogicalPlan.ResolvedRelation un : unnestRels) {
                    String ua = alias(un);
                    if (ua.equalsIgnoreCase(normalize(ji.leftAlias()))
                            || ua.equalsIgnoreCase(normalize(ji.rightAlias()))
                            || mentionsQualifier(ji.condition().text(), ua)) {
                        throw unsupported("UNNEST 关系不能参与 JOIN 连接条件；"
                                + "请改成 FROM 左表, UNNEST(表达式) AS " + ua + "(...) 的逗号写法");
                    }
                }
            }
        }

        // 分布式执行（开关默认关）：join 的扫描/分区/join 本身全部派发到 Worker 执行
        // （DistributedJoinOperator 统一：单 join = 1 元素链，多 join = N 元素链），
        // Master 只派发 + 收集结果。非等值等不支持 → 静默回退单机路径（行为不变）。
        // UNNEST 需逐页展开 LATERAL 结果，分布式 join 路径不承载它 → 退回单机组装。
        if (!hasUnnest && context.distributedExecution()) {
            List<WorkerRegistration> healthy = WorkerRegistry.get().healthy(context.workerHealthTimeoutMs());
            if (!healthy.isEmpty()) {
                PhysicalNode distributed = null;
                if (joins.size() == 1) {
                    // 派生表 join（派生表 = 单裸表 GROUP BY，可作等值 join 的任一 SQL 侧）先试；
                    // 返回 null → 落到普通两表 join 分布式。
                    distributed = tryBuildDerivedDistributedJoin(rels, joins, healthy);
                    if (distributed == null) {
                        distributed = tryBuildDistributedJoin(rels, joins, healthy);
                    }
                    // 两条专用路径都接不住时（典型：<b>两侧都是派生表</b>）落链式路径 —— 链式路径的
                    // 首表与各层右表本就能是派生源，单 join 即「2 元素链」，零额外机制。
                }
                if (distributed == null) {
                    distributed = tryBuildDistributedJoinChain(rels, joins, healthy);
                }
                if (distributed != null) {
                    return distributed;
                }
            }
        }

        Map<String, PhysicalNode> scans = new LinkedHashMap<>();
        Map<String, LogicalPlan.ResolvedRelation> relByAlias = new LinkedHashMap<>();
        for (LogicalPlan.ResolvedRelation rel : joinRels) {
            scans.put(alias(rel), buildScan(rel, Optional.empty(), null));
            relByAlias.put(alias(rel), rel);
        }

        LogicalPlan.JoinInfo first = joins.get(0);
        PhysicalNode left = scans.get(normalize(first.leftAlias()));
        PhysicalNode right = scans.get(normalize(first.rightAlias()));
        if (left == null || right == null) {
            throw unsupported("JOIN 关系无法定位: "
                    + first.leftAlias() + " / " + first.rightAlias());
        }
        PhysicalNode current = buildJoin(left, right, first,
                relByAlias.get(normalize(first.rightAlias())));

        for (int i = 1; i < joins.size(); i++) {
            LogicalPlan.JoinInfo ji = joins.get(i);
            PhysicalNode r = scans.get(normalize(ji.rightAlias()));
            if (r == null) throw unsupported("JOIN 右表无法定位: " + ji.rightAlias());
            current = buildJoin(current, r, ji, relByAlias.get(normalize(ji.rightAlias())));
        }
        // 按原始 FROM 顺序套上行展开（内连接下展开位置不影响结果；表达式可引用已 join 的列）
        if (hasUnnest) {
            for (LogicalPlan.ResolvedRelation rel : rels) {
                if (isUnnestRelation(rel)) {
                    current = buildUnnest(current, (LogicalPlan.Unnest) rel.subquery().get());
                }
            }
        }
        return current;
    }

    /** 表达式文本是否引用了某个限定名（{@code alias.col}）—— 用于拒绝 UNNEST 进 join 条件。 */
    private static boolean mentionsQualifier(String text, String qualifier) {
        return text != null && Pattern.compile(
                "(?i)(?<![\\w$])" + Pattern.quote(qualifier) + "\\s*\\.").matcher(text).find();
    }

    /**
     * 右表（build 侧）<b>声明列</b>：从连接器元数据取列类型。用于 build 侧一页都没收到时（空表 /
     * 谓词过滤光）仍能产出带右列的完整 schema —— 见 {@link HashJoinOperator#ensureOutputSchema}
     * 的「短 schema」说明。派生表（未展平）/ 元数据不可得时返回 null，退回旧行为。
     */
    private List<Field> declaredRightFields(LogicalPlan.ResolvedRelation rel) {
        if (rel == null) return null;
        if (rel.subquery().isPresent()) return declaredDerivedFields(rel);
        DojaoConnector connector = context.connectorProvider().get(rel.catalog());
        if (connector == null) return null;
        try {
            List<Field> fields = connector.sourceFields(rel.catalog(), rel.schema(), rel.table(),
                    rel.columns());
            return fields == null || fields.isEmpty() ? null : fields;
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * 派生表（未展平）的<b>声明列类型</b>，供空 build 侧的 join 仍能产出完整 schema。
     *
     * <p>典型触发：EXISTS/IN 去相关生成的 {@code (SELECT key, count(*) AS __subq_val ... GROUP BY key)}
     * 内层谓词把<b>所有行</b>都过滤掉 ⇒ build 侧一页都没有 ⇒ 旧实现（派生表恒返回 null）让
     * LEFT JOIN 输出只剩左列，上层 {@code Filter[COALESCE(__subq_val, 0) > 0]} 找不到列而报错。
     * 这里按「分组键取基表列类型、聚合按函数定类型」静态推出声明列；任一列推不出就返回 null
     * （回退旧行为），绝不用猜测的类型冒充。</p>
     */
    private List<Field> declaredDerivedFields(LogicalPlan.ResolvedRelation rel) {
        if (!(rel.subquery().orElse(null) instanceof LogicalPlan.Select s)) return null;
        List<String> cols = rel.columns();
        if (cols.isEmpty()) return null;
        Map<String, Field> baseFields = new HashMap<>();
        for (LogicalPlan.ResolvedRelation r : s.fromRelations()) {
            if (r.subquery().isPresent()) return null;   // 嵌套派生 → 不做静态推断
            DojaoConnector connector = context.connectorProvider().get(r.catalog());
            if (connector == null) return null;
            try {
                List<Field> fs = connector.sourceFields(r.catalog(), r.schema(), r.table(), r.columns());
                if (fs == null) return null;
                for (Field f : fs) {
                    baseFields.putIfAbsent(f.getName().toLowerCase(Locale.ROOT), f);
                }
            } catch (RuntimeException e) {
                return null;
            }
        }
        List<Field> out = new ArrayList<>(cols.size());
        for (String col : cols) {
            Field resolved = null;
            for (LogicalPlan.SelectItem item : s.selectItems()) {
                String text = item.expression().text().trim();
                String outName = item.alias().orElse(aggOutputName(text));
                if (!outName.equalsIgnoreCase(col)) continue;
                resolved = derivedFieldForItem(text, col, baseFields);
                break;
            }
            if (resolved == null) return null;
            out.add(resolved);
        }
        return out;
    }

    /** 派生表单个输出列的类型：基表裸列 → 原类型；count → BIGINT；sum/avg → DOUBLE；min/max → 入参列类型。 */
    private static Field derivedFieldForItem(String text, String col, Map<String, Field> baseFields) {
        Field base = baseFields.get(SqlTextUtil.stripQualifier(text).toLowerCase(Locale.ROOT));
        if (base != null) {
            return new Field(col, base.getFieldType(), null);
        }
        Optional<SqlTextUtil.AggInfo> agg = SqlTextUtil.parseAggregate(text);
        if (agg.isEmpty()) return null;
        String fn = agg.get().function().toLowerCase(Locale.ROOT);
        return switch (fn) {
            case "count", "count_distinct" ->
                    new Field(col, FieldType.nullable(new ArrowType.Int(64, true)), null);
            case "sum", "avg" ->
                    new Field(col, FieldType.nullable(
                            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            case "min", "max" -> {
                Field arg = baseFields.get(
                        SqlTextUtil.stripQualifier(agg.get().arg()).toLowerCase(Locale.ROOT));
                yield arg == null ? null : new Field(col, arg.getFieldType(), null);
            }
            default -> null;
        };
    }

    private PhysicalNode buildJoin(PhysicalNode left, PhysicalNode right,
                                   LogicalPlan.JoinInfo ji, LogicalPlan.ResolvedRelation rightRel) {
        // CROSS：无 join key，不分区，交给 CrossJoinOperator
        if (ji.joinType() == LogicalPlan.JoinType.CROSS) {
            return buildCrossJoin(left, right);
        }

        JoinType jt = mapJoinType(ji.joinType());

        List<String> leftKeys = new ArrayList<>();
        List<String> rightKeys = new ArrayList<>();

        String cond = SqlTextUtil.stripQualifiers(ji.condition().text());
        List<String> conjuncts = SqlTextUtil.splitTopLevelKeyword(cond, "AND");
        for (String c : conjuncts) {
            if (c.isBlank()) continue;
            List<String> sides = SqlTextUtil.splitTopLevelEquals(c);
            if (sides.size() != 2) {
                throw unsupported("非等值 JOIN 条件: " + c);
            }
            String a = SqlTextUtil.stripQualifier(sides.get(0));
            String b = SqlTextUtil.stripQualifier(sides.get(1));
            boolean aInLeft = left.hasColumn(a), aInRight = right.hasColumn(a);
            boolean bInLeft = left.hasColumn(b), bInRight = right.hasColumn(b);
            if (aInLeft && bInRight) {
                leftKeys.add(a);
                rightKeys.add(b);
            } else if (bInLeft && aInRight) {
                leftKeys.add(b);
                rightKeys.add(a);
            } else {
                // 带出两侧实际可见的列：键名对不上时，最常见的原因是某一侧的输出列集不合预期
                // （典型 = 派生表 select * 未展开真实列名）—— 只报键名等于让用户靠猜。
                throw unsupported(joinKeyError(ji, c, left.outputColumns(), right.outputColumns()));
            }
        }
        if (leftKeys.isEmpty()) {
            throw unsupported("未找到等值 JOIN 键: " + cond);
        }

        // 恒定并行 HashJoin：两侧各一个 HashPartitionOperator（每分区有界队列 + 后台分发线程），
        // N 个 join 工作线程按分区号各跑各的 build/probe/send。相同 join key 必然落到同一分区号，
        // 分区间互不相关，因此可真正并行计算；队列有界提供端到端背压。
        // parallelism==1 即 1 分区 1 worker，退化为串行但走同一条路。
        int partitions = Math.max(1, context.parallelism());
        // 全链路并行：每个分发线程对应一个 scan 线程，共享分区队列
        HashPartitionOperator leftPart = new HashPartitionOperator(
                left.operator(), new ArrayList<>(leftKeys), partitions, partitions);
        HashPartitionOperator rightPart = new HashPartitionOperator(
                right.operator(), new ArrayList<>(rightKeys), partitions, partitions);
        PhysicalNode leftNode = new PhysicalNode("HashPartition[" + leftKeys + "]",
                leftPart, left.outputColumns(), left);
        PhysicalNode rightNode = new PhysicalNode("HashPartition[" + rightKeys + "]",
                rightPart, right.outputColumns(), right);
        HashJoinOperator join = new HashJoinOperator(
                leftPart, rightPart, leftKeys, rightKeys, jt,
                context.memoryLimit(), partitions, declaredRightFields(rightRel));
        List<String> outCols = joinOutputColumns(jt, left.outputColumns(), right.outputColumns(),
                leftKeys, rightKeys);
        String name = "HashJoin[" + jt + ", " + leftKeys + "=" + rightKeys + "]";
        // 估算行数（EXPLAIN 用）：单机路径没有连接器列统计 → 外键假设（输出 ≈ 大的一侧，下界）
        long estimatedRows = JoinCardinality.estimateJoinRows(
                left.estimatedRows(), right.estimatedRows(),
                JoinCardinality.UNKNOWN, JoinCardinality.UNKNOWN);
        return new PhysicalNode(name, join, outCols, leftNode, rightNode).withEstimate(estimatedRows);
    }

    /**
     * 尝试构建分布式单 join（最小切片）：两侧扫描 + 哈希分区 + join 本身全部进 Worker，
     * Master 侧只留 {@link DistributedJoinOperator}（open 时切分 / 派发，next 时收集结果）。
     * 仅支持"恰好一个等值 join、两表、非 CROSS"；否则返回 null → 回退单机路径。
     */
    private PhysicalNode tryBuildDistributedJoin(List<LogicalPlan.ResolvedRelation> rels,
                                                 List<LogicalPlan.JoinInfo> joins,
                                                 List<WorkerRegistration> workers) {
        if (rels.size() != 2 || joins.size() != 1) {
            return null;
        }
        LogicalPlan.JoinInfo ji = joins.get(0);
        if (ji.joinType() == LogicalPlan.JoinType.CROSS) {
            return null;
        }
        LogicalPlan.ResolvedRelation leftRel = rels.get(0);
        LogicalPlan.ResolvedRelation rightRel = rels.get(1);
        if (leftRel.subquery().isPresent() || rightRel.subquery().isPresent()) {
            return null;   // 派生表分布式 join 后置
        }
        JoinKeys keys;
        try {
            keys = parseJoinKeys(new ArrayList<>(leftRel.columns()), new ArrayList<>(rightRel.columns()), ji);
        } catch (RuntimeException e) {
            return null;   // 无法解析等值键 → 回退单机
        }

        JoinType jt = mapJoinType(ji.joinType());
        int partitions = Math.max(1, context.parallelism());
        String queryId = "query-" + Long.toHexString(System.nanoTime());
        DojaoConnector leftProbe = context.connectorProvider().get(leftRel.catalog());
        DojaoConnector rightProbe = context.connectorProvider().get(rightRel.catalog());
        // 单表 WHERE 下推进 Worker 扫描：两侧过滤在扫描端执行，join 输入 / 回传数据量骤减。
        // 含数据源不认识的函数谓词 → 回退单机，由本地扫描后的 Filter 承接。
        if (!distributedFilterSupported(leftRel) || !distributedFilterSupported(rightRel)) {
            return null;
        }
        String leftFilter = relScanFilter(leftRel);
        String rightFilter = relScanFilter(rightRel);
        // ★ 规划期选型：大⋈大 → 排序归并，小(右)⋈大 → 广播，中⋈中 → 哈希（体量未知 → 排序归并，最保守）
        FragmentSpec.JoinAlgorithm algorithm = chooseJoinAlgorithm(
                leftRel.catalog(), leftRel.schema(), leftRel.table(), leftProbe, leftFilter,
                rightRel.catalog(), rightRel.schema(), rightRel.table(), rightProbe, rightFilter,
                jt, workers);
        // 列裁剪下推到两侧扫描：列集 = 语法层 ColumnPruning 已裁剪的 relation 列（join 键必在其中）。
        // join 输出列序 = 「左扫描列 + 右扫描列」，与该列表严格一致（outputFields / 声明字段同源）。
        List<String> leftColumns = scanColumns(new ArrayList<>(leftRel.columns()), leftProbe, leftFilter);
        List<String> rightColumns = scanColumns(new ArrayList<>(rightRel.columns()), rightProbe, rightFilter);
        DistributedJoinOperator op = new DistributedJoinOperator(
                queryId,
                leftRel.catalog(), leftRel.schema(), leftRel.table(), keys.leftKeys(),
                rightRel.catalog(), rightRel.schema(), rightRel.table(), keys.rightKeys(),
                jt, context.memoryLimit(), partitions,
                context.workerGateway(), leftProbe, rightProbe, workers, leftFilter, rightFilter,
                algorithm, leftColumns, rightColumns);

        List<String> outCols = joinOutputColumns(jt, new ArrayList<>(leftRel.columns()),
                new ArrayList<>(rightRel.columns()), keys.leftKeys(), keys.rightKeys());
        String name = "DistributedJoin[" + jt + ", " + keys.leftKeys() + "=" + keys.rightKeys()
                + ", " + JoinExplain.label(algorithm)
                + ", workers=" + workers.size() + ", partitions=" + partitions + "]";
        // 估算行数（EXPLAIN 用）：|L✕R| = |L|×|R|/max(ndv)；ndv 探不到 → 外键假设（输出 ≈ 大的一侧）
        long leftRows = JoinCardinality.sideRows(JoinDecider.StatsProbe.of(leftProbe),
                leftRel.catalog(), leftRel.schema(), leftRel.table(), leftFilter);
        long rightRows = JoinCardinality.sideRows(JoinDecider.StatsProbe.of(rightProbe),
                rightRel.catalog(), rightRel.schema(), rightRel.table(), rightFilter);
        long ndvLeft = JoinCardinality.ndvForKeys(
                safeStats(leftProbe, leftRel.catalog(), leftRel.schema(), leftRel.table()), keys.leftKeys());
        long ndvRight = JoinCardinality.ndvForKeys(
                safeStats(rightProbe, rightRel.catalog(), rightRel.schema(), rightRel.table()), keys.rightKeys());
        long estimatedRows = JoinCardinality.estimateJoinRows(leftRows, rightRows, ndvLeft, ndvRight);
        return new PhysicalNode(name, op, outCols, List.of()).withEstimate(estimatedRows);
    }

    /**
     * 尝试构建<b>派生表分布式 join</b>（Phase 1 + 扩展①②）—— 派生表 = 单裸表 GROUP BY，可作等值
     * join 的<b>任一 SQL 侧</b>（LEFT / RIGHT），join 类型 INNER/LEFT/RIGHT/FULL。整条链路派发
     * Worker：派生基表扫描（派生内层 WHERE 下推）→ 按分组键哈希分区 → 每分区一个
     * {@link FragmentSpec.MiddleAggregateConsumer} 本地聚合（派生内层 HAVING 过滤）→ 派生体输出
     * 重哈希推给派生流所在交换（<b>不回传 Master</b>）→ 与 peer 裸表 join 的最终 Consumer → 回传 Master。
     *
     * <p>两 rel 按 SQL 顺序落本地 join 通道（SQL 左 → operator 左 / probe，SQL 右 → operator 右 /
     * build），{@code derivedOnLeft} 只改派生流走哪个通道、交换名占 {@code -DL} / {@code -DR} 哪侧
     * —— join 键顺序、输出列顺序、LEFT/RIGHT/FULL 空值扩展语义与单机完全一致，无需列重排。</p>
     *
     * <p>派生内层限定：单裸基表 + GROUP BY 裸分组键 + 内置五聚合（count/sum/avg/min/max）+ 聚合输入
     * 支持裸列 / {@code CASE WHEN ... END} / {@code IF(...)} 条件表达式（逐行派生列，DA 头本地先算
     * 再聚合）+ 可空内层 WHERE（兑现下推的连接器：须无数据源不认识的函数残留；<b>不兑现的连接器
     * 一视同仁</b>：列谓词 &amp; 函数谓词都由数据所在地的 Worker 引擎 Filter 承接）+ 可空内层
     * HAVING（引用本地聚合输出列 / 分组键）；无窗口 / 标量子查询 / 分组键改名。其余情况返回 null →
     * 回退单机路径（行为不变，本地完整支持派生 join，分布式只是把整条链路搬去 Worker 执行）。</p>
     */
    private PhysicalNode tryBuildDerivedDistributedJoin(List<LogicalPlan.ResolvedRelation> rels,
                                                        List<LogicalPlan.JoinInfo> joins,
                                                        List<WorkerRegistration> workers) {
        if (rels.size() != 2 || joins.size() != 1) {
            return null;
        }
        LogicalPlan.JoinInfo ji = joins.get(0);
        if (ji.joinType() == LogicalPlan.JoinType.CROSS) {
            return null;
        }
        JoinType jt = mapJoinType(ji.joinType());
        // 按别名定位两侧 rel —— 与本地路径同一逻辑
        Map<String, LogicalPlan.ResolvedRelation> byAlias = new LinkedHashMap<>();
        for (LogicalPlan.ResolvedRelation rel : rels) {
            byAlias.put(alias(rel), rel);
        }
        LogicalPlan.ResolvedRelation leftRel = byAlias.get(normalize(ji.leftAlias()));
        LogicalPlan.ResolvedRelation rightRel = byAlias.get(normalize(ji.rightAlias()));
        if (leftRel == null || rightRel == null) {
            return null;
        }
        // 恰好一侧是派生表（子查询）、另一侧是裸表 → 才走本切片；两派生 / 两裸表后置
        boolean leftIsDerived = leftRel.subquery().isPresent();
        boolean rightIsDerived = rightRel.subquery().isPresent();
        if (leftIsDerived == rightIsDerived) {
            return null;
        }
        LogicalPlan.ResolvedRelation derivedRel = leftIsDerived ? leftRel : rightRel;
        LogicalPlan.ResolvedRelation peerRel = leftIsDerived ? rightRel : leftRel;
        // 派生体解析（裸基表 + 分组键 + 内置聚合 + CASE 派生列 + 内层 WHERE/HAVING）——
        // 与多表链里的派生源共用同一份实现（{@link #planDerived}）。
        DerivedPlan dp = planDerived(derivedRel);
        if (dp == null) {
            return null;   // 派生体形态不支持（表函数 / 窗口 / 分组键改名 …）→ 回退单机
        }
        if (dp.innerKeys().isEmpty() && !dp.isProjection() && !dp.isWindow()) {
            // <b>全局聚合</b>派生表（恒 1 行）：本路径的派生流水线按<b>分组键</b>哈希分区（n 个消费者），
            // 无分组键表达不了「恒 1 行」的语义 ⇒ 回退单机。该形状只由 tryBuildDistributedJoinChain
            // 的末层 CROSS + 单值广播承接（行数可证为 1 时不做未验证的等价变换）。
            // 投影型 / 窗口型派生源的分组键同样为空，但它们各有自己的派生体通道，不受此限。
            return null;
        }
        DojaoConnector innerProbe = context.connectorProvider().get(dp.innerRequest().catalog());
        DojaoConnector peerProbe = context.connectorProvider().get(peerRel.catalog());
        if (!distributedFilterSupported(peerRel)) {
            return null;   // peer 侧过滤含引擎函数 → 回退单机（本地 Filter 承接，结果正确）
        }

        // join 键按 SQL 顺序（派生侧是 left 或 right）解析 → 键序 = 通道序。
        // peer 侧单表 WHERE 由 Worker 侧的引擎 Filter 承接（连接器不兑现下推时）⇒ 谓词列的必须一并投影。
        List<String> peerCols = scanColumns(new ArrayList<>(peerRel.columns()), peerProbe,
                relScanFilter(peerRel));
        JoinKeys jk;
        try {
            jk = leftIsDerived
                    ? parseJoinKeys(dp.outputColumns(), peerCols, ji)
                    : parseJoinKeys(peerCols, dp.outputColumns(), ji);
        } catch (RuntimeException e) {
            return null;   // 无法解析等值键 → 回退单机
        }
        List<String> derivedKeys = leftIsDerived ? jk.leftKeys() : jk.rightKeys();
        List<String> peerKeys = leftIsDerived ? jk.rightKeys() : jk.leftKeys();

        int partitions = Math.max(1, context.parallelism());
        String queryId = "query-" + Long.toHexString(System.nanoTime());
        // 分布式派生 join 的算法选型：build 侧装不下时 HASH 会 fail-fast（HashJoinOperator
        // 「build side too large」），压测里 EXISTS/NOT EXISTS 的派生聚合（2865 万 key）在
        // 5.33MB/分区的预算下 100% 失败。体量未知或超预算 → SORT_MERGE（两侧排序均可溢写）。
        FragmentSpec.JoinAlgorithm joinAlgorithm = chooseDerivedJoinAlgorithm(
                dp, peerRel, peerProbe, innerProbe, leftIsDerived, workers);
        DistributedDerivedJoinOperator op = new DistributedDerivedJoinOperator(
                queryId, leftIsDerived,
                dp.innerRequest(), dp.innerKeys(), dp.aggregations(), dp.caseProjections(),
                derivedKeys, dp.havingFilter(),
                peerRel.catalog(), peerRel.schema(), peerRel.table(), peerKeys, peerCols,
                relScanFilter(peerRel),
                jt, context.memoryLimit(), partitions,
                context.workerGateway(), innerProbe, peerProbe, workers, dp.projections(), dp.window(),
                joinAlgorithm);

        List<String> outCols = leftIsDerived
                ? joinOutputColumns(jt, dp.outputColumns(), peerCols, derivedKeys, peerKeys)
                : joinOutputColumns(jt, peerCols, dp.outputColumns(), peerKeys, derivedKeys);
        String name = "DistributedDerivedJoin[" + jt + (leftIsDerived ? ", derived=L" : ", derived=R")
                + (dp.isProjection() ? ", projection=" + dp.projections().size() : "")
                + (dp.isWindow() ? ", window=" + dp.window().functions().size() : "")
                + (dp.isProjection() || dp.isWindow() ? "" : ", groupBy=" + dp.innerKeys())
                + (dp.havingFilter() != null ? ", having=" + dp.havingFilter() : "")
                + ", " + derivedKeys + "=" + peerKeys + ", algo=" + JoinExplain.label(joinAlgorithm)
                + ", workers=" + workers.size() + ", partitions=" + partitions + "]";
        return new PhysicalNode(name, op, outCols, List.of());
    }

    /**
     * 分布式派生 join 的算法选型（可行性只约束 build 侧，与并行度无关）。
     *
     * <p>与两表分布式 join 的 {@link #chooseChainAlgorithm} 同口径：体量探不到或超过单台 Worker
     * 的 join 预算 {@code U} → {@link FragmentSpec.JoinAlgorithm#SORT_MERGE}（两侧排序阶段都可溢写，
     * 任何体量都能跑完）；否则 HASH（更快，但 build 侧超预算会 fail-fast）。</p>
     *
     * <p><b>为什么派生侧用「内层源表体量」而不是派生输出体量</b>：派生聚合的输出行数 = 分组数，
     * 规划期不可知。源表体量是它的<b>上界</b> —— 源表都装得下才敢赌 HASH；装不下/探不到时
     * 让 SORT_MERGE 兜底，宁可慢一点也不要压测里那种「6MB 超 5.33MB 直接失败」。</p>
     */
    private FragmentSpec.JoinAlgorithm chooseDerivedJoinAlgorithm(
            DerivedPlan dp, LogicalPlan.ResolvedRelation peerRel, DojaoConnector peerProbe,
            DojaoConnector innerProbe, boolean leftIsDerived, List<WorkerRegistration> workers) {
        long unit = JoinDecider.unit(context.memoryLimit(), weakestFreeBytes(workers));
        long buildBytes;
        if (leftIsDerived) {
            // 派生在 SQL 左 ⇒ 右表（peer）是 build 侧。
            buildBytes = JoinDecider.estimateBytes(JoinDecider.StatsProbe.of(peerProbe),
                    peerRel.catalog(), peerRel.schema(), peerRel.table(), relScanFilter(peerRel));
        } else {
            // 派生在 SQL 右 ⇒ 派生流（内层聚合结果）是 build 侧，用内层源表体量做上界。
            ReadRequest req = dp.innerRequest();
            buildBytes = JoinDecider.estimateBytes(JoinDecider.StatsProbe.of(innerProbe),
                    req.catalog(), req.schema(), req.table(), req.filterExpression().orElse(null));
        }
        FragmentSpec.JoinAlgorithm auto =
                JoinDecider.tier(buildBytes, unit) == JoinDecider.Tier.LARGE
                        ? FragmentSpec.JoinAlgorithm.SORT_MERGE
                        : FragmentSpec.JoinAlgorithm.HASH;
        // 逃生门与两表 join 同口径（query.join.algorithm = hash / sort_merge）：
        // 广播不适用于派生 join（StageScheduler 未切 REPLICATE 分区），强制 broadcast 时忽略。
        FragmentSpec.JoinAlgorithm algorithm = auto;
        String forced = ResourceConfig.joinAlgorithm();
        if (forced != null && !forced.isBlank() && !"auto".equalsIgnoreCase(forced)) {
            FragmentSpec.JoinAlgorithm want = switch (forced.trim().toLowerCase()) {
                case "hash" -> FragmentSpec.JoinAlgorithm.HASH;
                case "sort_merge" -> FragmentSpec.JoinAlgorithm.SORT_MERGE;
                default -> null;
            };
            if (want != null) {
                algorithm = want;
            }
        }
        System.err.printf("[Planner] 分布式派生 join 选型 algo=%s buildBytes=%d unit=%d%n",
                JoinExplain.label(algorithm), buildBytes, unit);
        return algorithm;
    }

    /**
     * 解析派生表（子查询 / CTE，<b>展平不了的那种</b>）为分布式执行描述 —— 限「单裸基表 + GROUP BY
     * 裸分组键 + 内置五聚合（count / sum / avg / min / max）+ 聚合输入为裸列 / {@code CASE WHEN ... END} /
     * {@code IF(...)} / 算术表达式 + 可空内层 WHERE（合并 {@code inner.where()} 与优化器可能挂到基表的
     * {@code filter}）+ 可空内层 HAVING（引用本地聚合输出列 / 分组键）」。
     *
     * <p><b>不兑现谓词下推的连接器（如 Hive）一视同仁</b>：列谓词 &amp; 函数谓词都由数据所在地的 Worker
     * 引擎 Filter 承接（{@code ConnectorScanOperator.withEngineFilter}），谓词列一并并进扫描投影
     * （投影先于过滤，缺列会响亮报错）。连接器能力不该决定「派生表能否分布式」。</p>
     *
     * <p>两表派生 join 与多表链里的派生源共用本方法 —— 派生体形态判定只有这一处实现。</p>
     *
     * @return {@code null} = 形态不支持（分组键改名 / 非内置聚合 / 嵌套派生 / 无聚合 / 窗口 / 标量子查询…）
     *         → 调用方回退单机路径（本地完整支持，行为不变）
     */
    private DerivedPlan planDerived(LogicalPlan.ResolvedRelation derivedRel) {
        // 物化的多引用 CTE：派生分布式 join 会重新规划查询体（== 又扫一遍源表）→ 拒绝，
        // 回退本地 join（各侧仍经 buildScan 挂物化重放叶子，查询体只执行一次）。
        if (derivedRel.subquery().isPresent()
                && materializedCtes.containsKey(derivedRel.subquery().get())) {
            return null;
        }
        if (!(derivedRel.subquery().get() instanceof LogicalPlan.Select inner)) {
            return null;   // 表函数等派生体后置
        }
        // 派生内层限定：单裸基表 + 无 join/标量子查询 + 非空 groupBy（WHERE/HAVING 扩展①支持）
        if (inner.fromRelations().size() != 1 || !inner.joins().isEmpty()
                || !inner.scalarSubqueries().isEmpty()) {
            return null;
        }
        if (inner.groupBy().isEmpty()) {
            // 无 GROUP BY 但含聚合函数 = <b>全局聚合</b>（恒 1 行）—— 非相关标量子查询去相关后的派生体
            // `(SELECT count(*) FROM 基表 [WHERE p])`。分组键为空 → 继续走下面的聚合分支，落成
            // 「全局聚合派生源」（哈希落单桶、单消费者），由交叉连接的单值广播喂给每台 Worker。
            boolean globalAgg = inner.selectItems().stream().anyMatch(Planner::itemHasAggregate);
            if (!globalAgg) {
                // 先试「窗口型派生表」—— `(SELECT 裸列, win() OVER (PARTITION BY k ORDER BY o)
                // FROM 基表 WHERE p)` 语义等价于「基表扫描 + 按 k 分区 + 分区内本地窗口」，同样可完全分布式。
                for (LogicalPlan.SelectItem item : inner.selectItems()) {
                    if (item.expression() instanceof Expression.WindowFunction) {
                        return planDerivedWindow(derivedRel, inner);
                    }
                }
                // 再试「投影型派生表」—— `(SELECT 裸列, 表达式 AS 别名 FROM 基表 WHERE p)`
                // 语义等价于「基表扫描 + 逐行表达式物化派生列」，同样可完全分布式（无需拉回 Master）。
                return planDerivedProjection(derivedRel, inner);
            }
        }
        LogicalPlan.ResolvedRelation baseRel = inner.fromRelations().get(0);
        if (baseRel.subquery().isPresent()) {
            return null;   // 派生基表须为裸表（嵌套派生后置）
        }

        // 分组键（裸列，groupBy 顺序去重）—— 与 buildAggregation 同一解析
        List<String> keys = new ArrayList<>();
        for (Expression gb : inner.groupBy()) {
            String col = SqlTextUtil.stripQualifier(gb.text().trim());
            if (!SqlTextUtil.isSingleIdentifier(col)
                    || !containsIgnoreCase(baseRel.columns(), col)) {
                return null;   // GROUP BY 表达式 / 未知列 → 后置
            }
            if (!containsIgnoreCase(keys, col)) {
                keys.add(col);
            }
        }
        if (keys.isEmpty() && !inner.groupBy().isEmpty()) {
            return null;   // 有 GROUP BY 但键解析不出（表达式 / 未知列）→ 后置；空 groupBy 的全局聚合合法
        }

        // 派生输出列 = 聚合算子输出序（分组键在前 + 聚合输出列在后），与局部 buildAggregation 一致
        List<String> streamCols = new ArrayList<>(keys);
        List<String> needed = new ArrayList<>(keys);
        boolean[] keySelected = new boolean[keys.size()];
        Set<String> builtinAggs = Set.of("COUNT", "SUM", "AVG", "MIN", "MAX");
        List<AggregationSpec> aggs = new ArrayList<>();
        // CASE / IF 条件聚合输入（扩展②）：转成派生列投影，DA 头本地先逐行算派生列再聚合。
        // 合成列名 __case_k 是内部产物，不进派生体可见输出（streamCols），仅作聚合输入。
        List<CaseProjection> caseProjections = new ArrayList<>();
        // 外层实际引用的派生输出列（列裁剪收窄后的子集）。仅在此基础上产出/聚合被引用的列，
        // 避免派生体算完再被外层投影丢掉。空集 = 无从裁（保持原行为：全保留）。
        Set<String> wanted = lowerSet(derivedRel.columns());
        // ★只在「外层至少引用了一个聚合输出列」时才裁剪聚合：否则把唯一/全部聚合裁掉会令
        //   aggs 空 → 本路径放弃（派生表退化为「只分组无聚合」= 去重，分布式路径表达不了）。
        //   保留它们几乎零成本（如 COUNT(*) 本就不读列），却守住「派生在首表的链仍分布式」。
        boolean narrowAggs = !wanted.isEmpty() && hasWantedAggregate(inner, wanted);
        for (LogicalPlan.SelectItem item : inner.selectItems()) {
            String text = item.expression().text().trim();
            if (isStarExpansion(text)) {
                return null;   // 派生内层 SELECT * → 后置
            }
            Optional<SqlTextUtil.AggInfo> info = SqlTextUtil.parseAggregate(text);
            if (info.isPresent()) {
                SqlTextUtil.AggInfo a = info.get();
                String fn = a.function().toUpperCase(Locale.ROOT);
                if (!builtinAggs.contains(fn) || (a.star() && !"COUNT".equals(fn))) {
                    return null;   // 仅内置五聚合；SUM(*) 等非法本就后置
                }
                String outName = item.alias().orElse(aggOutputName(text));
                // 外层未引用的聚合列 → 跳过：不建规格、不读其输入列（分组键仍强制保留，见下方 keySelected）。
                if (narrowAggs && !wanted.contains(outName.toLowerCase(Locale.ROOT))) {
                    continue;
                }
                String inputCol = null;
                if (!a.star()) {
                    String candidate = SqlTextUtil.stripQualifier(a.arg());
                    if (SqlTextUtil.isColumnRef(a.arg())) {
                        if (!containsIgnoreCase(baseRel.columns(), candidate)) {
                            return null;   // 裸列不在派生基表 → 后置
                        }
                        inputCol = candidate;
                        if (!containsIgnoreCase(needed, inputCol)) {
                            needed.add(inputCol);
                        }
                    } else {
                        // CASE WHEN ... END / IF(...) 条件表达式或算术表达式（a+b / price*0.8）：复用本地
                        // CaseProject 解析器（对派生基表列集做存在性校验），条件列 / 结果列引用补进扫描
                        // needed（DA 头逐行算派生列需要）。
                        String derived = "__case_" + caseProjections.size();
                        CaseProjection cp;
                        try {
                            cp = parseConditionalArg(a.arg(), derived,
                                    derivedInnerBaseNode(baseRel), fn);
                        } catch (RuntimeException e) {
                            return null;   // 不支持的表达式形态 → 回退单机（行为与本地一致）
                        }
                        caseProjections.add(cp);
                        inputCol = derived;
                        addCaseReferencedColumns(cp, baseRel.columns(), needed);
                    }
                }
                AggregationSpec spec = switch (fn) {
                    case "COUNT" -> a.star()
                            ? AggregationSpec.count(outName)
                            : AggregationSpec.count(inputCol, outName);
                    case "SUM" -> AggregationSpec.sum(inputCol, outName);
                    case "AVG" -> AggregationSpec.avg(inputCol, outName);
                    case "MIN" -> AggregationSpec.min(inputCol, outName);
                    default -> AggregationSpec.max(inputCol, outName);
                };
                aggs.add(spec);
                streamCols.add(outName);
            } else {
                // 裸分组键列：须是 GROUP BY 键且不改名（派生体列名 = 外层可见列名）
                String col = SqlTextUtil.stripQualifier(text);
                if (!SqlTextUtil.isSingleIdentifier(col)) {
                    return null;   // 窗口 / 标量函数投影 → 后置
                }
                int ki = indexOfIgnoreCase(keys, col);
                if (ki < 0) {
                    return null;   // 非分组键裸列（等价 DISTINCT / 非法）→ 后置
                }
                if (item.alias().isPresent() && !item.alias().get().equals(col)) {
                    return null;   // 分组键改名 → 后置（聚合算子只产原始键名）
                }
                keySelected[ki] = true;
            }
        }
        if (aggs.isEmpty()) {
            return null;   // GROUP BY 无聚合（等价 DISTINCT）→ 后置
        }
        for (boolean sel : keySelected) {
            if (!sel) {
                return null;   // 分组键未全部 SELECT → 派生体可见列 != 聚合输出，后置
            }
        }
        // 全局聚合 + COUNT(*) 之类不引用任何列 → 列裁剪集为空。空集在连接器契约里等于「读全列」
        // （pruneColumns(空) 原样返回请求），对 Hive 宽表意味着整表列 I/O —— 而聚合根本不消费输入列。
        // 补读基表首列即可撑起扫描 schema，把 I/O 从「全列」压到「一列」。
        if (needed.isEmpty() && !baseRel.columns().isEmpty()) {
            needed.add(baseRel.columns().get(0));
        }

        // 派生内层 WHERE（扩展①）：inner.where() + baseRel.filter()（优化器可能把谓词挂基表）合并为
        // 单一谓词再拆下推/承接。
        DojaoConnector innerProbe = context.connectorProvider().get(baseRel.catalog());
        String innerFilter = null;
        List<String> whereParts = new ArrayList<>(2);
        inner.where().ifPresent(e -> whereParts.add(SqlTextUtil.stripQualifiers(e.text())));
        baseRel.filter().ifPresent(e -> whereParts.add(SqlTextUtil.stripQualifiers(e.text())));
        if (!whereParts.isEmpty()) {
            String merged = String.join(" AND ", whereParts);
            // 谓词整体下发给<b>数据所在地</b>的扫描节点，由它按连接器能力自行拆解
            // （{@code ConnectorScanOperator.openWithEngineFilter}）：兑现下推的连接器拿走自己认识的
            // 合取项，引擎注册函数谓词由本节点 {@code FilterOperator} 就地求值；不兑现的连接器整串都由
            // 引擎求值。★此处<b>不再按连接器能力回退单机</b> —— 连接器能力不该决定「派生表能否分布式」。
            // ★只有<b>引擎真正要求值</b>的那部分谓词列必须一并投影：投影发生在过滤之前，缺列会让 Filter
            // 响亮报错（或旧版静默 0 行）；数据源自己会过滤的部分不必多读（列裁剪保持最紧）。
            innerFilter = merged;
            String enginePart = pushdownCapable(innerProbe)
                    ? splitScanPredicate(merged).residual()
                    : merged;
            if (enginePart != null) {
                for (String c : engineFilterColumns(List.of(enginePart))) {
                    if (!containsIgnoreCase(needed, c)) {
                        needed.add(c);
                    }
                }
            }
        }

        // 派生内层 HAVING（扩展①）：聚合调用 → 输出列名替换 + 去限定符，接受范围镜像 buildHaving。
        // 过滤列必须是本地聚合输出列（分组键 + 聚合输出列名）—— FilterOperator 缺列会静默丢行/放行。
        String havingFilter = null;
        if (inner.having().isPresent()) {
            Map<String, String> aggMap = new HashMap<>();
            for (LogicalPlan.SelectItem item : inner.selectItems()) {
                String t = item.expression().text();
                if (SqlTextUtil.parseAggregate(t).isPresent()) {
                    aggMap.put(SqlTextUtil.normalize(t), item.alias().orElse(aggOutputName(t)));
                }
            }
            String replaced = SqlTextUtil.replaceAggregates(inner.having().get().text(), aggMap);
            String cond = SqlTextUtil.stripQualifiers(replaced);
            // ★必须用 filterableOnColumns 把关（与「挂在派生关系上的谓词」同一道闸门）：它要求每个合取项
            // 都能被 FilterOperator 编译出真实列引用。无别名的聚合输出列名是归一化文本（count(*) /
            // sum(rating)）—— FilterOperator 认不出（不是裸标识符、也不是已注册标量函数）⇒ 落
            // Form.UNKNOWN = 逐行放行，HAVING 被静默丢弃。这种形态在 Worker 侧没有改名通道（派生流水线
            // 只传谓词串，不传别名算子）⇒ 返回 null 回退单机，由 buildHaving 的改名方案正确过滤。
            if (!filterableOnColumns(cond, streamCols)) {
                return null;
            }
            havingFilter = cond;
        }

        // 派生关系上挂的谓词（优化器把外层 WHERE 推到派生表之上）：谓词只可能引用派生体<b>输出列</b>
        // ⇒ 语义等价于 HAVING（派生体算完之后逐行过滤），落进同一个「聚合后过滤」通道。
        // 列不在输出列集、或谓词编译不出引用列（UNKNOWN 会静默放行全部行）→ 回退单机（行为不变）。
        List<String> postConds = new ArrayList<>(2);
        if (havingFilter != null) {
            postConds.add("(" + havingFilter + ")");
        }
        if (derivedRel.filter().isPresent()) {
            String cond = SqlTextUtil.stripQualifiers(derivedRel.filter().get().text());
            if (!filterableOnColumns(cond, streamCols)) {
                return null;
            }
            postConds.add("(" + cond + ")");
        }
        String postFilter = postConds.isEmpty() ? null : String.join(" AND ", postConds);

        // 派生基表列裁剪（分组键 + 聚合输入列）；WHERE 下推谓词随 request 交给扫描
        ReadRequest innerRequest = innerProbe.pruneColumns(
                ReadRequest.of(baseRel.catalog(), baseRel.schema(), baseRel.table()), needed);
        innerRequest = (innerFilter == null || innerFilter.isBlank())
                ? innerRequest : innerRequest.withFilter(innerFilter);
        return new DerivedPlan(innerRequest, keys, aggs, caseProjections, postFilter, streamCols);
    }

    /**
     * 解析<b>投影型</b>派生表 —— {@code (SELECT 裸列…, 表达式 AS 别名… FROM 单裸基表 WHERE p)}：无分组、
     * 无聚合，仅逐行物化表达式派生列。语义等价于「基表扫描 + 投影 + 过滤」，但派生列在 Worker
     * <b>Producer 侧、哈希分区之前</b>物化（{@code ExpressionProjectOperator}），随后按该层 join 键
     * 分区进交换 —— 与聚合型派生源同一套片段机制，零新算子 / 零新片段类型，数据全程不回流 Master。
     *
     * <p><b>表达式类型必须能静态推断</b>：派生列随 {@code JoinStep.declaredRightFields} 下发
     * （build 侧空桶退回声明值时必须与实收页逐列同型，否则该分区列数/类型与其他分区不一致 → 下游越界）。
     * 引擎的表达式求值类型是<b>数据相关</b>的（任一行字符串 ⇒ Utf8…），故仅接受可确定推断的形态
     * （裸列 / 数值字面量 / 数值算术 / 比较与布尔 / 同型 CASE），其余（函数调用 / 混合分支 CASE /
     * 字符串参与算术）一律返回 null → 回退单机（行为不变，本地完整支持）。</p>
     *
     * <p>派生<b>关系上挂的谓词</b>（{@code derivedRel.filter()}）引用派生输出列、需在投影之后再过滤 ——
     * 本版不接受（返回 null 回退单机），保持「不引入未验证语义」。</p>
     *
     * @param derivedRel 派生关系（{@code subquery} 非空）
     * @param inner      派生体内层的 {@code Select}（调用方已判：单 FROM / 无 join / 无标量子查询 / 空 GROUP BY）
     */
    private DerivedPlan planDerivedProjection(LogicalPlan.ResolvedRelation derivedRel,
                                              LogicalPlan.Select inner) {
        if (inner.distinct() || inner.having().isPresent()
                || !inner.orderBy().isEmpty() || inner.limit().isPresent() || inner.offset().isPresent()) {
            return null;   // DISTINCT / HAVING / 排序·分页：投影型无法表达 → 回退单机
        }
        if (derivedRel.filter().isPresent()) {
            return null;   // 派生关系上的谓词：要在投影之后再过滤 → 后置
        }
        LogicalPlan.ResolvedRelation baseRel = inner.fromRelations().get(0);
        if (baseRel.subquery().isPresent()) {
            return null;   // 嵌套派生：应由 tryFlattenSimpleDerived 逐层展平（不在此重复）
        }
        DojaoConnector probe = context.connectorProvider().get(baseRel.catalog());
        if (probe == null) {
            return null;
        }
        List<Field> baseFields;
        try {
            baseFields = AbstractDistributedOperator.scanFields(probe,
                    baseRel.catalog(), baseRel.schema(), baseRel.table(), baseRel.columns());
        } catch (RuntimeException e) {
            return null;   // 元数据不可得 → 类型无法推断
        }
        Map<String, Field> typeOf = new HashMap<>();
        for (Field f : baseFields) {
            typeOf.put(f.getName().toLowerCase(Locale.ROOT), f);
        }

        List<String> needed = new ArrayList<>();
        List<String> outputColumns = new ArrayList<>();
        List<StageScheduler.DerivedProjection> projections = new ArrayList<>();
        // 外层实际引用的派生输出列（列裁剪收窄后的子集）：只物化被引用的列 / 只读其输入列。
        Set<String> wanted = lowerSet(derivedRel.columns());
        // ★只在「外层至少引用了一个派生表达式列」时才裁剪表达式项：否则把唯一/全部表达式裁掉
        //   会令 projections 空 → 本路径放弃（退化为纯扫描，本可由展平路径承接）。
        boolean narrowExprs = !wanted.isEmpty() && hasWantedProjection(inner, wanted);
        for (LogicalPlan.SelectItem item : inner.selectItems()) {
            String text = item.expression().text().trim();
            if (isStarExpansion(text)) {
                return null;   // SELECT * → 交给展平路径（那里按基表真实列展开）
            }
            if (!item.windowCalls().isEmpty()) {
                return null;   // 窗口嵌在表达式里 → 非「逐行投影」形态，回退单机
            }
            if (!(item.expression() instanceof Expression.WindowFunction) && hasAggregateCall(text)) {
                return null;   // 聚合（含嵌在表达式里的）→ 非「逐行投影」形态，回退单机
            }
            if (SqlTextUtil.isColumnRef(text)) {
                String col = SqlTextUtil.stripQualifier(text);
                int ci = indexOfIgnoreCase(baseRel.columns(), col);
                if (ci < 0) {
                    return null;   // 裸列不在基表 → 回退单机
                }
                String canonical = baseRel.columns().get(ci);
                if (item.alias().isPresent() && !item.alias().get().equalsIgnoreCase(canonical)) {
                    return null;   // 裸列改名 → 回退单机（表达式投影器只追加、不改名）
                }
                if (!wanted.isEmpty() && !wanted.contains(canonical.toLowerCase(Locale.ROOT))) {
                    continue;   // 外层未引用的裸列 → 跳过（不读该列）
                }
                if (!containsIgnoreCase(needed, canonical)) {
                    needed.add(canonical);
                }
                outputColumns.add(canonical);
                continue;
            }
            String outName = item.alias().isPresent()
                    ? item.alias().get()
                    : (SqlTextUtil.parseAggregate(text).isPresent() ? null : SqlTextUtil.normalize(text));
            if (outName == null) {
                return null;   // 无别名的聚合调用 → 非本形态
            }
            // 外层未引用的派生列 → 跳过：不解析表达式、不物化、不读输入列。
            if (narrowExprs && !wanted.contains(outName.toLowerCase(Locale.ROOT))) {
                continue;
            }
            ExprNode node;
            try {
                node = ExprParser.parse(text);
            } catch (RuntimeException e) {
                return null;   // 表达式不能解析（含窗口 / 标量函数未注册）→ 后置
            }
            Set<String> srcCols = new ExprEvaluator(node).columnArgs();
            Map<String, Field> srcFields = new HashMap<>(srcCols.size());
            for (String c : srcCols) {
                int idx = indexOfIgnoreCase(baseRel.columns(), c);
                if (idx < 0) {
                    return null;   // 表达式引用列不在基表 → 回退单机
                }
                String canonical = baseRel.columns().get(idx);
                Field f = typeOf.get(canonical.toLowerCase(Locale.ROOT));
                if (f == null) {
                    return null;   // 元数据里没有该列的类型 → 回退单机
                }
                srcFields.put(c, f);
            }
            Field field = inferExprField(outName, node, srcFields);
            if (field == null) {
                return null;   // 类型不可静态推断 → 回退单机
            }
            for (String c : srcCols) {
                String canonical = baseRel.columns().get(indexOfIgnoreCase(baseRel.columns(), c));
                if (!containsIgnoreCase(needed, canonical)) {
                    needed.add(canonical);
                }
            }
            projections.add(new StageScheduler.DerivedProjection(outName, text, field));
            outputColumns.add(outName);
        }
        // 外层实际引用的派生列必须全部能产出（列裁剪可能把 rel.columns() 收窄成子集）
        for (String c : derivedRel.columns()) {
            if (!containsIgnoreCase(outputColumns, c)) {
                return null;   // 改名 / 未选出的列 → 回退单机
            }
        }

        // 派生内层 WHERE（inner.where + 优化器可能挂到基表的 filter）合并为单一谓词，整体下发扫描节点；
        // 引擎真正求值的那部分谓词列必须一并投影（投影先于过滤）。
        String innerFilter = null;
        List<String> whereParts = new ArrayList<>(2);
        inner.where().ifPresent(e -> whereParts.add(SqlTextUtil.stripQualifiers(e.text())));
        baseRel.filter().ifPresent(e -> whereParts.add(SqlTextUtil.stripQualifiers(e.text())));
        if (!whereParts.isEmpty()) {
            String merged = String.join(" AND ", whereParts);
            innerFilter = merged;
            String enginePart = pushdownCapable(probe) ? splitScanPredicate(merged).residual() : merged;
            if (enginePart != null) {
                for (String c : engineFilterColumns(List.of(enginePart))) {
                    if (!containsIgnoreCase(needed, c)) {
                        needed.add(c);
                    }
                }
            }
        }
        ReadRequest innerRequest = probe.pruneColumns(
                ReadRequest.of(baseRel.catalog(), baseRel.schema(), baseRel.table()), needed);
        innerRequest = (innerFilter == null || innerFilter.isBlank())
                ? innerRequest : innerRequest.withFilter(innerFilter);
        return new DerivedPlan(innerRequest, List.of(), List.of(), List.of(), null, outputColumns,
                projections);
    }

    /**
     * 解析<b>窗口型</b>派生表 —— {@code (SELECT 裸列…, win() OVER (PARTITION BY k ORDER BY o) FROM 单裸基表 WHERE p)}：
     * 无分组、无聚合，仅按 {@code k} 哈希分区后由每分区本地排序 + 逐行计算（{@code MiddleWindowConsumer}），
     * 随后按该层 join 键分区进交换 —— 派生体留在 Worker 侧流向 join，数据不回流 Master。
     *
     * <p>限「单窗口组」（所有窗口函数 PARTITION BY / ORDER BY 完全一致，与单机分布式窗口同一判据）、
     * 裸基表、SELECT 项为裸列（不改名）或窗口函数（{@code toWindowSpec} 支持的形态）。其余
     * （多窗口组 / 派生关系上的谓词 / 表达式与标量函数投影 / 嵌套派生）返回 null → 回退单机。</p>
     *
     * @param derivedRel 派生关系（{@code subquery} 非空）
     * @param inner      派生体内层 {@code Select}（调用方已判：单 FROM / 无 join / 无标量子查询 / 空 GROUP BY / 含窗口）
     */
    private DerivedPlan planDerivedWindow(LogicalPlan.ResolvedRelation derivedRel,
                                          LogicalPlan.Select inner) {
        if (inner.distinct() || inner.having().isPresent()
                || !inner.orderBy().isEmpty() || inner.limit().isPresent() || inner.offset().isPresent()) {
            return null;   // DISTINCT / HAVING / 排序·分页：窗口型无法表达 → 回退单机
        }
        if (derivedRel.filter().isPresent()) {
            return null;   // 派生关系上的谓词：要在窗口之后再过滤 → 后置
        }
        LogicalPlan.ResolvedRelation baseRel = inner.fromRelations().get(0);
        if (baseRel.subquery().isPresent()) {
            return null;   // 嵌套派生：应由 tryFlattenSimpleDerived 逐层展平（不在此重复）
        }
        DojaoConnector probe = context.connectorProvider().get(baseRel.catalog());
        if (probe == null) {
            return null;
        }
        try {
            // 窗口结果列的声明类型（LAG/LEAD）要按基表列类型推断 → 元数据不可得即放弃
            AbstractDistributedOperator.scanFields(probe,
                    baseRel.catalog(), baseRel.schema(), baseRel.table(), baseRel.columns());
        } catch (RuntimeException e) {
            return null;
        }

        List<LogicalPlan.SelectItem> windowItems = new ArrayList<>();
        for (LogicalPlan.SelectItem item : inner.selectItems()) {
            if (item.expression() instanceof Expression.WindowFunction) {
                windowItems.add(item);
            }
        }
        if (windowItems.isEmpty()) {
            return null;
        }
        // 单窗口组：所有窗口函数必须相同的 PARTITION BY + ORDER BY（与 tryBuildDistributedWindow 同判据）
        Expression.WindowFunction first = (Expression.WindowFunction) windowItems.get(0).expression();
        String groupKey = first.partitionBy() + "§" + first.orderBy();
        for (LogicalPlan.SelectItem item : windowItems) {
            Expression.WindowFunction wf = (Expression.WindowFunction) item.expression();
            if (!(wf.partitionBy() + "§" + wf.orderBy()).equals(groupKey)) {
                return null;   // 多窗口组 → 分区键不同，单次哈希分区表达不了 → 回退单机
            }
        }

        List<String> needed = new ArrayList<>();
        List<String> partitionBy = new ArrayList<>();
        for (String p : first.partitionBy()) {
            String col = SqlTextUtil.stripQualifier(p);
            int idx = indexOfIgnoreCase(baseRel.columns(), col);
            if (idx < 0) {
                return null;   // PARTITION BY 引用列不在派生基表 → 后置
            }
            String canonical = baseRel.columns().get(idx);
            partitionBy.add(canonical);
            if (!containsIgnoreCase(needed, canonical)) {
                needed.add(canonical);
            }
        }
        List<String> orderBy = new ArrayList<>();
        List<Boolean> ascending = new ArrayList<>();
        for (String o : first.orderBy()) {
            SqlTextUtil.OrderKey ok = SqlTextUtil.parseOrderKey(o);
            int idx = indexOfIgnoreCase(baseRel.columns(), ok.column());
            if (idx < 0) {
                return null;   // ORDER BY 引用列不在派生基表 → 后置
            }
            String canonical = baseRel.columns().get(idx);
            orderBy.add(canonical);
            ascending.add(ok.ascending());
            if (!containsIgnoreCase(needed, canonical)) {
                needed.add(canonical);
            }
        }

        // SELECT 项：裸列（须 ∈ 基表、不改名）或窗口函数（输出列名 = 别名 / 默认名）
        List<String> outputColumns = new ArrayList<>();
        List<String> windowColumns = new ArrayList<>();
        List<WindowFunctionSpec> specs = new ArrayList<>();
        // 外层实际引用的派生输出列（列裁剪收窄后的子集）：只产生被引用的列。
        Set<String> wanted = lowerSet(derivedRel.columns());
        // ★只在「外层至少引用了一个窗口列」时才裁剪窗口项：否则把唯一/全部窗口函数裁掉会令
        //   specs 空 → 本路径放弃（退化为普通扫描/投影，本可由其它路径承接）。
        boolean narrowWin = !wanted.isEmpty() && hasWantedWindow(inner, wanted);
        for (LogicalPlan.SelectItem item : inner.selectItems()) {
            String text = item.expression().text().trim();
            if (isStarExpansion(text)) {
                return null;   // SELECT * → 交给展平路径（那里按基表真实列展开）
            }
            if (!item.windowCalls().isEmpty()) {
                return null;   // 窗口嵌在表达式里 → 派生窗口下沉只支持整项窗口，回退单机
            }
            if (item.expression() instanceof Expression.WindowFunction wf) {
                String out = windowOutputName(item, wf);
                if (narrowWin && !wanted.contains(out.toLowerCase(Locale.ROOT))) {
                    continue;   // 外层未引用的窗口列 → 跳过（不建窗口规格）
                }
                WindowFunctionSpec spec;
                try {
                    spec = toWindowSpec(wf, out);
                } catch (RuntimeException e) {
                    return null;   // 不支持的窗口函数 / 参数形态 → 回退单机
                }
                if (spec.inputColumn() != null) {
                    int ii = indexOfIgnoreCase(baseRel.columns(), spec.inputColumn());
                    if (ii < 0) {
                        return null;   // 窗口输入列不在基表 → 后置
                    }
                    // ★窗口输入列（SUM(amount) OVER ... 的 amount）必须进扫描投影：
                    //   WindowFunctionOperator 按列名解析输入列，缺列即算不出（全 NULL / 报错）。
                    String canonicalIn = baseRel.columns().get(ii);
                    if (!containsIgnoreCase(needed, canonicalIn)) {
                        needed.add(canonicalIn);
                    }
                }
                specs.add(spec);
                windowColumns.add(out);
                outputColumns.add(out);
                continue;
            }
            if (!SqlTextUtil.isColumnRef(text)) {
                return null;   // 非裸列非窗口（表达式 / 标量函数）→ 后置
            }
            String col = SqlTextUtil.stripQualifier(text);
            int idx = indexOfIgnoreCase(baseRel.columns(), col);
            if (idx < 0) {
                return null;   // 裸列不在基表 → 回退单机
            }
            String canonical = baseRel.columns().get(idx);
            if (item.alias().isPresent() && !item.alias().get().equalsIgnoreCase(canonical)) {
                return null;   // 裸列改名 → 回退单机（窗口算子只追加、不改名）
            }
            if (!wanted.isEmpty() && !wanted.contains(canonical.toLowerCase(Locale.ROOT))) {
                continue;   // 外层未引用的裸列 → 跳过（不读该列）
            }
            if (!containsIgnoreCase(needed, canonical)) {
                needed.add(canonical);
            }
            outputColumns.add(canonical);
        }
        if (specs.isEmpty()) {
            return null;
        }
        // 外层实际引用的派生列必须全部能产出（列裁剪可能把 rel.columns() 收窄成子集）
        for (String c : derivedRel.columns()) {
            if (!containsIgnoreCase(outputColumns, c)) {
                return null;   // 改名 / 未选出的列 → 回退单机
            }
        }

        // 派生内层 WHERE（inner.where + 优化器可能挂到基表的 filter）合并为单一谓词，整体下发扫描节点；
        // 引擎真正求值的那部分谓词列必须一并投影（投影先于过滤）。
        String innerFilter = null;
        List<String> whereParts = new ArrayList<>(2);
        inner.where().ifPresent(e -> whereParts.add(SqlTextUtil.stripQualifiers(e.text())));
        baseRel.filter().ifPresent(e -> whereParts.add(SqlTextUtil.stripQualifiers(e.text())));
        if (!whereParts.isEmpty()) {
            String merged = String.join(" AND ", whereParts);
            innerFilter = merged;
            String enginePart = pushdownCapable(probe) ? splitScanPredicate(merged).residual() : merged;
            if (enginePart != null) {
                for (String c : engineFilterColumns(List.of(enginePart))) {
                    if (!containsIgnoreCase(needed, c)) {
                        needed.add(c);
                    }
                }
            }
        }
        ReadRequest innerRequest = probe.pruneColumns(
                ReadRequest.of(baseRel.catalog(), baseRel.schema(), baseRel.table()), needed);
        innerRequest = (innerFilter == null || innerFilter.isBlank())
                ? innerRequest : innerRequest.withFilter(innerFilter);
        StageScheduler.DerivedWindow window = new StageScheduler.DerivedWindow(
                partitionBy, orderBy, ascending, specs, windowColumns);
        return new DerivedPlan(innerRequest, List.of(), List.of(), List.of(), null, outputColumns,
                List.of(), window);
    }

    /**
     * 静态推断表达式的 Arrow 列（供投影型派生源的<b>声明列</b>）—— 返回 {@code null} = 推断不出。
     *
     * <p>必须与 {@code ExprEvaluator} 的<b>数据相关</b>类型提升规则一致：任一行字符串 ⇒ Utf8；
     * 否则任一行为浮点/BigDecimal ⇒ Float8；否则任一行整数 ⇒ Int64；否则仅布尔 ⇒ Bool。因此只接受
     * 结果类型<b>与数据无关</b>的形态，其余保守返回 null（回退单机比计划错列型安全）。</p>
     */
    private static Field inferExprField(String name, ExprNode node, Map<String, Field> srcFields) {
        ArrowType t = inferArrowType(node, srcFields);
        return t == null ? null : new Field(name, FieldType.nullable(t), null);
    }

    /**
     * 表达式结果 Arrow 类型的静态推断（{@code null} = 不可推断）。
     *
     * <p>统一委托 {@link com.dojao.core.operator.expr.ExprTypeInference}：DECIMAL 保持 Decimal(p,s)
     * 而不是退化成 Float8 —— 否则 {@code CAST(x AS DECIMAL(20,2))} / decimal 列参与算术的投影
     * 派生源声明列会与实际页 schema 不一致（下游按声明宽度解码 ⇒ 静默错值）。</p>
     */
    private static ArrowType inferArrowType(ExprNode node, Map<String, Field> srcFields) {
        return com.dojao.core.operator.expr.ExprTypeInference.infer(node, srcFields);
    }

    /**
     * 谓词能否在<b>给定输出列集</b>上逐项求值 —— 按顶层 AND 拆开后，每个合取项都必须是
     * {@link FilterOperator} 可编译的形态（至少取出一个引用列），且引用列<b>全部</b>在输出列集里。
     *
     * <p>任一项落 {@code UNKNOWN}（解析不出列）→ false：{@code FilterOperator} 对无法解析的谓词
     * <b>保守放行全部行</b>，那等于谓词没生效（静默多返回行）—— 宁可回退单机。</p>
     */
    private static boolean filterableOnColumns(String cond, List<String> outputColumns) {
        List<String> conjuncts = SqlTextUtil.splitConjuncts(cond);
        if (conjuncts.isEmpty()) {
            return false;
        }
        for (String part : conjuncts) {
            if (part.isEmpty()) {
                return false;
            }
            List<String> cols = FilterOperator.referencedColumnsOf(part);
            if (cols.isEmpty()) {
                return false;   // UNKNOWN 形态 → 过滤不会真的发生
            }
            for (String c : cols) {
                if (!containsIgnoreCase(outputColumns, c)) {
                    return false;
                }
            }
        }
        return true;
    }

    /** {@link DerivedPlan} → 舞台层的派生源描述（{@link StageScheduler.DerivedSource}）。 */
    private static StageScheduler.DerivedSource derivedSource(DerivedPlan dp, DojaoConnector innerProbe) {
        return new StageScheduler.DerivedSource(innerProbe, dp.innerRequest(), dp.innerKeys(),
                dp.aggregations(), dp.caseProjections(), dp.havingFilter(), dp.projections(), dp.window());
    }

    /** 派生基表的轻量虚节点：只承载列清单供共享 CASE/IF 解析器做列存在性校验（永不执行算子）。 */
    private static PhysicalNode derivedInnerBaseNode(LogicalPlan.ResolvedRelation baseRel) {
        return PhysicalNode.leaf("DerivedInnerBase", null, baseRel.columns());
    }

    /** 收集 CASE 派生列引用的基表列（条件列 + 结果列引用）补进扫描 needed —— DA 头逐行算派生列需要这些列。 */
    private void addCaseReferencedColumns(CaseProjection cp, List<String> baseCols, List<String> needed) {
        for (CaseProjection.CaseBranch br : cp.branches()) {
            addCaseCondColumn(br.condition(), baseCols, needed);
            addCaseValueColumn(br.value(), baseCols, needed);
        }
        addCaseValueColumn(cp.elseValue(), baseCols, needed);
    }

    /**
     * 条件列：{@code col op literal} 左列 / {@code col IN (...)} 左列 / {@code col [NOT] LIKE '模式'} 左列，
     * 或通用表达式内引用的全部列。形状判定与 {@link #validateCondition} / CaseProjectOperator 保持一致
     * （LIKE 由 {@link SqlTextUtil#parseLike} 单点判定）。
     */
    private void addCaseCondColumn(String cond, List<String> baseCols, List<String> needed) {
        ScalarExpr.Comparison cmp = ScalarExpr.splitComparison(cond).orElse(null);
        if (cmp != null && SqlTextUtil.isColumnRef(cmp.left()) && ScalarExpr.isLiteral(cmp.right())) {
            addBaseColumn(SqlTextUtil.stripQualifier(cmp.left()), baseCols, needed);
            return;
        }
        if (cmp == null) {
            String inCol = tryParseInColumn(cond);
            if (inCol != null) {
                addBaseColumn(inCol, baseCols, needed);
                return;
            }
        }
        SqlTextUtil.LikeSpec like = SqlTextUtil.parseLike(cond);
        if (like != null) {
            addBaseColumn(like.column(), baseCols, needed);
            return;
        }
        ExprNode node = parseExprOrNull(cond);
        if (node != null) {
            for (String src : new ExprEvaluator(node).columnArgs()) addBaseColumn(src, baseCols, needed);
        }
    }

    /** 结果列：数值字面量跳过；列引用（NULL 已在解析时归一化为 null）收集；表达式收集其中引用的列。 */
    private void addCaseValueColumn(String v, List<String> baseCols, List<String> needed) {
        if (v == null) return;
        String t = v.trim();
        try {
            Double.parseDouble(t);
            return;
        } catch (NumberFormatException ignored) { }
        // 算术 / 函数表达式（sum(a+b) 的无分支派生列、CASE 结果值 rating-1）→ 收集表达式内引用的列。
        ExprNode expr = parseExprOrNull(t);
        if (expr != null && !(expr instanceof ExprNode.Column)) {
            for (String src : new ExprEvaluator(expr).columnArgs()) {
                addBaseColumn(src, baseCols, needed);
            }
            return;
        }
        addBaseColumn(SqlTextUtil.stripQualifier(t), baseCols, needed);
    }

    /** 裸列（∈ 基表列集且未在 needed）追加。 */
    private void addBaseColumn(String col, List<String> baseCols, List<String> needed) {
        if (col != null && SqlTextUtil.isSingleIdentifier(col)
                && containsIgnoreCase(baseCols, col)
                && !containsIgnoreCase(needed, col)) {
            needed.add(col);
        }
    }

    /** 列表是否含某列名（大小写不敏感）。 */
    private static boolean containsIgnoreCase(List<String> cols, String name) {
        for (String c : cols) {
            if (c.equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    /** 列名在列表中的下标（大小写不敏感），-1 未命中。 */
    private static int indexOfIgnoreCase(List<String> cols, String name) {
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).equalsIgnoreCase(name)) return i;
        }
        return -1;
    }

    /**
     * 尝试构建<b>分布式左深多 join 链</b>（1 表及以上；单 join 即「2 元素链」）—— 用
     * {@link DistributedJoinOperator} 把整条链路派发到 Worker：每层 join 的消费者把输出按下一层左键
     * 重新哈希推给下一层交换（{@link FragmentSpec.MiddleJoinConsumer}），只有最末层 join 回传 Master。
     *
     * <p><b>链上任一关系可以是派生表 / CTE</b>（子查询，展平不了的那种）：该层关系改由
     * 「扫派生基表（列裁剪 + 内层 WHERE）→ 按分组键分区 → 每分区本地聚合（HAVING）→ 按本层 join 键
     * 重哈希」的迷你流水线喂入（{@link StageScheduler.DerivedSource} +
     * {@code StageScheduler.emitDerivedSource}），<b>数据同样不回传 Master</b>。派生关系上挂的谓词
     * 由 {@link #planDerived} 落成聚合后过滤（HAVING 同款）。派生体形态不支持 → 整条链返回 null
     * 回退单机（行为不变）。</p>
     *
     * <p>本路径也是<b>两侧都是派生表</b>的两表 join 的落点（{@code tryBuildDerivedDistributedJoin}
     * 要求恰一侧派生、{@code tryBuildDistributedJoin} 要求两侧都是裸表，两者都接不住 ⇒ 单 join 的
     * 「派生 ⋈ 派生」由本路径的 2 元素链承担）。</p>
     *
     * <p>其余限制不变：无 CROSS。派生源的层一律哈希（体量不可估 → 不选广播/排序归并）。</p>
     */
    private PhysicalNode tryBuildDistributedJoinChain(List<LogicalPlan.ResolvedRelation> rels,
                                                      List<LogicalPlan.JoinInfo> joins,
                                                      List<WorkerRegistration> workers) {
        if (joins.isEmpty()) {
            return null;   // 无 join（单表）不走链式
        }
        // CROSS 环节只允许出现在<b>最末层</b>，且其右表必须是<b>全局聚合派生源</b>（恒 1 行）——
        // 「非相关标量子查询」去相关后的形状（build 侧单值广播到全部 join 分区，末层本机空键哈希 join）。
        // 其余 CROSS（逗号连接 = 左深链左端不唯一、右表是普通基表要整份广播）仍后置回退单机。
        for (int i = 0; i < joins.size(); i++) {
            if (joins.get(i).joinType() == LogicalPlan.JoinType.CROSS && i != joins.size() - 1) {
                return null;   // 中间层 CROSS 后置
            }
        }

        // 关系定位：键 = alias(rel)（已 normalize，与 buildJoins 一致）
        Map<String, LogicalPlan.ResolvedRelation> byAlias = new LinkedHashMap<>();
        for (LogicalPlan.ResolvedRelation rel : rels) {
            byAlias.put(alias(rel), rel);
        }

        // 首 join 左表 = 第一个 join 的 leftAlias（左深链唯一的显式左关系）
        LogicalPlan.ResolvedRelation firstLeft = byAlias.get(normalize(joins.get(0).leftAlias()));
        if (firstLeft == null) {
            return null;
        }

        // 派生源解析（下标 0 = 首表；下标 k = 第 k 层 join 的右表）—— null 元素 = 普通基表扫描。
        List<StageScheduler.DerivedSource> sources = new ArrayList<>(joins.size() + 1);
        DerivedPlan firstPlan = null;
        if (firstLeft.subquery().isPresent()) {
            // 派生关系上挂的谓词由 planDerived 落成「聚合后过滤」（HAVING 同款）；形态不支持才回退
            firstPlan = planDerived(firstLeft);
            if (firstPlan == null) {
                return null;   // 派生体形态不支持（窗口 / 分组键改名 / 嵌套派生 …）
            }
        }
        List<String> firstLeftCols = firstPlan == null
                ? new ArrayList<>(firstLeft.columns()) : firstPlan.outputColumns();
        // 派生源无连接器元数据 → 探针取派生基表的连接器；累加体量不可知 → UNKNOWN
        DojaoConnector firstProbe = firstPlan == null
                ? context.connectorProvider().get(firstLeft.catalog())
                : context.connectorProvider().get(firstPlan.innerRequest().catalog());
        sources.add(firstPlan == null ? null : derivedSource(firstPlan, firstProbe));
        String firstFilter = firstPlan == null ? relScanFilter(firstLeft) : null;
        // 左表（probe 侧）累计估算 —— 末层选型用它 + 右表体量跑完整决策（P1-a：可给出 BROADCAST）。
        // 非末层不消费（那几层只用右表 = 可行性判据）。行宽用左首表口径（join 输出更宽 → 低估 → 保守）。
        // 首表是派生源时体量不可知（UNKNOWN）→ 末层选型走最保守的可溢写排序归并。
        long runningLeftRows = firstPlan == null
                ? JoinCardinality.sideRows(JoinDecider.StatsProbe.of(firstProbe),
                        firstLeft.catalog(), firstLeft.schema(), firstLeft.table(), firstFilter)
                : JoinCardinality.UNKNOWN;
        long runningLeftBytes = firstPlan == null
                ? JoinDecider.estimateBytes(JoinDecider.StatsProbe.of(firstProbe),
                        firstLeft.catalog(), firstLeft.schema(), firstLeft.table(), firstFilter)
                : JoinCardinality.UNKNOWN;
        long avgRowBytes = (runningLeftRows > 0 && runningLeftBytes > 0)
                ? Math.max(1L, runningLeftBytes / runningLeftRows) : JoinCardinality.UNKNOWN;

        List<String> accumCols = firstLeftCols;
        List<StageScheduler.JoinLink> links = new ArrayList<>();
        List<String> firstLeftKeys = List.of();
        // 末层输出行数估算（EXPLAIN 用）—— 逐层推进，末层结果即整条链的输出基数
        long chainEstimatedRows = runningLeftRows;
        try {
            for (int i = 0; i < joins.size(); i++) {
                LogicalPlan.JoinInfo ji = joins.get(i);
                LogicalPlan.ResolvedRelation rightRel = byAlias.get(normalize(ji.rightAlias()));
                if (rightRel == null) {
                    return null;
                }
                // 本层右表也可是派生源（子查询 / CTE）：解析不了 → 整条链回退单机
                DerivedPlan rightPlan = null;
                if (rightRel.subquery().isPresent()) {
                    rightPlan = planDerived(rightRel);
                    if (rightPlan == null) {
                        return null;   // 派生体形态不支持 → 回退单机
                    }
                }
                List<String> rightCols = rightPlan == null
                        ? new ArrayList<>(rightRel.columns()) : rightPlan.outputColumns();
                DojaoConnector rightProbe = rightPlan == null
                        ? context.connectorProvider().get(rightRel.catalog())
                        : context.connectorProvider().get(rightPlan.innerRequest().catalog());
                sources.add(rightPlan == null ? null : derivedSource(rightPlan, rightProbe));
                // CROSS 环节（末层 + 右表是全局聚合派生源，恒 1 行）：无 join 键 —— build 侧整份
                // 复制到每个分区（单值广播），probe 侧按<b>左表全列</b>打散。仅此形状可分布式。
                boolean cross = ji.joinType() == LogicalPlan.JoinType.CROSS;
                // 全局聚合 = 聚合型派生源 + 无分组键（投影型 / 窗口型的分组键同样为空，但各有通道）
                boolean globalAggRight = rightPlan != null
                        && !rightPlan.isProjection() && !rightPlan.isWindow()
                        && rightPlan.innerKeys().isEmpty();
                if (cross && !globalAggRight) {
                    return null;   // CROSS 右表不是全局聚合派生源（如逗号连接）→ 回退单机
                }
                if (!cross && globalAggRight) {
                    // 全局聚合派生表（恒 1 行）只支持 CROSS 的「单值广播」形态：等值 join 的派生流水线
                    // 按<b>分组键</b>哈希分区，无分组键无从表达 ⇒ 回退单机（行为不变，本地完整支持）。
                    return null;
                }
                JoinKeys keys = cross
                        ? new JoinKeys(List.of(), List.of())
                        : parseJoinKeys(accumCols, rightCols, ji);
                if (i == 0) {
                    // 首 join 的 probe 侧分区键：普通哈希 join = join 左键（共分区不变量）；
                    // CROSS 无键 ⇒ 用左表全列打散（空键分区会把左表全塞进 0 号桶 → 退化单机，
                    // 且只有 0 号桶消费得到 REPLICATE 的 build 侧 ⇒ 其余分区的左行 join 不上、丢行）。
                    firstLeftKeys = cross ? new ArrayList<>(accumCols) : keys.leftKeys();
                }
                if (rightPlan == null && !distributedFilterSupported(rightRel)) {
                    return null;   // 含引擎函数谓词 → 回退单机（本地 Filter 承接，结果正确）
                }
                // 单表 WHERE 下推进 Worker 扫描：各基表过滤在扫描端执行，join 输入 / 回传数据量骤减
                String rightFilter = rightPlan == null ? relScanFilter(rightRel) : null;
                JoinType jt = mapJoinType(ji.joinType());
                boolean last = (i == joins.size() - 1);
                // 选型：非末层只看右表（可行性：哈希只约束 build 侧）；
                //       末层用「左表累计估算 + 右表体量」跑完整决策 → 右小 + 左大 → 广播（省一次 shuffle）。
                //       右表是派生源（无直扫切片、体量/ndv 都不可估）→ 一律哈希（不能广播 / 排序归并）。
                FragmentSpec.JoinAlgorithm algorithm = rightPlan != null
                        ? FragmentSpec.JoinAlgorithm.HASH
                        : last
                                ? chooseLastChainAlgorithm(runningLeftBytes, jt, rightProbe, rightRel,
                                        rightFilter, workers)
                                : chooseChainAlgorithm(rightProbe, rightRel.catalog(), rightRel.schema(),
                                        rightRel.table(), rightFilter, workers);
                links.add(new StageScheduler.JoinLink(keys.leftKeys(), keys.rightKeys(), jt,
                        rightPlan == null ? rightRel.catalog() : null,
                        rightPlan == null ? rightRel.schema() : null,
                        rightPlan == null ? rightRel.table() : null, rightProbe,
                        rightFilter, algorithm,
                        rightPlan == null ? scanColumns(rightCols, rightProbe, rightFilter) : null,
                        // CROSS 环节的左（probe）侧分区键 = 本层左表的全列（见 StageScheduler.JoinLink）
                        cross ? new ArrayList<>(accumCols) : null));
                // 本层输出估算（EXPLAIN 末层行数 / 非末层累计下推）：左表的 join 键 ndv 拿不到
                // （是上一层 join 的输出，连接器没有它的列统计）→ 传 UNKNOWN，由估算器按「维表报 pk、
                // 事实表无统计」的形态用右侧 ndv 兜底；两侧都拿不到则退回外键假设（输出 ≈ 大的一侧）。
                // 刻意不传「左表行数」当 ndv —— 那等于假设左键唯一，输出会被压成小的一侧。
                long outRows;
                if (rightPlan != null) {
                    // 派生源层：右表体量 / ndv 都拿不到（派生体不是表，无列统计）→ 估算归零
                    outRows = JoinCardinality.UNKNOWN;
                } else {
                    long rightRows = JoinCardinality.sideRows(JoinDecider.StatsProbe.of(rightProbe),
                            rightRel.catalog(), rightRel.schema(), rightRel.table(), rightFilter);
                    long ndvRight = JoinCardinality.ndvForKeys(
                            safeStats(rightProbe, rightRel.catalog(), rightRel.schema(), rightRel.table()),
                            keys.rightKeys());
                    outRows = JoinCardinality.estimateJoinRows(
                            runningLeftRows, rightRows, JoinCardinality.UNKNOWN, ndvRight);
                }
                chainEstimatedRows = outRows;
                if (!last) {
                    runningLeftRows = outRows;
                    runningLeftBytes = (rightPlan == null && outRows > 0 && avgRowBytes > 0)
                            ? outRows * avgRowBytes : JoinCardinality.UNKNOWN;
                }
                accumCols = joinOutputColumns(jt, accumCols, rightCols, keys.leftKeys(), keys.rightKeys());
            }
        } catch (RuntimeException e) {
            return null;   // 无法解析等值键 → 回退单机
        }

        int partitions = Math.max(1, context.parallelism());
        String queryId = "query-" + Long.toHexString(System.nanoTime());
        if (firstPlan == null && !distributedFilterSupported(firstLeft)) {
            return null;   // 含引擎函数谓词 → 回退单机（本地 Filter 承接，结果正确）
        }
        // 首表（T0）同样按语法层裁剪后的列集扫描；各层右表列集已随 JoinLink.rightColumns() 带上。
        // 首表是派生源时不做直扫（firstColumns = null），由派生聚合流水线喂入（sources[0]）。
        DistributedJoinOperator op = new DistributedJoinOperator(
                queryId,
                firstPlan == null ? firstLeft.catalog() : null,
                firstPlan == null ? firstLeft.schema() : null,
                firstPlan == null ? firstLeft.table() : null,
                firstLeftKeys, firstProbe,
                firstFilter,
                links, context.memoryLimit(), partitions,
                context.workerGateway(), workers, null,
                firstPlan == null ? scanColumns(firstLeftCols, firstProbe, firstFilter) : null,
                sources);

        // 逐层算法 / 派生源位置写进 EXPLAIN —— 多表链最容易看不懂的就是「每层用了什么」，不写出来只能靠猜。
        int derivedCount = (int) sources.stream().filter(java.util.Objects::nonNull).count();
        String name = "DistributedJoin[" + joins.size() + " joins, workers=" + workers.size()
                + ", partitions=" + partitions
                + ", algos=" + links.stream().map(l -> JoinExplain.label(l.algorithm())).toList()
                + (derivedCount > 0 ? ", derivedSources=" + derivedCount : "")
                + "]";
        return new PhysicalNode(name, op, accumCols, List.of()).withEstimate(chainEstimatedRows);
    }

    /** 安全探表统计（探查失败 / 抛错 → null，估算层据此退回外键假设）。 */
    private static TableStats safeStats(DojaoConnector probe, String catalog, String schema, String table) {
        try {
            return probe.getTableStats(catalog, schema, table);
        } catch (Exception e) {
            return null;
        }
    }

    /** 行数估算用的合并谓词文本（两段都空 → null；否则 AND 相连，各自补括号保持优先级）。 */
    private static String estimateFilter(String base, String extra) {
        boolean hasBase = base != null && !base.isBlank();
        boolean hasExtra = extra != null && !extra.isBlank();
        if (!hasBase) return hasExtra ? SqlTextUtil.stripQualifiers(extra) : null;
        if (!hasExtra) return base;
        return "(" + base + ") AND (" + SqlTextUtil.stripQualifiers(extra) + ")";
    }

    /** 从 {@code Limit[offset=0, limit=5]} 的显示名里取 limit 值；取不到 → null。 */
    private static final Pattern LIMIT_IN_NAME = Pattern.compile("\\blimit=(\\d+)");

    /**
     * 自下而上传播估算行数（EXPLAIN 用）—— 只在节点自身没有估算时补：
     * 透传型（Project / Sort / HashPartition / 窗口 …）取子节点估算；
     * {@code Limit} 取 {@code min(子节点估算, limit)}（LIMIT 是硬上界，不能沿用全量估算）；
     * 集合运算求和；交叉连接求积；Filter 取输入（无选择率统计，上界口径）。
     * 聚合不在此推断（分组基数需要列统计 / NDV，猜错比不显示更糟）。
     */
    private static PhysicalNode propagateEstimates(PhysicalNode node) {
        if (node == null) return null;
        List<PhysicalNode> kids = new ArrayList<>(node.children().size());
        for (PhysicalNode child : node.children()) {
            kids.add(propagateEstimates(child));
        }
        PhysicalNode rebuilt = new PhysicalNode(node.name(), node.operator(),
                node.outputColumns(), kids, node.estimatedRows());
        if (rebuilt.estimatedRows() >= 0L) {
            return rebuilt;
        }
        String type = rebuilt.type();
        long est = JoinCardinality.UNKNOWN;
        if (type.startsWith("Limit")) {
            Long limit = limitFromName(rebuilt.name());
            long childEst = kids.isEmpty() ? JoinCardinality.UNKNOWN : kids.get(0).estimatedRows();
            if (limit != null) {
                est = childEst >= 0L ? Math.min(childEst, limit) : limit;
            } else {
                est = childEst;
            }
        } else if (type.equals("Project") || type.startsWith("ScalarProject")
                || type.startsWith("ColumnAlias") || type.startsWith("Sort")
                || type.startsWith("HashPartition")
                || type.startsWith("SinglePartition") || type.startsWith("Filter")
                || type.startsWith("ExpressionProject") || type.startsWith("CaseProject")
                || type.equals("WindowFunction")) {
            if (!kids.isEmpty()) est = kids.get(0).estimatedRows();
        } else if (type.startsWith("UnionAll") || type.equals("Union")) {
            long sum = 0L;
            boolean any = false;
            for (PhysicalNode kid : kids) {
                if (kid.estimatedRows() > 0L) {
                    sum += kid.estimatedRows();
                    any = true;
                }
            }
            est = any ? sum : JoinCardinality.UNKNOWN;
        } else if (type.startsWith("CrossJoin") && kids.size() == 2) {
            long l = kids.get(0).estimatedRows(), r = kids.get(1).estimatedRows();
            if (l > 0L && r > 0L) {
                est = l > Long.MAX_VALUE / r ? Long.MAX_VALUE : l * r;
            }
        }
        return est >= 0L ? rebuilt.withEstimate(est) : rebuilt;
    }

    /** 从 {@code Limit[offset=..., limit=...]} 显示名里取 limit；无该字段（如全量 Limit 节点）→ null。 */
    private static Long limitFromName(String name) {
        Matcher m = LIMIT_IN_NAME.matcher(name);
        return m.find() ? Long.valueOf(m.group(1)) : null;
    }

    // ==================== Join 算法选型（JoinDecider） ====================

    /**
     * 两表分布式 join 的<b>算法选型</b>（规划期 / Probe 阶段，Master 决策）：
     * 探两侧表的估算体量 → 折算单台 Worker 的 join 预算 {@code U} → {@link JoinDecider} 出结论。
     *
     * <p><b>为什么在 Master 且在此刻</b>：只有规划期拿得到连接器统计（{@code getTableStats} /
     * {@code estimateRows}），而算法是「数据量 + 集群容量」的函数。Worker 拿不到（也不该拿）这些
     * 统计 —— 故 Master 只把<b>结论</b>随 {@link FragmentSpec.JoinStep} 下发。</p>
     *
     * <p>统计探不到时 {@link JoinDecider#estimateBytes} 返回 {@code UNKNOWN} → 判定走最保守的
     * 排序归并（可溢写，不赌内存）。用户可用 {@code query.join.algorithm} 强制覆盖做 A/B 对照。</p>
     */
    private FragmentSpec.JoinAlgorithm chooseJoinAlgorithm(String leftCatalog, String leftSchema, String leftTable,
                                                           DojaoConnector leftProbe, String leftFilter,
                                                           String rightCatalog, String rightSchema, String rightTable,
                                                           DojaoConnector rightProbe, String rightFilter,
                                                           JoinType jt, List<WorkerRegistration> workers) {
        long leftBytes = JoinDecider.estimateBytes(JoinDecider.StatsProbe.of(leftProbe),
                leftCatalog, leftSchema, leftTable, leftFilter);
        long rightBytes = JoinDecider.estimateBytes(JoinDecider.StatsProbe.of(rightProbe),
                rightCatalog, rightSchema, rightTable, rightFilter);
        long unit = JoinDecider.unit(context.memoryLimit(), weakestFreeBytes(workers));
        int nWorkers = workers.size();

        JoinDecider.Decision forced = JoinDecider.forced(ResourceConfig.joinAlgorithm(),
                leftBytes, rightBytes, unit, nWorkers, jt);
        JoinDecider.Decision d = forced != null ? forced
                : JoinDecider.decide(leftBytes, rightBytes, unit, nWorkers, jt);
        System.err.printf("[Planner] join 选型 %s%n", d.describe());
        return FragmentSpec.JoinAlgorithm.valueOf(d.algorithm().name());
    }

    /**
     * 多 join 链<b>非末层</b>的算法选择 —— 只用右表（build 侧）体量判定（可行性启发式，不选广播）。
     *
     * <p>依据：哈希 join 的可行性条件 {@code B ≤ M} <b>只约束 build 侧</b>，与并行度、集群规模无关。
     * 链式路径里左表是「上一层 join 的输出」，体量在规划期不可知 —— 但这不影响结论，因为左表多大
     * 都不影响哈希表装不装得下。故每层独立判：右表超出预算 → 该层排序归并，否则哈希。</p>
     *
     * <p><b>本方法不选广播</b>：不是「中间层不能广播」——{@code MiddleJoinConsumer} 本来就把
     * join 输出按下一层左键重哈希，能吸收任意输入分区方式。真障碍是链上广播要求「上一层 Consumer
     * 片段直接充当下一层执行者、膨胀的中间结果原地不动」，是一套新的片段编排，本期只做了<b>末层</b>
     * 广播（{@link #chooseLastChainAlgorithm} + {@code StageScheduler.planJoinChain} 的末层切分）。</p>
     */
    private FragmentSpec.JoinAlgorithm chooseChainAlgorithm(DojaoConnector rightProbe, String rightCatalog,
                                                            String rightSchema, String rightTable, String rightFilter,
                                                            List<WorkerRegistration> workers) {
        long rightBytes = JoinDecider.estimateBytes(JoinDecider.StatsProbe.of(rightProbe),
                rightCatalog, rightSchema, rightTable, rightFilter);
        long unit = JoinDecider.unit(context.memoryLimit(), weakestFreeBytes(workers));
        return JoinDecider.tier(rightBytes, unit) == JoinDecider.Tier.LARGE
                ? FragmentSpec.JoinAlgorithm.SORT_MERGE
                : FragmentSpec.JoinAlgorithm.HASH;
    }

    /**
     * 多 join 链<b>末层</b>的算法选择 —— 用「左表（中间结果）累计估算 + 右表体量」跑完整的
     * {@link JoinDecider#decide}，因此<b>可能给出 BROADCAST</b>（P1-a）。
     *
     * <p>为什么末层能用左表体量而中间层不能：中间层广播要改片段编排（见
     * {@link #chooseChainAlgorithm}），而末层广播只是把「最后一层的分区方式」从 HASH/n 换成
     * REPLICATE+FIXED/w —— 复用单 join 广播已实现的机制，{@code StageScheduler} 侧零新算子。</p>
     *
     * <p>左表估算来自 {@link JoinCardinality}（{@code |L⋈R| = |L|×|R|/ndv}，ndv 取不到按外键假设
     * → 输出 ≈ 大的一侧）。估不到（{@code UNKNOWN}）时 {@code decide} 一律走最保守的排序归并 ——
     * 与统计不可得时的行为一致。</p>
     *
     * <p><b>安全门由 {@code decide} 兜住</b>：只有 INNER / LEFT 允许广播（RIGHT / FULL 复制 build 侧
     * 会重复输出「无匹配的 build 行」），{@code query.join.broadcast.enabled=false} 可整体关闭。</p>
     */
    private FragmentSpec.JoinAlgorithm chooseLastChainAlgorithm(long leftBytes, JoinType jt,
                                                                DojaoConnector rightProbe,
                                                                LogicalPlan.ResolvedRelation rightRel,
                                                                String rightFilter,
                                                                List<WorkerRegistration> workers) {
        long rightBytes = JoinDecider.estimateBytes(JoinDecider.StatsProbe.of(rightProbe),
                rightRel.catalog(), rightRel.schema(), rightRel.table(), rightFilter);
        long unit = JoinDecider.unit(context.memoryLimit(), weakestFreeBytes(workers));
        int nWorkers = workers.size();

        JoinDecider.Decision forced = JoinDecider.forced(ResourceConfig.joinAlgorithm(),
                leftBytes, rightBytes, unit, nWorkers, jt);
        JoinDecider.Decision d = forced != null ? forced
                : JoinDecider.decide(leftBytes, rightBytes, unit, nWorkers, jt);
        System.err.printf("[Planner] join 链末层选型 %s%n", d.describe());
        return FragmentSpec.JoinAlgorithm.valueOf(d.algorithm().name());
    }

    /** 最弱一台 Worker 的物理可用内存（字节）；无心跳数据 → {@code -1}（判定只用配置值）。 */
    private static long weakestFreeBytes(List<WorkerRegistration> workers) {
        long min = -1L;
        for (WorkerRegistration w : workers) {
            if (w.lastResource() == null) continue;
            long free = Math.max(0L, w.lastResource().totalMemoryBytes() - w.lastResource().usedMemoryBytes());
            if (free > 0 && (min < 0 || free < min)) {
                min = free;
            }
        }
        return min;
    }

    /** 等值 join 键对：左键列名 + 右键列名（按 ON 条件顶层 AND 逐个合取解析）。 */
    private record JoinKeys(List<String> leftKeys, List<String> rightKeys) {
    }

    /** 解析 ON 条件的等值 join 键（与 {@link #buildJoin} 同一逻辑，供分布式路径复用）。 */
    private JoinKeys parseJoinKeys(List<String> leftCols, List<String> rightCols, LogicalPlan.JoinInfo ji) {
        String cond = SqlTextUtil.stripQualifiers(ji.condition().text());
        List<String> leftKeys = new ArrayList<>();
        List<String> rightKeys = new ArrayList<>();
        for (String c : SqlTextUtil.splitTopLevelKeyword(cond, "AND")) {
            if (c.isBlank()) continue;
            List<String> sides = SqlTextUtil.splitTopLevelEquals(c);
            if (sides.size() != 2) {
                throw unsupported("非等值 JOIN 条件: " + c);
            }
            String a = SqlTextUtil.stripQualifier(sides.get(0));
            String b = SqlTextUtil.stripQualifier(sides.get(1));
            boolean aInLeft = leftCols.contains(a), aInRight = rightCols.contains(a);
            boolean bInLeft = leftCols.contains(b), bInRight = rightCols.contains(b);
            if (aInLeft && bInRight) {
                leftKeys.add(a);
                rightKeys.add(b);
            } else if (bInLeft && aInRight) {
                leftKeys.add(b);
                rightKeys.add(a);
            } else {
                throw unsupported(joinKeyError(ji, c, leftCols, rightCols));
            }
        }
        if (leftKeys.isEmpty()) {
            throw unsupported("未找到等值 JOIN 键: " + cond);
        }
        return new JoinKeys(leftKeys, rightKeys);
    }

    /**
     * JOIN 键解析失败的报错文本。
     *
     * <p>{@code col = 字面量} 这类<b>单侧过滤</b>是压测里最常见的形态：INNER JOIN 会被优化器
     * （{@code PredicatePushdown}）提前搬进扫描；外连接的<b>保留侧</b>不能提前过滤（未匹配行
     * 仍须输出），所以会走到这里 —— 必须明确告诉用户该搬到 WHERE，而不是笼统的「无法解析」。</p>
     */
    private static String joinKeyError(LogicalPlan.JoinInfo ji, String conjunct,
                                       List<String> leftCols, List<String> rightCols) {
        List<String> sides = SqlTextUtil.splitTopLevelEquals(conjunct);
        if (sides.size() == 2) {
            boolean leftLiteral = isLiteralToken(sides.get(0));
            boolean rightLiteral = isLiteralToken(sides.get(1));
            if (leftLiteral != rightLiteral) {
                String column = SqlTextUtil.stripQualifier(leftLiteral ? sides.get(1) : sides.get(0));
                if (leftCols.contains(column) || rightCols.contains(column)) {
                    if (ji.joinType() == LogicalPlan.JoinType.INNER) {
                        return "JOIN ON 中的单侧过滤条件不是连接键: " + conjunct.trim()
                                + "（INNER JOIN 应由谓词下推提前过滤，请确认优化器已开启）";
                    }
                    return "JOIN ON 中的单侧过滤条件无法提前下推: " + conjunct.trim()
                            + "。" + ji.joinType() + " JOIN 的保留侧不能用 ON 条件过滤"
                            + "（未匹配行仍须输出），请将该条件移到 WHERE；INNER JOIN 可直接写在 ON 中。";
                }
            }
        }
        return "无法解析 JOIN 键: " + conjunct
                + "（左: " + leftCols + " / 右: " + rightCols + "）";
    }

    /** {@code 字面量} 粗判（引号串 / 数字 / TRUE / FALSE / NULL），仅用于拼报错。 */
    private static boolean isLiteralToken(String side) {
        String t = side.trim();
        if (t.isEmpty()) return false;
        if (SqlTextUtil.isQuotedLiteral(t)) return true;
        if (t.equalsIgnoreCase("null") || t.equalsIgnoreCase("true") || t.equalsIgnoreCase("false")) {
            return true;
        }
        return t.matches("[+-]?\\d+(\\.\\d+)?([eE][+-]?\\d+)?");
    }

    private PhysicalNode buildCrossJoin(PhysicalNode left, PhysicalNode right) {
        CrossJoinOperator join = new CrossJoinOperator(left.operator(), right.operator());
        List<String> outCols = joinOutputColumns(JoinType.CROSS, left.outputColumns(), right.outputColumns(),
                List.of(), List.of());
        // 估算行数 = 两侧乘积（任一未知 → UNKNOWN；溢出按 Long.MAX_VALUE 封顶）
        long estimatedRows = JoinCardinality.UNKNOWN;
        long l = left.estimatedRows(), r = right.estimatedRows();
        if (l > 0 && r > 0) {
            estimatedRows = l > Long.MAX_VALUE / r ? Long.MAX_VALUE : l * r;
        }
        return new PhysicalNode("CrossJoin", join, outCols, left, right).withEstimate(estimatedRows);
    }

    /**
     * 输出列 = 左表全部列 + 右表列。
     *
     * <p><b>去重口径（按 join 类型）</b>：仅 INNER 剔除「与左键同名的右键」——内连接两侧键值恒等，
     * 剔掉重复列不改变语义（{@code select *} 少一列同值列）。<b>外连接必须保留右键列</b>：
     * 未匹配行的右键为 NULL，而左键非 NULL，两者语义不同；丢掉右键列后外层 {@code b.key} 会按
     * 裸列名静默绑到左键列（恒非 NULL），{@code b.key IS NULL} / {@code count(b.key)} 全错。</p>
     */
    private static List<String> joinOutputColumns(JoinType joinType, List<String> leftOut, List<String> rightOut,
                                                   List<String> leftKeys, List<String> rightKeys) {
        List<String> out = new ArrayList<>(leftOut);
        boolean dedup = joinType == JoinType.INNER;
        for (String rc : rightOut) {
            if (dedup && rightKeys.contains(rc) && leftKeys.contains(rc)) continue;
            out.add(rc);
        }
        return out;
    }

    // ==================== 外连接同名列消歧 ====================

    /**
     * 内部消歧列名前缀：外连接保留下来的右键列与左键同名时，追加一个 {@code __jk_<col>_<k>}
     * 副本（{@link ColumnAliasOperator}），限定引用改写为该副本名。
     *
     * <p>该前缀同时是「内部列」约定：{@code SELECT *} 展开（规划期 + 运行期 Page.select）必须排除。</p>
     */
    static final String JOIN_KEY_ALIAS_PREFIX = "__jk_";

    /**
     * 窗口键表达式物化列前缀：{@code PARTITION BY coalesce(x,'-')} 之类不可能直接按列名求值，
     * 规划期先用 {@link ScalarProjectOperator} 物化成 {@code __win_key_N} 再交给窗口算子。
     *
     * <p>同为「内部列」约定：{@code SELECT *} 展开（规划期 + 运行期 Page.select）必须排除。</p>
     */
    static final String WINDOW_KEY_ALIAS_PREFIX = "__win_key_";
    /** 窗口函数<b>值参数</b>表达式物化列前缀（如 {@code lag(CAST(x AS DOUBLE),1)} 的 CAST）。 */
    static final String WINDOW_ARG_ALIAS_PREFIX = "__win_arg_";

    /**
     * 「窗口嵌在表达式里」时的窗口值物化列前缀（{@code __win_expr_<SELECT 项序号>_<窗口序号>}）。
     *
     * <p>{@code gmv / sum(gmv) OVER ()} 的 SELECT 项保留完整原文，窗口部分单独物化成一列，
     * 最后由标量投影把合成列回写进表达式求值 —— 否则外层 {@code + - * /} 会被静默丢弃。</p>
     */
    static final String WINDOW_EXPR_ALIAS_PREFIX = "__win_expr_";

    /** 窗口值物化列名。 */
    static String windowExprColumn(int itemIndex, int windowIndex) {
        return WINDOW_EXPR_ALIAS_PREFIX + itemIndex + "_" + windowIndex;
    }

    /** {@link #aliasOuterJoinKeyRefs} 的返回：改写后的 SELECT + 可能被包了一层的物理节点。 */
    private record OuterJoinRefRewrite(LogicalPlan.Select select, PhysicalNode node) {}

    /**
     * 外连接同名列消歧（LEFT / RIGHT / FULL）—— 修「{@code b.key IS NULL} 恒假 / {@code count(b.key)}
     * 退化成 {@code count(*)}」。
     *
     * <p><b>根因</b>：外连接的右键列必须保留（NULL vs 非 NULL 是语义），而左右键同名
     * （{@code a.user_id = b.user_id}）⇒ join 输出出现两个 {@code user_id}。物理层表达式按裸列名
     * 解析、同名取首个 ⇒ 限定引用 {@code b.user_id} 静默绑到左列（外连接后恒非 NULL）。</p>
     *
     * <p><b>修法</b>：给每一个重复出现的同名列（左右两侧、按出现次序）追加唯一命名副本
     * （{@code __jk_<col>_<k>}），把 SELECT / WHERE / GROUP BY / HAVING / ORDER BY / 窗口定义里的
     * <b>限定引用</b> {@code alias.col} 改写为该关系对应的副本名，使限定引用真正落到自己那一侧。
     * 无别名的裸列引用不在此改写（保持原语义）。</p>
     *
     * <p><b>保守口径</b>：关系链声明的列按序拼不出实际 join 输出列（派生 / 展平 / 裁剪过的形态）时
     * 原样返回，绝不猜测 —— 宁可不修也不改错。</p>
     */
    private OuterJoinRefRewrite aliasOuterJoinKeyRefs(PhysicalNode node, LogicalPlan.Select select) {
        List<String> outCols = node.outputColumns();
        if (select.joins().isEmpty() || outCols.size() < 2) {
            return new OuterJoinRefRewrite(select, node);
        }

        // ① 左深关系链顺序（join0.left + 各 join 的右表）—— 与 buildJoins 的输出列拼接顺序一致
        Map<String, LogicalPlan.ResolvedRelation> byAlias = new LinkedHashMap<>();
        for (LogicalPlan.ResolvedRelation rel : select.fromRelations()) {
            byAlias.put(alias(rel), rel);
        }
        List<LogicalPlan.ResolvedRelation> order = new ArrayList<>();
        order.add(byAlias.get(normalize(select.joins().get(0).leftAlias())));
        for (LogicalPlan.JoinInfo ji : select.joins()) {
            order.add(byAlias.get(normalize(ji.rightAlias())));
        }
        for (LogicalPlan.ResolvedRelation rel : order) {
            if (rel == null) return new OuterJoinRefRewrite(select, node);
        }

        // ② 校验：各关系声明列按序拼接 == 实际 join 输出列（大小写不敏感，按名逐一比对）
        List<String> flat = new ArrayList<>();
        for (LogicalPlan.ResolvedRelation rel : order) flat.addAll(rel.columns());
        if (flat.size() != outCols.size()) return new OuterJoinRefRewrite(select, node);
        for (int i = 0; i < flat.size(); i++) {
            if (!normalize(flat.get(i)).equals(normalize(outCols.get(i)))) {
                return new OuterJoinRefRewrite(select, node);
            }
        }

        // ③ 重复列名（INNER 已去重，出现重复即外连接保留的右键冲突列）
        Map<String, Integer> total = new HashMap<>();
        for (String c : outCols) total.merge(normalize(c), 1, Integer::sum);

        // ④ 重复出现的列（左右两侧同名）→ 每个出现次序各给一个副本名。
        //    ★左右两侧都要别名：物理层同名裸列解析并不一致（ExprEvaluator 取首个、
        //    VectorSchemaRoot.getVector 取最后一个），任何一侧靠"裸名默认"都可能绑错。
        //    给每一次出现都建唯一副本后，限定引用（a.col / b.col）各自改写到自己那一侧，
        //    彻底不依赖同名解析顺序。
        Map<String, String> qualified = new LinkedHashMap<>();   // "alias.col"（小写） → 副本名
        List<ColumnAliasOperator.Alias> aliases = new ArrayList<>();
        Set<String> used = new HashSet<>();
        for (String c : outCols) used.add(normalize(c));
        Map<String, Integer> seen = new HashMap<>();
        for (int r = 0; r < order.size(); r++) {
            LogicalPlan.ResolvedRelation rel = order.get(r);
            String relKey = alias(rel);
            for (String col : rel.columns()) {
                String key = normalize(col);
                int occ = seen.merge(key, 1, Integer::sum);
                if (total.getOrDefault(key, 0) <= 1) continue;
                String aliasName = JOIN_KEY_ALIAS_PREFIX + col + "_" + occ;
                int dup = 2;
                while (used.contains(normalize(aliasName))) {
                    aliasName = JOIN_KEY_ALIAS_PREFIX + col + "_" + occ + "_" + (dup++);
                }
                used.add(normalize(aliasName));
                aliases.add(new ColumnAliasOperator.Alias(col, occ, aliasName));
                qualified.put(relKey + "." + key, aliasName);
            }
        }
        if (aliases.isEmpty()) return new OuterJoinRefRewrite(select, node);

        // ⑤ 改写 SELECT 各子句中的限定引用
        List<LogicalPlan.SelectItem> items = new ArrayList<>(select.selectItems().size());
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            // 窗口调用清单必须一并保留并改写：丢掉它会让「嵌在表达式里的窗口」变回普通表达式。
            List<LogicalPlan.WindowCall> calls = new ArrayList<>(item.windowCalls().size());
            for (LogicalPlan.WindowCall wc : item.windowCalls()) {
                calls.add(new LogicalPlan.WindowCall(wc.text(),
                        (Expression.WindowFunction) rewriteJoinKeyRefs(wc.window(), qualified)));
            }
            items.add(new LogicalPlan.SelectItem(rewriteJoinKeyRefs(item.expression(), qualified),
                    item.alias(), calls));
        }
        List<Expression> groupBy = new ArrayList<>(select.groupBy().size());
        for (Expression e : select.groupBy()) groupBy.add(rewriteJoinKeyRefs(e, qualified));
        List<LogicalPlan.SortItem> orderBy = new ArrayList<>(select.orderBy().size());
        for (LogicalPlan.SortItem si : select.orderBy()) {
            orderBy.add(new LogicalPlan.SortItem(rewriteJoinKeyRefs(si.expression(), qualified),
                    si.ascending(), si.nullsFirst()));
        }
        LogicalPlan.Select rewritten = new LogicalPlan.Select(items, select.fromRelations(),
                select.joins(), select.where().map(e -> rewriteJoinKeyRefs(e, qualified)),
                groupBy, select.having().map(e -> rewriteJoinKeyRefs(e, qualified)),
                select.scalarSubqueries(), orderBy, select.limit(), select.offset(),
                select.distinct());

        ColumnAliasOperator op = new ColumnAliasOperator(node.operator(), outCols, aliases);
        PhysicalNode wrapped = new PhysicalNode("ColumnAlias" + aliases.stream()
                .map(ColumnAliasOperator.Alias::aliasName).toList(), op, op.outputColumns(), node);
        return new OuterJoinRefRewrite(rewritten, wrapped);
    }

    /** 表达式里的限定引用 {@code alias.col} → 消歧副本名（副本不存在时原样返回）。 */
    private static Expression rewriteJoinKeyRefs(Expression e, Map<String, String> qualified) {
        if (qualified.isEmpty()) return e;
        if (e instanceof Expression.ColumnRef cr) {
            if (cr.qualifier() == null) return e;
            String aliasName = qualified.get(normalize(cr.qualifier()) + "." + normalize(cr.columnName()));
            return aliasName == null ? e : new Expression.ColumnRef(cr.qualifier(), aliasName);
        }
        if (e instanceof Expression.OpaqueExpr op) {
            String text = rewriteQualifiedText(op.originalText(), qualified);
            return text.equals(op.originalText()) ? e
                    : new Expression.OpaqueExpr(text, op.resolvedColumns());
        }
        if (e instanceof Expression.WindowFunction w) {
            List<String> args = rewriteAll(w.args(), qualified);
            List<String> partitionBy = rewriteAll(w.partitionBy(), qualified);
            List<String> orderBy = rewriteAll(w.orderBy(), qualified);
            if (args == w.args() && partitionBy == w.partitionBy() && orderBy == w.orderBy()) return e;
            return new Expression.WindowFunction(w.functionName(), args, partitionBy, orderBy,
                    w.windowFrame(), w.resolvedColumns());
        }
        return e;
    }

    /** 逐项改写、无变化时返回原 list（便于调用方判断是否需要重建表达式）。 */
    private static List<String> rewriteAll(List<String> texts, Map<String, String> qualified) {
        boolean changed = false;
        List<String> out = new ArrayList<>(texts.size());
        for (String t : texts) {
            String rewritten = rewriteQualifiedText(t, qualified);
            if (!rewritten.equals(t)) changed = true;
            out.add(rewritten);
        }
        return changed ? out : texts;
    }

    /**
     * 文本级改写：把 {@code qualifier.column} 形式的引用替换为消歧副本名。
     * 跳过字符串字面量与带引号的标识符（{@code '...'} / {@code "..."} / {@code `...`}）；
     * 三段式（{@code db.schema.col}）不处理。
     */
    private static String rewriteQualifiedText(String text, Map<String, String> qualified) {
        if (text == null || text.isEmpty()) return text;
        StringBuilder out = new StringBuilder(text.length());
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                char quote = c;
                out.append(c);
                i++;
                while (i < text.length()) {
                    char d = text.charAt(i);
                    out.append(d);
                    i++;
                    if (d == quote) {
                        if (i < text.length() && text.charAt(i) == quote) {
                            out.append(quote);   // 连写两个引号 = 转义
                            i++;
                        } else {
                            break;
                        }
                    }
                }
                continue;
            }
            if (Character.isLetter(c) || c == '_') {
                int start = i;
                while (i < text.length()
                        && (Character.isLetterOrDigit(text.charAt(i)) || text.charAt(i) == '_')) {
                    i++;
                }
                String first = text.substring(start, i);
                int dot = i;
                while (dot < text.length() && Character.isWhitespace(text.charAt(dot))) dot++;
                if (dot < text.length() && text.charAt(dot) == '.') {
                    int secondStart = dot + 1;
                    while (secondStart < text.length()
                            && Character.isWhitespace(text.charAt(secondStart))) {
                        secondStart++;
                    }
                    if (secondStart < text.length()
                            && (Character.isLetter(text.charAt(secondStart))
                                || text.charAt(secondStart) == '_')) {
                        int secondEnd = secondStart;
                        while (secondEnd < text.length()
                                && (Character.isLetterOrDigit(text.charAt(secondEnd))
                                    || text.charAt(secondEnd) == '_')) {
                            secondEnd++;
                        }
                        boolean threePart = secondEnd < text.length()
                                && text.charAt(secondEnd) == '.';
                        if (!threePart) {
                            String second = text.substring(secondStart, secondEnd);
                            String aliasName = qualified.get(
                                    normalize(first) + "." + normalize(second));
                            if (aliasName != null) {
                                out.append(aliasName);
                                i = secondEnd;
                                continue;
                            }
                        }
                    }
                }
                out.append(first);
                continue;
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    /** 内部消歧列名 → 用户可见列名（{@code __jk_user_id_2} → {@code user_id}）；非内部名原样返回。 */
    static String displayJoinAliasName(String name) {
        if (name == null || !name.startsWith(JOIN_KEY_ALIAS_PREFIX)) return name;
        String rest = name.substring(JOIN_KEY_ALIAS_PREFIX.length());
        int us = rest.lastIndexOf('_');
        if (us <= 0 || us >= rest.length() - 1) return name;
        String suffix = rest.substring(us + 1);
        for (int i = 0; i < suffix.length(); i++) {
            if (!Character.isDigit(suffix.charAt(i))) return name;
        }
        return rest.substring(0, us);
    }

    /** 内部列（外连接消歧副本 / 窗口键物化列 / 窗口值物化列）—— {@code SELECT *} 展开必须排除。 */
    private static boolean isJoinKeyAlias(String name) {
        return name != null
                && (name.startsWith(JOIN_KEY_ALIAS_PREFIX)
                    || name.startsWith(WINDOW_KEY_ALIAS_PREFIX)
                    || name.startsWith(WINDOW_ARG_ALIAS_PREFIX)
                    || name.startsWith(WINDOW_EXPR_ALIAS_PREFIX));
    }

    private static JoinType mapJoinType(LogicalPlan.JoinType jt) {
        return switch (jt) {
            case INNER -> JoinType.INNER;
            case LEFT -> JoinType.LEFT;
            case RIGHT -> JoinType.RIGHT;
            case FULL -> JoinType.FULL;
            case CROSS -> JoinType.CROSS;
        };
    }

    // ==================== WHERE（残留） ====================

    /**
     * 在 {@code upstream} 之上施加残留 WHERE —— 逐顶层 AND 合取项各包一层 {@link FilterOperator}。
     *
     * <p><b>★判据 = {@code FilterOperator} 能不能编译，而不是一张硬编码白名单。</b>原先此处（及
     * {@code buildScan} 的派生表外层 WHERE 分支）用的判据是 {@code isSimpleFilter || isFunctionPredicate}，
     * 这两者随 {@code FilterOperator} 陆续支持 {@code [NOT] IN (...)} / {@code [NOT] LIKE} 而<b>漂移滞后</b>：
     * 白名单外、算子却能算的谓词（如 {@code col IN (2,3,4)}）被误判「不支持」直接抛错 —— 实测
     * 派生 join 子查询里的 {@code WHERE o.order_status IN (2,3,4)} 规划失败。这里改为：
     * 每个合取项都要能被 {@code FilterOperator} 编译出引用列（{@link FilterOperator#referencedColumnsOf}），
     * 否则落 {@code Form.UNKNOWN} → <b>静默放行全部行</b>（谓词失效），宁可报不支持。</p>
     *
     * <p>旧形态（裸列比较 / 引擎标量函数谓词）仍直接接受 —— 保留原行为，不做列存在性核对。</p>
     */
    private PhysicalNode applyFilterIfSimple(PhysicalNode upstream, String whereText, String context) {
        String cond = SqlTextUtil.stripQualifiers(whereText);
        if (!SqlTextUtil.isSimpleFilter(cond) && !ScalarExpr.isFunctionPredicate(cond)
                && !filterCompilable(cond)) {
            throw unsupported(context + ": " + cond);
        }
        List<String> leaves = SqlTextUtil.splitConjuncts(cond);
        if (leaves.isEmpty()) {
            throw unsupported(context + ": " + cond);
        }
        // 逐项包 Filter（而非一条 AND 合取串）：FilterOperator 只编译单个简单谓词，合取串会落
        // UNKNOWN 静默放行全部行（见 buildScan 的同一处理）。
        PhysicalNode current = upstream;
        for (String leaf : leaves) {
            FilterOperator filter = new FilterOperator(current.operator(), leaf);
            current = new PhysicalNode("Filter[" + leaf + "]", filter, current.outputColumns(), current);
        }
        return tryPushDownWindowFilter(current);
    }

    /**
     * 谓词的每个顶层 AND 合取项都能被 {@link FilterOperator} 编译出引用列（形态已知，不会落
     * {@code UNKNOWN} 静默放行全部行）—— 覆盖 {@code isSimpleFilter}/{@code isFunctionPredicate}
     * 之外、算子本就支持的形态：{@code col [NOT] IN (...)}、{@code col [NOT] LIKE '模式'}，
     * 以及由多个可编译项 AND 成的合取。口径与 {@link #filterableOnColumns} 同源
     * （{@link FilterOperator#referencedColumnsOf} 是「算子能不能算」的唯一真相）。
     */
    private static boolean filterCompilable(String cond) {
        List<String> leaves = SqlTextUtil.splitConjuncts(cond);
        if (leaves.isEmpty()) {
            return false;
        }
        for (String leaf : leaves) {
            if (FilterOperator.referencedColumnsOf(leaf).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    // ==================== 标量子查询去相关 ====================

    /**
     * 去相关结果：派生关系（{@code subquery} 非空，可像普通派生表一样喂给
     * {@link #buildJoins} 从而走分布式 join 路径）+ LEFT JOIN 的 ON 条件
     * （非相关为 null → CROSS JOIN）。
     */
    private record DecorrelatedJoin(LogicalPlan.ResolvedRelation relation, String onCondition) {}

    /** 去相关结果：重写后的 SELECT 项 / WHERE + 需要 join 的派生表列表。 */
    private record Decorrelation(List<LogicalPlan.SelectItem> rewrittenItems,
                                  Optional<Expression> rewrittenWhere,
                                  List<DecorrelatedJoin> joins) {}

    private static final int REF_INNER = 0;   // 引用内层表列
    private static final int REF_OUTER = 1;   // 引用外层列
    private static final int REF_UNKNOWN = 2; // 无法识别

    /**
     * 把 SELECT / WHERE 中的标量聚合子查询去相关：每个 {@link Expression.ScalarSubquery}
     * （按原文去重，同文共享一张派生表）改写为 LEFT JOIN 内层聚合派生表
     * （相关键 GROUP BY 聚合），SELECT 项 / WHERE 文本中的子查询调用替换为派生列
     * {@code __subq_val} / {@code __subq_val_N}。
     */
    private Decorrelation decorrelate(LogicalPlan.Select select, List<String> outerCols) {
        List<String> outerAliases = select.fromRelations().stream().map(Planner::alias).toList();

        // 原文 → 替换文本：普通标量子查询直接替换为派生列；EXISTS 内层已改写为 count(*)，
        // 替换为对派生列的判定（NOT EXISTS 用 COALESCE 处理 LEFT JOIN 未命中时的 NULL）。
        Map<String, String> replacementByText = new LinkedHashMap<>();
        Set<String> negatedTexts = new HashSet<>();
        Map<String, DecorrelatedJoin> joinByText = new LinkedHashMap<>();
        int seq = 0;
        for (Expression.ScalarSubquery sub : select.scalarSubqueries()) {
            String text = sub.originalText();
            if (replacementByText.containsKey(text)) continue;
            String name = (seq == 0) ? "__subq_val" : "__subq_val_" + seq;
            seq++;
            // EXISTS 内层是 count(*)：COALESCE 处理 LEFT JOIN 未命中的 NULL
            // （正向判定必须落到 false 而不是 NULL；NOT EXISTS 必须落到 true）。
            String replacement = !sub.exists() ? name
                    : sub.negated()
                        ? "(COALESCE(" + name + ", 0) = 0)"
                        : "(COALESCE(" + name + ", 0) > 0)";
            if (sub.negated()) negatedTexts.add(text);
            replacementByText.put(text, replacement);
            joinByText.put(text, buildDerivedJoin(sub, name, outerCols, outerAliases));
        }

        // 重写 SELECT 项：OpaqueExpr 文本中把子查询调用替换为派生列（其余表达式原样保留）
        List<LogicalPlan.SelectItem> rewrittenItems = new ArrayList<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            if (item.expression() instanceof Expression.OpaqueExpr oe) {
                String t = oe.originalText();
                for (Map.Entry<String, String> e : replacementByText.entrySet()) {
                    t = replaceSubqueryText(t, e.getKey(), e.getValue(),
                            negatedTexts.contains(e.getKey()));
                }
                rewrittenItems.add(new LogicalPlan.SelectItem(
                        new Expression.OpaqueExpr(t), item.alias(), item.windowCalls()));
            } else {
                rewrittenItems.add(item);
            }
        }

        // 重写 WHERE：子查询调用 → 派生列，随后由 Filter（列间比较）求值
        Optional<Expression> rewrittenWhere = select.where().map(w -> {
            String t = w.text();
            for (Map.Entry<String, String> e : replacementByText.entrySet()) {
                t = replaceSubqueryText(t, e.getKey(), e.getValue(),
                        negatedTexts.contains(e.getKey()));
            }
            return (Expression) new Expression.OpaqueExpr(t);
        });

        return new Decorrelation(rewrittenItems, rewrittenWhere, List.copyOf(joinByText.values()));
    }

    /**
     * 把表达式文本中的子查询原文替换为改写后的判定。
     *
     * <p>{@code NOT EXISTS (...)} 的语法形态是 {@code NOT(booleanExpression(EXISTS ...))}
     * —— {@code ExistsContext} 原文不包含前导 NOT，故反向子查询还要把紧邻的 {@code NOT}
     * 一并吃掉（否则会得到 {@code NOT (count = 0)}，语义正好相反）。</p>
     *
     * <p>{@code NOT (EXISTS (...))} 还要跨过包裹子查询的括号（报告 BUG-009）：从匹配点向后跳过
     * 空白与平衡的 {@code (}，找到 {@code NOT} 后连开关括号一并吞掉 —— 否则会得到
     * {@code NOT ((count = 0))}（双重取反）。括号不配对时只吞 {@code NOT}、保留括号：语义仍正确
     * （括号原样包住替换文本）。</p>
     */
    private static String replaceSubqueryText(String text, String original,
                                              String replacement, boolean stripLeadingNot) {
        int from = 0;
        StringBuilder out = new StringBuilder(text.length());
        while (true) {
            int i = text.indexOf(original, from);
            if (i < 0) {
                out.append(text, from, text.length());
                return out.toString();
            }
            int repStart = i;
            int repEnd = i + original.length();
            if (stripLeadingNot) {
                int j = i - 1;
                while (j >= 0 && Character.isWhitespace(text.charAt(j))) j--;
                int openParens = 0;
                while (j >= 0 && text.charAt(j) == '(') {
                    openParens++;
                    j--;
                    while (j >= 0 && Character.isWhitespace(text.charAt(j))) j--;
                }
                int notStart = j - 2;   // "NOT" 的起点
                if (notStart >= 0 && text.regionMatches(true, notStart, "NOT", 0, 3)
                        && (notStart == 0 || !isIdentifierChar(text.charAt(notStart - 1)))) {
                    repStart = notStart;
                    // 与被跳过的开括号一一配对地吞掉尾部右括号（只跨空白）；任一不配对 → 全部保留
                    int k = repEnd;
                    for (int p = 0; p < openParens; p++) {
                        while (k < text.length() && Character.isWhitespace(text.charAt(k))) k++;
                        if (k < text.length() && text.charAt(k) == ')') {
                            k++;
                        } else {
                            k = repEnd;
                            break;
                        }
                    }
                    repEnd = k;
                }
            }
            out.append(text, from, repStart).append(replacement);
            from = repEnd;
        }
    }

    private static boolean isIdentifierChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    /**
     * 为单个标量聚合子查询构建去相关派生表：
     * {@code (SELECT max(rating) FROM user_review r2 WHERE r2.user_id = r1.user_id AND r2.s = 'a')}
     * → 派生表 {@code SELECT user_id AS user_id, max(rating) AS __subq_val FROM user_review r2
     * WHERE s = 'a' GROUP BY user_id}，外层按 {@code r1.user_id = user_id} LEFT JOIN（相关键）
     * 或 CROSS JOIN（非相关全局聚合，恒 1 行）。
     *
     * <p>仅支持：内层为单表 SELECT、单聚合函数、无 JOIN/GROUP BY/HAVING/嵌套子查询；
     * 相关谓词须为等值（一侧内层列、一侧外层列）；纯内层谓词下推进内层派生表 WHERE。
     * 其余形态抛清晰错误（不静默错）。</p>
     */
    private DecorrelatedJoin buildDerivedJoin(Expression.ScalarSubquery sub, String subqVal,
                                              List<String> outerCols, List<String> outerAliases) {
        String kind = sub.exists() ? "EXISTS 子查询" : "标量子查询";
        if (!(sub.inner() instanceof LogicalPlan.Select inner)) {
            throw unsupported(kind + "内层非 SELECT: " + sub.originalText());
        }
        if (inner.selectItems().size() != 1) {
            throw unsupported(kind + "须为单个聚合: " + sub.originalText());
        }
        // ★报错文案按「用户该怎么改」拆开（缺陷报告 BUG-012a/b/c）：旧文案把
        //   「JOIN / GROUP BY / HAVING / 嵌套子查询」混成一句，而<b>单层 GROUP BY 实际可用</b>
        //   （存在性语义与分组无关，语义层已丢弃）—— 用户据此改写会白白放弃可行写法。
        if (inner.fromRelations().size() != 1) {
            throw unsupported(kind + "仅支持单表 FROM（内层多表 JOIN 暂不支持）。"
                    + "可改写为：JOIN (SELECT <键> FROM <表1> [JOIN <表2> ON ...] GROUP BY <键>) v"
                    + " ON <外层键> = v.<键>，由 JOIN 承接过滤: " + sub.originalText());
        }
        if (!inner.joins().isEmpty()) {
            throw unsupported(kind + "内层多表 JOIN 暂不支持。"
                    + "可改写为：JOIN (SELECT <键> FROM <表1> JOIN <表2> ON ... GROUP BY <键>) v"
                    + " ON <外层键> = v.<键>，由 JOIN 承接过滤: " + sub.originalText());
        }
        if (inner.having().isPresent()) {
            throw unsupported(kind + "内层 HAVING 暂不支持。"
                    + "可改写为：JOIN (SELECT <键> FROM <表> GROUP BY <键> HAVING ...) v"
                    + " ON <外层键> = v.<键>: " + sub.originalText());
        }
        if (!inner.scalarSubqueries().isEmpty()) {
            throw unsupported(kind + "不支持嵌套 EXISTS / 内层标量子查询: " + sub.originalText());
        }
        LogicalPlan.ResolvedRelation innerRel = inner.fromRelations().get(0);
        if (innerRel.subquery().isPresent()) {
            throw unsupported(kind + "不支持内层 FROM 派生表。"
                    + "可改写为：JOIN (SELECT <键> FROM <表> GROUP BY <键>) v"
                    + " ON <外层键> = v.<键>: " + sub.originalText());
        }
        String aggText = inner.selectItems().get(0).expression().text().trim();
        if (SqlTextUtil.parseAggregate(aggText).isEmpty()) {
            throw unsupported("标量子查询须为聚合函数（count/sum/avg/min/max）: " + aggText);
        }

        String innerAlias = alias(innerRel);
        List<String> innerCols = innerRel.columns();

        // 相关性检测：内层 WHERE 逐合取。相关谓词 → (外层列, 内层键) 对；纯内层谓词 → 下推。
        List<String> corrKeys = new ArrayList<>();   // 内层 GROUP BY 键（裸列名）
        List<String> onParts = new ArrayList<>();    // 外层 ON 条件各等值项
        List<String> pushdownPreds = new ArrayList<>();
        if (inner.where().isPresent()) {
            for (String conjunct : SqlTextUtil.splitTopLevelKeyword(inner.where().get().text(), "AND")) {
                if (conjunct.isBlank()) continue;
                // 任意比较运算（= / > / < / >= / <= / !=），不限于等值 —— 纯内层谓词（含非等值、
                // 列 op 字面量）一样能安全下推进内层派生表的 WHERE；只有「引用外层列」的谓词才
                // 必须是等值（非等值的相关谓词去相关后无法表达成 LEFT JOIN 的 ON 等值条件）。
                ScalarExpr.Comparison cmp = ScalarExpr.splitComparison(conjunct).orElse(null);
                if (cmp == null) {
                    throw unsupported(kind + "内层谓词无法识别: " + conjunct);
                }
                boolean equi = "=".equals(cmp.op());
                String a = cmp.left().trim();
                String b = cmp.right().trim();
                int ak = classifyRef(a, innerAlias, innerCols, outerAliases, outerCols);
                int bk = classifyRef(b, innerAlias, innerCols, outerAliases, outerCols);
                boolean aLit = ScalarExpr.isLiteral(a);
                boolean bLit = ScalarExpr.isLiteral(b);
                if (equi && ((ak == REF_INNER && bk == REF_OUTER) || (ak == REF_OUTER && bk == REF_INNER))) {
                    String innerSide = (ak == REF_INNER) ? a : b;
                    String outerSide = (ak == REF_OUTER) ? a : b;
                    corrKeys.add(SqlTextUtil.stripQualifier(innerSide));
                    onParts.add(outerSide + " = " + SqlTextUtil.stripQualifier(innerSide));
                } else if (pureInnerPredicate(ak, bk, aLit, bLit)) {
                    // 不引用外层列 → 纯内层谓词：`内层列 op 内层列` / `内层列 op 字面量` / 常量比较。
                    // 原来 `内层列 = 字面量`（如 `s = 'a'`）落到 else 抛「相关谓词无法识别」——
                    // 字面量既不是内层列也不是外层列（classifyRef 返回 UNKNOWN），被误判成非法谓词。
                    pushdownPreds.add(conjunct);
                } else {
                    throw unsupported(kind + "相关谓词须为等值（须引用内层表列与外层列）: " + conjunct);
                }
            }
        }

        // 构造内层派生表：SELECT 相关键列 AS <键>, <agg> AS __subq_val FROM 内层表 [WHERE 下推] [GROUP BY 相关键]
        List<LogicalPlan.SelectItem> items = new ArrayList<>();
        for (String key : corrKeys) {
            items.add(new LogicalPlan.SelectItem(new Expression.OpaqueExpr(key), Optional.of(key)));
        }
        items.add(new LogicalPlan.SelectItem(new Expression.OpaqueExpr(aggText), Optional.of(subqVal)));
        List<Expression> groupBy = corrKeys.stream()
                .map(k -> (Expression) new Expression.OpaqueExpr(k)).toList();
        Optional<Expression> derivedWhere = pushdownPreds.isEmpty()
                ? Optional.empty()
                : Optional.of((Expression) new Expression.OpaqueExpr(String.join(" AND ", pushdownPreds)));
        LogicalPlan.Select derived = new LogicalPlan.Select(
                items, List.of(innerRel), List.of(),
                derivedWhere, groupBy, Optional.empty(), List.of());

        // 包成派生关系（而非立刻物化物理节点）：交给 buildJoins 后即可复用现成的分布式
        // 派生 join 路径 —— 内层聚合 + join 全在 Worker 执行，不再把内层聚合结果拉回 Master 本地 join。
        List<String> derivedCols = new ArrayList<>(corrKeys);
        derivedCols.add(subqVal);
        LogicalPlan.ResolvedRelation derivedRel = new LogicalPlan.ResolvedRelation(
                "", "", subqVal, Optional.of(subqVal), derivedCols,
                Optional.empty(), Optional.of(derived));
        String onCondition = onParts.isEmpty() ? null : String.join(" AND ", onParts);
        return new DecorrelatedJoin(derivedRel, onCondition);
    }

    /**
     * 该合取项是否「不引用外层列」—— 即只引用内层表列 / 字面量，可整体下推进内层派生表的 WHERE。
     *
     * <p>{@link #classifyRef} 对字面量、以及「两张表都没有的列名」一律返回 {@link #REF_UNKNOWN}，
     * 故必须再用 {@link ScalarExpr#isLiteral} 把字面量鉴别出来 —— 否则 {@code 内层列 = 字面量}
     * （如 {@code s = 'a'}）会被当成「无法识别的相关谓词」而抛错。仍是 {@code REF_UNKNOWN} 且
     * 非字面量的（拼错的列名等）保持抛错：宁可报清晰错误，也不静默把一个引用未知列的谓词
     * 下推给内层扫描。</p>
     */
    private static boolean pureInnerPredicate(int ak, int bk, boolean aLit, boolean bLit) {
        if (ak == REF_OUTER || bk == REF_OUTER) {
            return false;                      // 引用外层列 → 不是纯内层谓词
        }
        if (ak == REF_UNKNOWN && bk == REF_UNKNOWN) {
            return aLit && bLit;               // 常量比较（1 = 1）：下推无害
        }
        if (ak == REF_INNER) {
            return bk == REF_INNER || (bk == REF_UNKNOWN && bLit);
        }
        // bk == REF_INNER（ak 为 INNER / UNKNOWN）
        return ak == REF_INNER || (ak == REF_UNKNOWN && aLit);
    }

    /**
     * 判定引用归属：限定符优先（内层别名 / 外层别名），裸列名按 SQL 作用域
     * 内层表列 → 外层列 顺序解析；均不匹配返回 {@link #REF_UNKNOWN}。
     */
    private static int classifyRef(String ref, String innerAlias, List<String> innerCols,
                                   List<String> outerAliases, List<String> outerCols) {
        String t = ref.trim().replace("`", "");
        int dot = t.lastIndexOf('.');
        if (dot >= 0) {
            String qual = t.substring(0, dot).trim();
            if (qual.equalsIgnoreCase(innerAlias)) return REF_INNER;
            for (String oa : outerAliases) {
                if (qual.equalsIgnoreCase(oa)) return REF_OUTER;
            }
            return REF_UNKNOWN;
        }
        for (String c : innerCols) {
            if (c.equalsIgnoreCase(t)) return REF_INNER;
        }
        for (String c : outerCols) {
            if (c.equalsIgnoreCase(t)) return REF_OUTER;
        }
        return REF_UNKNOWN;
    }

    // ==================== 窗口过滤下推 ====================

    /**
     * 窗口过滤条件下推：若当前节点是 {@code Filter(col op 值)} 且子节点是
     * {@link WindowFunctionOperator}，且 col 对应窗口函数输出列，则把过滤条件注入
     * {@link WindowSortOperator} 和 {@link WindowFunctionOperator}，并消除该 Filter 节点。
     *
     * <h3>支持</h3>
     * <p>仅单调递增函数：ROW_NUMBER / COUNT / RANK / DENSE_RANK。
     *   等值 = N 和区间 <= / < / >= / > 全部支持。
     *   SUM / AVG / MIN / MAX 不支持（非单调，需分组全量数据）。</p>
     */
    private PhysicalNode tryPushDownWindowFilter(PhysicalNode node) {
        if (!(node.operator() instanceof FilterOperator filter)) return node;
        List<PhysicalNode> children = node.children();
        if (children.size() != 1) return node;
        PhysicalNode child = children.get(0);
        String expr = filter.filterExpression();
        ParsedFilter pf = parseFilter(expr);
        if (pf == null) return node;

        // 派生表展平常留下「列重排/透传」的纯投影（Project）夹在 Filter 与窗口算子之间
        // （形如 Filter → Project → DistributedWindow）。只要该投影**保留过滤列原名**
        // （过滤列同时出现在投影输出与其上游窗口输出里），就可穿过它定位转下推到窗口算子；
        // 返回的仍是 child（投影本身），列序/计划形态不变，只是 Filter 节点被消除。
        PhysicalNode winNode = child;
        while (winNode.operator() instanceof ProjectOperator && winNode.children().size() == 1
                && containsColumn(winNode.outputColumns(), pf.column)
                && containsColumn(winNode.children().get(0).outputColumns(), pf.column)) {
            winNode = winNode.children().get(0);
        }

        // 分布式窗口（含 join→窗口链）：条件下推进 WindowStep，Worker 侧 WindowSort 就地裁剪，
        // 避免整组窗口结果回传 Master 再过滤。等值覆写在 Worker 侧由配置重建（FragmentExecutor）。
        if (winNode.operator() instanceof DistributedWindowOperator dwin) {
            WindowFilterConfig cfg = buildWindowFilterConfig(dwin.functions(), dwin.orderBy().isEmpty(),
                    name -> indexOfColumn(dwin.projection(), name), expr);
            if (cfg == null) return node;
            dwin.setFilterConfig(cfg);
            return child; // Filter 已下推，消除 Filter 节点
        }
        if (winNode.operator() instanceof DistributedChainedOperator dchain) {
            final List<String> chainOutputs = winNode.outputColumns();
            WindowFilterConfig cfg = buildWindowFilterConfig(dchain.windowFunctions(),
                    dchain.windowOrderBy().isEmpty(),
                    name -> indexOfColumn(chainOutputs, name), expr);
            if (cfg == null) return node;
            dchain.setWindowFilter(cfg);
            return child;
        }

        if (!(winNode.operator() instanceof WindowFunctionOperator winFunc)) return node;

        WindowSortOperator sort = winFunc.getWindowSort();
        WindowFilterConfig cfg = buildWindowFilterConfig(winFunc.getFunctionSpecs(),
                sort.getOrderKeyColumns().isEmpty(),
                name -> resolveAggregateColIndex(child, name), expr);
        if (cfg == null) return node;
        sort.setFilterConfig(cfg);

        // 等值场景：WindowFunction 值覆写（跳过逐行计算）。
        // SUM 不设覆写：per-key 符号检查下，混合符号分组不过滤→覆写值会错误。
        if (cfg.operator() == WindowFilterConfig.FilterOp.EQ && cfg.functionType() != WindowFunctionType.SUM) {
            winFunc.setValueOverride(cfg.functionType(), cfg.targetValue());
        }
        return child; // Filter 已下推，消除 Filter 节点
    }

    /**
     * 解析外层窗口过滤条件（{@code 窗口函数列 op N}），命中受支持的窗口函数则生成下推配置。
     *
     * <p><b>支持</b>（单调 / 分区级聚合）—— 全量（无 ORDER BY）：ROW_NUMBER / COUNT / RANK /
     * DENSE_RANK / MIN / MAX / SUM / AVG；有排序（ORDER BY）：ROW_NUMBER / COUNT / RANK /
     * DENSE_RANK / MIN / MAX（AVG 有排序不单调；SUM 有排序需 per-key 符号检查）。<b>不支持</b>
     * （非单调、无法预判最终值）→ 返回 null，保留外层 Filter。
     *
     * @param noOrderBy 该窗口组是否无 ORDER BY（全量窗口 · 分区级聚合值）
     * @param aggColIdx 聚合型（MIN/MAX/SUM/AVG）输入列索引解析器；返回 -1 表示不可解析
     */
    static WindowFilterConfig buildWindowFilterConfig(List<WindowFunctionSpec> specs,
                                                      boolean noOrderBy,
                                                      java.util.function.ToIntFunction<String> aggColIdx,
                                                      String expr) {
        ParsedFilter pf = parseFilter(expr);
        if (pf == null) return null;
        for (WindowFunctionSpec spec : specs) {
            if (!spec.outputColumn().equalsIgnoreCase(pf.column)) continue;
            WindowFunctionType type = spec.type();

            boolean isRowCount = type == WindowFunctionType.ROW_NUMBER
                    || type == WindowFunctionType.COUNT;
            boolean isRank = type == WindowFunctionType.RANK
                    || type == WindowFunctionType.DENSE_RANK;
            boolean isMinMax = type == WindowFunctionType.MIN
                    || type == WindowFunctionType.MAX;
            boolean isSum = type == WindowFunctionType.SUM;
            boolean isAvg = type == WindowFunctionType.AVG;

            // AVG 有排序时不单调（即使同号，如 [10,1,1] → 10, 5.5, 4）
            if (isAvg && !noOrderBy) continue;
            if (!isRowCount && !isRank && !isMinMax && !isSum && !isAvg) continue;

            if (isMinMax || isSum || isAvg) {
                int aggCol = aggColIdx.applyAsInt(spec.inputColumn());
                if (aggCol < 0) continue;
                return new WindowFilterConfig(type, pf.targetValue, aggCol, noOrderBy, pf.op);
            }
            // ★ 位置型 / 排名型也必须如实带上 noOrderBy：3 参构造把它硬编码成 true，
            //   会让「有 ORDER BY 的 row_number()」被误当成全量窗口 ⇒ WindowSortOperator.pruneKeepRows()
            //   的门（!noOrderBy）永远关着 ⇒ Phase 1 预裁剪失效 ⇒ 整组全量溢写、Phase 1.5 反复归并卡死。
            return new WindowFilterConfig(type, pf.targetValue, -1, noOrderBy, pf.op);
        }
        return null;
    }

    /** 在列名列表中查索引（大小写不敏感）；未命中返回 -1。 */
    private static int indexOfColumn(List<String> columns, String name) {
        if (name == null || name.isEmpty() || columns == null) return -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equalsIgnoreCase(name)) return i;
        }
        return -1;
    }

    /** 列名列表（大小写不敏感）是否包含指定列。 */
    private static boolean containsColumn(List<String> columns, String name) {
        return indexOfColumn(columns, name) >= 0;
    }

    /** 解析过滤表达式 "col op value"，返回解析结果；不支持则返回 null。 */
    private static ParsedFilter parseFilter(String expr) {
        WindowFilterConfig.FilterOp op;
        int opPos;
        if (expr.contains("!=")) return null; // 不等值不支持
        if (expr.contains(">=")) { op = WindowFilterConfig.FilterOp.GE; opPos = expr.indexOf(">="); }
        else if (expr.contains("<=")) { op = WindowFilterConfig.FilterOp.LE; opPos = expr.indexOf("<="); }
        else if (expr.contains("<>")) return null;
        else if (expr.contains(">")) { op = WindowFilterConfig.FilterOp.GT; opPos = expr.indexOf(">"); }
        else if (expr.contains("<")) { op = WindowFilterConfig.FilterOp.LT; opPos = expr.indexOf("<"); }
        else if (expr.contains("=")) { op = WindowFilterConfig.FilterOp.EQ; opPos = expr.indexOf("="); }
        else return null;

        String col = expr.substring(0, opPos).trim();
        String valStr = expr.substring(opPos + (op == WindowFilterConfig.FilterOp.GE
                || op == WindowFilterConfig.FilterOp.LE ? 2 : 1)).trim();
        long targetValue;
        try {
            targetValue = (long) Double.parseDouble(valStr);
        } catch (NumberFormatException e) {
            return null;
        }
        if (targetValue <= 0) return null;
        return new ParsedFilter(col, op, targetValue);
    }

    private record ParsedFilter(String column, WindowFilterConfig.FilterOp op, long targetValue) {}

    /** 从上游输出列中解析聚合列的索引（大小写不敏感）。 */
    private static int resolveAggregateColIndex(PhysicalNode winFuncNode, String colName) {
        if (colName == null || colName.isEmpty()) return -1;
        List<PhysicalNode> upstream = winFuncNode.children();
        if (upstream.isEmpty()) return -1;
        List<String> cols = upstream.get(0).outputColumns();
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).equalsIgnoreCase(colName)) return i;
        }
        return -1;
    }

    // ==================== 聚合 ====================

    private PhysicalNode buildAggregation(PhysicalNode upstream, LogicalPlan.Select select) {
        return buildAggregation(upstream, select, null);
    }

    /**
     * @param expand 多维聚合展开（{@code ROLLUP/CUBE/GROUPING SETS} 一趟扫描形态，可空）。
     *               非空时分组键末尾补 {@code __grouping_id}，并把展开描述交给分布式算子
     *               （{@code Producer} / 最末层 join 的 Worker 侧插入）或本地回退路径插入算子。
     *               <b>刻意不在函数入口插本地算子</b>：{@code extractBaseTable} / 分布式 join 探测
     *               都会被未知算子挡掉，整条查询会退回 Master 单机执行 —— 与优化目标相反。
     */
    private PhysicalNode buildAggregation(PhysicalNode upstream, LogicalPlan.Select select,
                                          FragmentSpec.ExpandStep expand) {
        List<String> keys = new ArrayList<>();
        // GROUP BY 表达式（year(created_at) / floor(rating/10) / CASE WHEN ... END）派生列：
        // 表达式解析成 AST（ExprParser），源列 requireColumn 校验 + 函数存在性校验，派生列
        // 在分区前物化（ExpressionProjectOperator，Producer 侧），分组键引用派生列名聚合。
        List<GroupKeyExpression> groupKeyExpressions = new ArrayList<>();
        for (Expression gb : select.groupBy()) {
            String t = gb.text().trim();
            String col = SqlTextUtil.stripQualifier(t);
            if (SqlTextUtil.isSingleIdentifier(col)) {
                requireColumn(upstream, col, "GROUP BY 键");
                if (!keys.contains(col)) keys.add(col);
                continue;
            }
            ExprNode node = parseGroupKeyExpression(t);
            String derived = groupKeyDerivedName(select, t);
            for (String src : new ExprEvaluator(node).columnArgs()) {
                requireColumn(upstream, src, "GROUP BY 表达式源列");
            }
            validateExprFunctions(node, t);
            if (!keys.contains(derived)) {
                keys.add(derived);
                groupKeyExpressions.add(new GroupKeyExpression(derived, t));
            }
        }
        // 多维聚合展开：__grouping_id 是分组键的一部分 —— 同一维度组合多份副本靠它区分，
        // 且「真实 NULL」与「本组合不参与的占位 NULL」不会落进同一分组。
        if (expand != null && !containsColumn(keys, ExpandOperator.GROUPING_ID_COLUMN)) {
            keys.add(ExpandOperator.GROUPING_ID_COLUMN);
        }

        List<AggregationSpec> aggs = new ArrayList<>();
        List<String> aggOutCols = new ArrayList<>();
        List<CaseProjection> caseProjections = new ArrayList<>();
        List<String> derivedCols = new ArrayList<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            String text = item.expression().text();
            Optional<SqlTextUtil.AggInfo> info = SqlTextUtil.parseAggregate(text);
            if (info.isEmpty()) continue;
            SqlTextUtil.AggInfo a = info.get();
            String outName = item.alias().orElse(aggOutputName(text));
            aggs.add(buildAggregationSpec(outName, a, upstream, caseProjections, derivedCols));
            aggOutCols.add(outName);
        }
        // ★聚合嵌在表达式里（`round(sum(amt),2) as gmv` / `sum(a)*100.0/sum(b)`）—— 上面的循环
        //   只认「整项就是一个聚合调用」。内层聚合在此补建规格：输出列名取 `__agg_N`（内部派生列，
        //   不进最终结果），投影层用 replaceAggregates 把表达式里的聚合调用改写成该列引用再求值。
        for (NestedAggregate na : nestedAggregates(select)) {
            if (aggOutCols.contains(na.outputColumn())) continue;   // 同文本整项聚合的别名已提供该列
            aggs.add(buildAggregationSpec(na.outputColumn(), na.info(), upstream,
                    caseProjections, derivedCols));
            aggOutCols.add(na.outputColumn());
        }
        // ★HAVING 引用、但 SELECT 未投影的聚合：补建为内部输出列（__having_agg_N），供 buildHaving 过滤。
        //   放在这里（distinctOnly 判定之前）—— 有它则聚合规格非空 ⇒ 不会误退化「只输出分组键」的
        //   去重路径（那条路根本不计算被 HAVING 引用的计数）。输出列名不进 SELECT ⇒ 最终投影自然裁掉。
        for (HavingAgg ha : havingExtraAggs(select)) {
            if (aggOutCols.contains(ha.outputColumn())) continue;
            aggs.add(buildAggregationSpec(ha.outputColumn(), ha.info(), upstream,
                    caseProjections, derivedCols));
            aggOutCols.add(ha.outputColumn());
        }

        // SELECT 列全是分组键、一个聚合函数都没有 ⇒ 等价 DISTINCT：输出就只有分组键、每组一行。
        // 复用「聚合列为空」的聚合机制（与 UNION DISTINCT 的 distinctOver 同一套：
        // HashAggregationOperator 输出 = 分组键 + 各聚合列，聚合列为空时自然退化成每组一行）。
        boolean distinctOnly = aggs.isEmpty();

        // 分组键表达式派生列：先包一层 ExpressionProject（分区前物化 —— 分组键是分区键，须在
        // Producer 侧 HashPartition 之前算出，同值行才能落同一分区）。派生列追加到上游输出，
        // 与下游 CaseProject / 聚合按名解析互不影响。
        if (!groupKeyExpressions.isEmpty()) {
            List<String> gkDerived = new ArrayList<>();
            for (GroupKeyExpression gk : groupKeyExpressions) gkDerived.add(gk.derivedColumn());
            List<String> gkOutCols = new ArrayList<>(upstream.outputColumns());
            gkOutCols.addAll(gkDerived);
            ExpressionProjectOperator gkOp = new ExpressionProjectOperator(upstream.operator(), groupKeyExpressions);
            upstream = new PhysicalNode("ExpressionProject" + gkDerived, gkOp, gkOutCols, upstream);
        }

        // ★外层全局聚合直接压在内层分布式聚合之上（CTE / 派生表）：CASE 投影 + partial 聚合下沉
        //   到各 Worker（每分区只回传 1 行），Master 只做 N 行 partial 合并。否则内层全部结果行
        //   回传 Master、由单机 HashAggregation 逐行扫 —— 线上表现为「Agg 完成后 505 行/秒」卡死。
        //   必须放在下面的 CaseProject 包装之前：CASE 派生列随二级聚合一起下发（Worker 侧先算再聚合）。
        if (context.distributedExecution() && keys.isEmpty() && groupKeyExpressions.isEmpty()) {
            PhysicalNode overAgg = tryBuildChainedAggregationOverAggregation(upstream, aggs, caseProjections);
            if (overAgg != null) {
                return overAgg;
            }
        }

        // CASE 派生列：在上游包一层 CaseProject 算子，先逐行算好派生列（单机 / 分布式统一前置）
        if (!caseProjections.isEmpty()) {
            List<String> caseOutCols = new ArrayList<>(upstream.outputColumns());
            caseOutCols.addAll(derivedCols);
            CaseProjectOperator caseOp = new CaseProjectOperator(upstream.operator(), caseProjections);
            upstream = new PhysicalNode("CaseProject[" + derivedCols + "]", caseOp, caseOutCols, upstream);
        }

        // ★DISTINCT：必须放在上面两处派生列包装<b>之后</b>（分组键可能是派生列名，得先在流里存在）。
        //   刻意走 Master 本地聚合而非分布式下推：分布式聚合的片元协议 / 终端汇总路径此前只在
        //   「至少一个聚合函数」下验证过，空聚合规格（去重）那条路未在真集群上跑过 —— 不做未验证的
        //   优化。去重的哈希表通常很小（city×province 这类分组的基数低），Master 侧内存可控；
        //   待分布式空规格路径在集群上验证通过后，再把它接进下面的分布式分支。
        if (distinctOnly) {
            long distinctMemoryLimit = Math.min(context.memoryLimit(), ResourceConfig.masterAggregationMemoryBytes());
            HashAggregationOperator dedupe = new HashAggregationOperator(
                    upstream.operator(), keys, List.of(), distinctMemoryLimit);
            return new PhysicalNode("HashAggregation[Distinct]", dedupe, new ArrayList<>(keys), upstream);
        }

        // 分布式执行（开关默认关）：上游为裸表扫描时，把聚合整条链路派发到 Worker 执行
        // （扫描 → 按 key 哈希分区 → Worker 本地聚合 → 回传 Master，cowork without master）；
        // 上游为分布式 join 且 GROUP BY 键 == join 键时，链式聚合零二次交换（Phase 2）。
        // 其余情况静默回退单机路径（行为不变）。collect-evaluate 型同样分布式：key 哈希到唯一
        // 分区、每分区独立攒值（框架 Java 侧）→ 攒齐调外部语言 evaluate → 回传，Worker 侧按名
        // 从 CollectAggregateRegistry 重建（伪分布式同 JVM 静态表共享即命中）。
        if (context.distributedExecution()) {
            List<WorkerRegistration> healthy = WorkerRegistry.get().healthy(context.workerHealthTimeoutMs());
            if (!healthy.isEmpty()) {
                // ★多维聚合展开时整条链交给下面的分布式算子（展开由 Worker 侧在扫描 / 最末层 join
                //   之后插入）：分组键含 __grouping_id，二级 COUNT(DISTINCT) 的子桶改写与「join 键
                //   = 分组键」的链式零交换都失去前提 → 跳过，直接走分布式聚合 / 二次 shuffle 聚合。
                if (expand == null) {
                    // ★单热点分组的 COUNT(DISTINCT)：按 hash(distinct 列) 再分 K 个子桶做二级聚合，
                    //   把「一个分组一个去重集合」摊到全部 Worker（精确等价，详见 tryBuildTwoLevelCountDistinct）。
                    //   不满足严格判据（多 distinct / avg / 表达式 / 全局 / <2 Worker）时返回 null 原样回退。
                    PhysicalNode twoLevel = tryBuildTwoLevelCountDistinct(
                            upstream, keys, aggs, caseProjections, groupKeyExpressions, healthy);
                    if (twoLevel != null) {
                        return twoLevel;
                    }
                }
                PhysicalNode distributed = tryBuildDistributedAggregation(upstream, keys, aggs, caseProjections, groupKeyExpressions, healthy, expand);
                if (distributed != null) {
                    return distributed;
                }
                // GROUP BY 表达式（派生分区键）v1 不支持 join 上游的分布式路径：派生列随裸表扫描的
                // Producer 物化，join 输出的 Worker 侧重建无从接入 → 直接回退 Master 单机聚合
                // （正确性保留：join 结果流回 Master 后仍先 ExpressionProject 再 HashAggregation）。
                if (groupKeyExpressions.isEmpty()) {
                    // 链式零交换要求「分组键 == join 键」，展开后键里多了 __grouping_id ⇒ 不成立
                    if (expand == null) {
                        PhysicalNode chained = tryBuildChainedAggregation(upstream, keys, aggs, healthy);
                        if (chained != null) {
                            return chained;
                        }
                        PhysicalNode chainedMulti = tryBuildChainedMultiJoinAggregation(upstream, keys, aggs, caseProjections, healthy);
                        if (chainedMulti != null) {
                            return chainedMulti;
                        }
                    }
                    // join 输出上的 GROUP BY，但分组键与 join 键不共分区（≠ / 未覆盖最末层分区键）：
                    // 走二次 shuffle 最终聚合 —— join 留在 worker，按分组键重哈希，每分区最终聚合后回传。
                    PhysicalNode shuffled = tryBuildShuffledJoinAggregation(upstream, keys, aggs, caseProjections, healthy, expand);
                    if (shuffled != null) {
                        return shuffled;
                    }
                }
            }
        }

        // 回退到 Master 单机聚合（分布式下推失败：多 join / 非裸表上游）：聚合哈希表在 Master 进程内
        // 累积全量分组。Master 堆小（如 1G），JVM 直接内存限额 = -Xmx，slot 内存（默认 1G）与限额同级，
        // 聚合槽 + 流式回传页 + gRPC 帧叠加必 OOM。用显式小预算让聚合提前溢写磁盘替代内存爆掉；
        // Worker 侧算子不受影响（仍按 context.memoryLimit 传，Worker 有独立大内存）。
        long aggMemoryLimit = Math.min(context.memoryLimit(), ResourceConfig.masterAggregationMemoryBytes());
        // 多维聚合展开（Master 单机路径）：聚合之前插展开算子，把每行复制成 N 份并按 gid 区分
        if (expand != null) {
            ExpandOperator expandOp = new ExpandOperator(
                    upstream.operator(), expand.dimColumns(), expand.groupingSets());
            upstream = new PhysicalNode("Expand[" + expand.groupingSets().size() + " sets]",
                    expandOp, ExpandOperator.outputColumnsOf(upstream.outputColumns()), upstream);
        }
        HashAggregationOperator agg = new HashAggregationOperator(
                upstream.operator(), keys, aggs, aggMemoryLimit);
        List<String> outCols = new ArrayList<>(keys);
        outCols.addAll(aggOutCols);
        return new PhysicalNode("HashAggregation", agg, outCols, upstream);
    }

    /**
     * 由单个聚合调用（{@link SqlTextUtil.AggInfo}）构造 {@link AggregationSpec}：剥离
     * {@code COUNT(DISTINCT x)} 的 {@code distinct} 前缀、解析聚合参数列（裸列直通 / CASE·算术表达式
     * 转派生列）、按函数名分派。整项聚合与「嵌在表达式里的内层聚合」共用这一份实现。
     */
    private AggregationSpec buildAggregationSpec(String outName, SqlTextUtil.AggInfo a,
                                                 PhysicalNode upstream,
                                                 List<CaseProjection> caseProjections,
                                                 List<String> derivedCols) {
        // COUNT(DISTINCT x)：parseAggregate 把 "distinct" 原样留在参数里（AggInfo 无 distinct 位），
        // 此处剥离（否则 "distinct city" 会被当表达式解析 → 「参数表达式」不支持）。
        String argText = a.arg();
        boolean distinctAgg = false;
        if (!a.star() && argText != null) {
            String t = argText.trim();
            if (t.length() > 8 && t.regionMatches(true, 0, "distinct", 0, 8)
                    && Character.isWhitespace(t.charAt(8))) {
                distinctAgg = true;
                argText = t.substring(8).trim();
            }
        }
        // 聚合参数：裸列名直接引用；CASE 表达式改写为派生列（CaseProject 预计算后聚合）
        String inputCol = a.star() ? null : resolveAggInput(argText, a.function(), upstream,
                caseProjections, derivedCols);
        if (distinctAgg && !"COUNT".equals(a.function())
                && !"MIN".equals(a.function()) && !"MAX".equals(a.function())) {
            // MIN/MAX(DISTINCT) 与不带 DISTINCT 完全等价（同一组值的极值不变）⇒ 下面按普通 MIN/MAX 建规格，
            // 去重语义自然满足。SUM/AVG(DISTINCT) 需另立函数，明确报不支持，不做静默降级（降级会给出错的数）。
            throw unsupported(a.function() + "(DISTINCT ...)（当前仅支持 COUNT / MIN / MAX(DISTINCT ...)）");
        }
        return switch (a.function()) {
            // 分区元数据扫描的行集是「一行为一个非空分区」：count(*) 必须按每分区精确行数求和
            // （sum_int64 保 BIGINT，普通 sum 会漂成 DOUBLE）。行数列只会由该扫描路径产生。
            case "COUNT" -> a.star()
                    ? upstream.outputColumns().contains(DojaoConnector.PARTITION_ROW_COUNT_COLUMN)
                            ? AggregationSpec.sumInt64(DojaoConnector.PARTITION_ROW_COUNT_COLUMN, outName)
                            : AggregationSpec.count(outName)
                    : distinctAgg
                            ? AggregationSpec.countDistinct(inputCol, outName)
                            : AggregationSpec.count(inputCol, outName);
            case "SUM" -> AggregationSpec.sum(inputCol, outName);
            case "AVG" -> AggregationSpec.avg(inputCol, outName);
            case "MIN" -> AggregationSpec.min(inputCol, outName);
            case "MAX" -> AggregationSpec.max(inputCol, outName);
            default -> {
                // 先查 collect-evaluate 型（CollectAggregateRegistry，框架攒值），再查增量型
                CollectAggregateFunction collectFn = CollectAggregateRegistry.getGlobal(a.function());
                if (collectFn != null) {
                    List<String> collectCols = validateCollectArgs(a, collectFn, upstream);
                    yield AggregationSpec.collect(outName, collectFn, collectCols);
                }
                // 自定义聚合（CREATE FUNCTION 加载后注册进 AggregateFunctionRegistry）按名取用
                AggregateFunction<?> custom = AggregateFunctionRegistry.getGlobal(a.function());
                if (custom == null) {
                    throw unsupported("聚合函数: " + a.function());
                }
                yield AggregationSpec.aggregate(inputCol, outName, custom);
            }
        };
    }

    // ==================== 聚合嵌在表达式里（内层聚合） ====================

    /** 一个「嵌在表达式里的聚合调用」及其内部输出列名。 */
    private record NestedAggregate(String callText, SqlTextUtil.AggInfo info, String outputColumn) {
    }

    /**
     * 收集 SELECT 列表里<b>嵌在表达式内</b>的聚合调用（{@code round(sum(x),2)} 的 {@code sum(x)}、
     * {@code sum(a)*100.0/sum(b)} 的两个 sum）。整项即聚合调用的项不在此列（输出名由别名 / 归一化文本
     * 决定，走原路径）。
     *
     * <p>输出列名：优先复用「同文本整项聚合的<b>别名</b>」（{@code select sum(x) as s, round(sum(x),2)}
     * → 内层调用直接用 {@code s}，不重复计算）；否则生成 {@code __agg_N}（内部派生列）。别名必须是
     * 合法标识符才能当表达式里的列引用（无别名的整项聚合列名形如 {@code sum(x)}，含括号，不能用）。
     * 跨项按规范化文本去重 —— 同一调用只建一个聚合规格。</p>
     *
     * <p>本方法只依赖 {@code select}，确定性输出 ⇒ {@link #buildAggregation}（建规格）与
     * {@link #buildProject}（改写表达式）各自调用得到同一份规划。</p>
     */
    private static List<NestedAggregate> nestedAggregates(LogicalPlan.Select select) {
        Map<String, String> wholeAlias = new HashMap<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            if (item.expression() instanceof Expression.WindowFunction) continue;   // 窗口项：文本扫描会误命中
            String text = item.expression().text().trim();
            if (SqlTextUtil.parseAggregate(text).isEmpty()) continue;
            item.alias().filter(SqlTextUtil::isSingleIdentifier)
                    .ifPresent(al -> wholeAlias.putIfAbsent(SqlTextUtil.normalize(text), al));
        }
        List<NestedAggregate> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            if (item.expression() instanceof Expression.WindowFunction) continue;   // 窗口项：不是聚合
            String text = item.expression().text().trim();
            if (SqlTextUtil.parseAggregate(text).isPresent()) continue;
            for (String call : SqlTextUtil.findAggregateCalls(text)) {
                if (!seen.add(SqlTextUtil.normalize(call))) continue;
                String col = wholeAlias.get(SqlTextUtil.normalize(call));
                if (col == null) col = "__agg_" + out.size();
                out.add(new NestedAggregate(call, SqlTextUtil.parseAggregate(call).orElseThrow(), col));
            }
        }
        return out;
    }

    /**
     * 内层聚合调用的改写映射：{@code normalize(调用文本) → 输出列名}，供
     * {@link SqlTextUtil#replaceAggregates} 把表达式里的聚合调用换成列引用。
     */
    private static Map<String, String> nestedAggregateRewrite(LogicalPlan.Select select) {
        Map<String, String> map = new LinkedHashMap<>();
        for (NestedAggregate na : nestedAggregates(select)) {
            map.put(SqlTextUtil.normalize(na.callText()), na.outputColumn());
        }
        return map;
    }

    /** SELECT 项文本里是否含聚合调用（含「整项即聚合」与「聚合嵌在表达式里」两种形态）。 */
    private static boolean hasAggregateCall(String text) {
        return !SqlTextUtil.findAggregateCalls(text).isEmpty();
    }

    /**
     * SELECT 项是否参与聚合（整项聚合 / 聚合嵌在表达式里）—— <b>窗口函数项不算</b>。
     *
     * <p>必须先判窗口再扫文本：{@code sum(x) OVER (PARTITION BY k)} / {@code count(*) OVER ()} 的文本
     * 扫描会命中括号里那个 {@code sum(x)} / {@code count(*)}，把它误当聚合 ⇒ 整条 SELECT 被判成聚合
     * 查询（凭空多出一个 HashAggregation，窗口所需的基表列反被裁光 → 规划期报「找不到 PARTITION BY
     * 列，可用列: []」）。窗口函数是独立形态（{@link Expression.WindowFunction}），由窗口算子承接。</p>
     */
    private static boolean itemHasAggregate(LogicalPlan.SelectItem item) {
        if (item.expression() instanceof Expression.WindowFunction) return false;
        return hasAggregateCall(item.expression().text());
    }

    // ==================== GROUP BY 表达式（分组键派生列） ====================

    /** 解析 GROUP BY 表达式文本为 AST；不可解析 → 转 UnsupportedOperationException。 */
    private static ExprNode parseGroupKeyExpression(String text) {
        try {
            return ExprParser.parse(text);
        } catch (IllegalArgumentException e) {
            throw unsupported("GROUP BY 表达式 '" + text + "' 无法解析: " + e.getMessage());
        }
    }

    /**
     * 校验表达式里每个函数调用都存在：ExprBuiltins 内置 或 已注册标量函数
     * （ScalarFunctionRegistry）。缺名的函数列会被当列引用 → requireColumn 已拦；此处把「已识别为
     * 函数但未注册」的调用 fail-fast，避免运行期才炸。递归覆盖 CASE / 布尔等嵌套。
     */
    private static void validateExprFunctions(ExprNode node, String text) {
        List<ExprNode.FuncCall> calls = new ArrayList<>();
        collectFuncCalls(node, calls);
        for (ExprNode.FuncCall f : calls) {
            String name = f.name().toLowerCase(Locale.ROOT);
            if (!ExprBuiltins.isBuiltin(name) && !ScalarFunctionRegistry.isRegistered(name)) {
                throw unsupported("表达式函数: " + name + "（表达式: " + text + "）");
            }
        }
    }

    private static void collectFuncCalls(ExprNode n, List<ExprNode.FuncCall> out) {
        if (n instanceof ExprNode.FuncCall f) {
            out.add(f);
            for (ExprNode a : f.args()) collectFuncCalls(a, out);
        } else if (n instanceof ExprNode.Arith a) {
            collectFuncCalls(a.left(), out);
            collectFuncCalls(a.right(), out);
        } else if (n instanceof ExprNode.Negate g) {
            collectFuncCalls(g.operand(), out);
        } else if (n instanceof ExprNode.Compare c) {
            collectFuncCalls(c.left(), out);
            collectFuncCalls(c.right(), out);
        } else if (n instanceof ExprNode.IsNull i) {
            collectFuncCalls(i.operand(), out);
        } else if (n instanceof ExprNode.InList il) {
            collectFuncCalls(il.operand(), out);
            for (ExprNode v : il.values()) collectFuncCalls(v, out);
        } else if (n instanceof ExprNode.Like lk) {
            collectFuncCalls(lk.operand(), out);
            collectFuncCalls(lk.pattern(), out);
        } else if (n instanceof ExprNode.BoolOp b) {
            collectFuncCalls(b.left(), out);
            collectFuncCalls(b.right(), out);
        } else if (n instanceof ExprNode.Not nt) {
            collectFuncCalls(nt.operand(), out);
        } else if (n instanceof ExprNode.Case c) {
            for (ExprNode.WhenThen w : c.branches()) {
                collectFuncCalls(w.cond(), out);
                collectFuncCalls(w.then(), out);
            }
            if (c.elseExpr() != null) collectFuncCalls(c.elseExpr(), out);
        } else if (n instanceof ExprNode.Cast c) {
            collectFuncCalls(c.operand(), out);
        }
    }

    /**
     * 分组键表达式的派生列名：命中 SELECT 里同文表达式的别名取别名（{@code select year(x) as yr
     * group by year(x)} → {@code yr}），否则取表达式归一化文本（{@code year(x)}）。
     * 聚合项 / {@code *} 展开项不参与匹配（它们是消费派生列而非产生者）。
     */
    private static String groupKeyDerivedName(LogicalPlan.Select select, String exprText) {
        String norm = SqlTextUtil.normalize(exprText);
        for (LogicalPlan.SelectItem it : select.selectItems()) {
            String t = it.expression().text().trim();
            if (isStarExpansion(t)) continue;
            if (SqlTextUtil.parseAggregate(t).isPresent()) continue;
            if (SqlTextUtil.normalize(t).equals(norm)) {
                return it.alias().orElse(norm);
            }
        }
        return norm;
    }

    /**
     * SELECT 项文本是否命中<b>非裸列</b> GROUP BY 表达式（year(x) / floor(rating/10) /
     * CASE WHEN ...）。命中 → SELECT 投影直接引用分组键派生列（聚合输出列），不再走
     * 裸列 / 标量投影路径。
     */
    private static boolean isGroupKeyExpression(LogicalPlan.Select select, String selectText) {
        String norm = SqlTextUtil.normalize(selectText);
        for (Expression gb : select.groupBy()) {
            String t = gb.text().trim();
            if (SqlTextUtil.isSingleIdentifier(SqlTextUtil.stripQualifier(t))) continue;   // 裸列不在此列
            if (SqlTextUtil.normalize(t).equals(norm)) return true;
        }
        return false;
    }

    /**
     * collect-evaluate 型参数校验：SQL 调用参数<b>按位</b>即实际收集列（参数数 = 声明输入列数）。
     * 声明名只是参数位标签，<b>不要求与 SQL 列同名</b> —— 如 {@code @collect_aggregate(("num",))}
     * 声明的函数可作用于任意列 {@code stddev(discount_amount)}。返回实际收集列（规范名，交给算子
     * 按位收集后交付给外挂函数）。
     */
    private List<String> validateCollectArgs(SqlTextUtil.AggInfo a, CollectAggregateFunction fn,
                                             PhysicalNode upstream) {
        List<String> declared = fn.inputColumns();
        if (declared.isEmpty()) {
            throw unsupported("collect 聚合 " + a.function() + " 未声明输入列（装饰器 inputs 为空）");
        }
        if (a.star()) {
            if (declared.size() != 1) {
                throw unsupported("collect 聚合 " + a.function() + " 声明输入列 " + declared
                        + "，不支持 *（须显式传列名）");
            }
            String canonical = indexOfOutputColumn(upstream.outputColumns(), declared.get(0));
            if (canonical == null) {
                throw unsupported("collect 聚合 " + a.function() + " 输入列 '" + declared.get(0)
                        + "' 不在上游输出列中（可用列: " + upstream.outputColumns() + "）");
            }
            return List.of(canonical);
        }
        String[] sqlArgs = a.arg().split(",");
        if (sqlArgs.length != declared.size()) {
            throw unsupported("collect 聚合 " + a.function() + " SQL 参数数 " + sqlArgs.length
                    + " != 声明输入列数 " + declared.size() + "（参数须与声明输入列一一对应）");
        }
        List<String> actual = new ArrayList<>(sqlArgs.length);
        for (String raw : sqlArgs) {
            String sqlCol = SqlTextUtil.stripQualifier(raw.trim());
            if (!SqlTextUtil.isSingleIdentifier(sqlCol)) {
                throw unsupported("collect 聚合 " + a.function() + " 参数须为简单列引用: " + raw.trim());
            }
            String canonical = indexOfOutputColumn(upstream.outputColumns(), sqlCol);
            if (canonical == null) {
                throw unsupported("collect 聚合 " + a.function() + " 输入列 '" + sqlCol
                        + "' 不在上游输出列中（可用列: " + upstream.outputColumns() + "）");
            }
            actual.add(canonical);
        }
        return actual;
    }

    /**
     * 尝试把<b>外层全局聚合</b>（无 GROUP BY）直接压在内层分布式聚合之上（CTE / 派生表场景）：
     * CASE 派生列 + 二级 partial 聚合随内层 Consumer 一起在 Worker 上执行，每分区只回传 1 行，
     * Master 侧 {@link PartialAggregationMergeOperator} 合并成最终 1 行。
     *
     * <p><b>为什么需要</b>：内层分布式聚合的结果行（千万级）若全回传 Master、再由单机
     * {@code HashAggregationOperator} 逐行扫一遍，就是线上「Agg 完成后 505 行/秒」的瓶颈。
     * 内层结果本就按桶分散在 Worker 上，把外层 COUNT/SUM/MIN/MAX 的 partial 段留在同一 Worker
     * （零交换）只需每分区加一次本地聚合。</p>
     *
     * <p><b>只下沉可合并的聚合</b>：avg（需要和+计数）、count(distinct) / collect / 自定义 UDAF
     * 的 partial 形态不可合并 → 返回 null 回退单机（行为不变）。</p>
     */
    private PhysicalNode tryBuildChainedAggregationOverAggregation(PhysicalNode upstream,
                                                                    List<AggregationSpec> aggs,
                                                                    List<CaseProjection> caseProjections) {
        if (aggs.isEmpty()) return null;
        for (AggregationSpec spec : aggs) {
            if (spec.isCollect() || spec.aggregate() == null) return null;
            String name = spec.aggregate().functionName();
            if (!"count".equals(name) && !"sum".equals(name)
                    && !"min".equals(name) && !"max".equals(name)) {
                return null;
            }
        }
        PhysicalNode distNode = extractDistributedAggregationNode(upstream);
        if (distNode == null) return null;
        DistributedAggregationOperator dist = (DistributedAggregationOperator) distNode.operator();

        // 二级聚合：无分组键 = 全局 partial（每分区聚出 1 行）；桶数与内层一致（pid 分槽）
        FragmentSpec.AggregateStep step = new FragmentSpec.AggregateStep(
                List.of(), aggs.stream().map(FragmentSpec.AggregationDto::of).toList(),
                caseProjections, context.memoryLimit(), dist.numPartitions());
        dist.setFollowUp(step);

        // 内层分布式聚合现在回传的列 = 二级聚合输出列（merge 节点的真实上游 schema）
        List<String> outCols = new ArrayList<>();
        for (AggregationSpec a : aggs) outCols.add(a.outputColumn());
        PhysicalNode partialNode = new PhysicalNode(distNode.name(), dist, outCols, List.of());
        PartialAggregationMergeOperator merge = new PartialAggregationMergeOperator(dist, aggs);
        return new PhysicalNode("AggregationMerge", merge, outCols, partialNode);
    }

    /**
     * 从上游节点向下探测分布式聚合：穿过<b>纯投影</b>（Project，不改列名/列集）取
     * {@link DistributedAggregationOperator}；遇 Filter / CASE / 表达式投影 / 跨列裁剪的 Project
     * 或其它算子 → 返回 null（这类节点必须留在链路上，跳过会改变语义）。
     */
    private static PhysicalNode extractDistributedAggregationNode(PhysicalNode node) {
        PhysicalNode cur = node;
        while (true) {
            Operator op = cur.operator();
            if (op instanceof DistributedAggregationOperator) return cur;
            if (op instanceof ProjectOperator proj && !proj.isAliased() && cur.children().size() == 1) {
                PhysicalNode child = cur.children().get(0);
                // 只跳过「同列同名同序」的空投影；真裁剪/改名的投影跳过会改变输出 schema
                if (!cur.outputColumns().equals(child.outputColumns())) return null;
                cur = child;
                continue;
            }
            return null;
        }
    }

    /**
     * 尝试构建分布式单表聚合（最小切片）：扫描 → 按聚合 key 哈希分区 → Worker 本地聚合。
     * 仅支持"上游是裸表扫描"（无 join / 子查询 / 前序聚合或窗口）；否则返回 null → 回退单机。
     */
    private PhysicalNode tryBuildDistributedAggregation(PhysicalNode upstream,
                                                        List<String> keys, List<AggregationSpec> aggs,
                                                        List<CaseProjection> caseProjections,
                                                        List<GroupKeyExpression> groupKeyExpressions,
                                                        List<WorkerRegistration> workers,
                                                        FragmentSpec.ExpandStep expand) {
        BaseTable base = extractBaseTable(upstream);
        if (base == null) {
            return null;
        }
        // ★ 桶数 vs 扫描并行度（两者刻意分开）：
        //   桶数 = 聚合的并行度。全局聚合（无分组键）时，HashPartitionOperator 对每一行算出的哈希
        //   恒为 0（键数组为空 → 哈希循环不执行 → hash=0）⇒ 所有行只进 0 号桶，其余桶一页都收不到。
        //   若仍按并行度开 8 个消费者，那 7 个仍须逐个派发并等 partitionEnd 才能收尾 —— 查询得等
        //   它们的调度轮次（实测 8 消费者 / 3 Worker 下这段空转尾巴约 6.7s，日志满是
        //   [SHUFFLE-STALL] ... 队列余量=0）。故键为空时直接单桶（见 {@link #aggBuckets}）。
        //   但扫描不能跟着压到 1 片 —— 那会把读变回单线程串行，故另传 scanParallelism。
        int scanParallelism = Math.max(1, context.parallelism());
        int partitions = aggBuckets(keys, scanParallelism);
        String queryId = "query-" + Long.toHexString(System.nanoTime());
        DojaoConnector probe = context.connectorProvider().get(base.catalog);

        // ★ 列裁剪（分布式聚合必须在此做，不能依赖 optimizeColumns）：
        //   tryBuildDistributedAggregation 在 optimizeColumns（列裁剪）之前执行，且返回的物理节点
        //   无子节点 → 列裁剪优化够不到这里。若不裁剪，Worker 扫描退化为 SELECT * —— 把 review_content
        //   这类 TEXT 大列全程读 + JDBC→Arrow 转换 + 跨 worker shuffle，Phase 1 慢数倍（20M 行 TEXT ≈ 27s）。
        //   这里按聚合实际需要的列（分组键 + 聚合输入列）裁剪；有 CASE / GROUP BY 表达式派生列时
        //   保守不裁剪（keys 含派生列名而非基表列，表达式源列难在此可靠解析）。
        //   ★ 连接器不兑现下推时，WHERE 里的列由引擎在 Worker 扫描节点过滤（withEngineFilter）→
        //     必须一并保留，否则 group by 的键之外那列（如 `where content like ...` 的 content）
        //     会被裁掉，Worker 侧 Filter 找不到列。
        //   ★ CASE / IF / 算术表达式聚合输入（sum(if(os=..,1,0)) / sum(a+b)）会被改写成派生列
        //     __case_N，它<b>不是基表列</b> —— 直接进投影会让连接器报「列不存在」。故收集派生列名
        //     从 needed 中排除，真正该保留的基表列由 addCaseReferencedColumns 解析条件列 / 结果列
        //     引用得出（Worker 侧 CaseProjectOperator 逐行求值需要它们）。此前守卫是「有 CASE 就
        //     整段放弃裁剪」，导致这类查询的 Worker 扫描退化成 SELECT * —— 读全列 + 全列 shuffle。
        ReadRequest scanRequest = base.baseRequest;
        if (probe != null) {
            Set<String> derived = new HashSet<>();
            for (CaseProjection cp : caseProjections) derived.add(cp.derivedColumn());
            // ★GROUP BY 表达式派生列（__gk_N）也不是基表列：不进 needed，改按表达式解析源列
            //   （二级 COUNT(DISTINCT) 的合成键 hash(v)%K 走同一通道）。此前 gke 非空整段放弃
            //   裁剪 ⇒ Worker 扫描退化成 SELECT *，宽表读全列 + 全列 shuffle。
            Set<String> gkDerived = new HashSet<>();
            for (GroupKeyExpression gk : groupKeyExpressions) gkDerived.add(gk.derivedColumn());

            List<String> needed = new ArrayList<>();
            for (String k : keys) {
                // __grouping_id 是展开算子的合成列（不在扫描源里）→ 不进投影
                if (ExpandOperator.GROUPING_ID_COLUMN.equalsIgnoreCase(k)) continue;
                if (!derived.contains(k) && !gkDerived.contains(k) && !needed.contains(k)) {
                    needed.add(k);
                }
            }
            for (AggregationSpec a : aggs) {
                if (a.isCollect()) {
                    List<String> inputs = a.collectInputColumns();
                    if (inputs != null) {
                        for (String c : inputs) {
                            if (!derived.contains(c) && !needed.contains(c)) needed.add(c);
                        }
                    }
                } else if (a.inputColumn() != null && !derived.contains(a.inputColumn())
                        && !needed.contains(a.inputColumn())) {
                    needed.add(a.inputColumn());
                }
            }
            for (GroupKeyExpression gk : groupKeyExpressions) {
                ExprNode node = parseGroupKeyExpression(gk.exprText());
                for (String src : new ExprEvaluator(node).columnArgs()) {
                    if (!needed.contains(src)) needed.add(src);
                }
            }
            for (CaseProjection cp : caseProjections) {
                addCaseReferencedColumns(cp, upstream.outputColumns(), needed);
            }
            if (!probe.supportsPredicatePushdown()) {
                for (String c : engineFilterColumns(base.predicates)) {
                    if (!needed.contains(c)) needed.add(c);
                }
            }
            scanRequest = probe.pruneColumns(base.baseRequest, needed);
            System.err.printf("[ColumnPruning] DistributedAggScan 投影: %s%n", needed);
        }
        DistributedAggregationOperator op = new DistributedAggregationOperator(
                queryId, scanRequest, base.predicates,
                keys, aggs, caseProjections, groupKeyExpressions, context.memoryLimit(),
                partitions, scanParallelism,
                context.workerGateway(), probe, workers, expand);

        List<String> outCols = new ArrayList<>(keys);
        outCols.addAll(aggs.stream().map(AggregationSpec::outputColumn).toList());
        String name = "DistributedAggregation[" + keys
                + ", workers=" + workers.size() + ", partitions=" + partitions + "]";
        return new PhysicalNode(name, op, outCols, List.of());
    }

    /** 二级 COUNT(DISTINCT) 的合成子桶键列名（内层 GROUP BY 的最后一列，外层丢弃）。 */
    private static final String TWO_LEVEL_HASH_KEY = "__cd_hash_0";

    /**
     * 子桶键的混洗素数。{@code hash()} 对数值列走 {@code Double.hashCode}，整数值的哈希低位
     * 恒为 0（如 1.0/2.0/3.0 全是 4 的倍数）——直接 {@code % K} 会让同一子桶吃掉全部行。
     * 先用奇素数 1021（{@code 2^20 mod 1021 = 9}，把指数与尾数搅进低位）再取模 K，保证
     * 数值去重列也能真正摊开。字符串列的 {@code String.hashCode} 本就均匀，多一次取模无害。
     */
    private static final int TWO_LEVEL_MIX_PRIME = 1021;

    /**
     * 尝试把「有 GROUP BY 的单列 COUNT(DISTINCT)」改写成<b>精确的二级聚合</b>：内层按
     * {@code GROUP BY k..., hash(v) % K} 分组算子计数，外层按 {@code k...} 合并（count/sum/min/max
     * 各自可加/可结合）。外层是 Master 本地 {@link HashAggregationOperator}，直接消费内层
     * {@link DistributedAggregationOperator} 回传的页。
     *
     * <p><b>为什么</b>：分组键基数低（如 {@code event_type} 只有两组）时 {@code aggBuckets} 只能开
     * 2 个桶，再加 round-robin 落盘可能把两个热点桶共置到同一台 Worker —— 单组几百万去重值全压
     * 一台机器。子键 {@code hash(v)%K} 是 {@code v} 的确定性函数，各子键去重集合两两不相交 ⇒
     * Σ 子计数 <b>精确等于</b>原 {@code COUNT(DISTINCT)}（无近似、无误差）。</p>
     *
     * <p><b>严格判据</b>（任一不满足 → 返回 null 逐字节回退原单级路径）：配置开关开启、有非空
     * GROUP BY、≥2 台健康 Worker、恰好一个 {@code count_distinct} 且输入是真实列（非 CASE / GROUP BY
     * 表达式派生列）、其余聚合只有 count/sum/min/max（拒绝 avg / collect / 自定义 UDAF）。</p>
     */
    private PhysicalNode tryBuildTwoLevelCountDistinct(PhysicalNode upstream, List<String> keys,
                                                       List<AggregationSpec> aggs,
                                                       List<CaseProjection> caseProjections,
                                                       List<GroupKeyExpression> groupKeyExpressions,
                                                       List<WorkerRegistration> healthy) {
        if (!ResourceConfig.distinctTwoLevelEnabled()) return null;
        if (keys.isEmpty() || healthy.size() < 2) return null;

        int distinctIdx = -1;
        for (int i = 0; i < aggs.size(); i++) {
            AggregationSpec spec = aggs.get(i);
            if (spec.isCollect() || spec.aggregate() == null) return null;
            String fn = spec.aggregate().functionName();
            if ("count_distinct".equals(fn)) {
                if (distinctIdx >= 0) return null;   // 多个 COUNT(DISTINCT) → 回退（一列一个子桶映射）
                distinctIdx = i;
            } else if (!"count".equals(fn) && !"sum".equals(fn)
                    && !"min".equals(fn) && !"max".equals(fn)) {
                return null;                         // avg / 自定义 UDAF → 回退
            }
        }
        if (distinctIdx < 0) return null;
        String distinctCol = aggs.get(distinctIdx).inputColumn();
        if (distinctCol == null) return null;
        // 派生列不是基表列：内层 hash(...) 表达式在 Worker 扫描流里取不到该列 → 回退
        for (CaseProjection cp : caseProjections) {
            if (cp.derivedColumn().equals(distinctCol)) return null;
        }
        for (GroupKeyExpression gk : groupKeyExpressions) {
            if (gk.derivedColumn().equals(distinctCol)) return null;
        }
        if (keys.contains(TWO_LEVEL_HASH_KEY)
                || upstream.outputColumns().contains(TWO_LEVEL_HASH_KEY)) {
            return null;                             // 合成键名冲突 → 回退
        }

        int k = ResourceConfig.distinctTwoLevelBuckets();
        if (k <= 0) k = Math.max(1, context.parallelism());
        k = Math.max(2, Math.min(k, 256));

        // 内层：GROUP BY k..., floorMod(hash(v) mod 1021, K)。先经混洗素数把数值哈希的低位
        // 零陷阱打散（见 TWO_LEVEL_MIX_PRIME），再取模得到 0..K-1（hash 可能为负，中段
        // +1021 归一成正余数，故最终 % K 不会为负）。
        List<String> innerKeys = new ArrayList<>(keys);
        innerKeys.add(TWO_LEVEL_HASH_KEY);
        List<GroupKeyExpression> innerGke = new ArrayList<>(groupKeyExpressions);
        innerGke.add(new GroupKeyExpression(TWO_LEVEL_HASH_KEY,
                "(((hash(" + distinctCol + ") % " + TWO_LEVEL_MIX_PRIME + ") + "
                        + TWO_LEVEL_MIX_PRIME + ") % " + TWO_LEVEL_MIX_PRIME + ") % " + k));

        List<AggregationSpec> innerAggs = new ArrayList<>(aggs.size());
        List<String> subNames = new ArrayList<>(aggs.size());
        for (int i = 0; i < aggs.size(); i++) {
            AggregationSpec s = aggs.get(i);
            String sub = "__cd_sub_" + i;
            subNames.add(sub);
            String in = s.inputColumn();
            innerAggs.add(switch (s.aggregate().functionName()) {
                case "count_distinct" -> AggregationSpec.countDistinct(in, sub);
                case "count" -> in == null ? AggregationSpec.count(sub)
                        : AggregationSpec.count(in, sub);
                case "sum" -> AggregationSpec.sum(in, sub);
                case "min" -> AggregationSpec.min(in, sub);
                case "max" -> AggregationSpec.max(in, sub);
                default -> throw new IllegalStateException("unreachable");
            });
        }
        PhysicalNode inner = tryBuildDistributedAggregation(
                upstream, innerKeys, innerAggs, caseProjections, innerGke, healthy, null);
        if (inner == null) return null;

        // 外层：按原分组键合并子结果。count/count_distinct 必须用 sum_int64（普通 sum 会把
        // BIGINT 漂成 DOUBLE）；sum/min/max 与子结果同型可加/可结合。
        List<AggregationSpec> outerAggs = new ArrayList<>(aggs.size());
        for (int i = 0; i < aggs.size(); i++) {
            AggregationSpec s = aggs.get(i);
            String sub = subNames.get(i);
            String out = s.outputColumn();
            outerAggs.add(switch (s.aggregate().functionName()) {
                case "count_distinct", "count" -> AggregationSpec.sumInt64(sub, out);
                case "sum" -> AggregationSpec.sum(sub, out);
                case "min" -> AggregationSpec.min(sub, out);
                case "max" -> AggregationSpec.max(sub, out);
                default -> throw new IllegalStateException("unreachable");
            });
        }
        long aggMemoryLimit = Math.min(context.memoryLimit(), ResourceConfig.masterAggregationMemoryBytes());
        HashAggregationOperator outer = new HashAggregationOperator(
                inner.operator(), keys, outerAggs, aggMemoryLimit);
        List<String> outerCols = new ArrayList<>(keys);
        for (AggregationSpec a : outerAggs) outerCols.add(a.outputColumn());
        return new PhysicalNode("TwoLevelCountDistinct[" + keys + ", K=" + k + "]",
                outer, outerCols, inner);
    }

    /**
     * 分布式聚合的<b>桶数</b>（交换分区数）：全局聚合（无分组键）恒为 <b>1</b>，否则等于并行度。
     *
     * <p>为什么全局聚合单桶：{@code HashPartitionOperator} 的分桶哈希由「各分组键列的值」折叠而成，
     * 键数组为空时哈希循环一次都不执行 ⇒ <b>每行的哈希恒为 0</b> ⇒ 所有行只进 0 号桶。此时若仍按
     * 并行度开 N 个消费者，那 N-1 个收不到任何数据，却必须逐个派发、注册读端、等 {@code partitionEnd}
     * 才能收尾 —— 查询的结束时间被拖到最后一个空消费者完成（实测 8 桶 / 3 Worker 约 6.7s 的空转尾巴，
     * 日志满是 {@code [SHUFFLE-STALL] ... 队列余量=0}）。单桶把这整段消掉，且结果完全一致
     * （数据本就只在一个桶里聚完）。</p>
     *
     * <p>注意<b>不要</b>拿它去改扫描并行度：桶数管的是聚合交换，切片数管的是读。全局聚合压到 1 桶
     * 时扫描仍须多片并行，否则读退化成单线程串行（见 {@code DistributedAggregationOperator.scanParallelism}）。</p>
     */
    static int aggBuckets(List<String> keys, int parallelism) {
        return keys.isEmpty() ? 1 : Math.max(1, parallelism);
    }

    /** 裸表扫描描述（分布式聚合 / 窗口的基表探测）：baseRequest 未带谓词，predicates 需复下推。 */
    private record BaseTable(String catalog, ReadRequest baseRequest, List<String> predicates) {
    }

    /**
     * 从上游节点向下探测基表：穿过纯投影（Project）取叶子扫描的请求；遇 Filter
     * （残余标量谓词）或非扫描叶子 → 返回 null（分布式需回退单机，保证过滤不丢失）。
     */
    private BaseTable extractBaseTable(PhysicalNode node) {
        Operator op = node.operator();
        if (op instanceof ProjectOperator) {
            if (node.children().size() == 1) return extractBaseTable(node.children().get(0));
            return null;
        }
        if (op instanceof CaseProjectOperator) {
            if (node.children().size() == 1) return extractBaseTable(node.children().get(0));
            return null;
        }
        if (op instanceof ExpressionProjectOperator) {
            if (node.children().size() == 1) return extractBaseTable(node.children().get(0));
            return null;
        }
        if (op instanceof FilterOperator) {
            return null;
        }
        if (op instanceof ParallelTableScanOperator s) {
            ReadRequest r = s.baseRequest();
            return new BaseTable(r.catalog(), r, s.predicates());
        }
        if (op instanceof DistributedScanOperator s) {
            ReadRequest r = s.baseRequest();
            return new BaseTable(r.catalog(), r, s.predicates());
        }
        return null;
    }

    /**
     * 尝试构建分布式 <b>join→聚合</b> 链（Phase 2，零二次交换）：上游是分布式单 join 且
     * GROUP BY 键 == join 一侧的 key → join 输出已在归属 Worker 上按 key 分布，聚合直接在
     * 本地 join 输出上执行。否则返回 null → 回退单机。
     */
    private PhysicalNode tryBuildChainedAggregation(PhysicalNode upstream,
                                                    List<String> keys, List<AggregationSpec> aggs,
                                                    List<WorkerRegistration> workers) {
        DistributedJoinOperator djo = extractDistributedJoin(upstream);
        // 派生源链不能重建为 DistributedChainedOperator（无 sources 参数 → 流水线丢失）→ Master 本地聚合兜底
        if (djo == null || hasCrossLink(djo) || hasDerivedSource(djo)) {
            return null;
        }
        // 同 key 前置：GROUP BY 键必须与 join 一侧的 key 完全一致（顺序敏感 → 哈希输入一致 → 零交换）
        if (!keys.equals(djo.leftKeys()) && !keys.equals(djo.rightKeys())) {
            return null;
        }
        int partitions = djo.numPartitions();
        String queryId = "query-" + Long.toHexString(System.nanoTime());
        FragmentSpec.AggregateStep step = new FragmentSpec.AggregateStep(
                keys, aggs.stream().map(FragmentSpec.AggregationDto::of).toList(),
                context.memoryLimit(), partitions);
        DistributedChainedOperator op = new DistributedChainedOperator(
                queryId,
                djo.leftCatalog(), djo.leftSchema(), djo.leftTable(), djo.leftKeys(),
                djo.rightCatalog(), djo.rightSchema(), djo.rightTable(), djo.rightKeys(),
                djo.joinType(), djo.memoryLimit(), partitions,
                djo.gateway(), djo.leftProbe(), djo.rightProbe(), djo.workers(), step,
                djo.leftFilter(), djo.rightFilter(),
                djo.firstColumns(), djo.links().get(0).rightColumns());

        List<String> outCols = new ArrayList<>(keys);
        outCols.addAll(aggs.stream().map(AggregationSpec::outputColumn).toList());
        String name = "DistributedJoinAggregation[" + keys
                + ", workers=" + workers.size() + ", partitions=" + partitions + "]";
        return new PhysicalNode(name, op, outCols, List.of());
    }

    /**
     * 从上游节点向下探测分布式 join 基：穿过纯投影（Project）取 {@link DistributedJoinOperator}；
     * 遇 Filter（残余谓词须在 join 之后执行）或其它算子 → 返回 null（链式需回退，保证过滤不丢失）。
     */
    private DistributedJoinOperator extractDistributedJoin(PhysicalNode node) {
        Operator op = node.operator();
        if (op instanceof ProjectOperator) {
            if (node.children().size() == 1) return extractDistributedJoin(node.children().get(0));
            return null;
        }
        if (op instanceof FilterOperator) {
            return null;
        }
        // 仅单 join（links.size()==1）走链式单 join 语义（DistributedChainedOperator 重建舞台）；
        // 多 join 链由 extractMultiJoin 走 withAggregate 路径，两个提取器按 links().size() 互斥。
        if (op instanceof DistributedJoinOperator djo && djo.links().size() == 1) {
            return djo;
        }
        return null;
    }

    /**
     * 尝试构建分布式 <b>多 join → 聚合</b> 链（零二次交换）：上游是分布式多 join
     * {@link DistributedJoinOperator}（links.size() &gt;= 2）且 GROUP BY 键<b>包含</b>最末层
     * join 的分区键（last link 左键）→ 分区键 ⊆ 分组键 → 每个分组完全落在同一分区
     * （co-partitioning 不变量），Worker 本地 join 输出上直接聚合即为最终结果，Master 零合并。
     * 聚合（含 CASE 派生列）随 {@code AggregateStep} 下发给 final Consumer
     * （{@code ChainedJoinConsumer}）执行。否则返回 null → 回退单机。
     */
    private PhysicalNode tryBuildChainedMultiJoinAggregation(PhysicalNode upstream,
                                                             List<String> keys, List<AggregationSpec> aggs,
                                                             List<CaseProjection> caseProjections,
                                                             List<WorkerRegistration> workers) {
        DistributedJoinOperator mjo = extractMultiJoin(upstream);
        // 派生源链同 CROSS：withAggregate 重建的算子不带 sources → 派生流水线丢失 ⇒ 回退本地聚合
        if (mjo == null || mjo.links().isEmpty() || hasCrossLink(mjo) || hasDerivedSource(mjo)) {
            return null;
        }
        // 同 key 前置：分组键须覆盖最末层 join 的分区键（last link 左键），否则同组跨分区不完整
        List<String> lastLeftKeys = mjo.links().get(mjo.links().size() - 1).leftKeys();
        if (!keys.containsAll(lastLeftKeys)) {
            return null;
        }
        int partitions = mjo.numPartitions();
        DistributedJoinOperator op = mjo.withAggregate(keys, aggs, caseProjections);

        List<String> outCols = new ArrayList<>(keys);
        outCols.addAll(aggs.stream().map(AggregationSpec::outputColumn).toList());
        String name = "DistributedJoinChainAggregation[" + keys
                + ", workers=" + workers.size() + ", partitions=" + partitions + "]";
        return new PhysicalNode(name, op, outCols, List.of());
    }

    /**
     * 从上游节点向下探测分布式<b>多 join</b> 基：穿过纯投影（Project）与 CASE 派生列
     * （CaseProject，聚合的 {@code sum(if(...))}/{@code sum(case when ...)} 前置算子）取
     * {@link DistributedJoinOperator}（仅 links.size() &gt;= 2 的多 join 链）；遇 Filter
     * （残余谓词须在 join 之后执行）或其它算子 → 返回 null（链式需回退，保证过滤不丢失）。
     * 单 join 由 {@link #extractDistributedJoin} 走 DistributedChainedOperator 路径，二者互斥。
     */
    private DistributedJoinOperator extractMultiJoin(PhysicalNode node) {
        Operator op = node.operator();
        if (op instanceof ProjectOperator) {
            if (node.children().size() == 1) return extractMultiJoin(node.children().get(0));
            return null;
        }
        if (op instanceof CaseProjectOperator) {
            if (node.children().size() == 1) return extractMultiJoin(node.children().get(0));
            return null;
        }
        if (op instanceof FilterOperator) {
            return null;
        }
        if (op instanceof DistributedJoinOperator mjo && mjo.links().size() >= 2) {
            return mjo;
        }
        return null;
    }

    /**
     * 尝试构建分布式 <b>join→二次 shuffle 最终聚合</b>（Plan B 兜底，非共分区 GROUP BY）：上游是
     * 分布式 join {@link DistributedJoinOperator}（单 / 多 join 皆可），但其上的 GROUP BY 键与 join
     * 键不共分区（链式零二次交换的 {@code tryBuildChainedAggregation} /
     * {@code tryBuildChainedMultiJoinAggregation} 已返回 null）→ 每个分组跨 Worker 不完整。
     *
     * <p>本 builder 让 join 输出留在 Worker：最末层 join 改作 {@code MiddleJoinConsumer} 按
     * <b>分组键</b>重哈希进每分区独立的聚合交换，再由每分区一个 {@code AggregateConsumer} 做最终
     * 聚合后回传 Master —— Master 永远不做归约（消除此前的 Master 单机 {@code HashAggregationOperator}
     * 兜底）。正确性 = co-partitioning：分组键经同一 round-robin owner 映射哈希 → 每个分组落唯一
     * 分区 / Worker → 本地聚合即最终结果，Master 零合并。整条 join 链被重建为新算子的舞台，
     * 原 {@code DistributedJoinOperator} 丢弃（不被 open），其探针连接器移交给新算子。</p>
     *
     * @return 新 {@code DistributedJoinAggregationOperator} 物理节点；上游非分布式 join / 中间有
     *         Filter（残余谓词须在 join 后执行）→ null → 回退 Master 单机聚合（行为不变）。
     */
    private PhysicalNode tryBuildShuffledJoinAggregation(PhysicalNode upstream,
                                                         List<String> keys, List<AggregationSpec> aggs,
                                                         List<CaseProjection> caseProjections,
                                                         List<WorkerRegistration> workers,
                                                         FragmentSpec.ExpandStep expand) {
        DistributedJoinOperator djo = extractDistributedJoinAny(upstream);
        if (djo == null || djo.links().isEmpty()) {
            return null;
        }
        // 链上含 CROSS 环节（非相关标量子查询的单值广播）或<b>派生源</b>（子查询 / CTE 的迷你聚合
        // 流水线）：本路径重建 join 舞台时不带派生源（{@code DistributedJoinAggregationOperator}
        // 无 sources 参数）⇒ 该层关系既无生产者也无扫描（firstTable 为 null）→ open 阶段 NPE。
        // 回退 Master 本地聚合 —— join 仍在 Worker 执行、结果流回 Master 再聚合，正确但非分布式。
        if (hasCrossLink(djo) || hasDerivedSource(djo)) {
            return null;
        }
        int partitions = djo.numPartitions();
        String queryId = "query-" + Long.toHexString(System.nanoTime());
        DistributedJoinAggregationOperator op = new DistributedJoinAggregationOperator(
                queryId,
                djo.leftCatalog(), djo.leftSchema(), djo.leftTable(), djo.leftKeys(),
                djo.leftProbe(), djo.leftFilter(),
                // 本路径把 join 输出按分组键重哈希（非共分区），分区方式必须仍是「按 join 键哈希」——
                // 广播结论（probe 侧就地分桶）在此不成立，降级为哈希（只是标注，执行同为 HashJoin）
                StageScheduler.downgradeBroadcast(djo.links()), djo.memoryLimit(),
                keys, aggs, caseProjections, partitions,
                djo.gateway(), djo.workers(), djo.firstColumns(), expand);

        List<String> outCols = new ArrayList<>(keys);
        outCols.addAll(aggs.stream().map(AggregationSpec::outputColumn).toList());
        String name = "DistributedJoinShuffledAggregation[" + keys
                + ", workers=" + workers.size() + ", partitions=" + partitions + "]";
        return new PhysicalNode(name, op, outCols, List.of());
    }

    /**
     * 从上游节点向下探测分布式 join 基（<b>任意 join 链长</b>，链式单 join / 多 join 提取器之外
     * 的兜底探测）：穿过纯投影（Project）与 CASE 派生列（CaseProject，{@code sum(if(...))} 聚合的
     * 前置算子）取 {@link DistributedJoinOperator}；遇 Filter（残余谓词须在 join 之后执行）或其它
     * 算子 → 返回 null（二次 shuffle 需回退，保证过滤不丢失）。与 {@code extractDistributedJoin} /
     * {@code extractMultiJoin} 的差异：不按 {@code links().size()} 限定场景 —— 二次 shuffle 最终
     * 聚合对单 / 多 join 一视同仁。
     */
    /**
     * 链上是否含 <b>CROSS 环节</b>（非相关标量子查询的「单值广播」build 侧）。
     *
     * <p>上挂聚合（{@code tryBuildChainedAggregation} / {@code tryBuildChainedMultiJoinAggregation}
     * 重建 join 舞台、{@code tryBuildShuffledJoinAggregation} 重建二次 shuffle 聚合）都<b>不带
     * 派生源</b>，CROSS 环节的派生流水线（{@code MiddleAggregateConsumer} + 派生基表 Producer）
     * 会整段丢失 ⇒ 一律回退，由 Master 本地聚合兜底（正确但非分布式）。</p>
     */
    private static boolean hasCrossLink(DistributedJoinOperator djo) {
        for (StageScheduler.JoinLink link : djo.links()) {
            if (link.joinType() == JoinType.CROSS) {
                return true;
            }
        }
        return false;
    }

    /**
     * 链上是否有<b>派生源</b>（子查询 / CTE 的迷你聚合流水线，{@code DistributedJoinOperator.sources}）。
     *
     * <p>与 {@link #hasCrossLink} 同源的理由：{@code DistributedChainedOperator} /
     * {@code DistributedJoinAggregationOperator} 都<b>没有 sources 参数</b>，从 {@code djo} 重建舞台
     * 时派生流水线整段丢失 ⇒ 该层关系既没有生产者也没有扫描（{@code firstTable} 为 null）→
     * open 阶段 NPE。凡「重建 join 舞台并接上聚合/窗口」的 builder 都必须先过这道闸门，命中即
     * 返回 null，由 Master 本地聚合 / 本地窗口兜底（join 仍在 Worker 执行、派生源正确喂入，只是
     * 后续计算不分布式）。</p>
     */
    private static boolean hasDerivedSource(DistributedJoinOperator djo) {
        List<StageScheduler.DerivedSource> sources = djo.sources();
        if (sources == null) {
            return false;
        }
        for (StageScheduler.DerivedSource ds : sources) {
            if (ds != null) {
                return true;
            }
        }
        return false;
    }

    private DistributedJoinOperator extractDistributedJoinAny(PhysicalNode node) {
        Operator op = node.operator();
        if (op instanceof ProjectOperator) {
            if (node.children().size() == 1) return extractDistributedJoinAny(node.children().get(0));
            return null;
        }
        if (op instanceof CaseProjectOperator) {
            if (node.children().size() == 1) return extractDistributedJoinAny(node.children().get(0));
            return null;
        }
        if (op instanceof FilterOperator) {
            return null;
        }
        if (op instanceof DistributedJoinOperator djo) {
            return djo;
        }
        return null;
    }

    /**
     * 解析聚合输入：裸列名（{@code col} / {@code tbl.col}）直接返回列名（校验上游存在）；
     * {@code CASE WHEN ... END} / {@code IF(cond, a, b)} 条件表达式与算术/函数表达式
     * （{@code a+b}、{@code price*0.8}）均改写为合成派生列名并登记 caseProjections
     * （由上游 CaseProject 预计算派生列）。
     */
    private String resolveAggInput(String arg, String fnName, PhysicalNode upstream,
                                   List<CaseProjection> caseProjections, List<String> derivedCols) {
        // 用 isColumnRef 判裸列：stripQualifier 只取最后一个 '.' 之后的部分，直接判会把
        // "a.b + c.d" 误认成列 "d"（→ 静默算错）。限定符本身仍由 stripQualifier 剥掉。
        if (SqlTextUtil.isColumnRef(arg)) {
            String col = SqlTextUtil.stripQualifier(arg);
            requireColumn(upstream, col, fnName + " 输入列");
            return col;
        }
        String derived = "__case_" + caseProjections.size();
        caseProjections.add(parseConditionalArg(arg, derived, upstream, fnName));
        derivedCols.add(derived);
        return derived;
    }

    /**
     * 解析聚合参数的条件表达式为派生列投影：{@code CASE WHEN ... END} / {@code IF(...)}。
     *
     * <ul>
     *   <li>{@code CASE [WHEN <cond> THEN <v>]+ [ELSE <v>] END}：支持<b>多个 WHEN</b>（顺序短路）；
     *   <li>{@code IF(<cond>, <v>, <v>)}：语义等价单分支 CASE，三参必填。
     * </ul>
     * 条件支持 {@code col op literal} 或 {@code col IN (lit, ...)} 且条件列存在于上游；
     * 结果值支持数值字面量、NULL 或<b>列引用</b>（列须存在于上游）。
     */
    private CaseProjection parseConditionalArg(String text, String derived, PhysicalNode upstream, String fnName) {
        String t = text.trim();
        String upper = t.toUpperCase(Locale.ROOT);
        if (upper.startsWith("CASE ") && upper.endsWith(" END")) {
            return parseCaseBranches(t, derived, upstream, fnName);
        }
        if (upper.startsWith("IF(") && t.endsWith(")")) {
            int paren = t.indexOf('(');
            int close = SqlTextUtil.findMatchingParen(t, paren);
            if (close == t.length() - 1) {
                List<String> args = ScalarExpr.splitTopLevelComma(t.substring(paren + 1, close).trim());
                if (args.size() == 3) {
                    return buildConditionalProjection(List.of(
                                    new CaseProjection.CaseBranch(args.get(0).trim(), normalizeNull(args.get(1)))),
                            normalizeNull(args.get(2)), derived, upstream, fnName);
                }
            }
        }
        // 非条件表达式（sum(a+b) / sum(price*0.8) / sum(a*b) ...）：无分支、else 即逐行表达式结果 ——
        // 复用 CaseProjection 的派生列机制（Worker 侧同样以 CaseProjectOperator 重建，分布式零改动）。
        CaseProjection exprProjection = tryBuildExprProjection(t, derived, upstream, fnName);
        if (exprProjection != null) return exprProjection;
        throw unsupported(fnName + " 参数表达式: " + text);
    }

    /**
     * 非条件聚合参数的表达式派生列：{@code sum(a+b)} / {@code sum(price*0.8)} 等。表达式引用的列
     * 须全部存在于上游；返回<b>空分支 + elseValue=表达式文本</b>的 {@link CaseProjection}
     * （Worker 侧 {@link com.dojao.core.operator.CaseProjectOperator} 对每行求值，全行命中 ELSE）。
     * 裸列引用返回 null（由 {@link #resolveAggInput} 直通路径处理）。
     */
    private CaseProjection tryBuildExprProjection(String t, String derived, PhysicalNode upstream, String fnName) {
        ExprNode expr = parseExprOrNull(t);
        if (expr == null || expr instanceof ExprNode.Column) return null;
        for (String src : new ExprEvaluator(expr).columnArgs()) {
            requireColumn(upstream, src, fnName + " 表达式列");
        }
        return new CaseProjection(derived, List.of(), t);
    }

    /** "NULL" → null（表示 NULL 值），null / 空串原样返回，其余原样返回。 */
    private static String normalizeNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.equalsIgnoreCase("NULL") ? null : t;
    }

    /** 解析 {@code CASE WHEN cond THEN v [WHEN cond THEN v] [ELSE v] END} 为有序分支列表。 */
    private CaseProjection parseCaseBranches(String t, String derived, PhysicalNode upstream, String fnName) {
        // 外层 CASE ... END 之间：从 "WHEN " 起逐段切 WHEN/THEN/ELSE（顶层关键字，括号/引号内忽略）
        String inner = t.substring(4, t.length() - 4).trim();
        if (!inner.regionMatches(true, 0, "WHEN ", 0, 5)) {
            throw unsupported(fnName + " CASE 参数: " + t);
        }
        List<CaseProjection.CaseBranch> branches = new ArrayList<>();
        String elseVal = null;
        int pos = 5;   // 跳过首个 "WHEN "
        while (pos < inner.length()) {
            int thenIdx = findTopLevelKeyword(inner, "THEN", pos);
            if (thenIdx < 0) {
                throw unsupported(fnName + " CASE 参数缺 THEN: " + t);
            }
            String cond = inner.substring(pos, thenIdx).trim();
            int nextWhen = findTopLevelKeyword(inner, "WHEN", thenIdx + 5);
            int elseIdx = findTopLevelKeyword(inner, "ELSE", thenIdx + 5);
            int valueEnd = inner.length();
            if (elseIdx >= 0 && (nextWhen < 0 || elseIdx < nextWhen)) {
                valueEnd = elseIdx;
                elseVal = inner.substring(elseIdx + 5).trim();
            } else if (nextWhen >= 0) {
                valueEnd = nextWhen;
            }
            String val = inner.substring(thenIdx + 5, valueEnd).trim();
            branches.add(new CaseProjection.CaseBranch(cond, normalizeNull(val)));
            if (elseVal != null || nextWhen < 0) break;
            pos = nextWhen + 5;
        }
        return buildConditionalProjection(branches, normalizeNull(elseVal), derived, upstream, fnName);
    }

    /**
     * 条件表达式共享校验（CASE / IF 通用）：每个分支条件可解析为 {@code col op literal} 或
     * {@code col IN (lit, ...)} 且条件列存在；分支结果值 / ELSE 为数值字面量或上游已存在的列；
     * 返回派生列投影 DTO。
     */
    private CaseProjection buildConditionalProjection(List<CaseProjection.CaseBranch> branches, String elseVal,
                                                      String derived, PhysicalNode upstream, String fnName) {
        if (elseVal != null) validateBranchValue(elseVal, fnName, upstream);
        for (CaseProjection.CaseBranch br : branches) {
            validateCondition(br.condition(), fnName, upstream);
            validateBranchValue(br.value(), fnName, upstream);
        }
        return new CaseProjection(derived, branches, elseVal);
    }

    /**
     * 分支结果值：数值字面量、上游已存在的列引用、或算术/函数表达式（如 {@code rating-1}，
     * 逐行求值）。NULL 已归一化为 null，直接放行。
     */
    private void validateBranchValue(String v, String fnName, PhysicalNode upstream) {
        if (v == null) return;   // NULL 字面量（如 IF(cond, NULL, 0) / CASE 无 ELSE）
        String t = v.trim();
        try {
            Double.parseDouble(t);
            return;
        } catch (NumberFormatException ignored) {
            // 非数值字面量 → 尝试表达式 / 列引用
        }
        // 算术 / 函数表达式（rating-1、rating*2、a+b ...）：引用的列须全部存在于上游。
        // 裸列引用（ExprNode.Column）不在此分支，继续走下面的单标识符路径，语义不变。
        ExprNode expr = parseExprOrNull(t);
        if (expr != null && !(expr instanceof ExprNode.Column)) {
            for (String src : new ExprEvaluator(expr).columnArgs()) {
                requireColumn(upstream, src, fnName + " 表达式列");
            }
            return;
        }
        String col = SqlTextUtil.stripQualifier(t);
        if (!SqlTextUtil.isSingleIdentifier(col)) {
            throw unsupported(fnName + " 结果: " + v);
        }
        requireColumn(upstream, col, fnName + " 结果列");
    }

    /** 尝试把文本解析为标量表达式；不可解析返回 null（由调用方回退到列引用路径）。 */
    private static ExprNode parseExprOrNull(String text) {
        try {
            return ExprParser.parse(text);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** 条件校验：{@code col op literal} 或 {@code col IN (lit, ...)}，条件列须存在上游。 */
    private void validateCondition(String cond, String fnName, PhysicalNode upstream) {
        // 快路径 1：col op literal（左侧裸列 + 右侧简单字面量）—— 形状判定与 CaseProjectOperator 一致，
        // 形状不符（a = b、rating-1 > 3 ...）必须回退通用表达式，否则会被当成「列与字符串字面量比较」。
        ScalarExpr.Comparison cmp = ScalarExpr.splitComparison(cond).orElse(null);
        if (cmp != null && SqlTextUtil.isColumnRef(cmp.left()) && ScalarExpr.isLiteral(cmp.right())) {
            requireColumn(upstream, SqlTextUtil.stripQualifier(cmp.left()), fnName + " 条件列");
            return;
        }
        // 快路径 2：col IN (lit, ...)
        if (cmp == null) {
            String inCol = tryParseInColumn(cond);
            if (inCol != null) {
                requireColumn(upstream, inCol, fnName + " 条件列");
                return;
            }
        }
        // 快路径 3：col [NOT] LIKE '字面量模式' —— 形状判定收在 SqlTextUtil.parseLike，与
        // CaseProjectOperator 的求值、addCaseCondColumn 的列收集是同一份实现（三处不得漂移）。
        SqlTextUtil.LikeSpec like = SqlTextUtil.parseLike(cond);
        if (like != null) {
            requireColumn(upstream, like.column(), fnName + " 条件列");
            return;
        }
        // 通用表达式条件：rating-1 > 3、a > b、AND/OR/NOT、BETWEEN、NOT IN、IS NULL、函数调用 ...
        ExprNode node = parseExprOrNull(cond);
        if (node != null) {
            for (String src : new ExprEvaluator(node).columnArgs()) {
                requireColumn(upstream, src, fnName + " 条件表达式列");
            }
            validateExprFunctions(node, cond);
            return;
        }
        throw unsupported(fnName + " 条件: " + cond);
    }

    /** 解析 {@code col IN (lit, ...)} 的左列；非 IN 形式返回 null。 */
    private String tryParseInColumn(String cond) {
        String upper = cond.toUpperCase(Locale.ROOT);
        int inIdx = upper.indexOf(" IN (");
        if (inIdx < 0) return null;
        String left = cond.substring(0, inIdx).trim();
        String col = SqlTextUtil.stripQualifier(left);
        if (!SqlTextUtil.isSingleIdentifier(col)) return null;
        int open = cond.indexOf('(', inIdx);
        int close = SqlTextUtil.findMatchingParen(cond, open);
        if (close != cond.length() - 1) return null;
        List<String> items = ScalarExpr.splitTopLevelComma(cond.substring(open + 1, close).trim());
        if (items.isEmpty()) return null;
        for (String it : items) {
            String v = it.trim();
            if (v.isEmpty()) return null;
            boolean literal = v.equalsIgnoreCase("NULL");
            if (!literal && v.length() >= 2 && v.charAt(0) == '\'' && v.charAt(v.length() - 1) == '\'') literal = true;
            if (!literal) {
                try {
                    Double.parseDouble(v);
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
        return col;
    }

    /** 在括号深度 0、引号外查找顶层关键字 {@code " KW "}（前后须空白），返回关键字起始下标。 */
    private static int findTopLevelKeyword(String s, String kw, int from) {
        String needle = " " + kw.toLowerCase(Locale.ROOT) + " ";
        String lower = s.toLowerCase(Locale.ROOT);
        int depth = 0;
        char quote = 0;
        int start = Math.max(from, 0);
        for (int i = start; i < s.length(); i++) {
            char c = s.charAt(i);
            if (quote != 0) {
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') { quote = c; continue; }
            if (c == '(') { depth++; continue; }
            if (c == ')') { depth--; continue; }
            if (depth > 0) continue;
            if (lower.startsWith(needle, i)) {
                return i + 1;   // 返回关键字起始（跳过前导空格）
            }
        }
        return -1;
    }

    // ==================== HAVING ====================

    private PhysicalNode buildHaving(PhysicalNode upstream, LogicalPlan.Select select) {
        Map<String, String> aggMap = new HashMap<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            String text = item.expression().text();
            if (SqlTextUtil.parseAggregate(text).isPresent()) {
                aggMap.put(SqlTextUtil.normalize(text),
                        item.alias().orElse(aggOutputName(text)));
            }
        }
        // 「嵌在 SELECT 表达式里」的聚合（round(sum(x),2) 的 sum(x)）也被 HAVING 引用时，
        // 需映射到它们的内层输出列（__agg_N / 别名），否则替换不到会误判为不支持。
        aggMap.putAll(nestedAggregateRewrite(select));
        // HAVING 引用「SELECT 未投影」的聚合：这些聚合已由 buildAggregation 补建为内部输出列
        // （__having_agg_N），此处把它们的文本也映射到该列，使下面的替换/过滤能落到真实列上。
        for (HavingAgg ha : havingExtraAggs(select)) {
            aggMap.put(SqlTextUtil.normalize(ha.callText()), ha.outputColumn());
        }
        // 先做 SELECT 别名替换：HAVING 可引用别名（hive_amt），而其定义表达式文本里可能含聚合
        // （ROUND(SUM(x),2)）—— 不替换则整个谓词无从落到上游输出列上。已是输出列的别名不做替换。
        String aliasReplaced = SqlTextUtil.replaceAggregates(
                substituteHavingAliases(select.having().get().text(), select, upstream.outputColumns()),
                aggMap);
        String cond = SqlTextUtil.stripQualifiers(aliasReplaced);
        // 形态①/②：谓词能直接落到上游输出列上 → Filter（简单谓词）或一般布尔求值。
        PhysicalNode filtered = buildHavingFilter(upstream, cond);
        if (filtered != null) return filtered;

        // 形态③：谓词引用了「非标识符列名」的输出列 —— 无别名聚合的输出列名是归一化文本
        // （count(*) / sum(amount)），既不是裸标识符，也不是已注册标量函数 ⇒ 直接构造 FilterOperator
        // 会落 Form.UNKNOWN = 逐行放行（线上 `HAVING count(*) > 1` 完全失效的签名）。先把这些列
        // 复制成合法内部名（__having_ref_N，原列保留供最终投影按原名取），改写谓词后再走形态①②。
        // ★`SELECT *` 时不能用改名方案：星号投影会把新增的辅助列一起输出（与 havingExtraAggs
        // 的星号守卫同口径），宁可报不支持也不泄漏内部列。
        boolean starSelect = select.selectItems().stream()
                .anyMatch(it -> isStarExpansion(it.expression().text().trim()));
        // ★用<b>未去限定符</b>的文本做改名：输出列名可能自带限定符（`min(t.x)`），先 stripQualifiers
        // 会把 `min(t.x)` 腐蚀成 `min(x)`（两个不同列还会撞成同名）⇒ 改名后统一在下面去限定符。
        RenamedHaving renamed = starSelect ? null : renameHavingColumns(upstream, aliasReplaced);
        if (renamed != null) {
            String renamedCond = SqlTextUtil.stripQualifiers(renamed.cond());
            filtered = buildHavingFilter(renamed.upstream(), renamedCond);
            if (filtered != null) return filtered;
            throw unsupported("HAVING 条件: " + cond
                    + "（改写为 " + renamedCond + " 后仍无法求值）");
        }

        throw unsupported("HAVING 条件: " + cond
                + "（支持「分组键 / 聚合 / 表达式 构成的布尔谓词」）");
    }

    /**
     * 形态①/②：把已能落到上游输出列上的 HAVING 谓词变成过滤节点。返回 {@code null} 表示
     * 「这段谓词不能交给 FilterOperator / 一般布尔求值器」—— 调用方换形态或报错，<b>绝不静默放行</b>。
     *
     * <p>为什么不再下推到聚合算子（{@link AggregationHavingConfig}）：该配置用 {@code long} 比较，
     * 对 sum/avg 的小数结果会截断、对 min/max 的非数值结果会当 0 —— 宁可走精确的 Filter 求值。
     * HAVING 跑在聚合之后，行数 = 分组数（远小于输入行数），Filter 的代价可忽略。</p>
     */
    private PhysicalNode buildHavingFilter(PhysicalNode upstream, String cond) {
        // 形态①：单个简单谓词 —— 裸列比较（isSimpleFilter）或「左操作数是上游输出列」（无别名聚合
        // 的输出列名带括号，如 count(*)）。★必须由 FilterOperator 自身的编译结果把关：
        // isSimpleFilter 对 `a != b OR c > d` 会因空白 3 段误判为简单谓词，而非标识符列名会让
        // 编译落 Form.UNKNOWN —— 空引用列 = 该谓词逐行放行、过滤条件被静默丢弃。
        boolean simplePredicate = havingPredicateColumn(cond, upstream.outputColumns()) != null
                || SqlTextUtil.isSimpleFilter(cond);
        if (simplePredicate && !FilterOperator.referencedColumnsOf(cond).isEmpty()) {
            FilterOperator filter = new FilterOperator(upstream.operator(), cond);
            return new PhysicalNode("Filter[" + cond + "]", filter, upstream.outputColumns(), upstream);
        }
        // 形态②：一般布尔谓词（AND / OR / 比较 / 函数 / 算术 组合），如
        // `hive_items != mysql_items OR ABS(hive_amt - mysql_amt) > 0.01` —— FilterOperator 只编译单个
        // 简单谓词，故作布尔列投影（ScalarProject）+ `__having_cond = true` 过滤。
        return tryBuildGeneralHaving(upstream, cond);
    }

    /** 「非标识符列名」复制成安全内部名之后的谓词与新的上游节点。 */
    private record RenamedHaving(PhysicalNode upstream, String cond) {}

    private static final String HAVING_REF_PREFIX = "__having_ref_";

    /**
     * 把谓词里引用的、名字不是裸标识符的输出列（无别名聚合 = 归一化文本，如 {@code count(*)}）复制成
     * 合法内部列名 {@code __having_ref_N}（{@link ColumnAliasOperator}，原列保留供上层投影按原名取），
     * 并把谓词里的引用同步改成新名。
     *
     * <p>返回 {@code null} 表示谓词里没有这类列（或一个都没被引用）—— 调用方按原谓词报不支持。
     * 改名只对「引号外」的出现生效（字符串字面量里的相同文本不动）；长的列名先替换，避免
     * {@code count(*)} 抢先吃掉 {@code count(*)+1} 的前缀。传入的 {@code cond} 应是<b>尚未去掉
     * 限定符</b>的文本（输出列名可能形如 {@code min(t.x)}）；带限定符与去限定符两种写法都会尝试匹配。</p>
     */
    private static RenamedHaving renameHavingColumns(PhysicalNode upstream, String cond) {
        List<String> cols = upstream.outputColumns();
        List<String> tokens = new ArrayList<>();
        for (String c : cols) {
            if (c == null || c.isEmpty() || "*".equals(c)) continue;
            if (SqlTextUtil.isSingleIdentifier(c)) continue;
            if (!tokens.contains(c)) tokens.add(c);
        }
        if (tokens.isEmpty()) return null;
        tokens.sort((a, b) -> Integer.compare(b.length(), a.length()));

        String rewritten = cond;
        List<ColumnAliasOperator.Alias> aliases = new ArrayList<>();
        java.util.Set<String> used = new HashSet<>();
        int suffix = 0;
        for (String token : tokens) {
            // 内部名列不能与已存在的输出列（或本次已生成的副本列）重名，否则 Filter 取列会绑错
            String safe;
            do {
                safe = HAVING_REF_PREFIX + suffix++;
            } while (containsIgnoreCase(cols, safe) || used.contains(safe));
            used.add(safe);
            String replaced = replaceOutsideQuotes(rewritten, token, safe);
            if (replaced == null) {
                // SELECT `sum(t.amount)`（输出列名带限定符）而 HAVING 写 `sum(amount)`（或反之）：
                // 归一化文本对不上，但去限定符后等价 → 再试一次
                String stripped = SqlTextUtil.stripQualifiers(token);
                if (!stripped.equals(token)) replaced = replaceOutsideQuotes(rewritten, stripped, safe);
            }
            if (replaced == null) continue;      // 谓词没引用这列 → 不复制
            rewritten = replaced;
            aliases.add(new ColumnAliasOperator.Alias(token, 1, safe));
        }
        if (aliases.isEmpty()) return null;

        ColumnAliasOperator alias = new ColumnAliasOperator(upstream.operator(), cols, aliases);
        List<String> aliasNames = aliases.stream()
                .map(ColumnAliasOperator.Alias::aliasName).toList();
        PhysicalNode node = new PhysicalNode("ColumnAlias[" + String.join(", ", aliasNames) + "]",
                alias, alias.outputColumns(), upstream);
        return new RenamedHaving(node, rewritten);
    }

    /**
     * 把 {@code text} 里「引号外、按标识符边界」出现的 {@code token}（大小写不敏感）换成
     * {@code replacement}；一次都没命中返回 {@code null}（调用方据此判断「无需改名」）。
     */
    private static String replaceOutsideQuotes(String text, String token, String replacement) {
        StringBuilder sb = null;
        int from = 0;
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c == '\'') {                         // 字符串字面量整体跳过（'' 为转义引号）
                i++;
                while (i < text.length()) {
                    char d = text.charAt(i++);
                    if (d == '\'') {
                        if (i < text.length() && text.charAt(i) == '\'') { i++; continue; }
                        break;
                    }
                }
                continue;
            }
            boolean boundary = i == 0 || !Character.isLetterOrDigit(text.charAt(i - 1))
                    && text.charAt(i - 1) != '_';
            if (boundary && text.regionMatches(true, i, token, 0, token.length())) {
                if (sb == null) sb = new StringBuilder(text.length() + 16);
                sb.append(text, from, i).append(replacement);
                i += token.length();
                from = i;
                continue;
            }
            i++;
        }
        if (sb == null) return null;
        sb.append(text, from, text.length());
        return sb.toString();
    }

    /**
     * 把 HAVING 文本里引用的 <b>SELECT 别名</b>替换为其定义表达式（加括号保优先级）。已是上游输出列的
     * 别名不替换（直接按列引用即可，如 {@code COUNT(*) AS c} 的 {@code c}）。
     *
     * <p>为什么需要：{@code SELECT ... ROUND(SUM(x),2) AS amt ... HAVING ABS(amt - y) > 0.01} 里
     * {@code amt} 并非聚合算子的输出列（聚合只产出 {@code sum(x)} 的 {@code __agg_N}，{@code ROUND}
     * 在投影层才算），HAVING 直接按 {@code amt} 取列会找不到。替换成 {@code (ROUND(SUM(x),2))} 后，
     * 后续 {@code replaceAggregates} 再把它收敛到 {@code __agg_N}。</p>
     *
     * <p>只替换裸标识符：限定名（{@code t.amt}）与函数调用（{@code f(...)}）不动；字符串字面量按原样透传。</p>
     */
    private static String substituteHavingAliases(String expr, LogicalPlan.Select select,
                                                  List<String> outputColumns) {
        Map<String, String> aliasMap = new HashMap<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            if (item.alias().isEmpty()) continue;
            String alias = item.alias().get();
            if (isStarExpansion(alias)) continue;
            if (indexOfOutputColumn(outputColumns, alias) != null) continue; // 已是输出列，直接引用
            aliasMap.putIfAbsent(alias.toLowerCase(Locale.ROOT), item.expression().text().trim());
        }
        if (aliasMap.isEmpty()) return expr;

        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < expr.length()) {
            char c = expr.charAt(i);
            if (c == '\'') {                       // 字符串字面量整体透传
                sb.append(c);
                i++;
                while (i < expr.length()) {
                    char d = expr.charAt(i);
                    sb.append(d);
                    i++;
                    if (d == '\'') break;
                }
                continue;
            }
            if (Character.isJavaIdentifierStart(c)) {
                int j = i;
                while (j < expr.length() && Character.isJavaIdentifierPart(expr.charAt(j))) j++;
                String token = expr.substring(i, j);
                int k = j;
                while (k < expr.length() && Character.isWhitespace(expr.charAt(k))) k++;
                boolean qualifiedOrCall = k < expr.length()
                        && (expr.charAt(k) == '.' || expr.charAt(k) == '(');
                String repl = qualifiedOrCall ? null : aliasMap.get(token.toLowerCase(Locale.ROOT));
                if (repl != null) {
                    sb.append('(').append(repl).append(')');
                } else {
                    sb.append(token);
                }
                i = j;
                continue;
            }
            sb.append(c);
            i++;
        }
        return sb.toString();
    }

    /**
     * 一般布尔 HAVING 谓词：把谓词求值成布尔列 {@code __having_cond}（{@link ScalarProjectOperator}，
     * 复用 {@link ExprParser}/{@link ExprEvaluator} 对 AND / OR / 比较 / 算术 / 内置函数 的求值），
     * 再以 {@link FilterOperator} 过滤 {@code __having_cond = true}。最终投影按 SELECT 显式列裁剪时
     * 把 {@code __having_cond} 丢掉（不进结果）。
     *
     * <p>返回 {@code null} 表示「承接不了」—— 表达式不可解析 / 引用了上游不存在的列 / 含未知函数调用
     * （含聚合名，聚合调用应已被 replaceAggregates 收敛成列）→ 由调用方报不支持，绝不静默放行。</p>
     */
    private PhysicalNode tryBuildGeneralHaving(PhysicalNode upstream, String cond) {
        ExprNode node;
        try {
            node = ExprParser.parse(cond);
        } catch (RuntimeException e) {
            return null;
        }
        // 每个函数调用必须是内置或已注册标量函数（聚合名在这里出现说明没收敛成列 → 不承接）。
        List<ExprNode.FuncCall> calls = new ArrayList<>();
        collectFuncCalls(node, calls);
        for (ExprNode.FuncCall f : calls) {
            String name = f.name().toLowerCase(Locale.ROOT);
            if (!ExprBuiltins.isBuiltin(name) && !ScalarFunctionRegistry.isRegistered(name)) {
                return null;
            }
        }
        for (String c : new ExprEvaluator(node).columnArgs()) {
            if (indexOfOutputColumn(upstream.outputColumns(), c) == null) {
                return null;
            }
        }
        String condCol = "__having_cond";
        List<String> outCols = new ArrayList<>(upstream.outputColumns());
        if (!outCols.contains(condCol)) outCols.add(condCol);
        ScalarProjectOperator sp = new ScalarProjectOperator(
                upstream.operator(), outCols, Set.of(condCol),
                List.of(new ScalarProjectOperator.Projection(cond, condCol)));
        PhysicalNode projected = new PhysicalNode("ScalarProject[" + condCol + "]", sp, outCols, upstream);
        FilterOperator filter = new FilterOperator(projected.operator(), condCol + " = true");
        return new PhysicalNode("Filter[" + cond + "]", filter, outCols, projected);
    }

    /**
     * HAVING 谓词是否为「输出列 op 字面量」形态；是则返回左操作数列名（已与输出列大小写不敏感
     * 比对过），否则 {@code null}。
     *
     * <p>存在的理由：无别名的聚合输出列名 = 归一化文本（{@code count(*)}、{@code sum(rating)}），
     * 带括号 ⇒ 不是裸标识符 ⇒ {@link SqlTextUtil#isSimpleFilter} 判 false，但这些列确实能被
     * {@link FilterOperator} 按名取出求值。只接受<b>单个</b>比较且右侧为字面量：多条件
     * （{@code count(*)>2 and x>1}）放进去会被 FilterOperator 当成整串字符串比较而静默算错。</p>
     */
    private static String havingPredicateColumn(String cond, List<String> outputColumns) {
        Optional<ScalarExpr.Comparison> cmp = ScalarExpr.splitComparison(cond);
        if (cmp.isEmpty()) return null;
        ScalarExpr.Comparison c = cmp.get();
        if (!ScalarExpr.isLiteral(c.right())) return null;
        String left = c.left().trim();
        return containsIgnoreCase(outputColumns, left) ? left : null;
    }

    /** 一个「HAVING 引用、但 SELECT 未投影」的聚合调用及其内部输出列名（{@code __having_agg_N}）。 */
    private record HavingAgg(String callText, SqlTextUtil.AggInfo info, String outputColumn) {
    }

    /**
     * 收集 HAVING 引用、但<b>SELECT 列表未提供输出列</b>的聚合调用（{@code ... GROUP BY k HAVING COUNT(*) >= 3}
     * 的 {@code COUNT(*)}）。这些聚合由 {@link #buildAggregation} 补建为内部输出列（{@code __having_agg_N}），
     * 供 {@link #buildHaving} 把 HAVING 文本里的聚合调用替换成列引用后过滤；最终投影按 SELECT 显式列裁剪时
     * 自然把它们丢掉（不进结果）。
     *
     * <p>去重口径：已在 SELECT 里出现过的聚合（整项聚合 / 嵌在表达式里的内层聚合）<b>不</b>重复补建，
     * 直接用它们既有的输出列 —— 与 {@code aggMap}/{@code nestedAggregateRewrite} 同一份归一化文本。</p>
     *
     * <p>★带星号展开（{@code select *}）时返回空：投影输出写作哨兵 {@code "*"}，运行期 {@code Page.select}
     * 会连隐藏列一起输出 ⇒ 不能用隐藏列方案，HAVING 维持原有限制（宁可报不支持，不可泄漏内部列）。</p>
     *
     * <p>本方法只依赖 {@code select}，确定性输出 ⇒ {@link #buildAggregation}（建规格）与
     * {@link #buildHaving}（建替换映射）各自调用得到同一份规划。</p>
     */
    private static List<HavingAgg> havingExtraAggs(LogicalPlan.Select select) {
        List<HavingAgg> out = new ArrayList<>();
        if (select.having().isEmpty()) return out;
        for (LogicalPlan.SelectItem it : select.selectItems()) {
            if (isStarExpansion(it.expression().text().trim())) return out;
        }
        // SELECT 已提供输出列的聚合调用（归一化文本）：整项聚合 + 嵌在表达式里的内层聚合。
        Set<String> provided = new HashSet<>();
        for (LogicalPlan.SelectItem it : select.selectItems()) {
            String text = it.expression().text();
            if (SqlTextUtil.parseAggregate(text).isPresent()) provided.add(SqlTextUtil.normalize(text));
        }
        provided.addAll(nestedAggregateRewrite(select).keySet());
        Set<String> seen = new HashSet<>();
        for (String call : SqlTextUtil.findAggregateCalls(select.having().get().text())) {
            if (provided.contains(call) || !seen.add(call)) continue;
            Optional<SqlTextUtil.AggInfo> info = SqlTextUtil.parseAggregate(call);
            if (info.isEmpty()) continue;
            out.add(new HavingAgg(call, info.get(), "__having_agg_" + out.size()));
        }
        return out;
    }

    // ==================== 窗口函数 ====================

    /** 列裁剪：在构建窗口算子前，只保留真正需要的上游列。 */
    private PhysicalNode pruneForWindows(PhysicalNode upstream,
                                          List<LogicalPlan.SelectItem> windowItems,
                                          LogicalPlan.Select select,
                                          Set<String> requiredOutputColumns) {
        Set<String> needed = new LinkedHashSet<>();

        // 1. 窗口函数需要的列：PARTITION BY + ORDER BY + 输入列
        //    窗口键可以是表达式（coalesce(...) / CASE ...）→ 按表达式引用列收集，
        //    否则扫描会把源列裁光（下游物化窗口键时报「找不到 PARTITION BY 列」）。
        for (WindowUnit unit : windowUnits(windowItems, select)) {
            Expression.WindowFunction wf = unit.wf();
            for (String p : wf.partitionBy()) {
                String t = windowKeyText(p);
                if (SqlTextUtil.isSingleIdentifier(t)) {
                    needed.add(t);
                } else {
                    for (String c : referencedColumnsOfExpr(t)) {
                        if (upstream.hasColumn(c)) needed.add(c);
                    }
                }
            }
            for (String o : wf.orderBy()) {
                String t = windowKeyText(o);
                if (SqlTextUtil.isSingleIdentifier(t)) {
                    needed.add(t);
                } else {
                    for (String c : referencedColumnsOfExpr(t)) {
                        if (upstream.hasColumn(c)) needed.add(c);
                    }
                }
            }
            // 函数输入列（ROW_NUMBER/RANK/DENSE_RANK 无参数；COUNT(*) 无需列）
            // ★参数可以是表达式（如 lag(CAST(d AS DOUBLE),1)）→ 裸列名匹配不到时按表达式引用列
            //   收集；否则裁掉 d 后窗口前物化 __win_arg_* 会报「找不到用于计算表达式的列」。
            if (!wf.args().isEmpty() && !wf.args().get(0).trim().equals("*")) {
                for (String arg : wf.args()) {
                    String a = arg.trim();
                    if (a.isEmpty()) continue;
                    String col = SqlTextUtil.stripQualifier(a);
                    if (!col.isEmpty() && upstream.hasColumn(col)) {
                        needed.add(col);
                        continue;
                    }
                    for (String c : referencedColumnsOfExpr(a)) {
                        if (upstream.hasColumn(c)) needed.add(c);
                    }
                }
            }
        }

        // 2. 非窗口 SELECT 项中引用上游的列
        boolean hasStar = false;
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            if (item.expression() instanceof Expression.WindowFunction) continue;
            String text = item.expression().text().trim();
            if (!item.windowCalls().isEmpty()) {
                // 窗口嵌在表达式里：外层算术的操作数（gmv / 2 的 gmv）也必须在窗口之前保留。
                // 表达式解析器不认识 OVER，这里按「文本逐词匹配上游列名」收集（多留列只是不裁剪）。
                needed.addAll(mentionedColumns(text, upstream));
                continue;
            }
            if (text.endsWith("*")) { hasStar = true; continue; } // a.* / b.* → 无法按名裁剪
            String col = SqlTextUtil.stripQualifier(text);
            if (col != null && upstream.hasColumn(col)) needed.add(col);
            else if (upstream.hasColumn(text)) needed.add(text);
            else {
                // ★聚合项在窗口之前已被下层聚合算子物化为输出列（名 = 别名，无别名时 =
                //   聚合表达式文本）。上面按「表达式文本 vs 上游列名」匹配不到 `count(*)`
                //   → 把 `count(*) AS pv` 的 pv 裁掉，窗口之后 buildProject 报
                //   「找不到聚合输出列 'pv'」。这里按别名 / 聚合输出名补登记。
                String aggName = item.alias().orElseGet(() -> aggOutputName(text));
                if (!aggName.isEmpty() && upstream.hasColumn(aggName)) needed.add(aggName);
            }
        }
        if (hasStar) {
            // SELECT 含 *：若外层查询提供了列需求（derived table 场景），用它覆盖 * 展开
            if (requiredOutputColumns != null) {
                for (String col : requiredOutputColumns) {
                    if (upstream.hasColumn(col)) needed.add(col);
                }
            } else {
                return upstream; // 顶层查询 SELECT * → 无法裁剪，全部保留
            }
        }

        // 3. 如果需要的列 = 上游全部列 → 无需裁剪
        List<String> currentOut = upstream.outputColumns();
        boolean hasExtra = false;
        for (String col : currentOut) {
            if (!needed.contains(col)) { hasExtra = true; break; }
        }
        if (!hasExtra) return upstream;

        // 4. 按原顺序保留需要的列
        List<String> projectCols = new ArrayList<>();
        for (String col : currentOut) {
            if (needed.contains(col)) projectCols.add(col);
        }
        System.err.printf("[ColumnPruning] %d → %d columns (dropped %d)%n",
                currentOut.size(), projectCols.size(),
                currentOut.size() - projectCols.size());
        return wrapProject(upstream, projectCols);
    }

    /** 从外层 SELECT 项中提取简单列引用。含 * / 窗口函数 / 聚合函数则跳过对应项，含 * 返回 null。 */
    private static Set<String> computeRequiredFromSelect(LogicalPlan.Select select) {
        Set<String> cols = new LinkedHashSet<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            String text = item.expression().text().trim();
            if (text.endsWith("*")) return null;          // 外层也用 * → 无法裁剪
            if (item.expression() instanceof Expression.WindowFunction) continue;
            if (hasAggregateCall(text)) continue;         // 聚合项（含嵌在表达式里的）→ 不是裸列引用
            String col = SqlTextUtil.stripQualifier(text);
            if (SqlTextUtil.isSingleIdentifier(col)) cols.add(col);
        }
        return cols.isEmpty() ? null : cols;
    }

    /**
     * 尝试构建分布式窗口（最小切片）：扫描 → 按 PARTITION BY 哈希分区 → Worker 本地
     * 排序 + 逐行计算 → 回传 Master。仅支持"单窗口组（相同 PARTITION BY + ORDER BY）且
     * 上游是裸表扫描"；否则返回 null → 回退单机。
     */
    private PhysicalNode tryBuildDistributedWindow(PhysicalNode upstream,
                                                   List<LogicalPlan.SelectItem> windowItems,
                                                   List<WorkerRegistration> workers) {
        BaseTable base = extractBaseTable(upstream);
        if (base == null) {
            return null;
        }

        // 单窗口组：所有窗口函数必须相同的 PARTITION BY + ORDER BY
        List<Expression.WindowFunction> wfs = new ArrayList<>();
        for (LogicalPlan.SelectItem item : windowItems) {
            wfs.add((Expression.WindowFunction) item.expression());
        }
        Expression.WindowFunction first = wfs.get(0);
        // 窗口键含表达式（coalesce / CASE / 算术）→ 分布式窗口只认列名，回退单机物化合成列
        if (hasWindowKeyExpression(wfs)) {
            return null;
        }
        String groupKey = first.partitionBy() + "§" + first.orderBy();
        for (Expression.WindowFunction wf : wfs) {
            if (!(wf.partitionBy() + "§" + wf.orderBy()).equals(groupKey)) {
                return null;
            }
        }

        List<String> partitionBy = new ArrayList<>();
        for (String p : first.partitionBy()) {
            String col = SqlTextUtil.stripQualifier(p);
            requireWindowColumn(upstream, col, "PARTITION BY 列", p);
            partitionBy.add(col);
        }

        List<String> orderBy = new ArrayList<>();
        List<Boolean> ascending = new ArrayList<>();
        for (String o : first.orderBy()) {
            SqlTextUtil.OrderKey ok = SqlTextUtil.parseOrderKey(o);
            requireWindowColumn(upstream, ok.column(), "ORDER BY 列", o);
            orderBy.add(ok.column());
            ascending.add(ok.ascending());
        }

        List<WindowFunctionSpec> specs = new ArrayList<>();
        List<String> winOutCols = new ArrayList<>();
        for (LogicalPlan.SelectItem item : windowItems) {
            Expression.WindowFunction wf = (Expression.WindowFunction) item.expression();
            String out = windowOutputName(item, wf);
            specs.add(toWindowSpec(wf, out));
            winOutCols.add(out);
        }

        int partitions = Math.max(1, context.parallelism());
        String queryId = "query-" + Long.toHexString(System.nanoTime());
        DojaoConnector probe = context.connectorProvider().get(base.catalog);
        // 生产者扫描只取窗口前的输出列（projection），保证 Worker 输出 schema 与下游声明一致。
        // 连接器不兑现下推时，filterExpression 由 Worker 侧引擎 Filter 求值 → 谓词引用的列必须在
        // 扫描时就取回来，否则 Filter 找不到列（恒假 / 报错）。多取的列由上游 Project 裁掉，无害。
        List<String> projection = new ArrayList<>(upstream.outputColumns());
        if (probe != null && !probe.supportsPredicatePushdown()) {
            for (String c : engineFilterColumns(base.predicates)) {
                if (!projection.contains(c)) projection.add(c);
            }
        }
        DistributedWindowOperator op = new DistributedWindowOperator(
                queryId, base.baseRequest, base.predicates, projection,
                partitionBy, orderBy, ascending, specs,
                context.memoryLimit(), partitions, context.workerGateway(), probe, workers);
        // 对外输出列 = 上游输出列（不含引擎过滤列）。扫描投影 projection 多带的过滤列（不兑现下推
        // 时引擎要在 Worker 侧求值谓词）只用于 Filter，不能出现在回传页 / CTAS 建表里。
        op.setVisibleColumns(upstream.outputColumns());

        List<String> outCols = new ArrayList<>(upstream.outputColumns());
        outCols.addAll(winOutCols);
        String name = "DistributedWindow[partitionBy=" + partitionBy
                + ", workers=" + workers.size() + ", partitions=" + partitions + "]";
        return new PhysicalNode(name, op, outCols, List.of());
    }

    /**
     * 尝试构建分布式 <b>join→窗口</b> 链（Phase 2，零二次交换）：上游是分布式单 join 且
     * 单窗口组的 PARTITION BY == join 一侧的 key → join 输出已在归属 Worker 上按 key 分布，
     * 窗口直接在本地 join 输出上排序 + 逐行计算。否则返回 null → 回退单机。
     */
    private PhysicalNode tryBuildChainedWindow(PhysicalNode upstream,
                                               List<LogicalPlan.SelectItem> windowItems,
                                               List<WorkerRegistration> workers) {
        DistributedJoinOperator djo = extractDistributedJoin(upstream);
        // 派生源链不能重建为 DistributedChainedOperator（无 sources 参数 → 流水线丢失）→ Master 本地窗口兜底
        if (djo == null || hasDerivedSource(djo)) {
            return null;
        }

        // 单窗口组：所有窗口函数必须相同的 PARTITION BY + ORDER BY
        List<Expression.WindowFunction> wfs = new ArrayList<>();
        for (LogicalPlan.SelectItem item : windowItems) {
            wfs.add((Expression.WindowFunction) item.expression());
        }
        Expression.WindowFunction first = wfs.get(0);
        // 窗口键含表达式（coalesce / CASE / 算术）→ 分布式窗口只认列名，回退单机物化合成列
        if (hasWindowKeyExpression(wfs)) {
            return null;
        }
        String groupKey = first.partitionBy() + "§" + first.orderBy();
        for (Expression.WindowFunction wf : wfs) {
            if (!(wf.partitionBy() + "§" + wf.orderBy()).equals(groupKey)) {
                return null;
            }
        }

        List<String> partitionBy = new ArrayList<>();
        for (String p : first.partitionBy()) {
            String col = SqlTextUtil.stripQualifier(p);
            requireWindowColumn(upstream, col, "PARTITION BY 列", p);
            partitionBy.add(col);
        }

        // 同 key 前置：PARTITION BY 必须与 join 一侧的 key 完全一致（顺序敏感 → 哈希输入一致 → 零交换）
        if (!partitionBy.equals(djo.leftKeys()) && !partitionBy.equals(djo.rightKeys())) {
            return null;
        }

        List<String> orderBy = new ArrayList<>();
        List<Boolean> ascending = new ArrayList<>();
        for (String o : first.orderBy()) {
            SqlTextUtil.OrderKey ok = SqlTextUtil.parseOrderKey(o);
            requireWindowColumn(upstream, ok.column(), "ORDER BY 列", o);
            orderBy.add(ok.column());
            ascending.add(ok.ascending());
        }

        List<WindowFunctionSpec> specs = new ArrayList<>();
        List<String> winOutCols = new ArrayList<>();
        for (LogicalPlan.SelectItem item : windowItems) {
            Expression.WindowFunction wf = (Expression.WindowFunction) item.expression();
            String out = windowOutputName(item, wf);
            specs.add(toWindowSpec(wf, out));
            winOutCols.add(out);
        }

        int partitions = djo.numPartitions();
        String queryId = "query-" + Long.toHexString(System.nanoTime());
        FragmentSpec.WindowStep step = new FragmentSpec.WindowStep(
                partitionBy, orderBy, ascending, specs, context.memoryLimit(), partitions);
        DistributedChainedOperator op = new DistributedChainedOperator(
                queryId,
                djo.leftCatalog(), djo.leftSchema(), djo.leftTable(), djo.leftKeys(),
                djo.rightCatalog(), djo.rightSchema(), djo.rightTable(), djo.rightKeys(),
                djo.joinType(), djo.memoryLimit(), partitions,
                djo.gateway(), djo.leftProbe(), djo.rightProbe(), djo.workers(), step,
                djo.leftFilter(), djo.rightFilter(),
                djo.firstColumns(), djo.links().get(0).rightColumns());

        List<String> outCols = new ArrayList<>(upstream.outputColumns());
        outCols.addAll(winOutCols);
        String name = "DistributedJoinWindow[partitionBy=" + partitionBy
                + ", workers=" + workers.size() + ", partitions=" + partitions + "]";
        return new PhysicalNode(name, op, outCols, List.of());
    }

    /**
     * 尝试构建分布式 <b>聚合→窗口</b>（两级全下沉 Worker）：上游恰为分布式聚合算子
     * （{@link DistributedJoinAggregationOperator} join 二次 shuffle 聚合 / {@link DistributedAggregationOperator}
     * 裸表聚合）且单窗口组时命中 —— 把聚合段重建为 {@code MiddleAggregateConsumer}（非终端，聚合后按
     * 窗口键重哈希推窗口交换）+ 每 Worker 一个终端 {@code WindowConsumer}（本地窗口 → sink / 回传），
     * 两级都留在 Worker（数据不回 Master）。写形态（CTAS / INSERT INTO / INSERT OVERWRITE）因新算子
     * 是 {@code AbstractDistributedOperator} 而自动全分布式（{@code setSink} / {@code setOverwriteLifecycle}
     * / {@code outputFields} 全部继承自基类）。否则返回 null → 回退单机（或 OVERWRITE 报暂不支持）。
     *
     * <p>窗口键（PARTITION BY / ORDER BY / 输入列）须 ∈ 聚合输出列（分组键 + 聚合结果列，requireColumn
     * 校验）；全局窗口（空 PARTITION BY）→ 聚合输出全落 pid 0 → worker 0 单分区全局排序（已知边界）。</p>
     */
    private PhysicalNode tryBuildAggregateWindow(PhysicalNode upstream,
                                                 List<LogicalPlan.SelectItem> windowItems,
                                                 List<WorkerRegistration> workers) {
        Operator up = upstream.operator();
        if (!(up instanceof DistributedJoinAggregationOperator)
                && !(up instanceof DistributedAggregationOperator)) {
            return null;
        }
        // GROUP BY 表达式派生键：聚合→窗口的两级下沉重建无法在聚合 Producer 上重放派生列
        // （planAggregateWindow 不承载 groupKeyExpressions）→ 回退，窗口在 Master 侧对该聚合
        // 输出执行（聚合输出页含派生键列，Master 窗口按名解析仍正确，只是不过 Worker）。
        if (up instanceof DistributedAggregationOperator dao
                && !dao.groupKeyExpressions().isEmpty()) {
            return null;
        }

        // 单窗口组：所有窗口函数必须相同的 PARTITION BY + ORDER BY
        List<Expression.WindowFunction> wfs = new ArrayList<>();
        for (LogicalPlan.SelectItem item : windowItems) {
            wfs.add((Expression.WindowFunction) item.expression());
        }
        Expression.WindowFunction first = wfs.get(0);
        // 窗口键含表达式（coalesce / CASE / 算术）→ 分布式窗口只认列名，回退单机物化合成列
        if (hasWindowKeyExpression(wfs)) {
            return null;
        }
        String groupKey = first.partitionBy() + "§" + first.orderBy();
        for (Expression.WindowFunction wf : wfs) {
            if (!(wf.partitionBy() + "§" + wf.orderBy()).equals(groupKey)) {
                return null;
            }
        }

        List<String> partitionBy = new ArrayList<>();
        for (String p : first.partitionBy()) {
            String col = SqlTextUtil.stripQualifier(p);
            requireWindowColumn(upstream, col, "PARTITION BY 列", p);
            partitionBy.add(col);
        }

        List<String> orderBy = new ArrayList<>();
        List<Boolean> ascending = new ArrayList<>();
        for (String o : first.orderBy()) {
            SqlTextUtil.OrderKey ok = SqlTextUtil.parseOrderKey(o);
            requireWindowColumn(upstream, ok.column(), "ORDER BY 列", o);
            orderBy.add(ok.column());
            ascending.add(ok.ascending());
        }

        List<WindowFunctionSpec> specs = new ArrayList<>();
        List<String> winOutCols = new ArrayList<>();
        for (LogicalPlan.SelectItem item : windowItems) {
            Expression.WindowFunction wf = (Expression.WindowFunction) item.expression();
            String out = windowOutputName(item, wf);
            specs.add(toWindowSpec(wf, out));
            winOutCols.add(out);
        }

        int partitions = Math.max(1, context.parallelism());
        String queryId = "query-" + Long.toHexString(System.nanoTime());
        // 两类聚合源重建「聚合 → 窗口」两级下沉：join 源（links）重建 join 链（原算子丢弃，探针连接器
        // 移交给新算子统一释放）；scan 源重建裸表聚合链。
        AbstractDistributedOperator op;
        if (up instanceof DistributedJoinAggregationOperator djao) {
            op = new DistributedAggregateWindowOperator(queryId,
                    djao.firstCatalog(), djao.firstSchema(), djao.firstTable(), djao.firstLeftKeys(),
                    djao.firstProbe(), djao.firstFilter(), djao.links(),
                    djao.groupKeys(), djao.aggregations(), djao.caseProjections(), djao.memoryLimit(),
                    partitionBy, orderBy, ascending, specs, partitions,
                    context.workerGateway(), workers, djao.firstColumns());
        } else {
            DistributedAggregationOperator dao = (DistributedAggregationOperator) up;
            op = new DistributedAggregateWindowOperator(queryId,
                    dao.baseRequest(), dao.predicates(), dao.probe(),
                    dao.keyColumns(), dao.aggregations(), dao.caseProjections(), dao.memoryLimit(),
                    partitionBy, orderBy, ascending, specs, partitions,
                    context.workerGateway(), workers);
        }

        List<String> outCols = new ArrayList<>(upstream.outputColumns());
        outCols.addAll(winOutCols);
        String name = "DistributedAggregateWindow[partitionBy=" + partitionBy
                + ", workers=" + workers.size() + ", partitions=" + partitions + "]";
        return new PhysicalNode(name, op, outCols, List.of());
    }

    /** 一个待计算的窗口值：{@code wf} + 输出列（根窗口 = 别名/默认名，嵌套窗口 = 合成列）。 */
    private record WindowUnit(Expression.WindowFunction wf, String out) {}

    /** SELECT 项集合 → 待计算的窗口单元（按 SELECT 项顺序、项内窗口出现顺序）。 */
    private static List<WindowUnit> windowUnits(List<LogicalPlan.SelectItem> windowItems,
                                                LogicalPlan.Select select) {
        Map<LogicalPlan.SelectItem, Integer> itemIndex = new IdentityHashMap<>();
        int pos = 0;
        for (LogicalPlan.SelectItem it : select.selectItems()) itemIndex.put(it, pos++);
        List<WindowUnit> units = new ArrayList<>();
        for (LogicalPlan.SelectItem item : windowItems) {
            if (item.expression() instanceof Expression.WindowFunction wf) {
                units.add(new WindowUnit(wf, windowOutputName(item, wf)));
            } else {
                int base = itemIndex.getOrDefault(item, 0);
                int k = 0;
                for (LogicalPlan.WindowCall wc : item.windowCalls()) {
                    units.add(new WindowUnit(wc.window(), windowExprColumn(base, k++)));
                }
            }
        }
        return units;
    }

    private PhysicalNode buildWindows(PhysicalNode upstream, List<LogicalPlan.SelectItem> windowItems,
                                      LogicalPlan.Select select) {
        // 「窗口嵌在表达式里」（gmv / sum(gmv) OVER ()）只走单机物化：分布式窗口把「整项窗口」
        // 当输出列回传，外层算术只能在 Master 的标量投影里求值，没有对应的下发格式。
        boolean hasNested = windowItems.stream().anyMatch(it -> !it.windowCalls().isEmpty());
        // 分布式执行（开关默认关）：单窗口组 + 上游为裸表扫描时，把窗口整条链路派发到 Worker
        // 执行（扫描 → 按 PARTITION BY 哈希分区 → Worker 本地排序 + 逐行计算 → 回传 Master）；
        // 上游为分布式 join 且 PARTITION BY == join 键时，链式窗口零二次交换（Phase 2）。
        // 其余情况静默回退单机路径（行为不变）。
        if (context.distributedExecution() && !hasNested) {
            List<WorkerRegistration> healthy = WorkerRegistry.get().healthy(context.workerHealthTimeoutMs());
            if (!healthy.isEmpty()) {
                PhysicalNode distributed = tryBuildDistributedWindow(upstream, windowItems, healthy);
                if (distributed != null) {
                    return distributed;
                }
                PhysicalNode chained = tryBuildChainedWindow(upstream, windowItems, healthy);
                if (chained != null) {
                    return chained;
                }
                // 聚合→窗口（两级全下沉 Worker）：上游恰为分布式聚合算子（join 二次 shuffle / 裸表聚合）
                // → 聚合段重建为 MiddleAggregateConsumer（非终端）+ 终端 WindowConsumer，窗口输出就地写 / 回传
                PhysicalNode aggregateWindow = tryBuildAggregateWindow(upstream, windowItems, healthy);
                if (aggregateWindow != null) {
                    return aggregateWindow;
                }
            }
        }

        // 归一化成窗口单元：整项窗口的 output 列名 = 别名 / 默认窗口名；嵌在表达式里的窗口
        // 输出到合成列 __win_expr_<SELECT 项序号>_<窗口序号>，最终投影再把引用回写进表达式。
        List<WindowUnit> units = windowUnits(windowItems, select);

        Map<String, List<WindowUnit>> groups = new LinkedHashMap<>();
        for (WindowUnit u : units) {
            String key = u.wf().partitionBy() + "§" + u.wf().orderBy();
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(u);
        }

        // ★窗口键表达式：PARTITION BY / ORDER BY 里允许函数 / 算术（如 coalesce(province,'__NA__')）。
        // 窗口算子只能按列名分组 / 排序 ⇒ 先在窗口前把表达式物化为合成列（__win_key_N），
        // 窗口定义改写为引用合成列（与 GROUP BY 表达式、ORDER BY 表达式同法）。
        // ★窗口值参数表达式（如 lag(CAST(x AS DOUBLE),1)）同理物化为 __win_arg_N —— 否则
        //   Planning 期 stripWindowArg 只能接受裸列名，CAST 等表达式会被直接判为不支持。
        Map<String, String> keyExprCols = new LinkedHashMap<>();
        for (WindowUnit u : units) {
            Expression.WindowFunction wf = u.wf();
            for (String p : wf.partitionBy()) {
                String t = windowKeyText(p);
                if (!SqlTextUtil.isSingleIdentifier(t)) {
                    keyExprCols.putIfAbsent(t, WINDOW_KEY_ALIAS_PREFIX + keyExprCols.size());
                }
            }
            for (String o : wf.orderBy()) {
                String t = windowKeyText(o);
                if (!SqlTextUtil.isSingleIdentifier(t)) {
                    keyExprCols.putIfAbsent(t, WINDOW_KEY_ALIAS_PREFIX + keyExprCols.size());
                }
            }
            if (!wf.args().isEmpty()) {
                String a = wf.args().get(0).trim();
                if (!a.isEmpty() && !a.equals("*")
                        && !SqlTextUtil.isSingleIdentifier(SqlTextUtil.stripQualifier(a))) {
                    keyExprCols.putIfAbsent(a, WINDOW_ARG_ALIAS_PREFIX + keyExprCols.size());
                }
            }
        }

        PhysicalNode current = upstream;
        if (!keyExprCols.isEmpty()) {
            List<String> outCols = new ArrayList<>(current.outputColumns());
            outCols.addAll(keyExprCols.values());
            Set<String> computed = new LinkedHashSet<>(keyExprCols.values());
            List<ScalarProjectOperator.Projection> projections = new ArrayList<>();
            for (Map.Entry<String, String> e : keyExprCols.entrySet()) {
                validateProjectionExpr(current, e.getKey(), "窗口键表达式列");
                projections.add(new ScalarProjectOperator.Projection(e.getKey(), e.getValue()));
            }
            ScalarProjectOperator sp = new ScalarProjectOperator(
                    current.operator(), outCols, computed, projections);
            current = new PhysicalNode("ScalarProject[窗口键]", sp, outCols, current);
        }

        for (List<WindowUnit> group : groups.values()) {
            Expression.WindowFunction first = group.get(0).wf();

            List<String> partitionBy = new ArrayList<>();
            for (String p : first.partitionBy()) {
                String t = windowKeyText(p);
                String col = keyExprCols.getOrDefault(t, t);
                requireWindowColumn(current, col, "PARTITION BY 列", p);
                partitionBy.add(col);
            }

            List<String> orderBy = new ArrayList<>();
            List<Boolean> ascending = new ArrayList<>();
            for (String o : first.orderBy()) {
                SqlTextUtil.OrderKey ok = SqlTextUtil.parseOrderKey(SqlTextUtil.stripQualifiers(o));
                String col = keyExprCols.getOrDefault(ok.column(), ok.column());
                requireWindowColumn(current, col, "ORDER BY 列", o);
                orderBy.add(col);
                ascending.add(ok.ascending());
            }

            List<WindowFunctionSpec> specs = new ArrayList<>();
            List<String> winOutCols = new ArrayList<>();
            for (WindowUnit u : group) {
                specs.add(toWindowSpec(u.wf(), u.out(), keyExprCols));
                winOutCols.add(u.out());
            }

            WindowSortOperator sort = new WindowSortOperator(
                    current.operator(), partitionBy, orderBy, ascending, context.memoryLimit());
            WindowFunctionOperator win = new WindowFunctionOperator(
                    sort, partitionBy, specs, orderBy, new AggregateFunctionRegistry());

            List<String> outCols = new ArrayList<>(current.outputColumns());
            outCols.addAll(winOutCols);
            current = new PhysicalNode("WindowFunction", win, outCols, current);
        }
        return current;
    }

    private WindowFunctionSpec toWindowSpec(Expression.WindowFunction wf, String out) {
        return toWindowSpec(wf, out, null);
    }

    /**
     * @param argExprCols 窗口值参数表达式 → 物化列名（单机路径由 {@code buildWindows} 先建
     *                    ScalarProject 计算；null = 不做表达式参数改写，非裸列参数直接报不支持）
     */
    private WindowFunctionSpec toWindowSpec(Expression.WindowFunction wf, String out,
                                            Map<String, String> argExprCols) {
        String fn = wf.functionName().toUpperCase(Locale.ROOT);
        WindowFunctionSpec spec = switch (fn) {
            case "ROW_NUMBER" -> WindowFunctionSpec.rowNumber(out);
            case "RANK" -> WindowFunctionSpec.rank(out);
            case "DENSE_RANK" -> WindowFunctionSpec.denseRank(out);
            case "SUM" -> WindowFunctionSpec.sum(stripWindowArg(wf, 0, argExprCols), out);
            case "AVG" -> WindowFunctionSpec.avg(stripWindowArg(wf, 0, argExprCols), out);
            case "MIN" -> WindowFunctionSpec.min(stripWindowArg(wf, 0, argExprCols), out);
            case "MAX" -> WindowFunctionSpec.max(stripWindowArg(wf, 0, argExprCols), out);
            case "COUNT" -> WindowFunctionSpec.count(
                    wf.args().isEmpty() || wf.args().get(0).trim().equals("*")
                            ? null : stripWindowArg(wf, 0, argExprCols), out);
            case "LAG" -> {
                String col = stripWindowArg(wf, 0, argExprCols);
                int off = wf.args().size() > 1 ? parseLagLeadOffset("LAG", wf.args().get(1)) : 1;
                Object def = wf.args().size() > 2 ? literalValue(wf.args().get(2)) : null;
                yield WindowFunctionSpec.lag(col, out, off, def);
            }
            case "LEAD" -> {
                String col = stripWindowArg(wf, 0, argExprCols);
                int off = wf.args().size() > 1 ? parseLagLeadOffset("LEAD", wf.args().get(1)) : 1;
                Object def = wf.args().size() > 2 ? literalValue(wf.args().get(2)) : null;
                yield WindowFunctionSpec.lead(col, out, off, def);
            }
            default -> {
                // 自定义聚合函数（CREATE FUNCTION 加载后注册进 AggregateFunctionRegistry）按名取用，
                // 作为 AGGREGATE 窗口函数；内置 SUM/AVG/MIN/MAX/COUNT 仍走上面原生快速路径。
                String inputCol = wf.args().isEmpty() || wf.args().get(0).trim().equals("*")
                        ? null : stripWindowArg(wf, 0, argExprCols);
                if (AggregateFunctionRegistry.hasGlobal(fn)) {
                    yield WindowFunctionSpec.aggregate(fn, inputCol, out);
                }
                throw unsupported("窗口函数: " + fn);
            }
        };
        // ROWS 帧只对内置聚合窗口（SUM/AVG/COUNT/MIN/MAX）有意义；RANK/ROW_NUMBER/LAG/LEAD 按 SQL 语义忽略帧。
        // 解析失败（如 FOLLOWING 结束边界、非默认 RANGE）在此抛 UnsupportedOperationException（计划期报错）。
        if (spec.type() == WindowFunctionType.SUM || spec.type() == WindowFunctionType.AVG
                || spec.type() == WindowFunctionType.COUNT || spec.type() == WindowFunctionType.MIN
                || spec.type() == WindowFunctionType.MAX) {
            WindowFrame frame = WindowFrame.parse(wf.windowFrame().orElse(null));
            if (frame != null) {
                spec = spec.withFrame(frame);
            }
        }
        return spec;
    }

    /**
     * 窗口组是否含表达式键（PARTITION BY / ORDER BY）或表达式<b>值参数</b>——
     * 分布式窗口路径只认裸列名（物化合成列在 Master 侧单机路径完成），命中即回退单机。
     */
    private static boolean hasWindowKeyExpression(Expression.WindowFunction wf) {
        for (String p : wf.partitionBy()) {
            if (!SqlTextUtil.isSingleIdentifier(windowKeyText(p))) return true;
        }
        for (String o : wf.orderBy()) {
            if (!SqlTextUtil.isSingleIdentifier(windowKeyText(o))) return true;
        }
        if (!wf.args().isEmpty()) {
            String a = wf.args().get(0).trim();
            if (!a.isEmpty() && !a.equals("*")
                    && !SqlTextUtil.isSingleIdentifier(SqlTextUtil.stripQualifier(a))) return true;
        }
        return false;
    }

    /** 窗口组（全部函数）是否含表达式键 / 表达式值参数。 */
    private static boolean hasWindowKeyExpression(List<Expression.WindowFunction> wfs) {
        for (Expression.WindowFunction wf : wfs) {
            if (hasWindowKeyExpression(wf)) return true;
        }
        return false;
    }

    /** 窗口键原文 → 去限定符、去 ASC/DESC 的表达式文本（单列时即裸列名）。 */
    private static String windowKeyText(String key) {
        String t = SqlTextUtil.stripQualifiers(key).trim();
        String upper = t.toUpperCase(Locale.ROOT);
        if (upper.endsWith(" DESC")) return t.substring(0, t.length() - 5).trim();
        if (upper.endsWith(" ASC")) return t.substring(0, t.length() - 4).trim();
        return t;
    }

    /** 表达式文本引用的列（供列裁剪；标量函数走 ScalarExpr，其余走通用表达式解析）。解析失败返回空集。 */
    private static Set<String> referencedColumnsOfExpr(String text) {
        Set<String> cols = new LinkedHashSet<>();
        Optional<ScalarExpr> fn = ScalarExpr.tryParse(text);
        if (fn.isPresent()) {
            for (ScalarExpr.ScalarArg arg : fn.get().args()) {
                if (arg instanceof ScalarExpr.ScalarArg.ColumnArg c) cols.add(c.column());
            }
            return cols;
        }
        try {
            for (String c : new ExprEvaluator(ExprParser.parse(text)).columnArgs()) cols.add(c);
        } catch (RuntimeException ignored) {
            // 解析失败：保守为空（不裁剪列本身已经由 isUnprunable 等兜底）
        }
        return cols;
    }

    /**
     * 文本里出现的上游列（逐词匹配、大小写不敏感，跳过字符串字面量；{@code t.col} 命中 {@code col}）。
     *
     * <p>用于含 {@code OVER (...)} 的表达式 —— 通用表达式解析器不接受窗口语法，无法走
     * {@link #referencedColumnsOfExpr}。返回的是<b>上游规范列名</b>；识别多了只意味着不裁剪，
     * 不会算错。</p>
     */
    private static Set<String> mentionedColumns(String text, PhysicalNode upstream) {
        Set<String> out = new LinkedHashSet<>();
        Map<String, String> byLower = new HashMap<>();
        for (String c : upstream.outputColumns()) {
            byLower.putIfAbsent(c.toLowerCase(Locale.ROOT), c);
        }
        int n = text.length();
        int i = 0;
        while (i < n) {
            char c = text.charAt(i);
            if (c == '\'' || c == '"') {
                int j = i + 1;
                while (j < n) {
                    if (text.charAt(j) == c) {
                        if (j + 1 < n && text.charAt(j + 1) == c) { j += 2; continue; }
                        break;
                    }
                    j++;
                }
                i = Math.min(n, j + 1);
                continue;
            }
            if (!Character.isLetter(c) && c != '_') { i++; continue; }
            int j = i;
            while (j < n && (Character.isLetterOrDigit(text.charAt(j)) || text.charAt(j) == '_')) j++;
            String canon = byLower.get(text.substring(i, j).toLowerCase(Locale.ROOT));
            if (canon != null) out.add(canon);
            i = j;
        }
        return out;
    }

    private String stripWindowArg(Expression.WindowFunction wf, int idx) {
        return stripWindowArg(wf, idx, null);
    }

    /** 取窗口值参数列名；表达式参数查 {@code argExprCols} 得物化列（见 {@link #buildWindows}）。 */
    private String stripWindowArg(Expression.WindowFunction wf, int idx,
                                  Map<String, String> argExprCols) {
        String arg = wf.args().get(idx).trim();
        if (argExprCols != null) {
            String col = argExprCols.get(arg);
            if (col != null) return col;
        }
        if (!SqlTextUtil.isSingleIdentifier(SqlTextUtil.stripQualifier(arg))) {
            throw unsupported(wf.functionName() + " 窗口参数: " + arg);
        }
        return SqlTextUtil.stripQualifier(arg);
    }

    // ==================== 投影 ====================

    private PhysicalNode buildProject(PhysicalNode upstream, LogicalPlan.Select select) {
        // 先解析非 * / alias.* 项的输出名，供 *, x 展开 * 时排除，避免列重复
        List<String> explicitCols = new ArrayList<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            String text = item.expression().text().trim();
            if (!isStarExpansion(text)) {
                explicitCols.add(resolveSelectOutput(item, upstream, select));
            }
        }

        List<String> outCols = new ArrayList<>();
        // 与 outCols 平行的「用户可见列名」：select 项的别名优先（`u.province AS prov` → prov）。
        // 裸列按上游规范名输出（见 resolveSelectOutput），别名在此补上 —— 否则派生表对外
        // 声明的列名（analyzer 按别名派生）与物理输出名不一致，外层 `GROUP BY prov` 直接找不到列。
        List<String> displayCols = new ArrayList<>();
        // 标量函数投影项（表达式文本 → 输出列名）
        List<ScalarProjectOperator.Projection> scalarProjections = new ArrayList<>();
        Set<String> scalarOut = new LinkedHashSet<>();
        for (LogicalPlan.SelectItem item : select.selectItems()) {
            String text = item.expression().text().trim();
            if (isStarExpansion(text)) {
                // 能否在计划期把 * 直接展开为真实列名（语义等价于运行期 Page.select 的展开）：
                //   ① 上游列名已知（不含哨兵 "*" 自身 —— 嵌套 star 时上游也是哨兵，展不了）；
                //   ② 裸 * 总是可以；alias.* 仅在 FROM 只有唯一关系时才可 —— 多表 join 时上游
                //      列是各表之和，展开会连带别的表列，故 alias.* 多表时保留哨兵。
                boolean pureStar = "*".equals(text);
                boolean canExpand = !upstream.outputColumns().contains("*")
                        && (pureStar || (select.joins().isEmpty()
                                         && select.fromRelations().size() == 1));
                if (explicitCols.isEmpty() && !canExpand) {
                    // 无法静态展开（alias.* 多表 / 上游本身是哨兵）→ 保留 *，由 Page.select 运行期展开
                    outCols.add("*");
                    displayCols.add("*");
                } else {
                    // 展开为上游真实列，并去掉显式列（避免 *, x 时 x 重复输出）。
                    // 内部列（__jk_* / __win_key_*）不对用户暴露，跳过。
                    for (String c : upstream.outputColumns()) {
                        if (isJoinKeyAlias(c)) continue;
                        // HAVING 为「非标识符列名」的聚合输出列复制出的内部副本，不对用户暴露
                        if (c.startsWith(HAVING_REF_PREFIX)) continue;
                        // 无 FROM / 纯 UNNEST 的单行占位列不对用户暴露
                        if (SingleRowOperator.DUMMY_COLUMN.equals(c)) continue;
                        if (!explicitCols.contains(c)) {
                            outCols.add(c);
                            displayCols.add(c);
                        }
                    }
                }
                continue;
            }
            String out = resolveSelectOutput(item, upstream, select);
            outCols.add(out);
            displayCols.add(item.alias().orElse(out));
            // 窗口嵌在表达式里（gmv / sum(gmv) OVER ()）：窗口值已由窗口算子物化成 __win_expr_*，
            // 这里把窗口片段回写成合成列、整个表达式作为标量投影求值。绝不静默跳过 ——
            // 跳过就等于「整个表达式退化为窗口值」（线上占比/环比的 P0 静默错误）。
            if (!item.windowCalls().isEmpty()) {
                String exprText = rewriteWindowCalls(item, select);
                if (!isProjectableExpr(exprText)) {
                    throw unsupported("SELECT 表达式（含窗口函数）: " + text);
                }
                scalarOut.add(out);
                scalarProjections.add(new ScalarProjectOperator.Projection(exprText, out));
                continue;
            }
            // 表达式投影项 → 收集为 ScalarProject 计算列，两类形态：
            //   ① 已注册标量函数调用（参数为「列 / 字面量 / 列 op 字面量」简单形态）；
            //   ② 通用表达式（CASE WHEN ... END / IF(...) / 算术 / 布尔）。
            // 以下项除外：GROUP BY 表达式派生键（已由聚合输出提供，再求值会因源列缺失炸）、
            // 窗口函数项 / 整项聚合（由上游算子产出）、* 展开项。
            // 聚合套在表达式里：先把内层聚合调用改写成它们的输出列引用（__agg_N / 别名），
            // 再作为普通表达式投影求值 —— 直接拿原文求值会把聚合函数当标量函数算（或找不到列）。
            if (!isGroupKeyExpression(select, text)
                    && !(item.expression() instanceof Expression.WindowFunction)
                    && SqlTextUtil.parseAggregate(text).isEmpty()
                    && !isStarExpansion(text)) {
                String exprText = hasAggregateCall(text)
                        ? SqlTextUtil.replaceAggregates(text, nestedAggregateRewrite(select))
                        : text;
                if (isProjectableExpr(exprText)) {
                    scalarOut.add(out);
                    scalarProjections.add(new ScalarProjectOperator.Projection(exprText, out));
                } else if (hasAggregateCall(text)) {
                    // 含聚合的项必须能被投影求值（内层聚合已产出列），否则绝不能静默跳过 ——
                    // 跳过后 outCols 里留着一个没人计算的列名，结果是错的。
                    throw unsupported("SELECT 表达式（含聚合）: " + text);
                }
            }
        }

        // 含表达式投影 → 构建 ScalarProjectOperator（求值表达式 + 透传列按序组装）
        if (!scalarProjections.isEmpty()) {
            // 校验表达式引用的列存在（fail-fast）：标量函数参数列 / 通用表达式源列
            for (ScalarProjectOperator.Projection p : scalarProjections) {
                validateProjectionExpr(upstream, p.exprText(), "投影表达式列");
            }
            ScalarProjectOperator sp = new ScalarProjectOperator(
                    upstream.operator(), outCols, scalarOut, scalarProjections);
            String desc = scalarProjections.stream()
                    .map(p -> p.exprText() + " -> " + p.outputName())
                    .collect(java.util.stream.Collectors.joining(", "));
            return withDisplayNames(new PhysicalNode("ScalarProject[" + desc + "]", sp, outCols, upstream),
                    outCols, displayCols);
        }

        // 投影与上游输出完全一致 → 跳过 Project
        if (outCols.equals(upstream.outputColumns())) {
            return withDisplayNames(upstream, outCols, displayCols);
        }
        ProjectOperator proj = new ProjectOperator(upstream.operator(), outCols);
        return withDisplayNames(new PhysicalNode("Project", proj, outCols, upstream), outCols, displayCols);
    }

    /**
     * 把 SELECT 项里的每个窗口调用替换成它的合成列（{@code __win_expr_<项序号>_<窗口序号>}），
     * 其余部分（外层 {@code + - * /}、常量、其它函数）原样保留 —— 从右往左替换避免下标位移。
     *
     * <p>先做聚合替换（{@code round(sum(x),2) / sum(round(sum(x),2)) OVER ()} 这类混合形态），
     * 窗口定位在替换后的文本上做：{@code replaceAggregates} 不碰 OVER 片段，位置仍然有效。</p>
     */
    private static String rewriteWindowCalls(LogicalPlan.SelectItem item, LogicalPlan.Select select) {
        String text = item.expression().text();
        if (hasAggregateCall(text)) {
            text = SqlTextUtil.replaceAggregates(text, nestedAggregateRewrite(select));
        }
        List<SqlTextUtil.Span> spans = SqlTextUtil.findWindowCallSpans(text);
        if (spans.size() != item.windowCalls().size()) {
            throw unsupported("SELECT 表达式窗口函数定位失败（期望 " + item.windowCalls().size()
                    + " 个窗口调用，实际 " + spans.size() + "）: " + item.expression().text());
        }
        int base = 0;
        for (int i = 0; i < select.selectItems().size(); i++) {
            if (select.selectItems().get(i) == item) { base = i; break; }
        }
        for (int k = spans.size() - 1; k >= 0; k--) {
            SqlTextUtil.Span s = spans.get(k);
            text = text.substring(0, s.start()) + windowExprColumn(base, k) + text.substring(s.end());
        }
        return text;
    }

    private PhysicalNode withDisplayNames(PhysicalNode node, List<String> sourceCols) {
        return withDisplayNames(node, sourceCols, List.of());
    }

    /**
     * 把上游列名还原为用户可见列名：内部消歧列（{@code __jk_user_id_2} → {@code user_id}）
     * 与 select 别名（{@code u.province AS prov} → {@code prov}）。有变化时才追加一层改名
     * {@link ProjectOperator}，正常查询零变化。
     *
     * @param requested {@code sourceCols} 对应的期望列名（空/短列表表示只做内部名还原）
     */
    private PhysicalNode withDisplayNames(PhysicalNode node, List<String> sourceCols,
                                           List<String> requested) {
        List<String> display = new ArrayList<>(sourceCols.size());
        boolean changed = false;
        for (int i = 0; i < sourceCols.size(); i++) {
            String c = sourceCols.get(i);
            String want = i < requested.size() ? requested.get(i) : null;
            String d = (want == null || want.isBlank() || "*".equals(c)) ? c : want;
            d = displayJoinAliasName(d);
            display.add(d);
            if (!d.equals(c)) changed = true;
        }
        if (!changed) return node;
        List<ProjectOperator.ColumnRef> refs = new ArrayList<>(sourceCols.size());
        Map<String, Integer> occurrence = new HashMap<>();
        for (int i = 0; i < sourceCols.size(); i++) {
            String src = sourceCols.get(i);
            int k = occurrence.merge(src, 1, Integer::sum);
            refs.add(new ProjectOperator.ColumnRef(src, k, display.get(i)));
        }
        ProjectOperator proj = ProjectOperator.aliased(node.operator(), refs);
        return new PhysicalNode("Project", proj, display, node);
    }

    private String resolveSelectOutput(LogicalPlan.SelectItem item, PhysicalNode upstream,
                                       LogicalPlan.Select select) {
        Expression expr = item.expression();
        String text = expr.text().trim();

        if (expr instanceof Expression.WindowFunction wf) {
            String out = windowOutputName(item, wf);
            requireColumn(upstream, out, "窗口输出列");
            return out;
        }
        // 窗口嵌在表达式里：本项由投影层求值（引用 __win_expr_*），列还不存在于上游 → 不 requireColumn。
        // 命名与含聚合表达式项一致（别名优先，否则归一化文本）。
        if (!item.windowCalls().isEmpty()) {
            return item.alias().orElse(SqlTextUtil.normalize(text));
        }
        if (SqlTextUtil.parseAggregate(text).isPresent()) {
            String out = item.alias().orElse(aggOutputName(text));
            requireColumn(upstream, out, "聚合输出列");
            return out;
        }
        // 聚合套在表达式里（round(sum(x),2) / sum(a)*100.0/sum(b)）：内层聚合由聚合算子产出
        // （见 nestedAggregates），本项本身由投影层求值 —— 输出名取别名，否则沿用表达式命名。
        // 绝不能 requireColumn：它算出来的列还不存在于上游。
        if (hasAggregateCall(text)) {
            return item.alias().orElse(SqlTextUtil.normalize(text));
        }
        // GROUP BY 表达式派生键：select year(x) 同 group by year(x) → 直接引用分组键派生列
        // （聚合输出列名 = 派生列名，别名优先），不再按标量/裸列处理。
        if (isGroupKeyExpression(select, text)) {
            String out = groupKeyDerivedName(select, text);
            requireColumn(upstream, out, "GROUP BY 表达式输出列");
            return out;
        }
        // 已注册标量函数调用 → 输出别名或自动名（如 cosine_similarity_embedding）
        if (ScalarExpr.tryParse(text).isPresent()) {
            return item.alias().orElse(scalarOutputName(text));
        }
        // 通用表达式（CASE WHEN ... END / IF(...) / 算术 / 布尔）→ 别名或归一化文本
        ExprNode node = parseExprOrNull(text);
        if (node != null && !(node instanceof ExprNode.Column)) {
            return item.alias().orElse(SqlTextUtil.normalize(text));
        }
        // 裸列引用：输出列名取「上游的规范列名」（即数据源里的大小写原样），而不是用户 SQL 里敲的写法。
        // 两处收益：① 结果表列头原汁原味（`select USER_ID` 而表里是 user_id 时仍显示 user_id）；
        // ② 运行时按名投影（Page.select）+ 列裁剪按名匹配都是精确字符串比较，用 SQL 文本原名会
        //    匹配失败 → 列被静默丢弃 / 列裁剪失效。
        String col = SqlTextUtil.stripQualifier(text);
        if (SqlTextUtil.isSingleIdentifier(col)) {
            String canonical = indexOfOutputColumn(upstream.outputColumns(), col);
            if (canonical == null) {
                throw unsupported("SELECT 列 " + col + " 不在上游输出列中（可用列: "
                        + String.join(", ", upstream.outputColumns()) + "）");
            }
            return canonical;
        }
        throw unsupported("SELECT 表达式: " + text);
    }

    /**
     * 投影项能否由 {@link ScalarProjectOperator} 求值：
     * <ul>
     *   <li>已注册标量函数调用且参数为简单形态（列 / 字面量 / {@code 列 op 字面量}）——
     *       非简单形态（如 {@code IF(rating-1 > 3, ...)}）交由下一分支的通用表达式处理；</li>
     *   <li>或可解析的通用表达式（CASE / IF / 算术 / 布尔），裸列除外（裸列走透传）。</li>
     * </ul>
     */
    private static boolean isProjectableExpr(String text) {
        Optional<ScalarExpr> fn = ScalarExpr.tryParse(text);
        // 只有简单参数形态才交给 ScalarExpr 快路径；其余（IF 常量条件等）必须回落到通用
        // 表达式解析 —— 否则已注册函数名会被判定为「不可投影」，投影层只留列名不计算（A5）。
        if (fn.isPresent() && ScalarExpr.isSimpleArgs(fn.get())) return true;
        ExprNode node = parseExprOrNull(text);
        return node != null && !(node instanceof ExprNode.Column);
    }

    /**
     * 校验投影表达式引用的列存在于 {@code upstream} 且所用函数已注册（fail-fast）。标量函数调用走
     * 参数列校验，其余通用表达式（CASE / IF / 算术）走 {@link ExprEvaluator#columnArgs()}。{@code what}
     * 用于错误信息中的列语义标签（如「投影表达式列」/「ORDER BY 表达式列」）。
     */
    private void validateProjectionExpr(PhysicalNode upstream, String exprText, String what) {
        Optional<ScalarExpr> fn = ScalarExpr.tryParse(exprText);
        if (fn.isPresent() && ScalarExpr.isSimpleArgs(fn.get())) {
            for (ScalarExpr.ScalarArg a : fn.get().args()) {
                if (a instanceof ScalarExpr.ScalarArg.ColumnArg c) {
                    requireColumn(upstream, c.column(), what);
                } else if (a instanceof ScalarExpr.ScalarArg.ComparisonArg ca) {
                    requireColumn(upstream, SqlTextUtil.stripQualifier(ca.left()), what);
                }
            }
            return;
        }
        ExprNode node = parseExprOrNull(exprText);
        if (node == null) return;   // 不应发生：isProjectableExpr 已保证可解析
        for (String src : new ExprEvaluator(node).columnArgs()) {
            requireColumn(upstream, src, what);
        }
        validateExprFunctions(node, exprText);
    }

    /** alias.* 或 * 都视为星号展开。 */
    private static boolean isStarExpansion(String text) {
        return "*".equals(text) || text.endsWith(".*");
    }

    // ==================== 命名与工具 ====================

    /** 聚合输出列名：别名优先，否则取归一化文本（如 "count(*)"）。 */
    private static String aggOutputName(String exprText) {
        return SqlTextUtil.normalize(exprText);
    }

    /** 标量输出列名：别名优先，否则由函数名 + 首列参生成（如 cosine_similarity_embedding）。 */
    private static String scalarOutputName(String exprText) {
        int paren = exprText.indexOf('(');
        String fn = exprText.substring(0, paren).trim().toLowerCase(Locale.ROOT);
        int close = SqlTextUtil.findMatchingParen(exprText, paren);
        if (close <= paren) return fn;
        String argsText = exprText.substring(paren + 1, close).trim();
        if (argsText.isEmpty()) return fn;
        String first = SqlTextUtil.stripQualifier(argsText.split(",", 2)[0].trim());
        return first.isEmpty() ? fn : fn + "_" + first;
    }

    /** 窗口输出列名：别名优先，否则由函数名 + 首参数生成。 */
    private static String windowOutputName(LogicalPlan.SelectItem item, Expression.WindowFunction wf) {
        if (item.alias().isPresent()) return item.alias().get();
        String fn = wf.functionName().toLowerCase(Locale.ROOT);
        if (wf.args().isEmpty() || wf.args().get(0).trim().equals("*")) return fn;
        return fn + "_" + SqlTextUtil.stripQualifier(wf.args().get(0));
    }

    private static String alias(LogicalPlan.ResolvedRelation rel) {
        return normalize(rel.alias().orElse(rel.table()));
    }

    // ==================== 列裁剪优化 ====================

    /**
     * 列裁剪：自顶向下递归确定每个节点真正需要的列，对产出多余列的子节点插入 Project 剪枝，
     * 并修复受影响算子的上游引用链。
     *
     * <p>核心收益场景：
     * <ul>
     *   <li>多表 join + {@code select a.*, b.*} 导致中间结果携带全部列，但窗口算子只需少数几列</li>
     *   <li>JOIN key 不同名时输出所有列，下游只关心其中一部分</li>
     *   <li>扫描层：只从 MySQL 读取真正需要的列，减少 IO</li>
     * </ul>
     */
    private PhysicalNode optimizeColumns(PhysicalNode root) {
        return pruneRecursive(root, new LinkedHashSet<>(root.outputColumns()));
    }

    private PhysicalNode pruneRecursive(PhysicalNode node, Set<String> required) {
        Operator op = node.operator();
        List<PhysicalNode> children = node.children();

        // 叶子节点：推下列选择到扫描算子（避免 Project 包裹，直接减少 IO）
        if (children.isEmpty()) {
            if (op instanceof ParallelTableScanOperator scan) {
                return pruneScanColumns(node, scan, required);
            }
            if (op instanceof DistributedScanOperator dscan) {
                return pruneScanColumns(node, dscan, required);
            }
            return node;
        }

        // 0. 改名投影按父需求收窄投影项：改名投影在运行期会逐项找源列，父节点不需要的输出列
        //    若留在投影项里，下面的列裁剪会把其源列从子节点裁掉 ⇒ 运行期「投影源列不存在」。
        //    （如派生表 `SELECT help_keyword_id AS hkid, *, sum(..) OVER () AS x` 外层只用 x。）
        List<String> narrowedOut = null;
        if (op instanceof ProjectOperator proj && proj.isAliased() && !required.contains("*")) {
            List<ProjectOperator.ColumnRef> kept = new ArrayList<>();
            for (ProjectOperator.ColumnRef r : proj.refs()) {
                if (required.contains(r.outputName())) kept.add(r);
            }
            if (!kept.isEmpty() && kept.size() < proj.refs().size()) {
                proj.setRefs(kept);
                narrowedOut = new ArrayList<>(kept.size());
                for (ProjectOperator.ColumnRef r : kept) narrowedOut.add(r.outputName());
            }
        }

        // 1. 计算每个子节点需要的列
        List<Set<String>> childRequired = computeChildRequirements(node, required);

        // 2. 递归剪枝子节点，必要时插入 Project
        List<PhysicalNode> prunedChildren = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            PhysicalNode child = children.get(i);
            Set<String> need = childRequired.get(i);
            PhysicalNode pruned = pruneRecursive(child, need);
            // 如果剪枝后子节点产出的列多于需要的 → 插入 Project 裁掉冗余列
            List<String> childOutput = pruned.outputColumns();
            // 需要 *（全列）时无法按名裁剪子节点：Project[*] 在运行时按 root schema 展开，
            // 这里跳过插入 Project，避免产出空列投影（如 FROM TABLE(...) 的 SELECT *）。
            if (!childOutput.equals(new ArrayList<>(need))
                    && childHasMoreThan(childOutput, need)
                    && !need.contains("*")) {
                List<String> projectCols = projectColumns(childOutput, need);
                // 要 0 列时不插空 Project：零列页会让下游算子（Filter 找不到列、ScalarProject /
                // Limit 建不出 root）抛出极难定位的 "Operator X next failed"。多带几列永远安全，
                // 故放弃这次裁剪（上游本就产这些列，不额外增加计算）。
                if (projectCols.isEmpty()) {
                    prunedChildren.add(pruned);
                    continue;
                }
                // 若子节点已是 Project → 更新列合并，避免堆叠连续冗余 Project。
                // 改名投影不能合并（列名即语义，合并会退化成按裸列名取首个同名列）。
                if (pruned.operator() instanceof ProjectOperator existingProj && !existingProj.isAliased()) {
                    existingProj.setColumns(projectCols);
                    pruned = new PhysicalNode(pruned.name(), existingProj,
                            projectCols, pruned.children());
                } else {
                    System.err.printf("[ColumnPruning] %s child[%d]: %d → %d columns%n",
                            node.name(), i, childOutput.size(), projectCols.size());
                    pruned = wrapProject(pruned, projectCols);
                }
            }
            prunedChildren.add(pruned);
        }

        // 3. 重建当前节点（替换子节点），重算输出列，修复算子上游引用
        if (prunedChildren.equals(children)) {
            // 子节点未变，但改名投影的投影项可能已收窄 ⇒ 输出列元数据要同步
            if (narrowedOut == null) return node;
            return new PhysicalNode(node.name(), op, narrowedOut, children);
        }
        fixOperatorUpstreams(node, children, prunedChildren);
        List<String> newOutCols = computePrunedOutput(node, prunedChildren);
        // 当前节点是 Project 且子节点也是 Project 列相同 → 跳过冗余的外层 Project
        if (op instanceof ProjectOperator && prunedChildren.size() == 1
                && prunedChildren.get(0).operator() instanceof ProjectOperator
                && newOutCols.equals(prunedChildren.get(0).outputColumns())) {
            return prunedChildren.get(0);
        }
        return new PhysicalNode(node.name(), op, newOutCols, prunedChildren);
    }

    /** 叶子扫描算子列裁剪（本地并行扫描版）：推下列选择，返回列数减少后的节点。 */
    private static PhysicalNode pruneScanColumns(PhysicalNode node, ParallelTableScanOperator scan,
                                                 Set<String> required) {
        List<String> currentCols = node.outputColumns();
        List<String> neededCols = projectColumns(currentCols, required);
        if (isUnprunable(neededCols, currentCols, required)) return node;
        if (neededCols.size() < currentCols.size()) {
            scan.setColumns(neededCols);
            System.err.printf("[ColumnPruning] Scan %s: %d → %d columns%n",
                    node.name(), currentCols.size(), neededCols.size());
            return new PhysicalNode(node.name(), node.operator(), neededCols, List.of());
        }
        return node;
    }

    /**
     * 叶子扫描算子列裁剪（分布式扫描版）。
     *
     * <p>连接器不兑现下推时，谓词由引擎在<b>扫描节点</b>执行（{@code ConnectorScanOperator.withEngineFilter}）
     * → 谓词引用的列必须保留：少了它 Worker 侧 Filter 找不到列。多保留一列只是多读一列，不会错；
     * 少保留则整条谓词失效。</p>
     */
    private static PhysicalNode pruneScanColumns(PhysicalNode node, DistributedScanOperator dscan,
                                                 Set<String> required) {
        List<String> currentCols = node.outputColumns();
        Set<String> need = new LinkedHashSet<>(required);
        if (dscan.engineFilter()) {
            need.addAll(engineFilterColumns(dscan.predicates()));
        }
        List<String> neededCols = projectColumns(currentCols, need);
        if (isUnprunable(neededCols, currentCols, need)) return node;
        if (neededCols.size() < currentCols.size()) {
            dscan.setColumns(neededCols);
            System.err.printf("[ColumnPruning] Scan %s: %d → %d columns%n",
                    node.name(), currentCols.size(), neededCols.size());
            return new PhysicalNode(node.name(), node.operator(), neededCols, List.of());
        }
        return node;
    }

    /**
     * 扫描层不该被裁剪的两种情形 —— 直接原样返回（多带列永远安全，少带列必错）：
     *
     * <p>① {@code required} 含全列哨兵 {@code "*"}：{@code SELECT *} 的计划是 {@code Project[*] ← Scan}，
     * 哨兵按名匹配不到任何真实列 → 砍成 0 列（日志 {@code Scan ...: 7 → 0 columns}），
     * 下游 {@code Page.select} 找不到列 → 静默 0 行。这里与 {@code pruneRecursive} 插 Project 那条
     * 路径上的 {@code !need.contains("*")} 守卫同因同解。</p>
     *
     * <p>② 按名匹配结果为空（{@code required} 里的名字一个都不在扫描输出里，如仅剩派生列 / 聚合
     * 中间列的引用）：零列页会让下游算子建不出 root 抛极难定位的 {@code Operator X next failed}。</p>
     */
    private static boolean isUnprunable(List<String> neededCols, List<String> currentCols,
                                        Set<String> required) {
        return neededCols.isEmpty() && !currentCols.isEmpty() || required.contains("*");
    }

    /** 裁剪子节点后，重算父节点的实际输出列（反映子节点列数变化，避免冗余 Project 包裹）。 */
    private static List<String> computePrunedOutput(PhysicalNode node,
                                                     List<PhysicalNode> prunedChildren) {
        Operator op = node.operator();
        if (prunedChildren.size() == 1) {
            List<String> childOut = prunedChildren.get(0).outputColumns();
            if (op instanceof WindowFunctionOperator wf) {
                List<String> out = new ArrayList<>(childOut);
                for (WindowFunctionSpec spec : wf.getFunctionSpecs()) {
                    out.add(spec.outputColumn());
                }
                return out;
            }
            if (op instanceof CaseProjectOperator cp) {
                // CaseProject 输出 = 子节点列 + 派生列
                List<String> out = new ArrayList<>(childOut);
                out.addAll(cp.computedColumns());
                return out;
            }
            if (op instanceof ExpressionProjectOperator ep) {
                // ExpressionProject（分组键派生列）输出 = 子节点列 + 派生列
                List<String> out = new ArrayList<>(childOut);
                out.addAll(ep.computedColumns());
                return out;
            }
            if (op instanceof ColumnAliasOperator ca) {
                // 同名列副本算子输出 = 子节点列（原样）+ 各副本列
                List<String> out = new ArrayList<>(childOut);
                for (ColumnAliasOperator.Alias a : ca.aliases()) out.add(a.aliasName());
                return out;
            }
            if (op instanceof ExpandOperator) {
                // 多维聚合展开输出 = 上游列 + __grouping_id
                List<String> out = new ArrayList<>(childOut);
                out.add(ExpandOperator.GROUPING_ID_COLUMN);
                return out;
            }
            if (op instanceof UnnestOperator un) {
                // 行展开输出 = 上游列（截断后）+ 元素列（+ 序号列）
                List<String> out = new ArrayList<>(childOut);
                if (!out.contains(un.elementColumn())) out.add(un.elementColumn());
                if (un.ordinalityColumn() != null && !out.contains(un.ordinalityColumn())) {
                    out.add(un.ordinalityColumn());
                }
                return out;
            }
            if (op instanceof ProjectOperator proj && proj.isAliased()) {
                // 改名投影的输出列以收窄后的投影项为准（step 0 可能已按父需求收窄）
                List<String> out = new ArrayList<>(proj.refs().size());
                for (ProjectOperator.ColumnRef r : proj.refs()) out.add(r.outputName());
                return out;
            }
            // Filter / HashPartition / 溢写型透传算子（WindowSort / JoinSort 保持列不变）：透传子节点列。
            // 排除 HashAggregationOperator —— 聚合会新增输出列（keys + 各聚合 outName），透传会把
            // 输出列覆盖成子节点列，导致 as 别名丢失（count(*) as cnt → cnt 消失、SUM(CASE...) as x
            // → 输出 __case_N 派生名）。聚合节点应保留自身构建时已正确的输出列（见下方 return）。
            if (op instanceof FilterOperator || op instanceof HashPartitionOperator
                    || (op instanceof AbstractSpillableOperator && !(op instanceof HashAggregationOperator))) {
                return childOut;
            }
        }
        if (op instanceof HashJoinOperator hj) {
            List<String> leftOut = prunedChildren.get(0).outputColumns();
            List<String> rightOut = prunedChildren.get(1).outputColumns();
            List<String> out = new ArrayList<>(leftOut);
            List<String> leftKeys = hj.getLeftKeyColumns();
            List<String> rightKeys = hj.getRightKeyColumns();
            boolean dedup = hj.getJoinType() == JoinType.INNER;   // 外连接保留右键列（见 joinOutputColumns）
            for (String rc : rightOut) {
                if (dedup && rightKeys.contains(rc) && leftKeys.contains(rc)) continue;
                out.add(rc);
            }
            return out;
        }
        // Project / CrossJoin / Scan / WindowSort: 保持原有列（Project 的列由 buildProject 设定）
        return node.outputColumns();
    }

    /** 当子节点被 Project 包裹后，更新父算子中引用旧上游的地方。 */
    private static void fixOperatorUpstreams(PhysicalNode parent,
                                              List<PhysicalNode> oldChildren,
                                              List<PhysicalNode> newChildren) {
        Operator parentOp = parent.operator();
        for (int i = 0; i < oldChildren.size(); i++) {
            if (oldChildren.get(i) == newChildren.get(i)) continue;
            // 该子节点被 Project 包裹，父算子的上游引用需要更新
            Operator newUpstream = newChildren.get(i).operator();
            if (parentOp instanceof WindowFunctionOperator wf) {
                wf.getWindowSort().setUpstream(newUpstream);
            } else if (parentOp instanceof HashPartitionOperator hp) {
                hp.setUpstream(newUpstream);
            } else if (parentOp instanceof AbstractSpillableOperator aso) {
                aso.setUpstream(newUpstream);
            } else if (parentOp instanceof FilterOperator filter) {
                filter.setUpstream(newUpstream);
            } else if (parentOp instanceof ProjectOperator proj) {
                proj.setUpstream(newUpstream);
            } else if (parentOp instanceof ScalarProjectOperator sp) {
                sp.setUpstream(newUpstream);
            } else if (parentOp instanceof UnnestOperator un) {
                un.setUpstream(newUpstream);
            } else if (parentOp instanceof ColumnAliasOperator ca) {
                ca.setUpstream(newUpstream);
            } else if (parentOp instanceof CaseProjectOperator cp) {
                cp.setUpstream(newUpstream);
            } else if (parentOp instanceof ExpressionProjectOperator ep) {
                ep.setUpstream(newUpstream);
            } else if (parentOp instanceof ExpandOperator ex) {
                ex.setUpstream(newUpstream);
            } else if (parentOp instanceof PartialAggregationMergeOperator merge) {
                merge.setUpstream(newUpstream);
            } else if (parentOp instanceof UnionAllOperator ua) {
                // 某条 UNION 腿被 Project 包裹/折叠后，把 UnionAll 的第 i 个孩子引用换新
                ua.replaceChild(i, newUpstream);
            }
            // HashJoinOperator / CrossJoinOperator 的子节点是 HashPartition，
            // Project 包裹 HashPartition 会破坏 join 对 partitionQueue() 的访问，
            // 因此不会走到这里（剪枝在 HashPartition 的子节点上进行）
        }
    }

    /** 计算当前节点的每个子节点需要产生哪些列。 */
    private List<Set<String>> computeChildRequirements(PhysicalNode node,
                                                        Set<String> parentRequired) {
        Operator op = node.operator();
        List<PhysicalNode> children = node.children();
        if (children.isEmpty()) return List.of();

        if (op instanceof WindowFunctionOperator winFunc) {
            WindowSortOperator sort = winFunc.getWindowSort();
            Set<String> need = new LinkedHashSet<>(parentRequired);
            // 排除窗口函数输出列（由于节点产生的列，子节点不包含）
            for (WindowFunctionSpec spec : winFunc.getFunctionSpecs()) {
                need.remove(spec.outputColumn());
            }
            // PARTITION BY 列
            need.addAll(sort.getGroupKeyColumns());
            // ORDER BY 列
            need.addAll(sort.getOrderKeyColumns());
            // 函数输入列（如 sum(quantity) 需要 quantity）
            for (WindowFunctionSpec spec : winFunc.getFunctionSpecs()) {
                String inputCol = spec.inputColumn();
                if (inputCol != null && !inputCol.isEmpty()) need.add(inputCol);
            }
            return List.of(need);
        }

        if (op instanceof FilterOperator filter) {
            Set<String> need = new LinkedHashSet<>(parentRequired);
            // 谓词引用列取自 FilterOperator 构造期的编译结果（原文猜测会漏列，见 referencedColumns）
            for (String col : filter.referencedColumns()) {
                need.add(col);
            }
            // 兜底：Operator 认不出的谓词形态（Form.UNKNOWN）仍按原文取一个 token。
            // 多登记的列名只是多保留一列（不会误裁），少登记才会把上游裁空 —— 故宁多勿少。
            for (String col : extractFilterColumns(filter.filterExpression())) {
                need.add(col);
            }
            return List.of(need);
        }

        if (op instanceof HashJoinOperator) {
            List<String> leftOut = children.get(0).outputColumns();
            List<String> rightOut = children.get(1).outputColumns();

            Set<String> leftNeed = new LinkedHashSet<>();
            Set<String> rightNeed = new LinkedHashSet<>();
            // 两侧存在同名列时不能只喂左侧：join 输出按「左列 + 右列」顺序拼接，
            // 上层改名投影按「第 k 次出现」定位同名列（如 `a.amount, b.amount AS b_amount`
            // 的第二项定位 amount#2）。只保留左侧会让右侧同名列被裁掉 ⇒ 出现序号落空、
            // 按「复用最后一个同名列」的兜底静默绑到左列（压测 D2 的 b_amount 取成了 a.amount）。
            // 多保留一列只是多读一列，少保留则结果错。
            for (String col : parentRequired) {
                if (leftOut.contains(col)) leftNeed.add(col);
                if (rightOut.contains(col)) rightNeed.add(col);
            }
            // join key 列必须保留
            for (String k : ((HashJoinOperator) op).getLeftKeyColumns()) leftNeed.add(k);
            for (String k : ((HashJoinOperator) op).getRightKeyColumns()) rightNeed.add(k);
            return List.of(leftNeed, rightNeed);
        }

        if (op instanceof CrossJoinOperator) {
            List<String> leftOut = children.get(0).outputColumns();
            List<String> rightOut = children.get(1).outputColumns();
            Set<String> leftNeed = new LinkedHashSet<>();
            Set<String> rightNeed = new LinkedHashSet<>();
            // 同 HashJoin：两侧同名列都要保留（输出按位置拼接，上层按出现序号取列）
            for (String col : parentRequired) {
                if (leftOut.contains(col)) leftNeed.add(col);
                if (rightOut.contains(col)) rightNeed.add(col);
            }
            return List.of(leftNeed, rightNeed);
        }

        if (op instanceof HashPartitionOperator) {
            Set<String> need = new LinkedHashSet<>(parentRequired);
            // 分区键列必须保留
            for (String k : ((HashPartitionOperator) op).getPartitionKeys()) need.add(k);
            return List.of(need);
        }

        if (op instanceof ScalarProjectOperator sp) {
            // 计算列由 ScalarProject 自身产生（子节点不含）→ 移除；
            // 标量表达式引用的上游列必须保留
            Set<String> need = new LinkedHashSet<>(parentRequired);
            need.removeAll(sp.computedColumns());
            need.addAll(sp.expressionColumns());
            // ★透传列也必须保留：ScalarProject 运行期按<b>自己固定的</b> outputColumns 逐列从输入页
            // 拷（doNext 第 2 步），父节点没要求 ≠ 可以裁 —— 少了就抛「标量投影透传列不存在」。
            // 触发场景：HAVING 一般布尔谓词（tryBuildGeneralHaving）插在派生表内层时，外层只按名要
            // 分组键，改名副本列 + 无别名聚合列会被裁掉（H42：sum(amount)/count(*) > 20）。
            // 多带几列永远安全（最终投影会把它们丢掉）。
            for (String c : sp.outputColumns()) {
                if (!sp.computedColumns().contains(c)) need.add(c);
            }
            return List.of(need);
        }

        if (op instanceof CaseProjectOperator cp) {
            // 派生列由 CaseProject 自身产生（子节点不含）→ 移除；
            // CASE 条件引用的上游列必须保留
            Set<String> need = new LinkedHashSet<>(parentRequired);
            need.removeAll(cp.computedColumns());
            need.addAll(cp.conditionColumns());
            return List.of(need);
        }

        if (op instanceof ExpressionProjectOperator ep) {
            // 分组键派生列由 ExpressionProject 自身产生（子节点不含）→ 移除；
            // 表达式引用的上游列必须保留（否则扫描会裁掉源列 → 求值 findField 失败）
            Set<String> need = new LinkedHashSet<>(parentRequired);
            need.removeAll(ep.computedColumns());
            need.addAll(ep.sourceColumns());
            return List.of(need);
        }

        if (op instanceof ExpandOperator ex) {
            // __grouping_id 由本算子自身产生（子节点不含）→ 移除；维度列必须保留（掩码按名定位）
            Set<String> need = new LinkedHashSet<>(parentRequired);
            need.remove(ExpandOperator.GROUPING_ID_COLUMN);
            need.addAll(ex.dimensionColumns());
            return List.of(need);
        }

        if (op instanceof ColumnAliasOperator ca) {
            // 副本列由本算子自身产生（子节点不含）→ 移除；副本的源列必须保留
            // （同名列的所有出现都保留 —— 副本按出现序号定位，少一个就会错位）。
            Set<String> need = new LinkedHashSet<>(parentRequired);
            for (ColumnAliasOperator.Alias a : ca.aliases()) need.remove(a.aliasName());
            for (ColumnAliasOperator.Alias a : ca.aliases()) need.add(a.sourceName());
            return List.of(need);
        }

        if (op instanceof UnnestOperator un) {
            // 元素 / 序号列由本算子自身产生（子节点不含）→ 移除；
            // 展开表达式引用的上游列必须保留（否则被裁掉 → 运行期求值找不到列）。
            Set<String> need = new LinkedHashSet<>(parentRequired);
            need.remove(un.elementColumn());
            if (un.ordinalityColumn() != null) need.remove(un.ordinalityColumn());
            need.addAll(un.expressionColumns());
            return List.of(need);
        }

        if (op instanceof ProjectOperator proj) {
            // 投影的输出列名可能与源列名不同（改名投影）→ 按输出名映射回源列名，
            // 不能直接把 parentRequired（输出名）当子节点需求。
            Set<String> need = new LinkedHashSet<>();
            List<ProjectOperator.ColumnRef> refs = proj.refs();
            if (refs == null) {
                for (String c : proj.columns()) {
                    if (parentRequired.contains(c)) need.add(c);
                }
            } else {
                for (ProjectOperator.ColumnRef r : refs) {
                    if (parentRequired.contains(r.outputName())) need.add(r.sourceName());
                }
            }
            return List.of(need);
        }

        if (op instanceof PartialAggregationMergeOperator) {
            // 子节点（分布式聚合）回传的列名 = 二级聚合输出列，不能用父需求（输出列名相同但语义
            // 不同）去裁它；要求其全部输出列 → 不插 Project（插入的 Project 会把 partial 页按错列名取列）。
            return List.of(new LinkedHashSet<>(children.get(0).outputColumns()));
        }

        if (op instanceof HashAggregationOperator agg) {
            // 聚合输入列 + 分组键必须保留（聚合节点输出列不含输入列，默认分支会剪掉它们）。
            // collect-evaluate 型 inputColumn 为 null（列由 fn.inputColumns() 声明），必须显式保留，
            // 否则列裁剪会裁掉攒值列 → 分布式扫描 / 本地聚合都取不到输入 → 运行期 findField 失败。
            Set<String> need = new LinkedHashSet<>(parentRequired);
            for (String k : agg.getKeyColumns()) need.add(k);
            for (AggregationSpec s : agg.getAggregations()) {
                if (s.isCollect()) {
                    need.addAll(s.collectInputColumns());
                } else if (s.inputColumn() != null) {
                    need.add(s.inputColumn());
                }
            }
            return List.of(need);
        }

        if (op instanceof UnionAllOperator ua) {
            // UNION 各腿按位置对齐拼接：每腿必须产出自身的全部投影列（不做跨腿按名裁剪，
            // 否则会裁掉某腿独有的列名 → UnionAllOperator 对齐时列名解析落空）。
            List<Set<String>> req = new ArrayList<>(children.size());
            for (PhysicalNode c : children) {
                req.add(new LinkedHashSet<>(c.outputColumns()));
            }
            return req;
        }

        // 默认：传递所有需要的列
        return children.stream()
                .map(c -> new LinkedHashSet<>(parentRequired))
                .collect(Collectors.toList());
    }

    private static boolean childHasMoreThan(List<String> childOut, Set<String> needed) {
        for (String col : childOut) {
            if (!needed.contains(col)) return true;
        }
        return false;
    }

    /** 保留 childOut 中出现在 needed 里的列，顺行保留原有顺序。 */
    private static List<String> projectColumns(List<String> childOut, Set<String> needed) {
        List<String> cols = new ArrayList<>();
        for (String col : childOut) {
            if (needed.contains(col)) cols.add(col);
        }
        return cols;
    }

    private static PhysicalNode wrapProject(PhysicalNode upstream, List<String> columns) {
        ProjectOperator proj = new ProjectOperator(upstream.operator(), columns);
        return new PhysicalNode("Project", proj, columns, upstream);
    }

    /**
     * 基表扫描列集 = 语法层裁剪列 ∪ <b>引擎侧过滤所引用的列</b>。
     *
     * <p><b>为什么必须补</b>：连接器不兑现下推（{@link DojaoConnector#supportsPredicatePushdown()} = false，
     * 如 Hive）时，Worker 在扫描节点之上补一个 {@code FilterOperator} 执行该谓词（见
     * {@code ConnectorScanOperator.withEngineFilter}），而它按<b>列名</b>取向量、<b>缺列即逐行恒假</b>
     * ⇒ <b>静默丢光所有行</b>。实测：Hive 侧读 2200 万行、分发 0 行、join 结果空集，全程无报错。
     * 与单表聚合（{@code tryBuildDistributedAggregation}）/ 窗口（{@code tryBuildDistributedWindow}）/
     * {@code optimizeColumns} 三处既有做法一致 —— 那条 join 家族此前漏了这一补，正是空集的直接原因。</p>
     *
     * <p>列集为 null/空 = 「读全列」（本就有谓词列）⇒ <b>原样返回</b>，绝不改成只读谓词列。</p>
     */
    private static List<String> scanColumns(List<String> columns, DojaoConnector probe, String filter) {
        if (columns == null || columns.isEmpty() || probe == null || filter == null || filter.isBlank()) {
            return columns;
        }
        // 引擎侧真正要<b>求值</b>的那部分谓词：不兑现下推的连接器 → 整串；兑现的连接器 → 只剩
        // 引擎注册函数谓词（数据源求不了，扫描节点补 Filter，见 ConnectorScanOperator.openWithEngineFilter）。
        String enginePart = probe.supportsPredicatePushdown()
                ? splitScanPredicate(filter).residual()
                : filter;
        if (enginePart == null || enginePart.isBlank()) {
            return columns;   // 数据源全包了 → 无需为过滤多读列（列裁剪保持最紧）
        }
        List<String> cols = new ArrayList<>(columns);
        for (String c : engineFilterColumns(List.of(enginePart))) {
            if (!cols.contains(c)) {
                cols.add(c);
            }
        }
        return cols;
    }

    /**
     * 引擎侧执行的谓词（连接器不兑现下推 → Worker 扫描节点补 {@code Filter}）所引用的列。
     *
     * <p>与 Worker 端<b>完全同一套拆解</b>：先 {@link SqlTextUtil#splitConjuncts} 拆顶层 AND、再逐项问
     * {@link FilterOperator#referencedColumnsOf}（编译结果驱动，不做「第一个空格前的 token」猜测）——
     * 保证「保留的读取列」与「运行时真正要读的列」严格一致。少保留一列 = 谓词整体失效。</p>
     */
    private static List<String> engineFilterColumns(List<String> predicates) {
        List<String> cols = new ArrayList<>(4);
        for (String p : predicates) {
            for (String cond : SqlTextUtil.splitConjuncts(p)) {
                for (String c : FilterOperator.referencedColumnsOf(cond)) {
                    if (!cols.contains(c)) {
                        cols.add(c);
                    }
                }
            }
        }
        return cols;
    }

    /** 从简单过滤表达式中提取列名。 */
    /** 过滤谓词引用的列：函数谓词返回其标量函数参数列（字面量参数无列需求）；否则取第一个空白前的列名。 */
    private static List<String> extractFilterColumns(String expr) {
        if (expr == null) return List.of();
        if (ScalarExpr.isFunctionPredicate(expr)) {
            return ScalarExpr.predicateColumnArgs(expr);
        }
        String e = expr.trim();
        // "col IS NOT NULL" 或 "col > 5" 等
        int space = e.indexOf(' ');
        String col = space > 0 ? e.substring(0, space) : e;
        return col.isEmpty() ? List.of() : List.of(col);
    }

    private static String normalize(String s) {
        return s.toLowerCase(Locale.ROOT);
    }

    private static void requireColumn(PhysicalNode node, String col, String what) {
        if (!node.hasColumn(col)) {
            throw new UnsupportedOperationException(
                    "Dojao 物理规划: 找不到 " + what + " '" + col
                            + "'，可用列: " + node.outputColumns());
        }
    }

    /**
     * 窗口键（PARTITION BY / ORDER BY）必须能解析为上游列。派生表别名（{@code t.v}）在
     * 去限定后找不到列时给出明确的「别名解析失败」，而不是在运行期抛裸异常。
     */
    private static void requireWindowColumn(PhysicalNode node, String col, String what, String original) {
        if (node.hasColumn(col)) return;
        throw new UnsupportedOperationException(
                "Dojao 物理规划: 窗口 " + what + " 别名解析失败: '" + original.trim()
                        + "'（解析为列 '" + col + "'，上游可用列: " + node.outputColumns() + "）");
    }

    /**
     * LAG / LEAD 偏移量：必须是非负整数字面量。
     *
     * <p>负数此前被原样下发到 Worker：{@code WindowFunctionOperator} 用 {@code new Object[off]}
     * 建环形缓冲，直接抛 {@code NegativeArraySizeException: -1}（任务失败/查询侧长时间等待）；
     * 因此这里在<b>规划期</b>明确拒绝。非整数字面量同样报错，不再静默按 1 处理。</p>
     */
    private static int parseLagLeadOffset(String fn, String text) {
        String t = text.trim();
        int off;
        try {
            off = Integer.parseInt(t);
        } catch (NumberFormatException e) {
            throw new UnsupportedOperationException(
                    "Dojao 物理规划: " + fn + " 偏移量必须是非负整数字面量: '" + t + "'");
        }
        if (off < 0) {
            throw new UnsupportedOperationException(
                    "Dojao 物理规划: " + fn + " 偏移量不能为负数: " + t);
        }
        return off;
    }

    private static Object literalValue(String s) {
        String t = s.trim();
        if (t.startsWith("'") && t.endsWith("'") && t.length() >= 2) {
            return t.substring(1, t.length() - 1);
        }
        try {
            return Long.parseLong(t);
        } catch (NumberFormatException ignored) {}
        try {
            return Double.parseDouble(t);
        } catch (NumberFormatException ignored) {}
        return null;
    }

    private static UnsupportedOperationException unsupported(String what) {
        return new UnsupportedOperationException("Dojao 物理规划暂不支持: " + what);
    }
}
