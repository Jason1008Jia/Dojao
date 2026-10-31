package com.dojao.common.sql;

import java.util.List;
import java.util.Objects;

/**
 * 批量 SQL 的<b>串行</b>执行器：按列表顺序一条一条执行，前一条结束后才开始下一条。
 *
 * <p>语义与脚本执行一致：<b>任一条抛异常即停止</b>（fail-fast，后续语句不再执行）；
 * 取消只在两条语句<b>之间</b>生效（正在执行的那条由调用方的执行器自行中断）。</p>
 *
 * @author jiaxin
 * @date 2026/10/18
 */
public final class SqlBatchRunner {

    /** 单条语句执行器；{@code index} 从 0 开始，{@code total} 为总条数。 */
    @FunctionalInterface
    public interface StatementExecutor {
        void execute(String statement, int index, int total) throws Exception;
    }

    /** 取消判据（可为 null，表示不检查取消）。 */
    @FunctionalInterface
    public interface Cancellation {
        boolean isCanceled();
    }

    private SqlBatchRunner() {}

    /**
     * 串行执行全部语句。
     *
     * @return 实际执行的语句条数（取消 / 失败时小于 {@code statements.size()}）
     * @throws Exception 某条语句抛出的原异常（调用方据 {@code index} 定位失败语句）
     */
    public static int runAll(List<String> statements, StatementExecutor executor, Cancellation cancellation)
            throws Exception {
        Objects.requireNonNull(statements, "statements");
        Objects.requireNonNull(executor, "executor");
        int total = statements.size();
        int executed = 0;
        for (int i = 0; i < total; i++) {
            if (cancellation != null && cancellation.isCanceled()) {
                break;
            }
            executor.execute(statements.get(i), i, total);
            executed++;
        }
        return executed;
    }
}
