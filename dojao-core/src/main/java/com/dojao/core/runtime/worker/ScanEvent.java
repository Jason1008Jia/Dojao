package com.dojao.core.runtime.worker;

import com.dojao.common.data.Page;

/**
 * 扫描回传事件 —— Worker → Master 数据面的信封，携带「数据 / 成功 / 失败」三种终止语义。
 *
 * <p>取代 v1 的「{@link Page} + {@link Page#eof()} 结束标记」：EOF 语义从内容标记升级为
 * 显式事件，Master 算子据此区分「分区扫完（{@link ScanFinished}）」与「分区失败（{@link ScanFailed}）」，
 * 失败即可 fail-fast，杜绝静默残缺数据。</p>
 *
 * <p>每种事件都携带 {@code taskId}：Master 据此精确更新对应分区任务的进度（喂看门狗）与状态。
 * 失败事件不受背压限制（见 {@link WorkerGateway} 回传契约）。</p>
 *
 * @author jiaxin
 * @version 1.1
 * @date 2026/8/12
 */
public sealed interface ScanEvent permits ScanEvent.ScanPage, ScanEvent.ScanFinished, ScanEvent.ScanFailed {

    /** 数据页：taskId 标明来源分区。 */
    record ScanPage(String taskId, Page page) implements ScanEvent {
    }

    /**
     * 分区任务正常扫完（对应 v1 的 EOF 标记，语义更明确）。
     *
     * <p>{@code rows}：本分区<b>写入目标表</b>的行数（scan-sink 就地写路径经 FINISHED metadata
     * 回传；普通扫描回传路径无行数语义，恒 0）。</p>
     */
    record ScanFinished(String taskId, long rows) implements ScanEvent {
        /** 普通扫描（数据页回传）完成：无写行数。 */
        public ScanFinished(String taskId) {
            this(taskId, 0L);
        }
    }

    /** 分区任务失败：cause 为异常摘要，Master 据此 fail-fast（后续可据此重试）。 */
    record ScanFailed(String taskId, String cause) implements ScanEvent {
    }
}
