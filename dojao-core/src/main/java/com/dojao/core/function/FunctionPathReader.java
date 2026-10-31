package com.dojao.core.function;

import java.util.Set;

/**
 * 函数代码路径读取器 —— 把 {@code code_path}（{@code file://} / {@code shared://} /
 * {@code http(s)://} / {@code hdfs://} / {@code s3://} 或裸路径）统一解析成后端能消费的本地路径。
 *
 * <p><b>轻量原则</b>：不引 hadoop / aws SDK。hdfs 走 WebHDFS REST，s3 走 aws/mc CLI 壳
 * 或预签名 {@code https://} URL（走 http 读取器），全部用 JDK 内置能力实现，零新增依赖。</p>
 *
 * <p><b>扩展</b>：实现本接口并注册进 {@link PathReaders}（或 {@code META-INF/services}
 * 自动发现），即可支持任意新的存储介质 —— 引擎与后端零改动（多语言/多介质拓展外缘）。</p>
 *
 * @see PathReaders
 * @see ResolvedCode
 */
public interface FunctionPathReader {

    /** 支持的 URI scheme（不含 "://"，如 "file"/"shared"/"http"/"hdfs"/"s3"）。 */
    Set<String> schemes();

    /**
     * 把 codePath 解析成本地可消费路径。
     *
     * <p>本地/共享 → 直通；远程（http/hdfs/s3）→ 下载到本地缓存目录并返回缓存路径。
     * 路径不存在或下载失败 → 抛异常（fail-fast，CREATE 冒烟校验时透出，不落库）。</p>
     */
    ResolvedCode resolve(String codePath);
}
