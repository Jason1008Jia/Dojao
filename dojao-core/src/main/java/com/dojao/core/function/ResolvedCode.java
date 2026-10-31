package com.dojao.core.function;

/**
 * {@code code_path} 的解析结果。
 *
 * @param origin     原始 code_path
 * @param localPath  本地可消费路径（文件或目录，绝对路径）
 * @param temporary  true = 读取器下载到缓存目录（理论上可清理）；false = 用户本地/共享路径（不要动）
 */
public record ResolvedCode(String origin, String localPath, boolean temporary) {
}
