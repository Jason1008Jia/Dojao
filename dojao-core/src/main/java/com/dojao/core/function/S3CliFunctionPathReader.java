package com.dojao.core.function;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * S3 读取器 —— 轻量壳，零 SDK：优先 shell 调 {@code aws s3 cp} / {@code mc cp}
 * 下载到本地缓存；CLI 都没有或命令失败 → 报错，提示改用预签名 {@code https://} URL
 * （走 {@link HttpFunctionPathReader}）。
 *
 * <p>仅支持单文件下载；目录请 {@code tar} 后走单文件，或改用预签名对象 URL。</p>
 *
 * <p>错误分类：用户提供的 S3 地址若与当前集群网络不互通（连接超时/被拒），会把 CLI
 * 输出透出来并明确提示"网络不可达"，而不是含糊的"下载失败"；无权限/桶不存在也单独成句。</p>
 */
public final class S3CliFunctionPathReader implements FunctionPathReader {

    @Override
    public Set<String> schemes() {
        return Set.of("s3");
    }

    @Override
    public ResolvedCode resolve(String codePath) {
        Path dest = FunctionCodeCache.path(codePath, false);
        if (FunctionCodeCache.cached(dest)) {
            return new ResolvedCode(codePath, dest.toString(), true);
        }
        String errAws = run(new String[]{"aws", "s3", "cp"}, codePath, dest);
        if (errAws == null) {
            return new ResolvedCode(codePath, dest.toString(), true);
        }
        String errMc = run(new String[]{"mc", "cp"}, codePath, dest);
        if (errMc == null) {
            return new ResolvedCode(codePath, dest.toString(), true);
        }
        throw new IllegalStateException(
                "s3:// 下载失败，aws 与 mc 都无法读取 " + codePath + "：\n"
                        + "  aws: " + errAws + "\n"
                        + "  mc:  " + errMc + "\n"
                        + "请确认：① 该 S3 地址与当前集群网络互通；② 桶/对象存在且本机有读取权限"
                        + "（aws configure / mc alias set 已配置）；③ 或改用预签名 https:// URL（走 http 读取器）。");
    }

    /**
     * 执行一次下载。成功 → null；失败 → 按 CLI 输出关键词分类的提示。
     *
     * <p>stdout/stderr 合并重定向到临时日志文件（避免管道写满阻塞），失败时读回日志透出原因。</p>
     */
    private String run(String[] cmd, String src, Path dst) {
        Path log;
        try {
            log = Files.createTempFile("dojao-s3-", ".log");
        } catch (IOException e) {
            throw new IllegalStateException("创建 S3 下载日志文件失败", e);
        }
        try {
            Process p = new ProcessBuilder(cmd[0], cmd[1], cmd[2], src, dst.toString())
                    .redirectErrorStream(true)
                    .redirectOutput(log.toFile())
                    .start();
            boolean done = p.waitFor(3, TimeUnit.MINUTES);
            String output;
            try {
                output = Files.readString(log, StandardCharsets.UTF_8);
            } catch (IOException e) {
                output = "";
            }
            if (!done) {
                p.destroyForcibly();
                return classify(cmd[0], src, "执行超时（3 分钟）", output);
            }
            if (p.exitValue() != 0) {
                Files.deleteIfExists(dst);
                return classify(cmd[0], src, "命令退出码 " + p.exitValue(), output);
            }
            if (!Files.isRegularFile(dst) || Files.size(dst) == 0) {
                Files.deleteIfExists(dst);
                return classify(cmd[0], src, "产物为空", output);
            }
            return null;
        } catch (IOException e) {
            return classify(cmd[0], src, "命令不存在或启动失败", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return classify(cmd[0], src, "下载被中断", e.getMessage());
        } finally {
            try {
                Files.deleteIfExists(log);
            } catch (IOException ignored) {
            }
        }
    }

    /** 按 CLI 输出关键词分类失败：网络不可达 / 无权限 / 桶或对象不存在 / 其他。 */
    private static String classify(String cli, String src, String failStage, String output) {
        String o = output == null ? "" : output.trim();
        String lower = o.toLowerCase(Locale.ROOT);
        if (lower.contains("could not connect") || lower.contains("connection refused")
                || lower.contains("connect timed out") || lower.contains("timed out")
                || lower.contains("timeout") || lower.contains("no such host")
                || lower.contains("network is unreachable") || lower.contains("endpoint url")
                || lower.contains("unable to resolve host")) {
            return "[" + cli + "] " + src + " 网络不可达/连接超时——用户提供的 S3 地址与当前集群网络可能不互通。CLI 输出: " + o;
        }
        if (lower.contains("accessdenied") || lower.contains("access denied")
                || lower.contains("forbidden") || lower.contains("403") || lower.contains("401")
                || lower.contains("unable to locate credentials") || lower.contains("no credentials")) {
            return "[" + cli + "] " + src + " 无权限或凭据缺失。CLI 输出: " + o;
        }
        if (lower.contains("no such bucket") || lower.contains("nosuchbucket")
                || lower.contains("no such key") || lower.contains("not exist") || lower.contains("404")) {
            return "[" + cli + "] " + src + " 桶或对象不存在。CLI 输出: " + o;
        }
        return "[" + cli + "] " + src + " " + failStage + "。CLI 输出: " + o;
    }
}
