package com.dojao.core.function;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;

/**
 * 函数代码读取器共用的错误分类工具。
 *
 * <p>网络故障（目标地址与当前集群网络不互通、连接超时、被拒、DNS 解析失败）要
 * 与一般 IO 错误区分开——用户提供的 {@code hdfs://} / {@code s3://} / {@code https://}
 * 地址若不在当前集群可达的网络内，应当报出"网络不可达/超时"这类明确的错，并透出
 * 原始异常，而不是含糊的"下载失败"。</p>
 */
final class FunctionPathErrors {

    private FunctionPathErrors() {
    }

    /** IOException 是否源自"远端不可达 / 网络超时 / DNS 解析失败"。 */
    static boolean isNetworkUnreachable(IOException e) {
        Throwable t = e;
        while (t != null) {
            if (t instanceof ConnectException
                    || t instanceof HttpTimeoutException
                    || t instanceof SocketTimeoutException
                    || t instanceof NoRouteToHostException
                    || t instanceof UnknownHostException) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /** 网络不可达时抛出的错误（统一口径，把"哪一步 + 谁不可达"说清楚）。 */
    static IllegalArgumentException networkUnreachable(String display, String host, IOException e) {
        String hostPart = (host == null || host.isBlank()) ? "" : "（目标主机 " + host + "）";
        return new IllegalArgumentException(
                "无法读取用户指定的函数代码 " + display + "：网络不可达或连接超时" + hostPart
                        + "。用户提供的地址与当前集群网络可能不在同一网络，或目标服务未开启对应端口。"
                        + "原始异常: " + e.getClass().getSimpleName() + ": " + e.getMessage());
    }
}
