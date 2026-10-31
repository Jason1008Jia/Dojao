package com.dojao.core.function.scalar.crypto;

import com.dojao.core.function.scalar.ScalarFunctionRegistry;
import com.dojao.core.function.scalar.ScalarSupport;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.UUID;
import java.util.function.Function;
import java.util.zip.CRC32;

/**
 * Hive 标准加密/摘要标量函数。摘要类输入为字符串（按 UTF-8 取字节）或二进制；输出小写十六进制文本。
 *
 * @author jiaxin
 * @date 2026/10/11
 */
public final class CryptoFunctions {

    private CryptoFunctions() {
    }

    public static void registerAll() {
        reg("md5", "MD5 摘要（小写十六进制）", args -> {
            ScalarSupport.requireArity("md5", args, 1);
            return args[0] == null ? null : digest("MD5", bytes(args[0]));
        });
        reg("sha1", "SHA-1 摘要（小写十六进制）", args -> {
            ScalarSupport.requireArity("sha1", args, 1);
            return args[0] == null ? null : digest("SHA-1", bytes(args[0]));
        });
        reg("sha2", "SHA-2 摘要 sha2(s,bits)（bits∈224/256/384/512）", args -> {
            ScalarSupport.requireArity("sha2", args, 2);
            if (args[0] == null || args[1] == null) return null;
            int bits = ScalarSupport.asInt(args[1]);
            String alg = switch (bits) {
                case 224 -> "SHA-224";
                case 256 -> "SHA-256";
                case 384 -> "SHA-384";
                case 512 -> "SHA-512";
                default -> throw new IllegalArgumentException("sha2 位数仅支持 224/256/384/512，实得 " + bits);
            };
            return digest(alg, bytes(args[0]));
        });
        reg("crc32", "CRC32 校验值（无符号整型）", args -> {
            ScalarSupport.requireArity("crc32", args, 1);
            if (args[0] == null) return null;
            CRC32 crc = new CRC32();
            crc.update(bytes(args[0]));
            return crc.getValue();
        });
        reg("uuid", "随机 UUID 字符串", args -> {
            ScalarSupport.requireArity("uuid", args, 0);
            return UUID.randomUUID().toString();
        });
        reg("hash", "组合哈希 hash(a,b,...)（Java hashCode 组合 → long）", args -> {
            ScalarSupport.requireAtLeast("hash", args, 1);
            int h = 0;
            for (Object a : args) {
                h = h * 31 + argHash(a);
            }
            return (long) h;
        });
    }

    private static void reg(String name, String display, Function<Object[], Object> impl) {
        ScalarFunctionRegistry.register(ScalarSupport.fn(name, display, impl));
    }

    private static byte[] bytes(Object v) {
        return v instanceof byte[] b ? b : ScalarSupport.string(v).getBytes(StandardCharsets.UTF_8);
    }

    private static int argHash(Object a) {
        if (a == null) return 0;
        if (a instanceof Number) return Double.hashCode(ScalarSupport.asDouble(a));
        return ScalarSupport.string(a).hashCode();
    }

    private static String digest(String algorithm, byte[] data) {
        try {
            byte[] out = MessageDigest.getInstance(algorithm).digest(data);
            StringBuilder sb = new StringBuilder(out.length * 2);
            for (byte b : out) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString().toLowerCase(Locale.ROOT);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM 不支持摘要算法: " + algorithm, e);
        }
    }
}
