package com.dojao.ui.auth;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

/**
 * 口令哈希 —— PBKDF2WithHmacSHA256（JDK 自带，零第三方依赖）。
 *
 * <p>存储格式 {@code pbkdf2$<iterations>$<saltB64>$<hashB64>}：迭代数与盐随哈希一起存，
 * 将来调参不会让老口令失效。校验用 {@link MessageDigest#isEqual} 做常量时间比较，
 * 避免按比较耗时侧信道推测口令。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/9/17
 */
final class PasswordHasher {

    private static final String ALGORITHM = "PBKDF2WithHmacSHA256";
    private static final String PREFIX = "pbkdf2";

    /** OWASP 2023 对 PBKDF2-HMAC-SHA256 的建议下限。 */
    private static final int ITERATIONS = 210_000;
    private static final int SALT_BYTES = 16;
    private static final int KEY_BITS = 256;

    private static final SecureRandom RANDOM = new SecureRandom();

    private PasswordHasher() {
    }

    /** 生成 {@code pbkdf2$iter$salt$hash} 形式的可存储字符串。 */
    static String hash(String password) {
        byte[] salt = new byte[SALT_BYTES];
        RANDOM.nextBytes(salt);
        byte[] key = pbkdf2(password, salt, ITERATIONS);
        Base64.Encoder b64 = Base64.getEncoder();
        return PREFIX + '$' + ITERATIONS + '$' + b64.encodeToString(salt) + '$' + b64.encodeToString(key);
    }

    /** 校验明文口令与存储串是否匹配；任何格式异常一律返回 false（不抛）。 */
    static boolean verify(String password, String encoded) {
        if (password == null || encoded == null) {
            return false;
        }
        String[] parts = encoded.split("\\$");
        if (parts.length != 4 || !PREFIX.equals(parts[0])) {
            return false;
        }
        try {
            int iterations = Integer.parseInt(parts[1]);
            byte[] salt = Base64.getDecoder().decode(parts[2]);
            byte[] expected = Base64.getDecoder().decode(parts[3]);
            return MessageDigest.isEqual(expected, pbkdf2(password, salt, iterations));
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static byte[] pbkdf2(String password, byte[] salt, int iterations) {
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, KEY_BITS);
        try {
            return SecretKeyFactory.getInstance(ALGORITHM).generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException("PBKDF2 不可用: " + ALGORITHM, e);
        } finally {
            spec.clearPassword();
        }
    }
}
