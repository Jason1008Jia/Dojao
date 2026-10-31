package com.dojao.core.function.scalar.math;

import com.dojao.core.function.scalar.ScalarFunctionRegistry;
import com.dojao.core.function.scalar.ScalarSupport;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Hive 标准数学标量函数（{@code abs/ceil/floor/round} 见 {@code ExprBuiltins}，此处不重复）。
 *
 * <p>数值函数恒返回 {@link Double}（{@code sign} 值域 -1.0/0.0/1.0）；整数类（{@code bin/hex/factorial}）
 * 返回 {@link Long} / {@link String}。每个函数所有非空返回同类 —— 输出列类型由 {@code ScalarEvaluator}
 * 按首个非空结果推断，异类行会抛 {@code ClassCastException}。</p>
 *
 * @author jiaxin
 * @date 2026/10/11
 */
public final class MathFunctions {

    private MathFunctions() {
    }

    public static void registerAll() {
        reg("rand", "随机 double [0,1)；rand(seed) 用固定种子（每行同值，非 Hive 逐行序列）", args -> {
            ScalarSupport.requireArity("rand", args, 0, 1);
            if (args.length == 0) return ThreadLocalRandom.current().nextDouble();
            return new Random(ScalarSupport.asLong(args[0])).nextDouble();
        });
        reg("pow", "幂运算 pow(x,y)=x^y", args -> {
            ScalarSupport.requireArity("pow", args, 2);
            if (args[0] == null || args[1] == null) return null;
            return Math.pow(ScalarSupport.asDouble(args[0]), ScalarSupport.asDouble(args[1]));
        });
        reg("power", "幂运算 power(x,y)=x^y", args -> {
            ScalarSupport.requireArity("power", args, 2);
            if (args[0] == null || args[1] == null) return null;
            return Math.pow(ScalarSupport.asDouble(args[0]), ScalarSupport.asDouble(args[1]));
        });
        reg("sqrt", "平方根（负数抛异常）", args -> {
            ScalarSupport.requireArity("sqrt", args, 1);
            if (args[0] == null) return null;
            double x = ScalarSupport.asDouble(args[0]);
            if (x < 0) throw new IllegalArgumentException("sqrt 参数不能为负: " + x);
            return Math.sqrt(x);
        });
        reg("exp", "自然指数 e^x", args -> {
            ScalarSupport.requireArity("exp", args, 1);
            return args[0] == null ? null : Math.exp(ScalarSupport.asDouble(args[0]));
        });
        reg("ln", "自然对数（≤0 抛异常）", args -> {
            ScalarSupport.requireArity("ln", args, 1);
            return logBase(args[0], Math.E, "ln");
        });
        reg("log", "对数：log(x)=ln(x)，log(base,x)=以 base 为底（≤0 抛异常）", args -> {
            ScalarSupport.requireArity("log", args, 1, 2);
            if (args.length == 1) return logBase(args[0], Math.E, "log");
            Object base = args[0] == null || args[1] == null ? null : args[0];
            if (base == null) return null;
            double b = ScalarSupport.asDouble(base);
            double x = ScalarSupport.asDouble(args[1]);
            if (b <= 0 || x <= 0) throw new IllegalArgumentException("log 参数须 > 0: base=" + b + " x=" + x);
            return Math.log(x) / Math.log(b);
        });
        reg("log10", "常用对数 log10(x)（≤0 抛异常）", args -> {
            ScalarSupport.requireArity("log10", args, 1);
            return logBase(args[0], 10.0, "log10");
        });
        reg("log2", "以 2 为底 log2(x)（≤0 抛异常）", args -> {
            ScalarSupport.requireArity("log2", args, 1);
            return logBase(args[0], 2.0, "log2");
        });
        reg("pmod", "正模：pmod(a,b)=((a%b)+b)%b（b=0 → null）", args -> {
            ScalarSupport.requireArity("pmod", args, 2);
            if (args[0] == null || args[1] == null) return null;
            if (isIntegral(args[0]) && isIntegral(args[1])) {
                long b = ScalarSupport.asLong(args[1]);
                if (b == 0) return null;
                long a = ScalarSupport.asLong(args[0]);
                return ((a % b) + b) % b;
            }
            double b = ScalarSupport.asDouble(args[1]);
            if (b == 0) return null;
            double a = ScalarSupport.asDouble(args[0]);
            return ((a % b) + b) % b;
        });
        reg("sign", "符号：正→1.0 / 零→0.0 / 负→-1.0", args -> {
            ScalarSupport.requireArity("sign", args, 1);
            if (args[0] == null) return null;
            return Math.signum(ScalarSupport.asDouble(args[0]));
        });
        reg("bin", "二进制字符串（无 0b 前缀，负数补码）", args -> {
            ScalarSupport.requireArity("bin", args, 1);
            return args[0] == null ? null : Long.toBinaryString(ScalarSupport.asLong(args[0]));
        });
        reg("hex", "十六进制（大写）：数值→整型十六进制，字符串→UTF-8 字节十六进制", args -> {
            ScalarSupport.requireArity("hex", args, 1);
            Object v = args[0];
            if (v == null) return null;
            if (v instanceof Number) {
                return Long.toHexString(ScalarSupport.asLong(v)).toUpperCase(Locale.ROOT);
            }
            byte[] bytes = v instanceof byte[] b ? b : ScalarSupport.string(v).getBytes(StandardCharsets.UTF_8);
            return toHex(bytes);
        });
        reg("unhex", "十六进制字符串 → 原字节（以 UTF-8 文本呈现；非法十六进制抛异常）", args -> {
            ScalarSupport.requireArity("unhex", args, 1);
            if (args[0] == null) return null;
            String s = ScalarSupport.string(args[0]).trim();
            if ((s.length() & 1) != 0) throw new IllegalArgumentException("unhex 十六进制长度须为偶数: '" + s + "'");
            byte[] out = new byte[s.length() / 2];
            for (int i = 0; i < out.length; i++) {
                int hi = Character.digit(s.charAt(i * 2), 16);
                int lo = Character.digit(s.charAt(i * 2 + 1), 16);
                if (hi < 0 || lo < 0) throw new IllegalArgumentException("unhex 非法十六进制: '" + s + "'");
                out[i] = (byte) ((hi << 4) | lo);
            }
            return new String(out, StandardCharsets.UTF_8);
        });
        reg("factorial", "阶乘 n!（n<0 或 n>20 抛异常，防 long 溢出）", args -> {
            ScalarSupport.requireArity("factorial", args, 1);
            if (args[0] == null) return null;
            long n = ScalarSupport.asLong(args[0]);
            if (n < 0) throw new IllegalArgumentException("factorial 参数不能为负: " + n);
            if (n > 20) throw new IllegalArgumentException("factorial 参数不能超过 20（会溢出 long）: " + n);
            long r = 1;
            for (long i = 2; i <= n; i++) r *= i;
            return r;
        });
        reg("greatest", "多参最大值（忽略 null；全 null → null）", args -> extremum("greatest", args, true));
        reg("least", "多参最小值（忽略 null；全 null → null）", args -> extremum("least", args, false));
        reg("negative", "取负 -x", args -> {
            ScalarSupport.requireArity("negative", args, 1);
            Object v = args[0];
            if (v == null) return null;
            if (v instanceof Double || v instanceof Float || v instanceof java.math.BigDecimal) {
                return -ScalarSupport.asDouble(v);
            }
            return -ScalarSupport.asLong(v);
        });
        reg("positive", "取正（原值返回）", args -> {
            ScalarSupport.requireArity("positive", args, 1);
            return args[0];
        });
    }

    private static void reg(String name, String display, java.util.function.Function<Object[], Object> impl) {
        ScalarFunctionRegistry.register(ScalarSupport.fn(name, display, impl));
    }

    private static Object logBase(Object v, double base, String name) {
        if (v == null) return null;
        double x = ScalarSupport.asDouble(v);
        if (x <= 0) throw new IllegalArgumentException(name + " 参数须 > 0: " + x);
        return Math.log(x) / Math.log(base);
    }

    private static boolean isIntegral(Object o) {
        return o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte;
    }

    /** greatest/least：跳过 null，按「全数值则数值比较，否则字符串比较」取值。 */
    private static Object extremum(String name, Object[] args, boolean max) {
        ScalarSupport.requireAtLeast(name, args, 2);
        Object best = null;
        boolean allNumeric = true;
        for (Object a : args) {
            if (a == null) continue;
            if (!(a instanceof Number)) allNumeric = false;
        }
        for (Object a : args) {
            if (a == null) continue;
            if (best == null) {
                best = a;
                continue;
            }
            int c = compare(a, best, allNumeric);
            if (max ? c > 0 : c < 0) best = a;
        }
        return best;
    }

    private static int compare(Object a, Object b, boolean allNumeric) {
        if (allNumeric) return Double.compare(ScalarSupport.asDouble(a), ScalarSupport.asDouble(b));
        return ScalarSupport.string(a).compareTo(ScalarSupport.string(b));
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString().toUpperCase(Locale.ROOT);
    }
}
