package com.dojao.core.function.scalar;

import com.dojao.core.function.scalar.conditional.ConditionalFunctions;
import com.dojao.core.function.scalar.crypto.CryptoFunctions;
import com.dojao.core.function.scalar.datetime.DateTimeFunctions;
import com.dojao.core.function.scalar.math.MathFunctions;
import com.dojao.core.function.scalar.string.StringFunctions;

/**
 * 内置标量函数批量注册入口 —— 按分类汇总 Hive 标准函数，供 {@link ScalarFunctionRegistry} 静态块调用。
 *
 * <p>分类落地（{@code abs/round/floor/ceil/ceiling/upper/lcase/…/year/month/day/date} 见
 * {@code ExprBuiltins}，此处不重复注册，避免 {@code ScalarExpr.tryParse} 误路由）：</p>
 * <ul>
 *   <li>{@link MathFunctions} 数学：rand/pow/sqrt/exp/ln/log/sign/bin/hex/factorial/greatest/…</li>
 *   <li>{@link StringFunctions} 字符串：trim/concat_ws/substr/instr/regexp_replace/lpad/parse_url/get_json_object/…</li>
 *   <li>{@link DateTimeFunctions} 日期时间：current_date/unix_timestamp/to_date/add_months/date_format/…</li>
 *   <li>{@link ConditionalFunctions} 条件：nvl/coalesce/isnull/isnotnull/nullif/assert_true</li>
 *   <li>{@link CryptoFunctions} 加密：md5/sha1/sha2/crc32/uuid/hash</li>
 * </ul>
 *
 * @author jiaxin
 * @date 2026/10/11
 */
public final class BuiltinScalars {

    private BuiltinScalars() {
    }

    /** 注册全部内置标量函数（幂等：同名重复注册为覆盖写，静态块仅调一次）。 */
    public static void registerAll() {
        MathFunctions.registerAll();
        StringFunctions.registerAll();
        DateTimeFunctions.registerAll();
        ConditionalFunctions.registerAll();
        CryptoFunctions.registerAll();
    }
}
