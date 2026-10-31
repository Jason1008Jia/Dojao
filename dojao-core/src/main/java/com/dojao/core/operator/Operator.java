package com.dojao.core.operator;

import com.dojao.common.data.Page;

/**
 * 类描述：TODO 在dojao中这个是所有算子都要实现的接口
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/7/11 23:46
 */
public interface Operator {
    void open();                    // 初始化资源
    Page next();        // 获取下一批 Page（Arrow 数据）
    void close();                   // 释放资源

}
