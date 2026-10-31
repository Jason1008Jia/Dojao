package com.dojao.grammar.analyzer.error;

/**
 * 语义错误记录：包含错误消息及解析树中的位置信息。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27 22:27
 */
public record SemanticError(
    String message,
    int line,
    int charPositionInLine
) {

    public SemanticError(String message) {
        this(message, -1, -1);
    }

    @Override
    public String toString() {
        if (line >= 0) {
            return String.format("Line %d:%d - %s", line, charPositionInLine, message);
        }
        return message;
    }
}
