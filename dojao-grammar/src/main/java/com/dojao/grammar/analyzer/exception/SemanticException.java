package com.dojao.grammar.analyzer.exception;

import com.dojao.grammar.analyzer.error.SemanticError;

import java.util.List;

/**
 * 语义分析异常：包含所有语义错误的汇总异常。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/27 22:27
 */
public class SemanticException extends RuntimeException {

    private final List<SemanticError> errors;

    public SemanticException(List<SemanticError> errors) {
        super(formatMessage(errors));
        this.errors = List.copyOf(errors);
    }

    private static String formatMessage(List<SemanticError> errors) {
        StringBuilder sb = new StringBuilder("Semantic errors (" + errors.size() + "):");
        for (SemanticError e : errors) {
            sb.append("\n  - ").append(e);
        }
        return sb.toString();
    }

    public List<SemanticError> getErrors() {
        return errors;
    }
}
