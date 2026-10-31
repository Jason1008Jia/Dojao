package com.dojao.grammar.analyzer;

import com.dojao.grammar.analyzer.error.SemanticError;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import java.util.ArrayList;
import java.util.List;

/**
 * ANTLR 语法错误收集器：将语法错误收集到列表，后续统一抛出。
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/6/28
 */
public class SyntaxErrorCollector extends BaseErrorListener {

    private final List<SemanticError> errors = new ArrayList<>();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
                             Object offendingSymbol,
                             int line,
                             int charPositionInLine,
                             String msg,
                             RecognitionException e) {
        errors.add(new SemanticError(
            "Syntax error: " + msg, line, charPositionInLine));
    }

    public List<SemanticError> getErrors() {
        return errors;
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public void clear() {
        errors.clear();
    }
}
