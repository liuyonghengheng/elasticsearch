


package org.elasticsearch.sql.legacy.antlr.syntax;

import org.elasticsearch.sql.legacy.antlr.SqlAnalysisException;

/**
 * Exception for syntax analysis
 */
public class SyntaxAnalysisException extends SqlAnalysisException {

    public SyntaxAnalysisException(String message) {
        super(message);
    }
}
