


package org.elasticsearch.sql.legacy.antlr.semantic;

import org.elasticsearch.sql.legacy.antlr.SqlAnalysisException;

/**
 * Exception for semantic analysis
 */
public class SemanticAnalysisException extends SqlAnalysisException {

    public SemanticAnalysisException(String message) {
        super(message);
    }

}
