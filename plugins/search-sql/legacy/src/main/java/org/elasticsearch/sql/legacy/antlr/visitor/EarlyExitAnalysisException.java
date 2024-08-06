


package org.elasticsearch.sql.legacy.antlr.visitor;

/**
 * Exit visitor early due to some reason.
 */
public class EarlyExitAnalysisException extends RuntimeException {

    public EarlyExitAnalysisException(String message) {
        super(message);
    }
}
