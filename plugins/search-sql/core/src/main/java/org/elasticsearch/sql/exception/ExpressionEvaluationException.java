

package org.elasticsearch.sql.exception;

/**
 * Exception for Expression Evaluation.
 */
public class ExpressionEvaluationException extends QueryEngineException {
  public ExpressionEvaluationException(String message) {
    super(message);
  }
}
