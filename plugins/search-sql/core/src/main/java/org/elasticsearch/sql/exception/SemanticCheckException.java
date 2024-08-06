

package org.elasticsearch.sql.exception;

/**
 * Semantic Check Exception.
 */
public class SemanticCheckException extends QueryEngineException {
  public SemanticCheckException(String message) {
    super(message);
  }
}
