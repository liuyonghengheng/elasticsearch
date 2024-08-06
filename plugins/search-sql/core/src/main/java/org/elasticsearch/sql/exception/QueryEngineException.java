

package org.elasticsearch.sql.exception;

/**
 * Query analysis abstract exception.
 */
public class QueryEngineException extends RuntimeException {

  public QueryEngineException(String message) {
    super(message);
  }
}
