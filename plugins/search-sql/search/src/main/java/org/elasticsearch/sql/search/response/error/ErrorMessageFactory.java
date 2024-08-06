

package org.elasticsearch.sql.search.response.error;

import lombok.experimental.UtilityClass;
import org.elasticsearch.ElasticsearchException;

@UtilityClass
public class ErrorMessageFactory {
  /**
   * Create error message based on the exception type.
   * Exceptions of Elasticsearch exception type and exceptions with wrapped Elasticsearch exception causes
   * should create {@link ElasticsearchErrorMessage}
   *
   * @param e      exception to create error message
   * @param status exception status code
   * @return error message
   */
  public static ErrorMessage createErrorMessage(Throwable e, int status) {
    Throwable cause = unwrapCause(e);
    if (cause instanceof ElasticsearchException) {
      ElasticsearchException exception = (ElasticsearchException) cause;
      return new ElasticsearchErrorMessage(exception, exception.status().getStatus());
    }
    return new ErrorMessage(e, status);
  }

  protected static Throwable unwrapCause(Throwable t) {
    Throwable result = t;
    if (result instanceof ElasticsearchException) {
      return result;
    }
    if (result.getCause() == null) {
      return result;
    }
    result = unwrapCause(result.getCause());
    return result;
  }
}
