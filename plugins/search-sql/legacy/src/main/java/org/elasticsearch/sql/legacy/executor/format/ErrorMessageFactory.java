


package org.elasticsearch.sql.legacy.executor.format;

import org.elasticsearch.ElasticsearchException;

public class ErrorMessageFactory {
    /**
     * Create error message based on the exception type
     * Exceptions of Elasticsearch exception type and exceptions with wrapped Elasticsearch exception causes
     * should create {@link ElasticsearchErrorMessage}
     *
     * @param e         exception to create error message
     * @param status    exception status code
     * @return          error message
     */

    public static ErrorMessage createErrorMessage(Exception e, int status) {
        if (e instanceof ElasticsearchException) {
            return new ElasticsearchErrorMessage((ElasticsearchException) e,
                    ((ElasticsearchException) e).status().getStatus());
        } else if (unwrapCause(e) instanceof ElasticsearchException) {
            ElasticsearchException exception = (ElasticsearchException) unwrapCause(e);
            return new ElasticsearchErrorMessage(exception, exception.status().getStatus());
        }
        return new ErrorMessage(e, status);
    }

    public static Throwable unwrapCause(Throwable t) {
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
