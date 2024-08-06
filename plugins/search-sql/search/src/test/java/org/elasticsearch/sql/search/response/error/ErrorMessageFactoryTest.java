


package org.elasticsearch.sql.search.response.error;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

public class ErrorMessageFactoryTest {
  private Throwable nonElasticsearchThrowable = new Throwable();
  private Throwable elasticsearchThrowable = new ElasticsearchException(nonElasticsearchThrowable);

  @Test
  public void searchExceptionShouldCreateEsErrorMessage() {
    Exception exception = new ElasticsearchException(nonElasticsearchThrowable);
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertTrue(msg instanceof ElasticsearchErrorMessage);
  }

  @Test
  public void nonElasticsearchExceptionShouldCreateGenericErrorMessage() {
    Exception exception = new Exception(nonElasticsearchThrowable);
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertFalse(msg instanceof ElasticsearchErrorMessage);
  }

  @Test
  public void nonElasticsearchExceptionWithWrappedEsExceptionCauseShouldCreateEsErrorMessage() {
    Exception exception = (Exception) elasticsearchThrowable;
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertTrue(msg instanceof ElasticsearchErrorMessage);
  }

  @Test
  public void
      nonElasticsearchExceptionWithMultiLayerWrappedEsExceptionCauseShouldCreateEsErrorMessage() {
    Exception exception = new Exception(new Throwable(new Throwable(elasticsearchThrowable)));
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertTrue(msg instanceof ElasticsearchErrorMessage);
  }
}
