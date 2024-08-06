


package org.elasticsearch.sql.search.response.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.elasticsearch.rest.RestStatus.SERVICE_UNAVAILABLE;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;

@ExtendWith(MockitoExtension.class)
class ElasticsearchErrorMessageTest {

  @Mock
  private ElasticsearchException elasticsearchException;

  @Mock
  private SearchPhaseExecutionException searchPhaseExecutionException;

  @Mock
  private ShardSearchFailure shardSearchFailure;

  @Test
  public void fetchReason() {
    when(elasticsearchException.getMessage()).thenReturn("error");

    ElasticsearchErrorMessage errorMessage =
        new ElasticsearchErrorMessage(elasticsearchException, SERVICE_UNAVAILABLE.getStatus());
    assertEquals("Error occurred in Elasticsearch engine: error", errorMessage.fetchReason());
  }

  @Test
  public void fetchDetailsWithElasticsearchException() {
    when(elasticsearchException.getDetailedMessage()).thenReturn("detail error");

    ElasticsearchErrorMessage errorMessage =
        new ElasticsearchErrorMessage(elasticsearchException, SERVICE_UNAVAILABLE.getStatus());
    assertEquals("detail error\n"
            + "For more details, please send request for "
            + "Json format to see the raw response from Elasticsearch engine.",
        errorMessage.fetchDetails());
  }

  @Test
  public void fetchDetailsWithSearchPhaseExecutionException() {
    when(searchPhaseExecutionException.shardFailures())
        .thenReturn(new ShardSearchFailure[] {shardSearchFailure});
    when(shardSearchFailure.shardId()).thenReturn(1);
    when(shardSearchFailure.getCause()).thenReturn(new IllegalStateException("illegal state"));

    ElasticsearchErrorMessage errorMessage =
        new ElasticsearchErrorMessage(searchPhaseExecutionException,
            SERVICE_UNAVAILABLE.getStatus());
    assertEquals("Shard[1]: java.lang.IllegalStateException: illegal state\n"
            + "\n"
            + "For more details, please send request for Json format to see the "
            + "raw response from Elasticsearch engine.",
        errorMessage.fetchDetails());
  }
}
