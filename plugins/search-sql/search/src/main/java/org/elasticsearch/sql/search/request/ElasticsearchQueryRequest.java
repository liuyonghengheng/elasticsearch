

package org.elasticsearch.sql.search.request;

import com.google.common.annotations.VisibleForTesting;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.sql.search.data.value.ElasticsearchExprValueFactory;
import org.elasticsearch.sql.search.response.ElasticsearchResponse;

/**
 * Elasticsearch search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation. 2)
 * Indicate the search already done.
 */
@EqualsAndHashCode
@Getter
@ToString
public class ElasticsearchQueryRequest implements ElasticsearchRequest {

  /**
   * Default query timeout in minutes.
   */
  public static final TimeValue DEFAULT_QUERY_TIMEOUT = TimeValue.timeValueMinutes(1L);

  /**
   * {@link ElasticsearchRequest.IndexName}.
   */
  private final IndexName indexName;

  /**
   * Search request source builder.
   */
  private final SearchSourceBuilder sourceBuilder;


  /**
   * ElasticsearchExprValueFactory.
   */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final ElasticsearchExprValueFactory exprValueFactory;

  /**
   * Indicate the search already done.
   */
  private boolean searchDone = false;

  /**
   * Constructor of ElasticsearchQueryRequest.
   */
  public ElasticsearchQueryRequest(String indexName, int size,
                                ElasticsearchExprValueFactory factory) {
    this(new IndexName(indexName), size, factory);
  }

  /**
   * Constructor of ElasticsearchQueryRequest.
   */
  public ElasticsearchQueryRequest(IndexName indexName, int size,
      ElasticsearchExprValueFactory factory) {
    this.indexName = indexName;
    this.sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.from(0);
    sourceBuilder.size(size);
    sourceBuilder.timeout(DEFAULT_QUERY_TIMEOUT);
    this.exprValueFactory = factory;
  }

  @Override
  public ElasticsearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    if (searchDone) {
      return new ElasticsearchResponse(SearchHits.empty(), exprValueFactory);
    } else {
      searchDone = true;
      return new ElasticsearchResponse(searchAction.apply(searchRequest()), exprValueFactory);
    }
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    //do nothing.
  }

  /**
   * Generate Elasticsearch search request.
   *
   * @return search request
   */
  @VisibleForTesting
  protected SearchRequest searchRequest() {
    return new SearchRequest()
        .indices(indexName.getIndexNames())
        .source(sourceBuilder);
  }
}
