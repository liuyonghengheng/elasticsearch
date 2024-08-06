

package org.elasticsearch.sql.search.request;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.sql.search.data.value.ElasticsearchExprValueFactory;
import org.elasticsearch.sql.search.response.ElasticsearchResponse;

/**
 * Elasticsearch scroll search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation 2)
 * Maintain scroll ID between calls to client search method
 */
@EqualsAndHashCode
@Getter
@ToString
public class ElasticsearchScrollRequest implements ElasticsearchRequest {

  /** Default scroll context timeout in minutes. */
  public static final TimeValue DEFAULT_SCROLL_TIMEOUT = TimeValue.timeValueMinutes(1L);

  /**
   * {@link ElasticsearchRequest.IndexName}.
   */
  private final IndexName indexName;

  /** Index name. */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final ElasticsearchExprValueFactory exprValueFactory;

  /**
   * Scroll id which is set after first request issued. Because ElasticsearchClient is shared by
   * multi-thread so this state has to be maintained here.
   */
  @Setter
  private String scrollId;

  /** Search request source builder. */
  private final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

  public ElasticsearchScrollRequest(IndexName indexName, ElasticsearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.exprValueFactory = exprValueFactory;
  }

  public ElasticsearchScrollRequest(String indexName, ElasticsearchExprValueFactory exprValueFactory) {
    this(new IndexName(indexName), exprValueFactory);
  }

  @Override
  public ElasticsearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    SearchResponse searchResponse;
    if (isScrollStarted()) {
      searchResponse = scrollAction.apply(scrollRequest());
    } else {
      searchResponse = searchAction.apply(searchRequest());
    }
    setScrollId(searchResponse.getScrollId());

    return new ElasticsearchResponse(searchResponse, exprValueFactory);
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    try {
      if (isScrollStarted()) {
        cleanAction.accept(getScrollId());
      }
    } finally {
      reset();
    }
  }

  /**
   * Generate Elasticsearch search request.
   *
   * @return search request
   */
  public SearchRequest searchRequest() {
    return new SearchRequest()
        .indices(indexName.getIndexNames())
        .scroll(DEFAULT_SCROLL_TIMEOUT)
        .source(sourceBuilder);
  }

  /**
   * Is scroll started which means pages after first is being requested.
   *
   * @return true if scroll started
   */
  public boolean isScrollStarted() {
    return (scrollId != null);
  }

  /**
   * Generate Elasticsearch scroll request by scroll id maintained.
   *
   * @return scroll request
   */
  public SearchScrollRequest scrollRequest() {
    Objects.requireNonNull(scrollId, "Scroll id cannot be null");
    return new SearchScrollRequest().scroll(DEFAULT_SCROLL_TIMEOUT).scrollId(scrollId);
  }

  /**
   * Reset internal state in case any stale data. However, ideally the same instance is not supposed
   * to be reused across different physical plan.
   */
  public void reset() {
    scrollId = null;
  }
}
