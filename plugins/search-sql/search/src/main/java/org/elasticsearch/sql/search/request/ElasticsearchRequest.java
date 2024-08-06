

package org.elasticsearch.sql.search.request;

import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.sql.search.data.value.ElasticsearchExprValueFactory;
import org.elasticsearch.sql.search.response.ElasticsearchResponse;

/**
 * Elasticsearch search request.
 */
public interface ElasticsearchRequest {
  /**
   * Apply the search action or scroll action on request based on context.
   *
   * @param searchAction search action.
   * @param scrollAction scroll search action.
   * @return ElasticsearchResponse.
   */
  ElasticsearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                            Function<SearchScrollRequest, SearchResponse> scrollAction);

  /**
   * Apply the cleanAction on request.
   *
   * @param cleanAction clean action.
   */
  void clean(Consumer<String> cleanAction);

  /**
   * Get the SearchSourceBuilder.
   *
   * @return SearchSourceBuilder.
   */
  SearchSourceBuilder getSourceBuilder();

  /**
   * Get the ElasticsearchExprValueFactory.
   * @return ElasticsearchExprValueFactory.
   */
  ElasticsearchExprValueFactory getExprValueFactory();

  /**
   * Elasticsearch Index Name.
   * Indices are seperated by ",".
   */
  @EqualsAndHashCode
  class IndexName {
    private static final String COMMA = ",";

    private final String[] indexNames;

    public IndexName(String indexName) {
      this.indexNames = indexName.split(COMMA);
    }

    public String[] getIndexNames() {
      return indexNames;
    }

    @Override
    public String toString() {
      return String.join(COMMA, indexNames);
    }
  }
}
