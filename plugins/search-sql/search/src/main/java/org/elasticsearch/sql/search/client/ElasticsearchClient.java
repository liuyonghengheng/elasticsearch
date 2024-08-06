

package org.elasticsearch.sql.search.client;

import java.util.List;
import java.util.Map;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.sql.search.mapping.IndexMapping;
import org.elasticsearch.sql.search.request.ElasticsearchRequest;
import org.elasticsearch.sql.search.response.ElasticsearchResponse;

/**
 * Elasticsearch client abstraction to wrap different Elasticsearch client implementation. For
 * example, implementation by node client for Elasticsearch plugin or by REST client for
 * standalone mode.
 */
public interface ElasticsearchClient {

  String META_CLUSTER_NAME = "CLUSTER_NAME";

  /**
   * Fetch index mapping(s) according to index expression given.
   *
   * @param indexExpression index expression
   * @return index mapping(s) from index name to its mapping
   */
  Map<String, IndexMapping> getIndexMappings(String... indexExpression);

  /**
   * Perform search query in the search request.
   *
   * @param request search request
   * @return search response
   */
  ElasticsearchResponse search(ElasticsearchRequest request);

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  List<String> indices();

  /**
   * Get meta info of the cluster.
   *
   * @return meta info of the cluster.
   */
  Map<String, String> meta();

  /**
   * Clean up resources related to the search request, for example scroll context.
   *
   * @param request search request
   */
  void cleanup(ElasticsearchRequest request);

  /**
   * Schedule a task to run.
   *
   * @param task task
   */
  void schedule(Runnable task);

  NodeClient getNodeClient();
}
