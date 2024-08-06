

package org.elasticsearch.sql.search.request.system;

import static org.elasticsearch.sql.data.model.ExprValueUtils.stringValue;
import static org.elasticsearch.sql.search.client.ElasticsearchClient.META_CLUSTER_NAME;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.data.model.ExprTupleValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.search.client.ElasticsearchClient;

/**
 * Cat indices request.
 */
@RequiredArgsConstructor
public class ElasticsearchCatIndicesRequest implements ElasticsearchSystemRequest {

  private static final String DEFAULT_TABLE_CAT = "elasticsearch";

  private static final String DEFAULT_TABLE_TAPE = "BASE TABLE";

  /** Elasticsearch client connection. */
  private final ElasticsearchClient client;

  /**
   * search all the index in the data store.
   *
   * @return list of {@link ExprValue}
   */
  @Override
  public List<ExprValue> search() {
    List<ExprValue> results = new ArrayList<>();
    final Map<String, String> meta = client.meta();
    for (String index : client.indices()) {
      results.add(row(index, clusterName(meta)));
    }
    return results;
  }

  private ExprTupleValue row(String indexName, String clusterName) {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    valueMap.put("TABLE_CAT", stringValue(clusterName));
    valueMap.put("TABLE_NAME", stringValue(indexName));
    valueMap.put("TABLE_TYPE", stringValue(DEFAULT_TABLE_TAPE));
    return new ExprTupleValue(valueMap);
  }

  private String clusterName(Map<String, String> meta) {
    return meta.getOrDefault(META_CLUSTER_NAME, DEFAULT_TABLE_CAT);
  }

  @Override
  public String toString() {
    return "ElasticsearchCatIndicesRequest{}";
  }
}
