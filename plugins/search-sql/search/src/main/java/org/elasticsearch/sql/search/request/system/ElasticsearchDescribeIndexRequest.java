

package org.elasticsearch.sql.search.request.system;

import static org.elasticsearch.sql.data.model.ExprValueUtils.integerValue;
import static org.elasticsearch.sql.data.model.ExprValueUtils.stringValue;
import static org.elasticsearch.sql.search.client.ElasticsearchClient.META_CLUSTER_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.elasticsearch.sql.data.model.ExprTupleValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.search.data.type.ElasticsearchDataType;
import org.elasticsearch.sql.search.mapping.IndexMapping;
import org.elasticsearch.sql.search.request.ElasticsearchRequest;

/**
 * Describe index meta data request.
 */
public class ElasticsearchDescribeIndexRequest implements ElasticsearchSystemRequest {

  private static final String DEFAULT_TABLE_CAT = "elasticsearch";

  private static final Integer DEFAULT_NUM_PREC_RADIX = 10;

  private static final Integer DEFAULT_NULLABLE = 2;

  private static final String DEFAULT_IS_AUTOINCREMENT = "NO";

  /**
   * Type mapping from Elasticsearch data type to expression type in our type system in query
   * engine. TODO: geo, ip etc.
   */
  private static final Map<String, ExprType> EASYSEARCH_TYPE_TO_EXPR_TYPE_MAPPING =
      ImmutableMap.<String, ExprType>builder()
          .put("text", ElasticsearchDataType.EASYSEARCH_TEXT)
          .put("text_keyword", ElasticsearchDataType.EASYSEARCH_TEXT_KEYWORD)
          .put("keyword", ExprCoreType.STRING)
          .put("byte", ExprCoreType.BYTE)
          .put("short", ExprCoreType.SHORT)
          .put("integer", ExprCoreType.INTEGER)
          .put("long", ExprCoreType.LONG)
          .put("float", ExprCoreType.FLOAT)
          .put("half_float", ExprCoreType.FLOAT)
          .put("scaled_float", ExprCoreType.DOUBLE)
          .put("double", ExprCoreType.DOUBLE)
          .put("boolean", ExprCoreType.BOOLEAN)
          .put("nested", ExprCoreType.ARRAY)
          .put("object", ExprCoreType.STRUCT)
          .put("date", ExprCoreType.TIMESTAMP)
          .put("date_nanos", ExprCoreType.TIMESTAMP)
          .put("ip", ElasticsearchDataType.EASYSEARCH_IP)
          .put("geo_point", ElasticsearchDataType.EASYSEARCH_GEO_POINT)
          .put("binary", ElasticsearchDataType.EASYSEARCH_BINARY)
          .build();

  /**
   * Elasticsearch client connection.
   */
  private final ElasticsearchClient client;

  /**
   * {@link ElasticsearchRequest.IndexName}.
   */
  private final ElasticsearchRequest.IndexName indexName;

  public ElasticsearchDescribeIndexRequest(ElasticsearchClient client, String indexName) {
    this(client, new ElasticsearchRequest.IndexName(indexName));
  }

  public ElasticsearchDescribeIndexRequest(ElasticsearchClient client,
      ElasticsearchRequest.IndexName indexName) {
    this.client = client;
    this.indexName = indexName;
  }

  /**
   * search all the index in the data store.
   *
   * @return list of {@link ExprValue}
   */
  @Override
  public List<ExprValue> search() {
    List<ExprValue> results = new ArrayList<>();
    Map<String, String> meta = client.meta();
    int pos = 0;
    for (Map.Entry<String, ExprType> entry : getFieldTypes().entrySet()) {
      results.add(
          row(entry.getKey(), entry.getValue().legacyTypeName().toLowerCase(), pos++,
              clusterName(meta)));
    }
    return results;
  }

  /**
   * Get the mapping of field and type.
   *
   * @return mapping of field and type.
   */
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    Map<String, IndexMapping> indexMappings = client.getIndexMappings(indexName.getIndexNames());
    for (IndexMapping indexMapping : indexMappings.values()) {
      fieldTypes
          .putAll(indexMapping.getAllFieldTypes(this::transformESTypeToExprType).entrySet().stream()
              .filter(entry -> !ExprCoreType.UNKNOWN.equals(entry.getValue()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }
    return fieldTypes;
  }

  private ExprType transformESTypeToExprType(String searchType) {
    return EASYSEARCH_TYPE_TO_EXPR_TYPE_MAPPING.getOrDefault(searchType, ExprCoreType.UNKNOWN);
  }

  private ExprTupleValue row(String fieldName, String fieldType, int position, String clusterName) {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    valueMap.put("TABLE_CAT", stringValue(clusterName));
    valueMap.put("TABLE_NAME", stringValue(indexName.toString()));
    valueMap.put("COLUMN_NAME", stringValue(fieldName));
    // todo
    valueMap.put("TYPE_NAME", stringValue(fieldType));
    valueMap.put("NUM_PREC_RADIX", integerValue(DEFAULT_NUM_PREC_RADIX));
    valueMap.put("NULLABLE", integerValue(DEFAULT_NULLABLE));
    // There is no deterministic position of column in table
    valueMap.put("ORDINAL_POSITION", integerValue(position));
    // TODO Defaulting to unknown, need to check this
    valueMap.put("IS_NULLABLE", stringValue(""));
    // Defaulting to "NO"
    valueMap.put("IS_AUTOINCREMENT", stringValue(DEFAULT_IS_AUTOINCREMENT));
    // TODO Defaulting to unknown, need to check
    valueMap.put("IS_GENERATEDCOLUMN", stringValue(""));
    return new ExprTupleValue(valueMap);
  }

  private String clusterName(Map<String, String> meta) {
    return meta.getOrDefault(META_CLUSTER_NAME, DEFAULT_TABLE_CAT);
  }

  @Override
  public String toString() {
    return "ElasticsearchDescribeIndexRequest{"
        + "indexName='" + indexName + '\''
        + '}';
  }
}
