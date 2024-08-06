

package org.elasticsearch.sql.search.storage.system;

import static org.elasticsearch.sql.utils.SystemIndexUtils.systemTable;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.search.request.system.ElasticsearchCatIndicesRequest;
import org.elasticsearch.sql.search.request.system.ElasticsearchDescribeIndexRequest;
import org.elasticsearch.sql.search.request.system.ElasticsearchSystemRequest;
import org.elasticsearch.sql.planner.DefaultImplementor;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalRelation;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.storage.Table;
import org.elasticsearch.sql.utils.SystemIndexUtils;

/**
 * Elasticsearch System Index Table Implementation.
 */
public class ElasticsearchSystemIndex implements Table {
  /**
   * System Index Name.
   */
  private final Pair<ElasticsearchSystemIndexSchema, ElasticsearchSystemRequest> systemIndexBundle;

  public ElasticsearchSystemIndex(
      ElasticsearchClient client, String indexName) {
    this.systemIndexBundle = buildIndexBundle(client, indexName);
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return systemIndexBundle.getLeft().getMapping();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new ElasticsearchSystemIndexDefaultImplementor(), null);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public class ElasticsearchSystemIndexDefaultImplementor
      extends DefaultImplementor<Object> {

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, Object context) {
      return new ElasticsearchSystemIndexScan(systemIndexBundle.getRight());
    }
  }

  /**
   * Constructor of ElasticsearchSystemIndexName.
   *
   * @param indexName index name;
   */
  private Pair<ElasticsearchSystemIndexSchema, ElasticsearchSystemRequest> buildIndexBundle(
      ElasticsearchClient client, String indexName) {
    SystemIndexUtils.SystemTable systemTable = systemTable(indexName);
    if (systemTable.isSystemInfoTable()) {
      return Pair.of(ElasticsearchSystemIndexSchema.SYS_TABLE_TABLES,
          new ElasticsearchCatIndicesRequest(client));
    } else {
      return Pair.of(ElasticsearchSystemIndexSchema.SYS_TABLE_MAPPINGS,
          new ElasticsearchDescribeIndexRequest(client, systemTable.getTableName()));
    }
  }
}
