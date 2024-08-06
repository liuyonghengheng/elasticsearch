

package org.elasticsearch.sql.search.storage;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.sql.common.setting.Settings;
import org.elasticsearch.sql.common.utils.StringUtils;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.search.data.value.ElasticsearchExprValueFactory;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexAgg;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexScan;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalPlanOptimizerFactory;
import org.elasticsearch.sql.search.planner.physical.ADOperator;
import org.elasticsearch.sql.search.planner.physical.MLCommonsOperator;
import org.elasticsearch.sql.search.request.ElasticsearchRequest;
import org.elasticsearch.sql.search.request.system.ElasticsearchDescribeIndexRequest;
import org.elasticsearch.sql.search.response.agg.ElasticsearchAggregationResponseParser;
import org.elasticsearch.sql.search.storage.script.aggregation.AggregationQueryBuilder;
import org.elasticsearch.sql.search.storage.script.filter.FilterQueryBuilder;
import org.elasticsearch.sql.search.storage.script.sort.SortQueryBuilder;
import org.elasticsearch.sql.search.storage.serialization.DefaultExpressionSerializer;
import org.elasticsearch.sql.planner.DefaultImplementor;
import org.elasticsearch.sql.planner.logical.LogicalAD;
import org.elasticsearch.sql.planner.logical.LogicalMLCommons;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalRelation;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.storage.Table;

/** Elasticsearch table (index) implementation. */
public class ElasticsearchIndex implements Table {

  /** Elasticsearch client connection. */
  private final ElasticsearchClient client;

  private final Settings settings;

  /**
   * {@link ElasticsearchRequest.IndexName}.
   */
  private final ElasticsearchRequest.IndexName indexName;

  /**
   * The cached mapping of field and type in index.
   */
  private Map<String, ExprType> cachedFieldTypes = null;

  /**
   * Constructor.
   */
  public ElasticsearchIndex(ElasticsearchClient client, Settings settings, String indexName) {
    this.client = client;
    this.settings = settings;
    this.indexName = new ElasticsearchRequest.IndexName(indexName);
  }

  /*
   * TODO: Assume indexName doesn't have wildcard.
   *  Need to either handle field name conflicts
   *   or lazy evaluate when query engine pulls field type.
   */
  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      cachedFieldTypes = new ElasticsearchDescribeIndexRequest(client, indexName).getFieldTypes();
    }
    return cachedFieldTypes;
  }

  /**
   * TODO: Push down operations to index scan operator as much as possible in future.
   */
  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    ElasticsearchIndexScan indexScan = new ElasticsearchIndexScan(client, settings, indexName,
        new ElasticsearchExprValueFactory(getFieldTypes()));

    /*
     * Visit logical plan with index scan as context so logical operators visited, such as
     * aggregation, filter, will accumulate (push down) Elasticsearch query and aggregation DSL on
     * index scan.
     */
    return plan.accept(new ElasticsearchDefaultImplementor(indexScan, client), indexScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return ElasticsearchLogicalPlanOptimizerFactory.create().optimize(plan);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public static class ElasticsearchDefaultImplementor
      extends DefaultImplementor<ElasticsearchIndexScan> {
    private final ElasticsearchIndexScan indexScan;

    private final ElasticsearchClient client;

    @Override
    public PhysicalPlan visitNode(LogicalPlan plan, ElasticsearchIndexScan context) {
      if (plan instanceof ElasticsearchLogicalIndexScan) {
        return visitIndexScan((ElasticsearchLogicalIndexScan) plan, context);
      } else if (plan instanceof ElasticsearchLogicalIndexAgg) {
        return visitIndexAggregation((ElasticsearchLogicalIndexAgg) plan, context);
      } else {
        throw new IllegalStateException(StringUtils.format("unexpected plan node type %s",
            plan.getClass()));
      }
    }

    /**
     * Implement ElasticsearchLogicalIndexScan.
     */
    public PhysicalPlan visitIndexScan(ElasticsearchLogicalIndexScan node,
                                       ElasticsearchIndexScan context) {
      if (null != node.getSortList()) {
        final SortQueryBuilder builder = new SortQueryBuilder();
        context.pushDownSort(node.getSortList().stream()
            .map(sort -> builder.build(sort.getValue(), sort.getKey()))
            .collect(Collectors.toList()));
      }

      if (null != node.getFilter()) {
        FilterQueryBuilder queryBuilder = new FilterQueryBuilder(new DefaultExpressionSerializer());
        QueryBuilder query = queryBuilder.build(node.getFilter());
        context.pushDown(query);
      }

      if (node.getLimit() != null) {
        context.pushDownLimit(node.getLimit(), node.getOffset());
      }

      if (node.hasProjects()) {
        context.pushDownProjects(node.getProjectList());
      }
      return indexScan;
    }

    /**
     * Implement ElasticsearchLogicalIndexAgg.
     */
    public PhysicalPlan visitIndexAggregation(ElasticsearchLogicalIndexAgg node,
                                              ElasticsearchIndexScan context) {
      if (node.getFilter() != null) {
        FilterQueryBuilder queryBuilder = new FilterQueryBuilder(
            new DefaultExpressionSerializer());
        QueryBuilder query = queryBuilder.build(node.getFilter());
        context.pushDown(query);
      }
      AggregationQueryBuilder builder =
          new AggregationQueryBuilder(new DefaultExpressionSerializer());
      Pair<List<AggregationBuilder>, ElasticsearchAggregationResponseParser> aggregationBuilder =
          builder.buildAggregationBuilder(node.getAggregatorList(),
              node.getGroupByList(), node.getSortList());
      context.pushDownAggregation(aggregationBuilder);
      context.pushTypeMapping(
          builder.buildTypeMapping(node.getAggregatorList(),
              node.getGroupByList()));
      return indexScan;
    }

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, ElasticsearchIndexScan context) {
      return indexScan;
    }

    @Override
    public PhysicalPlan visitMLCommons(LogicalMLCommons node, ElasticsearchIndexScan context) {
      return new MLCommonsOperator(visitChild(node, context), node.getAlgorithm(),
              node.getArguments(), client.getNodeClient());
    }

    @Override
    public PhysicalPlan visitAD(LogicalAD node, ElasticsearchIndexScan context) {
      return new ADOperator(visitChild(node, context),
              node.getArguments(), client.getNodeClient());
    }
  }
}
