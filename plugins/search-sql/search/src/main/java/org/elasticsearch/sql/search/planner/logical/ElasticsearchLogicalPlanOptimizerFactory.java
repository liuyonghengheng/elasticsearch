

package org.elasticsearch.sql.search.planner.logical;

import java.util.Arrays;
import lombok.experimental.UtilityClass;
import org.elasticsearch.sql.search.planner.logical.rule.MergeAggAndIndexScan;
import org.elasticsearch.sql.search.planner.logical.rule.MergeAggAndRelation;
import org.elasticsearch.sql.search.planner.logical.rule.MergeFilterAndRelation;
import org.elasticsearch.sql.search.planner.logical.rule.MergeLimitAndIndexScan;
import org.elasticsearch.sql.search.planner.logical.rule.MergeLimitAndRelation;
import org.elasticsearch.sql.search.planner.logical.rule.MergeSortAndIndexAgg;
import org.elasticsearch.sql.search.planner.logical.rule.MergeSortAndIndexScan;
import org.elasticsearch.sql.search.planner.logical.rule.MergeSortAndRelation;
import org.elasticsearch.sql.search.planner.logical.rule.PushProjectAndIndexScan;
import org.elasticsearch.sql.search.planner.logical.rule.PushProjectAndRelation;
import org.elasticsearch.sql.planner.optimizer.LogicalPlanOptimizer;

/**
 * Elasticsearch storage specified logical plan optimizer.
 */
@UtilityClass
public class ElasticsearchLogicalPlanOptimizerFactory {

  /**
   * Create Elasticsearch storage specified logical plan optimizer.
   */
  public static LogicalPlanOptimizer create() {
    return new LogicalPlanOptimizer(Arrays.asList(
        new MergeFilterAndRelation(),
        new MergeAggAndIndexScan(),
        new MergeAggAndRelation(),
        new MergeSortAndRelation(),
        new MergeSortAndIndexScan(),
        new MergeSortAndIndexAgg(),
        new MergeSortAndIndexScan(),
        new MergeLimitAndRelation(),
        new MergeLimitAndIndexScan(),
        new PushProjectAndRelation(),
        new PushProjectAndIndexScan()
    ));
  }
}
