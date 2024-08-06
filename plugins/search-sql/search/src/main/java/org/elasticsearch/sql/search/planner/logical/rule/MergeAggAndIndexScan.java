

package org.elasticsearch.sql.search.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.elasticsearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexAgg;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexScan;
import org.elasticsearch.sql.planner.logical.LogicalAggregation;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.optimizer.Rule;

/**
 * Merge Aggregation -- Relation to IndexScanAggregation.
 */
public class MergeAggAndIndexScan implements Rule<LogicalAggregation> {

  private final Capture<ElasticsearchLogicalIndexScan> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalAggregation> pattern;

  /**
   * Constructor of MergeAggAndIndexScan.
   */
  public MergeAggAndIndexScan() {
    this.capture = Capture.newCapture();
    this.pattern = typeOf(LogicalAggregation.class)
        .with(source().matching(typeOf(ElasticsearchLogicalIndexScan.class)
            .matching(indexScan -> !indexScan.hasLimit())
            .capturedAs(capture)));
  }

  @Override
  public LogicalPlan apply(LogicalAggregation aggregation,
                           Captures captures) {
    ElasticsearchLogicalIndexScan indexScan = captures.get(capture);
    return ElasticsearchLogicalIndexAgg
        .builder()
        .relationName(indexScan.getRelationName())
        .filter(indexScan.getFilter())
        .aggregatorList(aggregation.getAggregatorList())
        .groupByList(aggregation.getGroupByList())
        .build();
  }
}
