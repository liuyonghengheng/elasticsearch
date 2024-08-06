

package org.elasticsearch.sql.search.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.elasticsearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexScan;
import org.elasticsearch.sql.planner.logical.LogicalLimit;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.optimizer.Rule;

@Getter
public class MergeLimitAndIndexScan implements Rule<LogicalLimit> {

  private final Capture<ElasticsearchLogicalIndexScan> indexScanCapture;

  @Accessors(fluent = true)
  private final Pattern<LogicalLimit> pattern;

  /**
   * Constructor of MergeLimitAndIndexScan.
   */
  public MergeLimitAndIndexScan() {
    this.indexScanCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalLimit.class)
        .with(source()
            .matching(typeOf(ElasticsearchLogicalIndexScan.class).capturedAs(indexScanCapture)));
  }

  @Override
  public LogicalPlan apply(LogicalLimit plan, Captures captures) {
    ElasticsearchLogicalIndexScan indexScan = captures.get(indexScanCapture);
    ElasticsearchLogicalIndexScan.ElasticsearchLogicalIndexScanBuilder builder =
        ElasticsearchLogicalIndexScan.builder();
    builder.relationName(indexScan.getRelationName())
        .filter(indexScan.getFilter())
        .offset(plan.getOffset())
        .limit(plan.getLimit());
    if (indexScan.getSortList() != null) {
      builder.sortList(indexScan.getSortList());
    }
    return builder.build();
  }
}
