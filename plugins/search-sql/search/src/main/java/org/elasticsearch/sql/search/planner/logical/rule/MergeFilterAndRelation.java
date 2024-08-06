

package org.elasticsearch.sql.search.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.elasticsearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexScan;
import org.elasticsearch.sql.planner.logical.LogicalFilter;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalRelation;
import org.elasticsearch.sql.planner.optimizer.Rule;

/**
 * Merge Filter -- Relation to LogicalIndexScan.
 */
public class MergeFilterAndRelation implements Rule<LogicalFilter> {

  private final Capture<LogicalRelation> relationCapture;
  private final Pattern<LogicalFilter> pattern;

  /**
   * Constructor of MergeFilterAndRelation.
   */
  public MergeFilterAndRelation() {
    this.relationCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalFilter.class)
        .with(source().matching(typeOf(LogicalRelation.class).capturedAs(relationCapture)));
  }

  @Override
  public Pattern<LogicalFilter> pattern() {
    return pattern;
  }

  @Override
  public LogicalPlan apply(LogicalFilter filter,
                           Captures captures) {
    LogicalRelation relation = captures.get(relationCapture);
    return ElasticsearchLogicalIndexScan
        .builder()
        .relationName(relation.getRelationName())
        .filter(filter.getCondition())
        .build();
  }
}
