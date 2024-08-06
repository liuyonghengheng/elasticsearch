

package org.elasticsearch.sql.search.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.elasticsearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexScan;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalRelation;
import org.elasticsearch.sql.planner.logical.LogicalSort;
import org.elasticsearch.sql.planner.optimizer.Rule;

/**
 * Merge Sort with Relation only when Sort by fields.
 */
public class MergeSortAndRelation implements Rule<LogicalSort> {

  private final Capture<LogicalRelation> relationCapture;
  private final Pattern<LogicalSort> pattern;

  /**
   * Constructor of MergeSortAndRelation.
   */
  public MergeSortAndRelation() {
    this.relationCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalSort.class).matching(OptimizationRuleUtils::sortByFieldsOnly)
        .with(source().matching(typeOf(LogicalRelation.class).capturedAs(relationCapture)));
  }

  @Override
  public Pattern<LogicalSort> pattern() {
    return pattern;
  }

  @Override
  public LogicalPlan apply(LogicalSort sort,
                           Captures captures) {
    LogicalRelation relation = captures.get(relationCapture);
    return ElasticsearchLogicalIndexScan
        .builder()
        .relationName(relation.getRelationName())
        .sortList(sort.getSortList())
        .build();
  }
}
