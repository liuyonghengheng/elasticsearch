

package org.elasticsearch.sql.planner.optimizer.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.elasticsearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.elasticsearch.sql.planner.logical.LogicalFilter;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalSort;
import org.elasticsearch.sql.planner.optimizer.Rule;

/**
 * Push Filter under Sort.
 * Filter - Sort - Child --> Sort - Filter - Child
 */
public class PushFilterUnderSort implements Rule<LogicalFilter> {

  private final Capture<LogicalSort> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalFilter> pattern;

  /**
   * Constructor of PushFilterUnderSort.
   */
  public PushFilterUnderSort() {
    this.capture = Capture.newCapture();
    this.pattern = typeOf(LogicalFilter.class)
        .with(source().matching(typeOf(LogicalSort.class).capturedAs(capture)));
  }

  @Override
  public LogicalPlan apply(LogicalFilter filter,
                           Captures captures) {
    LogicalSort sort = captures.get(capture);
    return new LogicalSort(
        filter.replaceChildPlans(sort.getChild()),
        sort.getSortList()
    );
  }
}
