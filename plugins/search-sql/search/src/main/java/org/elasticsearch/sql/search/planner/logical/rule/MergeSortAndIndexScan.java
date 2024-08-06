

package org.elasticsearch.sql.search.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.elasticsearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.sql.ast.tree.Sort;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexScan;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalSort;
import org.elasticsearch.sql.planner.optimizer.Rule;

/**
 * Merge Sort with IndexScan only when Sort by fields.
 */
public class MergeSortAndIndexScan implements Rule<LogicalSort> {

  private final Capture<ElasticsearchLogicalIndexScan> indexScanCapture;
  private final Pattern<LogicalSort> pattern;

  /**
   * Constructor of MergeSortAndRelation.
   */
  public MergeSortAndIndexScan() {
    this.indexScanCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalSort.class).matching(OptimizationRuleUtils::sortByFieldsOnly)
        .with(source()
            .matching(typeOf(ElasticsearchLogicalIndexScan.class).capturedAs(indexScanCapture)));
  }

  @Override
  public Pattern<LogicalSort> pattern() {
    return pattern;
  }

  @Override
  public LogicalPlan apply(LogicalSort sort,
                           Captures captures) {
    ElasticsearchLogicalIndexScan indexScan = captures.get(indexScanCapture);

    return ElasticsearchLogicalIndexScan
        .builder()
        .relationName(indexScan.getRelationName())
        .filter(indexScan.getFilter())
        .sortList(mergeSortList(indexScan.getSortList(), sort.getSortList()))
        .build();
  }

  private List<Pair<Sort.SortOption, Expression>> mergeSortList(List<Pair<Sort.SortOption,
      Expression>> l1, List<Pair<Sort.SortOption, Expression>> l2) {
    if (null == l1) {
      return l2;
    } else {
      return Stream.concat(l1.stream(), l2.stream()).collect(Collectors.toList());
    }
  }
}
