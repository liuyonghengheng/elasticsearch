

package org.elasticsearch.sql.search.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.elasticsearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.sql.ast.tree.Sort;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.ReferenceExpression;
import org.elasticsearch.sql.expression.aggregation.NamedAggregator;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexAgg;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalSort;
import org.elasticsearch.sql.planner.optimizer.Rule;

/**
 * Merge Sort -- IndexScanAggregation to IndexScanAggregation.
 */
public class MergeSortAndIndexAgg implements Rule<LogicalSort> {

  private final Capture<ElasticsearchLogicalIndexAgg> indexAggCapture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalSort> pattern;

  /**
   * Constructor of MergeAggAndIndexScan.
   */
  public MergeSortAndIndexAgg() {
    this.indexAggCapture = Capture.newCapture();
    final AtomicReference<LogicalSort> sortRef = new AtomicReference<>();

    this.pattern = typeOf(LogicalSort.class)
        .matching(OptimizationRuleUtils::sortByFieldsOnly)
        .matching(sort -> {
          sortRef.set(sort);
          return true;
        })
        .with(source().matching(typeOf(ElasticsearchLogicalIndexAgg.class)
            .matching(indexAgg -> !hasAggregatorInSortBy(sortRef.get(), indexAgg))
            .capturedAs(indexAggCapture)));
  }

  @Override
  public LogicalPlan apply(LogicalSort sort,
                           Captures captures) {
    ElasticsearchLogicalIndexAgg indexAgg = captures.get(indexAggCapture);
    return ElasticsearchLogicalIndexAgg.builder()
        .relationName(indexAgg.getRelationName())
        .filter(indexAgg.getFilter())
        .groupByList(indexAgg.getGroupByList())
        .aggregatorList(indexAgg.getAggregatorList())
        .sortList(sort.getSortList())
        .build();
  }

  private boolean hasAggregatorInSortBy(LogicalSort sort, ElasticsearchLogicalIndexAgg agg) {
    final Set<String> aggregatorNames =
        agg.getAggregatorList().stream().map(NamedAggregator::getName).collect(Collectors.toSet());
    for (Pair<Sort.SortOption, Expression> sortPair : sort.getSortList()) {
      if (aggregatorNames.contains(((ReferenceExpression) sortPair.getRight()).getAttr())) {
        return true;
      }
    }
    return false;
  }
}
