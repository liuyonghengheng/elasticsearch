

package org.elasticsearch.sql.search.planner.logical;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.sql.ast.tree.Sort;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.expression.aggregation.NamedAggregator;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalPlanNodeVisitor;

/**
 * Logical Index Scan Aggregation Operation.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class ElasticsearchLogicalIndexAgg extends LogicalPlan {

  private final String relationName;

  /**
   * Filter Condition.
   */
  @Setter
  private Expression filter;

  /**
   * Aggregation List.
   */
  @Setter
  private List<NamedAggregator> aggregatorList;

  /**
   * Group List.
   */
  @Setter
  private List<NamedExpression> groupByList;

  /**
   * Sort List.
   */
  @Setter
  private List<Pair<Sort.SortOption, Expression>> sortList;

  /**
   * ElasticsearchLogicalIndexAgg Constructor.
   */
  @Builder
  public ElasticsearchLogicalIndexAgg(
      String relationName,
      Expression filter,
      List<NamedAggregator> aggregatorList,
      List<NamedExpression> groupByList,
      List<Pair<Sort.SortOption, Expression>> sortList) {
    super(ImmutableList.of());
    this.relationName = relationName;
    this.filter = filter;
    this.aggregatorList = aggregatorList;
    this.groupByList = groupByList;
    this.sortList = sortList;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }
}
