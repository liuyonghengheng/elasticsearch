

package org.elasticsearch.sql.planner.physical;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.expression.aggregation.Aggregator;
import org.elasticsearch.sql.expression.aggregation.NamedAggregator;
import org.elasticsearch.sql.expression.span.SpanExpression;
import org.elasticsearch.sql.planner.physical.collector.Collector;
import org.elasticsearch.sql.storage.bindingtuple.BindingTuple;

/**
 * Group the all the input {@link BindingTuple} by {@link AggregationOperator#groupByExprList},
 * calculate the aggregation result by using {@link AggregationOperator#aggregatorList}.
 */
@EqualsAndHashCode
@ToString
public class AggregationOperator extends PhysicalPlan {
  @Getter
  private final PhysicalPlan input;
  @Getter
  private final List<NamedAggregator> aggregatorList;
  @Getter
  private final List<NamedExpression> groupByExprList;
  @Getter
  private final NamedExpression span;
  /**
   * {@link BindingTuple} Collector.
   */
  @EqualsAndHashCode.Exclude
  private final Collector collector;
  @EqualsAndHashCode.Exclude
  private Iterator<ExprValue> iterator;

  /**
   * AggregationOperator Constructor.
   *
   * @param input           Input {@link PhysicalPlan}
   * @param aggregatorList  List of {@link Aggregator}
   * @param groupByExprList List of group by {@link Expression}
   */
  public AggregationOperator(PhysicalPlan input, List<NamedAggregator> aggregatorList,
                             List<NamedExpression> groupByExprList) {
    this.input = input;
    this.aggregatorList = aggregatorList;
    if (hasSpan(groupByExprList)) {
      this.span = groupByExprList.get(0);
      this.groupByExprList = groupByExprList.subList(1, groupByExprList.size());
    } else {
      this.span = null;
      this.groupByExprList = groupByExprList;
    }
    this.collector = Collector.Builder.build(this.span, this.groupByExprList, this.aggregatorList);
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitAggregation(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }


  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public void open() {
    super.open();
    while (input.hasNext()) {
      collector.collect(input.next().bindingTuples());
    }
    iterator = collector.results().iterator();
  }

  private boolean hasSpan(List<NamedExpression> namedExpressionList) {
    return !namedExpressionList.isEmpty()
        && namedExpressionList.get(0).getDelegated() instanceof SpanExpression;
  }
}
