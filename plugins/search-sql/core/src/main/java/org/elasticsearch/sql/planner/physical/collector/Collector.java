
package org.elasticsearch.sql.planner.physical.collector;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.expression.aggregation.NamedAggregator;
import org.elasticsearch.sql.storage.bindingtuple.BindingTuple;

/**
 * Interface of {@link BindingTuple} Collector.
 */
public interface Collector {

  /**
   * Collect from {@link BindingTuple}.
   *
   * @param tuple {@link BindingTuple}.
   */
  void collect(BindingTuple tuple);

  /**
   * Get Result from Collector.
   *
   * @return list of {@link ExprValue}.
   */
  List<ExprValue> results();

  /**
   * {@link Collector} tree builder.
   */
  @UtilityClass
  class Builder {
    /**
     * build {@link Collector}.
     */
    public static Collector build(
        NamedExpression span, List<NamedExpression> buckets, List<NamedAggregator> aggregators) {
      if (span == null && buckets.isEmpty()) {
        return new MetricCollector(aggregators);
      } else if (span != null) {
        return new SpanCollector(span, () -> build(null, buckets, aggregators));
      } else {
        return new BucketCollector(
            buckets.get(0),
            () ->
                build(null, ImmutableList.copyOf(buckets.subList(1, buckets.size())), aggregators));
      }
    }
  }
}
