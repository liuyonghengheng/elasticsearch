
package org.elasticsearch.sql.planner.physical.collector;

import java.util.function.Supplier;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.expression.span.SpanExpression;
import org.elasticsearch.sql.storage.bindingtuple.BindingTuple;

/**
 * Span Collector.
 */
public class SpanCollector extends BucketCollector {

  /**
   * Span Expression.
   */
  private final SpanExpression spanExpr;

  /**
   * Rounding.
   */
  private final Rounding<?> rounding;

  /**
   * Constructor.
   */
  public SpanCollector(NamedExpression bucketExpr, Supplier<Collector> supplier) {
    super(bucketExpr, supplier);
    this.spanExpr = (SpanExpression) bucketExpr.getDelegated();
    this.rounding = Rounding.createRounding(spanExpr);
  }

  /**
   * Rounding bucket value.
   *
   * @param tuple {@link BindingTuple}.
   * @return {@link ExprValue}.
   */
  @Override
  protected ExprValue bucketKey(BindingTuple tuple) {
    return rounding.round(spanExpr.getField().valueOf(tuple));
  }

  /**
   * Allocates Buckets for building results.
   *
   * @return buckets.
   */
  @Override
  protected ExprValue[] allocateBuckets() {
    return rounding.createBuckets();
  }

  /**
   * Current Bucket index in allocated buckets.
   *
   * @param value bucket key.
   * @return index.
   */
  @Override
  protected int locateBucket(ExprValue value) {
    return rounding.locate(value);
  }
}
