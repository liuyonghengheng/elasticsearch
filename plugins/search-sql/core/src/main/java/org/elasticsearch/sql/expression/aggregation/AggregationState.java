

package org.elasticsearch.sql.expression.aggregation;

import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.storage.bindingtuple.BindingTuple;

/**
 * Maintain the state when {@link Aggregator} iterate on the {@link BindingTuple}.
 */
public interface AggregationState {
  /**
   * Get {@link ExprValue} result.
   */
  ExprValue result();
}
