

package org.elasticsearch.sql.data.utils;

import com.google.common.collect.Ordering;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.data.model.ExprValue;

/**
 * Idea from guava {@link Ordering}. The only difference is the special logic to handle {@link
 * org.elasticsearch.sql.data.model.ExprNullValue} and {@link
 * org.elasticsearch.sql.data.model.ExprMissingValue}
 */
@RequiredArgsConstructor
public class ReverseExprValueOrdering extends ExprValueOrdering {
  private final ExprValueOrdering forwardOrder;

  @Override
  public int compare(ExprValue left, ExprValue right) {
    return forwardOrder.compare(right, left);
  }

  @Override
  public ExprValueOrdering reverse() {
    return forwardOrder;
  }
}
