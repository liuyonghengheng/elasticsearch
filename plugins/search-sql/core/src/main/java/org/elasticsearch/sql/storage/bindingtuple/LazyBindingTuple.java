

package org.elasticsearch.sql.storage.bindingtuple;

import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.data.model.ExprTupleValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.expression.ReferenceExpression;

/**
 * Lazy Implementation of {@link BindingTuple}.
 */
@RequiredArgsConstructor
public class LazyBindingTuple extends BindingTuple {
  private final Supplier<ExprTupleValue> lazyBinding;

  @Override
  public ExprValue resolve(ReferenceExpression ref) {
    return ref.resolve(lazyBinding.get());
  }
}
