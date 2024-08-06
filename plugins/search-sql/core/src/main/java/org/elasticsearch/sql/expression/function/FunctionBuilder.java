

package org.elasticsearch.sql.expression.function;

import java.util.List;
import org.elasticsearch.sql.expression.Expression;

/**
 * The definition of function which create {@link FunctionImplementation}
 * from input {@link Expression} list.
 */
public interface FunctionBuilder {

  /**
   * Create {@link FunctionImplementation} from input {@link Expression} list.
   *
   * @param arguments {@link Expression} list
   * @return {@link FunctionImplementation}
   */
  FunctionImplementation apply(List<Expression> arguments);
}
