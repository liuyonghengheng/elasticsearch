

package org.elasticsearch.sql.expression.function;

import java.util.List;
import org.elasticsearch.sql.expression.Expression;

/**
 * The definition of Function Implementation.
 */
public interface FunctionImplementation {

  /**
   * Get Function Name.
   */
  FunctionName getFunctionName();

  /**
   * Get Function Arguments.
   */
  List<Expression> getArguments();
}
