

package org.elasticsearch.sql.expression.window;

import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.window.frame.WindowFrame;

/**
 * Window function abstraction.
 */
public interface WindowFunctionExpression extends Expression {

  /**
   * Create specific window frame based on window definition and what's current window function.
   * For now two types of cumulative window frame is returned:
   *  1. Ranking window functions: ignore frame definition and always operates on
   *      previous and current row.
   *  2. Aggregate window functions: frame partition into peers and sliding window is not supported.
   *
   * @param definition window definition
   * @return           window frame
   */
  WindowFrame createWindowFrame(WindowDefinition definition);

}
