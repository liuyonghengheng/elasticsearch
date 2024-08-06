

package org.elasticsearch.sql.expression.window.ranking;

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.sql.data.model.ExprIntegerValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.FunctionExpression;
import org.elasticsearch.sql.expression.env.Environment;
import org.elasticsearch.sql.expression.function.FunctionName;
import org.elasticsearch.sql.expression.window.WindowDefinition;
import org.elasticsearch.sql.expression.window.WindowFunctionExpression;
import org.elasticsearch.sql.expression.window.frame.CurrentRowWindowFrame;
import org.elasticsearch.sql.expression.window.frame.WindowFrame;
import org.elasticsearch.sql.storage.bindingtuple.BindingTuple;

/**
 * Ranking window function base class that captures same info across different ranking functions,
 * such as same return type (integer), same argument list (no arg).
 */
public abstract class RankingWindowFunction extends FunctionExpression
                                            implements WindowFunctionExpression {

  /**
   * Current rank number assigned.
   */
  protected int rank;

  public RankingWindowFunction(FunctionName functionName) {
    super(functionName, emptyList());
  }

  @Override
  public ExprType type() {
    return ExprCoreType.INTEGER;
  }

  @Override
  public WindowFrame createWindowFrame(WindowDefinition definition) {
    return new CurrentRowWindowFrame(definition);
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    return new ExprIntegerValue(rank((CurrentRowWindowFrame) valueEnv));
  }

  /**
   * Rank logic that sub-class needs to implement.
   * @param frame   window frame
   * @return        rank number
   */
  protected abstract int rank(CurrentRowWindowFrame frame);

  /**
   * Check sort field to see if current value is different from previous.
   * @param frame   window frame
   * @return        true if different, false if same or no sort list defined
   */
  protected boolean isSortFieldValueDifferent(CurrentRowWindowFrame frame) {
    if (isSortItemsNotDefined(frame)) {
      return false;
    }

    List<Expression> sortItems = frame.getWindowDefinition()
                                      .getSortList()
                                      .stream()
                                      .map(Pair::getRight)
                                      .collect(Collectors.toList());

    List<ExprValue> previous = resolve(frame, sortItems, frame.previous());
    List<ExprValue> current = resolve(frame, sortItems, frame.current());
    return !current.equals(previous);
  }

  private boolean isSortItemsNotDefined(CurrentRowWindowFrame frame) {
    return frame.getWindowDefinition().getSortList().isEmpty();
  }

  private List<ExprValue> resolve(WindowFrame frame, List<Expression> expressions, ExprValue row) {
    BindingTuple valueEnv = row.bindingTuples();
    return expressions.stream()
                      .map(expr -> expr.valueOf(valueEnv))
                      .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return getFunctionName() + "()";
  }
}
