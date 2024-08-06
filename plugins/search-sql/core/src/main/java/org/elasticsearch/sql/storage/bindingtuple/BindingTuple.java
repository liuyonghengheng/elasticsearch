

package org.elasticsearch.sql.storage.bindingtuple;

import org.elasticsearch.sql.data.model.ExprMissingValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.exception.ExpressionEvaluationException;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.ReferenceExpression;
import org.elasticsearch.sql.expression.env.Environment;

/**
 * BindingTuple represents the a relationship between bindingName and ExprValue.
 * e.g. The operation output column name is bindingName, the value is the ExprValue.
 */
public abstract class BindingTuple implements Environment<Expression, ExprValue> {
  public static BindingTuple EMPTY = new BindingTuple() {
    @Override
    public ExprValue resolve(ReferenceExpression ref) {
      return ExprMissingValue.of();
    }
  };

  /**
   * Resolve {@link Expression} in the BindingTuple environment.
   */
  @Override
  public ExprValue resolve(Expression var) {
    if (var instanceof ReferenceExpression) {
      return resolve(((ReferenceExpression) var));
    } else {
      throw new ExpressionEvaluationException(String.format("can resolve expression: %s", var));
    }
  }

  /**
   * Resolve the {@link ReferenceExpression} in BindingTuple context.
   */
  public abstract ExprValue resolve(ReferenceExpression ref);
}
