

package org.elasticsearch.sql.expression;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.expression.env.Environment;

/**
 * Literal Expression.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class LiteralExpression implements Expression {
  private final ExprValue exprValue;

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> env) {
    return exprValue;
  }

  @Override
  public ExprType type() {
    return exprValue.type();
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitLiteral(this, context);
  }

  @Override
  public String toString() {
    return exprValue.toString();
  }
}
