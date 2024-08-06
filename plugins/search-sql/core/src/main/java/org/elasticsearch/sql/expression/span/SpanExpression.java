
package org.elasticsearch.sql.expression.span;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.sql.ast.expression.SpanUnit;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.ExpressionNodeVisitor;
import org.elasticsearch.sql.expression.env.Environment;

@RequiredArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
public class SpanExpression implements Expression {
  private final Expression field;
  private final Expression value;
  private final SpanUnit unit;

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    return value.valueOf(valueEnv);
  }

  /**
   * Return type follows the following table.
   *  FIELD         VALUE     RETURN_TYPE
   *  int/long      integer   int/long (field type)
   *  int/long      double    double
   *  float/double  integer   float/double (field type)
   *  float/double  double    float/double (field type)
   *  other         any       field type
   */
  @Override
  public ExprType type() {
    if (field.type().isCompatible(value.type())) {
      return field.type();
    } else if (value.type().isCompatible(field.type())) {
      return value.type();
    } else {
      return field.type();
    }
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }
}
