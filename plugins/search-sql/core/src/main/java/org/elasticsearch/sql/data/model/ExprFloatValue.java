

package org.elasticsearch.sql.data.model;

import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Expression Float Value.
 */
public class ExprFloatValue extends AbstractExprNumberValue {

  public ExprFloatValue(Number value) {
    super(value);
  }

  @Override
  public Object value() {
    return floatValue();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.FLOAT;
  }

  @Override
  public String toString() {
    return floatValue().toString();
  }

  @Override
  public int compare(ExprValue other) {
    return Float.compare(floatValue(), other.floatValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return floatValue().equals(other.floatValue());
  }
}
