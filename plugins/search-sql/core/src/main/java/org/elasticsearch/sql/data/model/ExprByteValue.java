

package org.elasticsearch.sql.data.model;

import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Expression Byte Value.
 */
public class ExprByteValue extends AbstractExprNumberValue {

  public ExprByteValue(Number value) {
    super(value);
  }

  @Override
  public int compare(ExprValue other) {
    return Byte.compare(byteValue(), other.byteValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return byteValue().equals(other.byteValue());
  }

  @Override
  public Object value() {
    return byteValue();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.BYTE;
  }

  @Override
  public String toString() {
    return value().toString();
  }
}
