

package org.elasticsearch.sql.data.model;

import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Expression Double Value.
 */
public class ExprDoubleValue extends AbstractExprNumberValue {

  public ExprDoubleValue(Number value) {
    super(value);
  }

  @Override
  public Object value() {
    return doubleValue();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.DOUBLE;
  }

  @Override
  public String toString() {
    return doubleValue().toString();
  }

  @Override
  public int compare(ExprValue other) {
    return Double.compare(doubleValue(), other.doubleValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return doubleValue().equals(other.doubleValue());
  }
}
