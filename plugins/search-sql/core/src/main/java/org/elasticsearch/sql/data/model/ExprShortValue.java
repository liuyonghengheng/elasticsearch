

package org.elasticsearch.sql.data.model;

import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Expression Short Value.
 */
public class ExprShortValue extends AbstractExprNumberValue {

  public ExprShortValue(Number value) {
    super(value);
  }

  @Override
  public Object value() {
    return shortValue();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.SHORT;
  }

  @Override
  public String toString() {
    return shortValue().toString();
  }

  @Override
  public int compare(ExprValue other) {
    return Short.compare(shortValue(), other.shortValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return shortValue().equals(other.shortValue());
  }
}
