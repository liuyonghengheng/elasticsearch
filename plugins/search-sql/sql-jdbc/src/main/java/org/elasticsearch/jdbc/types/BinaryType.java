


package org.elasticsearch.jdbc.types;

import java.sql.SQLException;
import java.util.Map;

public class BinaryType implements TypeHelper<String> {

  public static final BinaryType INSTANCE = new BinaryType();

  private BinaryType() {

  }

  @Override
  public String fromValue(Object value, Map<String, Object> conversionParams) throws SQLException {
    if (value == null)
      return null;
    else
      return String.valueOf(value);
  }

  @Override
  public String getTypeName() {
    return "Binary";
  }
}
