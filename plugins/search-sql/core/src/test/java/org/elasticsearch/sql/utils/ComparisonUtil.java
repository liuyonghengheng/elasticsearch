


package org.elasticsearch.sql.utils;

import static org.elasticsearch.sql.data.model.ExprValueUtils.getDoubleValue;
import static org.elasticsearch.sql.data.model.ExprValueUtils.getFloatValue;
import static org.elasticsearch.sql.data.model.ExprValueUtils.getIntegerValue;
import static org.elasticsearch.sql.data.model.ExprValueUtils.getLongValue;
import static org.elasticsearch.sql.data.model.ExprValueUtils.getStringValue;

import org.elasticsearch.sql.data.model.ExprByteValue;
import org.elasticsearch.sql.data.model.ExprDoubleValue;
import org.elasticsearch.sql.data.model.ExprFloatValue;
import org.elasticsearch.sql.data.model.ExprIntegerValue;
import org.elasticsearch.sql.data.model.ExprLongValue;
import org.elasticsearch.sql.data.model.ExprShortValue;
import org.elasticsearch.sql.data.model.ExprStringValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.exception.ExpressionEvaluationException;

public class ComparisonUtil {
  /**
   * Util to compare the object (integer, long, float, double, string) values.
   * ExprValue A
   */
  public static int compare(ExprValue v1, ExprValue v2) {
    if (v1.isMissing() || v2.isMissing()) {
      throw new ExpressionEvaluationException("invalid to call compare operation on missing value");
    } else if (v1.isNull() || v2.isNull()) {
      throw new ExpressionEvaluationException("invalid to call compare operation on null value");
    }

    if (v1 instanceof ExprByteValue) {
      return v1.byteValue().compareTo(v2.byteValue());
    } else if (v1 instanceof ExprShortValue) {
      return v1.shortValue().compareTo(v2.shortValue());
    } else if (v1 instanceof ExprIntegerValue) {
      return getIntegerValue(v1).compareTo(getIntegerValue(v2));
    } else if (v1 instanceof ExprLongValue) {
      return getLongValue(v1).compareTo(getLongValue(v2));
    } else if (v1 instanceof ExprFloatValue) {
      return getFloatValue(v1).compareTo(getFloatValue(v2));
    } else if (v1 instanceof ExprDoubleValue) {
      return getDoubleValue(v1).compareTo(getDoubleValue(v2));
    } else if (v1 instanceof ExprStringValue) {
      return getStringValue(v1).compareTo(getStringValue(v2));
    } else {
      throw new ExpressionEvaluationException(
          String.format("%s instances are not comparable", v1.getClass().getSimpleName()));
    }
  }
}
