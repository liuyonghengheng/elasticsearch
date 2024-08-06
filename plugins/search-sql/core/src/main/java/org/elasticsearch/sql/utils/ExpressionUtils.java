

package org.elasticsearch.sql.utils;

import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.elasticsearch.sql.expression.Expression;

/**
 * Utils for {@link Expression}.
 */
@UtilityClass
public class ExpressionUtils {

  public static String PATH_SEP = ".";

  /**
   * Format the list of {@link Expression}.
   */
  public static String format(List<Expression> expressionList) {
    return expressionList.stream().map(Expression::toString).collect(Collectors.joining(","));
  }
}
