


package org.elasticsearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.elasticsearch.sql.utils.ComparisonUtil.compare;

import org.junit.jupiter.api.Test;
import org.elasticsearch.sql.exception.ExpressionEvaluationException;

public class ExprBooleanValueTest {

  @Test
  public void comparabilityTest() {
    ExprValue booleanValue = ExprValueUtils.booleanValue(false);
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> compare(booleanValue, booleanValue));
    assertEquals("ExprBooleanValue instances are not comparable", exception.getMessage());
  }
}
