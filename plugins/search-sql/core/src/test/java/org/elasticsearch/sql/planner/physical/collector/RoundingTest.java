

package org.elasticsearch.sql.planner.physical.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.elasticsearch.sql.data.type.ExprCoreType.TIME;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.elasticsearch.sql.data.model.ExprTimeValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.model.ExprValueUtils;
import org.elasticsearch.sql.exception.ExpressionEvaluationException;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.span.SpanExpression;

public class RoundingTest {
  @Test
  void time_rounding_illegal_span() {
    SpanExpression span = DSL.span(DSL.ref("time", TIME), DSL.literal(1), "d");
    Rounding rounding = Rounding.createRounding(span);
    assertThrows(ExpressionEvaluationException.class,
        () -> rounding.round(new ExprTimeValue("23:30:00")));
  }

  @Test
  void round_unknown_type() {
    SpanExpression span = DSL.span(DSL.ref("unknown", STRING), DSL.literal(1), "");
    Rounding rounding = Rounding.createRounding(span);
    assertNull(rounding.round(ExprValueUtils.integerValue(1)));
    assertNull(rounding.locate(ExprValueUtils.integerValue(1)));
    assertEquals(0, rounding.createBuckets().length);
  }

  @Test
  void resolve() {
    String illegalUnit = "illegal";
    assertThrows(IllegalArgumentException.class,
        () -> Rounding.DateTimeUnit.resolve(illegalUnit),
        "Unable to resolve unit " + illegalUnit);
  }
}
