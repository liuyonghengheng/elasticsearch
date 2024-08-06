


package org.elasticsearch.sql.search.storage.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.elasticsearch.sql.expression.DSL.literal;
import static org.elasticsearch.sql.expression.DSL.ref;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.ExpressionNodeVisitor;
import org.elasticsearch.sql.expression.config.ExpressionConfig;
import org.elasticsearch.sql.expression.env.Environment;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DefaultExpressionSerializerTest {

  /**
   * Initialize function repository manually to avoid dependency on Spring container.
   */
  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());

  private final ExpressionSerializer serializer = new DefaultExpressionSerializer();

  @Test
  public void can_serialize_and_deserialize_literals() {
    Expression original = literal(10);
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Test
  public void can_serialize_and_deserialize_references() {
    Expression original = ref("name", STRING);
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Test
  public void can_serialize_and_deserialize_predicates() {
    Expression original = dsl.or(literal(true), dsl.less(literal(1), literal(2)));
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Disabled("Bypass until all functions become serializable")
  @Test
  public void can_serialize_and_deserialize_functions() {
    Expression original = dsl.abs(literal(30.0));
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Test
  public void cannot_serialize_illegal_expression() {
    Expression illegalExpr = new Expression() {
      private final Object object = new Object(); // non-serializable
      @Override
      public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
        return null;
      }

      @Override
      public ExprType type() {
        return null;
      }

      @Override
      public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
        return null;
      }
    };
    assertThrows(IllegalStateException.class, () -> serializer.serialize(illegalExpr));
  }

  @Test
  public void cannot_deserialize_illegal_expression_code() {
    assertThrows(IllegalStateException.class, () -> serializer.deserialize("hello world"));
  }

}
