

package org.elasticsearch.sql.search.storage.script.aggregation.dsl;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.script.Script.DEFAULT_SCRIPT_TYPE;
import static org.elasticsearch.sql.search.storage.script.ExpressionScriptEngine.EXPRESSION_LANG_NAME;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.script.Script;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.FunctionExpression;
import org.elasticsearch.sql.expression.ReferenceExpression;
import org.elasticsearch.sql.search.storage.script.ScriptUtils;
import org.elasticsearch.sql.search.storage.serialization.ExpressionSerializer;

/**
 * Abstract Aggregation Builder.
 */
@RequiredArgsConstructor
public class AggregationBuilderHelper {

  private final ExpressionSerializer serializer;

  /**
   * Build AggregationBuilder from Expression.
   *
   * @param expression Expression
   * @return AggregationBuilder
   */
  public <T> T build(Expression expression, Function<String, T> fieldBuilder,
                 Function<Script, T> scriptBuilder) {
    if (expression instanceof ReferenceExpression) {
      String fieldName = ((ReferenceExpression) expression).getAttr();
      return fieldBuilder.apply(ScriptUtils.convertTextToKeyword(fieldName, expression.type()));
    } else if (expression instanceof FunctionExpression) {
      return scriptBuilder.apply(new Script(
          DEFAULT_SCRIPT_TYPE, EXPRESSION_LANG_NAME, serializer.serialize(expression),
          emptyMap()));
    } else {
      throw new IllegalStateException(String.format("metric aggregation doesn't support "
          + "expression %s", expression));
    }
  }
}
