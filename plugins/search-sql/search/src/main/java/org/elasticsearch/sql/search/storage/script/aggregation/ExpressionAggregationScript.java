

package org.elasticsearch.sql.search.storage.script.aggregation;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.sql.data.model.ExprNullValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.env.Environment;
import org.elasticsearch.sql.search.storage.script.core.ExpressionScript;

/**
 * Aggregation expression script that executed on each document.
 */
@EqualsAndHashCode(callSuper = false)
public class ExpressionAggregationScript extends AggregationScript {

  /**
   * Expression Script.
   */
  private final ExpressionScript expressionScript;

  /**
   * Constructor of ExpressionAggregationScript.
   */
  public ExpressionAggregationScript(
      Expression expression,
      SearchLookup lookup,
      LeafReaderContext context,
      Map<String, Object> params) {
    super(params, lookup, context);
    this.expressionScript = new ExpressionScript(expression);
  }

  @Override
  public Object execute() {
    return expressionScript.execute(this::getDoc, this::evaluateExpression).value();
  }

  private ExprValue evaluateExpression(Expression expression, Environment<Expression,
                                       ExprValue> valueEnv) {
    ExprValue result = expression.valueOf(valueEnv);

    // The missing value is treated as null value in doc_value, so we can't distinguish with them.
    if (result.isNull()) {
      return ExprNullValue.of();
    }
    return result;
  }
}
