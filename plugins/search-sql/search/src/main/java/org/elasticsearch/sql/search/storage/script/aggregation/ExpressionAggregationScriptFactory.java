

package org.elasticsearch.sql.search.storage.script.aggregation;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.sql.expression.Expression;

/**
 * Aggregation Expression script factory that generates leaf factory.
 */
@EqualsAndHashCode
public class ExpressionAggregationScriptFactory implements AggregationScript.Factory {

  private final Expression expression;

  public ExpressionAggregationScriptFactory(Expression expression) {
    this.expression = expression;
  }

  @Override
  public boolean isResultDeterministic() {
    // This implies the results are cacheable
    return true;
  }

  @Override
  public AggregationScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
    return new ExpressionAggregationScriptLeafFactory(expression, params, lookup);
  }
}
