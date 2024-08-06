

package org.elasticsearch.sql.search.storage.script.filter;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.sql.expression.Expression;

/**
 * Expression script factory that generates leaf factory.
 */
@EqualsAndHashCode
public class ExpressionFilterScriptFactory implements FilterScript.Factory {

  /**
   * Expression to execute.
   */
  private final Expression expression;

  public ExpressionFilterScriptFactory(Expression expression) {
    this.expression = expression;
  }

  @Override
  public boolean isResultDeterministic() {
    // This implies the results are cacheable
    return true;
  }

  @Override
  public FilterScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
    return new ExpressionFilterScriptLeafFactory(expression, params, lookup);
  }

}
