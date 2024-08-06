

package org.elasticsearch.sql.search.storage.script.filter.lucene;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Lucene query that builds range query for non-quality comparison.
 */
@RequiredArgsConstructor
public class RangeQuery extends LuceneQuery {

  public enum Comparison {
    LT, GT, LTE, GTE, BETWEEN
  }

  /**
   * Comparison that range query build for.
   */
  private final Comparison comparison;

  @Override
  protected QueryBuilder doBuild(String fieldName, ExprType fieldType, ExprValue literal) {
    Object value = value(literal);

    RangeQueryBuilder query = QueryBuilders.rangeQuery(fieldName);
    switch (comparison) {
      case LT:
        return query.lt(value);
      case GT:
        return query.gt(value);
      case LTE:
        return query.lte(value);
      case GTE:
        return query.gte(value);
      default:
        throw new IllegalStateException("Comparison is supported by range query: " + comparison);
    }
  }

  private Object value(ExprValue literal) {
    if (literal.type().equals(ExprCoreType.TIMESTAMP)) {
      return literal.timestampValue().toEpochMilli();
    } else {
      return literal.value();
    }
  }

}
