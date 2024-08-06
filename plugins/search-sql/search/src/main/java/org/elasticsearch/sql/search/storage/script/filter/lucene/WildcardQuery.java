

package org.elasticsearch.sql.search.storage.script.filter.lucene;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Lucene query that builds wildcard query.
 */
public class WildcardQuery extends LuceneQuery {

  @Override
  protected QueryBuilder doBuild(String fieldName, ExprType fieldType, ExprValue literal) {
    fieldName = convertTextToKeyword(fieldName, fieldType);
    String matchText = convertSqlWildcardToLucene(literal.stringValue());
    return QueryBuilders.wildcardQuery(fieldName, matchText);
  }

  private String convertSqlWildcardToLucene(String text) {
    return text.replace('%', '*')
               .replace('_', '?');
  }

}
