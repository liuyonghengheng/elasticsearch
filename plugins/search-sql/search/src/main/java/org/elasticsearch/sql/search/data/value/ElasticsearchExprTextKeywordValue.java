

package org.elasticsearch.sql.search.data.value;

import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_TEXT_KEYWORD;

import org.elasticsearch.sql.data.model.ExprStringValue;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Expression Text Keyword Value, it is an extension of the ExprValue by Elasticsearch.
 * This mostly represents a multi-field in Elasticsearch which has a text field and a
 * keyword field inside to preserve the original text.
 */
public class ElasticsearchExprTextKeywordValue extends ExprStringValue {

  public ElasticsearchExprTextKeywordValue(String value) {
    super(value);
  }

  @Override
  public ExprType type() {
    return EASYSEARCH_TEXT_KEYWORD;
  }

}
