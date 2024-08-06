

package org.elasticsearch.sql.search.data.value;

import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_TEXT;

import org.elasticsearch.sql.data.model.ExprStringValue;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Expression Text Value, it is a extension of the ExprValue by Elasticsearch.
 */
public class ElasticsearchExprTextValue extends ExprStringValue {
  public ElasticsearchExprTextValue(String value) {
    super(value);
  }

  @Override
  public ExprType type() {
    return EASYSEARCH_TEXT;
  }
}
