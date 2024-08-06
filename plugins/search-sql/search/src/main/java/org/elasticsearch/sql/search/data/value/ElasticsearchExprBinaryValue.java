

package org.elasticsearch.sql.search.data.value;

import lombok.EqualsAndHashCode;
import org.elasticsearch.sql.data.model.AbstractExprValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.search.data.type.ElasticsearchDataType;

/**
 * Elasticsearch BinaryValue.
 * Todo, add this to avoid the unknown value type exception, the implementation will be changed.
 */
@EqualsAndHashCode(callSuper = false)
public class ElasticsearchExprBinaryValue extends AbstractExprValue {
  private final String encodedString;

  public ElasticsearchExprBinaryValue(String encodedString) {
    this.encodedString = encodedString;
  }

  @Override
  public int compare(ExprValue other) {
    return encodedString.compareTo((String) other.value());
  }

  @Override
  public boolean equal(ExprValue other) {
    return encodedString.equals(other.value());
  }

  @Override
  public Object value() {
    return encodedString;
  }

  @Override
  public ExprType type() {
    return ElasticsearchDataType.EASYSEARCH_BINARY;
  }
}
