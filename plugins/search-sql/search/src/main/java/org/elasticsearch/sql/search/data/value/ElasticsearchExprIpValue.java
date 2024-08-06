

package org.elasticsearch.sql.search.data.value;

import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_IP;

import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.data.model.AbstractExprValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Elasticsearch IP ExprValue.
 * Todo, add this to avoid the unknown value type exception, the implementation will be changed.
 */
@RequiredArgsConstructor
public class ElasticsearchExprIpValue extends AbstractExprValue {

  private final String ip;

  @Override
  public Object value() {
    return ip;
  }

  @Override
  public ExprType type() {
    return EASYSEARCH_IP;
  }

  @Override
  public int compare(ExprValue other) {
    return ip.compareTo(((ElasticsearchExprIpValue) other).ip);
  }

  @Override
  public boolean equal(ExprValue other) {
    return ip.equals(((ElasticsearchExprIpValue) other).ip);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(ip);
  }
}
