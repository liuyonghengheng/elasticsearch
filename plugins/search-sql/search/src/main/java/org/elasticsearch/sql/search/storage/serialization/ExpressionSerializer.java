

package org.elasticsearch.sql.search.storage.serialization;

import org.elasticsearch.sql.expression.Expression;

/**
 * Expression serializer that (de-)serializes expression object.
 */
public interface ExpressionSerializer {

  /**
   * Serialize an expression.
   * @param expr  expression
   * @return      serialized string
   */
  String serialize(Expression expr);

  /**
   * Deserialize an expression.
   * @param code  serialized code
   * @return      original expression object
   */
  Expression deserialize(String code);

}
