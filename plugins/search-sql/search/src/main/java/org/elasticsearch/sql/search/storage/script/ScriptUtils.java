

package org.elasticsearch.sql.search.storage.script;

import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_TEXT_KEYWORD;

import lombok.experimental.UtilityClass;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Script Utils.
 */
@UtilityClass
public class ScriptUtils {

  /**
   * Text field doesn't have doc value (exception thrown even when you call "get")
   * Limitation: assume inner field name is always "keyword".
   */
  public static String convertTextToKeyword(String fieldName, ExprType fieldType) {
    if (fieldType == EASYSEARCH_TEXT_KEYWORD) {
      return fieldName + ".keyword";
    }
    return fieldName;
  }
}
