

package org.elasticsearch.sql.search.request.system;

import java.util.List;
import org.elasticsearch.sql.data.model.ExprValue;

/**
 * Elasticsearch system request query against the system index.
 */
public interface ElasticsearchSystemRequest {

  /**
   * Search.
   *
   * @return list of ExprValue.
   */
  List<ExprValue> search();
}
