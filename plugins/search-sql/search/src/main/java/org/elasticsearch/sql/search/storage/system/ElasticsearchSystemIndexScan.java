

package org.elasticsearch.sql.search.storage.system;

import java.util.Iterator;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.search.request.system.ElasticsearchSystemRequest;
import org.elasticsearch.sql.storage.TableScanOperator;

/**
 * Elasticsearch index scan operator.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class ElasticsearchSystemIndexScan extends TableScanOperator {
  /**
   * Elasticsearch client.
   */
  private final ElasticsearchSystemRequest request;

  /**
   * Search response for current batch.
   */
  private Iterator<ExprValue> iterator;

  @Override
  public void open() {
    iterator = request.search().iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public String explain() {
    return request.toString();
  }
}
