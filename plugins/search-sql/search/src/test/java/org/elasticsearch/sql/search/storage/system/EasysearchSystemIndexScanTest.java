


package org.elasticsearch.sql.search.storage.system;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.elasticsearch.sql.data.model.ExprValueUtils.stringValue;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.search.request.system.ElasticsearchSystemRequest;

@ExtendWith(MockitoExtension.class)
class ElasticsearchSystemIndexScanTest {

  @Mock
  private ElasticsearchSystemRequest request;

  @Test
  public void queryData() {
    when(request.search()).thenReturn(Collections.singletonList(stringValue("text")));
    final ElasticsearchSystemIndexScan systemIndexScan = new ElasticsearchSystemIndexScan(request);

    systemIndexScan.open();
    assertTrue(systemIndexScan.hasNext());
    assertEquals(stringValue("text"), systemIndexScan.next());
  }

  @Test
  public void explain() {
    when(request.toString()).thenReturn("request");
    final ElasticsearchSystemIndexScan systemIndexScan = new ElasticsearchSystemIndexScan(request);

    assertEquals("request", systemIndexScan.explain());
  }
}
