


package org.elasticsearch.sql.search.request.system;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.elasticsearch.sql.data.model.ExprValueUtils.stringValue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.search.client.ElasticsearchClient;

@ExtendWith(MockitoExtension.class)
class ElasticsearchCatIndicesRequestTest {

  @Mock
  private ElasticsearchClient client;

  @Test
  void testSearch() {
    when(client.indices()).thenReturn(Arrays.asList("index"));

    final List<ExprValue> results = new ElasticsearchCatIndicesRequest(client).search();
    assertEquals(1, results.size());
    assertThat(results.get(0).tupleValue(), anyOf(
        hasEntry("TABLE_NAME", stringValue("index"))
    ));
  }

  @Test
  void testToString() {
    assertEquals("ElasticsearchCatIndicesRequest{}",
        new ElasticsearchCatIndicesRequest(client).toString());
  }
}
