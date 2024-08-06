


package org.elasticsearch.sql.search.request.system;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.elasticsearch.sql.data.model.ExprValueUtils.stringValue;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.search.mapping.IndexMapping;

@ExtendWith(MockitoExtension.class)
class ElasticsearchDescribeIndexRequestTest {

  @Mock
  private ElasticsearchClient client;

  @Test
  void testSearch() {
    when(client.getIndexMappings("index"))
        .thenReturn(
            ImmutableMap.of(
                "test",
                new IndexMapping(
                    ImmutableMap.<String, String>builder()
                        .put("name", "keyword")
                        .build())));

    final List<ExprValue> results = new ElasticsearchDescribeIndexRequest(client, "index").search();
    assertEquals(1, results.size());
    assertThat(results.get(0).tupleValue(), anyOf(
        hasEntry("TABLE_NAME", stringValue("index")),
        hasEntry("COLUMN_NAME", stringValue("name")),
        hasEntry("TYPE_NAME", stringValue("STRING"))
    ));
  }

  @Test
  void testToString() {
    assertEquals("ElasticsearchDescribeIndexRequest{indexName='index'}",
        new ElasticsearchDescribeIndexRequest(client, "index").toString());
  }

  @Test
  void filterOutUnknownType() {
    when(client.getIndexMappings("index"))
        .thenReturn(
            ImmutableMap.of(
                "test",
                new IndexMapping(
                    ImmutableMap.<String, String>builder()
                        .put("name", "keyword")
                        .put("@timestamp", "alias")
                        .build())));

    final Map<String, ExprType> fieldTypes =
        new ElasticsearchDescribeIndexRequest(client, "index").getFieldTypes();
    assertEquals(1, fieldTypes.size());
    assertThat(fieldTypes, hasEntry("name", STRING));
  }
}
