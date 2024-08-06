


package org.elasticsearch.sql.search.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.elasticsearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.sql.common.setting.Settings;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.model.ExprValueUtils;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.search.data.value.ElasticsearchExprValueFactory;
import org.elasticsearch.sql.search.request.ElasticsearchQueryRequest;
import org.elasticsearch.sql.search.request.ElasticsearchRequest;
import org.elasticsearch.sql.search.response.ElasticsearchResponse;

@ExtendWith(MockitoExtension.class)
class ElasticsearchIndexScanTest {

  @Mock
  private ElasticsearchClient client;

  @Mock
  private Settings settings;

  private ElasticsearchExprValueFactory exprValueFactory = new ElasticsearchExprValueFactory(
      ImmutableMap.of("name", STRING, "department", STRING));

  @BeforeEach
  void setup() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
  }

  @Test
  void queryEmptyResult() {
    mockResponse();
    try (ElasticsearchIndexScan indexScan =
             new ElasticsearchIndexScan(client, settings, "test", exprValueFactory)) {
      indexScan.open();
      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void queryAllResults() {
    mockResponse(
        new ExprValue[]{employee(1, "John", "IT"), employee(2, "Smith", "HR")},
        new ExprValue[]{employee(3, "Allen", "IT")});

    try (ElasticsearchIndexScan indexScan =
             new ElasticsearchIndexScan(client, settings, "employees", exprValueFactory)) {
      indexScan.open();

      assertTrue(indexScan.hasNext());
      assertEquals(employee(1, "John", "IT"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(2, "Smith", "HR"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(3, "Allen", "IT"), indexScan.next());

      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void pushDownFilters() {
    assertThat()
        .pushDown(QueryBuilders.termQuery("name", "John"))
        .shouldQuery(QueryBuilders.termQuery("name", "John"))
        .pushDown(QueryBuilders.termQuery("age", 30))
        .shouldQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("name", "John"))
                .filter(QueryBuilders.termQuery("age", 30)))
        .pushDown(QueryBuilders.rangeQuery("balance").gte(10000))
        .shouldQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("name", "John"))
                .filter(QueryBuilders.termQuery("age", 30))
                .filter(QueryBuilders.rangeQuery("balance").gte(10000)));
  }

  private PushDownAssertion assertThat() {
    return new PushDownAssertion(client, exprValueFactory, settings);
  }

  private static class PushDownAssertion {
    private final ElasticsearchClient client;
    private final ElasticsearchIndexScan indexScan;
    private final ElasticsearchResponse response;
    private final ElasticsearchExprValueFactory factory;

    public PushDownAssertion(ElasticsearchClient client,
                             ElasticsearchExprValueFactory valueFactory,
                             Settings settings) {
      this.client = client;
      this.indexScan = new ElasticsearchIndexScan(client, settings, "test", valueFactory);
      this.response = mock(ElasticsearchResponse.class);
      this.factory = valueFactory;
      when(response.isEmpty()).thenReturn(true);
    }

    PushDownAssertion pushDown(QueryBuilder query) {
      indexScan.pushDown(query);
      return this;
    }

    PushDownAssertion shouldQuery(QueryBuilder expected) {
      ElasticsearchRequest request = new ElasticsearchQueryRequest("test", 200, factory);
      request.getSourceBuilder()
             .query(expected)
             .sort(DOC_FIELD_NAME, ASC);
      when(client.search(request)).thenReturn(response);
      indexScan.open();
      return this;
    }
  }

  private void mockResponse(ExprValue[]... searchHitBatches) {
    when(client.search(any()))
        .thenAnswer(
            new Answer<ElasticsearchResponse>() {
              private int batchNum;

              @Override
              public ElasticsearchResponse answer(InvocationOnMock invocation) {
                ElasticsearchResponse response = mock(ElasticsearchResponse.class);
                int totalBatch = searchHitBatches.length;
                if (batchNum < totalBatch) {
                  when(response.isEmpty()).thenReturn(false);
                  ExprValue[] searchHit = searchHitBatches[batchNum];
                  when(response.iterator()).thenReturn(Arrays.asList(searchHit).iterator());
                } else if (batchNum == totalBatch) {
                  when(response.isEmpty()).thenReturn(true);
                } else {
                  fail("Search request after empty response returned already");
                }

                batchNum++;
                return response;
              }
            });
  }

  protected ExprValue employee(int docId, String name, String department) {
    SearchHit hit = new SearchHit(docId);
    hit.sourceRef(
        new BytesArray("{\"name\":\"" + name + "\",\"department\":\"" + department + "\"}"));
    return tupleValue(hit);
  }

  private ExprValue tupleValue(SearchHit hit) {
    return ExprValueUtils.tupleValue(hit.getSourceAsMap());
  }
}
