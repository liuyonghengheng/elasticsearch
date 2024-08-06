


package org.elasticsearch.sql.search.response;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.sql.data.model.ExprIntegerValue;
import org.elasticsearch.sql.data.model.ExprTupleValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.search.data.value.ElasticsearchExprValueFactory;
import org.elasticsearch.sql.search.response.agg.ElasticsearchAggregationResponseParser;

@ExtendWith(MockitoExtension.class)
class ElasticsearchResponseTest {

  @Mock
  private SearchResponse searchResponse;

  @Mock
  private ElasticsearchExprValueFactory factory;

  @Mock
  private SearchHit searchHit1;

  @Mock
  private SearchHit searchHit2;

  @Mock
  private Aggregations aggregations;

  @Mock
  private ElasticsearchAggregationResponseParser parser;

  private ExprTupleValue exprTupleValue1 = ExprTupleValue.fromExprValueMap(ImmutableMap.of("id1",
      new ExprIntegerValue(1)));

  private ExprTupleValue exprTupleValue2 = ExprTupleValue.fromExprValueMap(ImmutableMap.of("id2",
      new ExprIntegerValue(2)));

  @Test
  void isEmpty() {
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit1, searchHit2},
                new TotalHits(2L, TotalHits.Relation.EQUAL_TO),
                1.0F));

    assertFalse(new ElasticsearchResponse(searchResponse, factory).isEmpty());

    when(searchResponse.getHits()).thenReturn(SearchHits.empty());
    when(searchResponse.getAggregations()).thenReturn(null);
    assertTrue(new ElasticsearchResponse(searchResponse, factory).isEmpty());

    when(searchResponse.getHits())
        .thenReturn(new SearchHits(null, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0));
    ElasticsearchResponse response3 = new ElasticsearchResponse(searchResponse, factory);
    assertTrue(response3.isEmpty());

    when(searchResponse.getHits()).thenReturn(SearchHits.empty());
    when(searchResponse.getAggregations()).thenReturn(new Aggregations(emptyList()));
    assertFalse(new ElasticsearchResponse(searchResponse, factory).isEmpty());
  }

  @Test
  void iterator() {
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit1, searchHit2},
                new TotalHits(2L, TotalHits.Relation.EQUAL_TO),
                1.0F));

    when(searchHit1.getSourceAsString()).thenReturn("{\"id1\", 1}");
    when(searchHit2.getSourceAsString()).thenReturn("{\"id1\", 2}");
    when(factory.construct(any())).thenReturn(exprTupleValue1).thenReturn(exprTupleValue2);

    int i = 0;
    for (ExprValue hit : new ElasticsearchResponse(searchResponse, factory)) {
      if (i == 0) {
        assertEquals(exprTupleValue1, hit);
      } else if (i == 1) {
        assertEquals(exprTupleValue2, hit);
      } else {
        fail("More search hits returned than expected");
      }
      i++;
    }
  }

  @Test
  void response_is_aggregation_when_aggregation_not_empty() {
    when(searchResponse.getAggregations()).thenReturn(aggregations);

    ElasticsearchResponse response = new ElasticsearchResponse(searchResponse, factory);
    assertTrue(response.isAggregationResponse());
  }

  @Test
  void response_isnot_aggregation_when_aggregation_is_empty() {
    when(searchResponse.getAggregations()).thenReturn(null);

    ElasticsearchResponse response = new ElasticsearchResponse(searchResponse, factory);
    assertFalse(response.isAggregationResponse());
  }

  @Test
  void aggregation_iterator() {
    when(parser.parse(any()))
        .thenReturn(Arrays.asList(ImmutableMap.of("id1", 1), ImmutableMap.of("id2", 2)));
    when(searchResponse.getAggregations()).thenReturn(aggregations);
    when(factory.getParser()).thenReturn(parser);
    when(factory.construct(anyString(), any()))
        .thenReturn(new ExprIntegerValue(1))
        .thenReturn(new ExprIntegerValue(2));

    int i = 0;
    for (ExprValue hit : new ElasticsearchResponse(searchResponse, factory)) {
      if (i == 0) {
        assertEquals(exprTupleValue1, hit);
      } else if (i == 1) {
        assertEquals(exprTupleValue2, hit);
      } else {
        fail("More search hits returned than expected");
      }
      i++;
    }
  }
}
