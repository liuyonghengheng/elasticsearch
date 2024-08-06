


package org.elasticsearch.sql.search.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.sql.search.data.value.ElasticsearchExprValueFactory;

@ExtendWith(MockitoExtension.class)
class ElasticsearchScrollRequestTest {

  @Mock
  private ElasticsearchExprValueFactory factory;

  private final ElasticsearchScrollRequest request =
      new ElasticsearchScrollRequest("test", factory);

  @Test
  void searchRequest() {
    request.getSourceBuilder().query(QueryBuilders.termQuery("name", "John"));

    assertEquals(
        new SearchRequest()
            .indices("test")
            .scroll(ElasticsearchScrollRequest.DEFAULT_SCROLL_TIMEOUT)
            .source(new SearchSourceBuilder().query(QueryBuilders.termQuery("name", "John"))),
        request.searchRequest());
  }

  @Test
  void isScrollStarted() {
    assertFalse(request.isScrollStarted());

    request.setScrollId("scroll123");
    assertTrue(request.isScrollStarted());
  }

  @Test
  void scrollRequest() {
    request.setScrollId("scroll123");
    assertEquals(
        new SearchScrollRequest()
            .scroll(ElasticsearchScrollRequest.DEFAULT_SCROLL_TIMEOUT)
            .scrollId("scroll123"),
        request.scrollRequest());
  }
}
