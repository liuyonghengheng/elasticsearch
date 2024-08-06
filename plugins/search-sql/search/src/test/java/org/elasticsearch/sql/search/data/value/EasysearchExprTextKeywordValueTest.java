


package org.elasticsearch.sql.search.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_TEXT_KEYWORD;

import org.junit.jupiter.api.Test;

class ElasticsearchExprTextKeywordValueTest {

  @Test
  public void testTypeOfExprTextKeywordValue() {
    assertEquals(EASYSEARCH_TEXT_KEYWORD, new ElasticsearchExprTextKeywordValue("A").type());
  }

}
