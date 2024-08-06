


package org.elasticsearch.sql.search.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_TEXT;

import org.junit.jupiter.api.Test;

class ElasticsearchExprTextValueTest {
  @Test
  public void typeOfExprTextValue() {
    assertEquals(EASYSEARCH_TEXT, new ElasticsearchExprTextValue("A").type());
  }
}
