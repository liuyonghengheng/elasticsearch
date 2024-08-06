


package org.elasticsearch.sql.search.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_BINARY;

import org.junit.jupiter.api.Test;

public class ElasticsearchExprBinaryValueTest {

  @Test
  public void compare() {
    assertEquals(
        0,
        new ElasticsearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==")
            .compare(new ElasticsearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==")));
  }

  @Test
  public void equal() {
    ElasticsearchExprBinaryValue value =
        new ElasticsearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==");
    assertTrue(value.equal(new ElasticsearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==")));
  }

  @Test
  public void value() {
    ElasticsearchExprBinaryValue value =
        new ElasticsearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==");
    assertEquals("U29tZSBiaW5hcnkgYmxvYg==", value.value());
  }

  @Test
  public void type() {
    ElasticsearchExprBinaryValue value =
        new ElasticsearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==");
    assertEquals(EASYSEARCH_BINARY, value.type());
  }

}
