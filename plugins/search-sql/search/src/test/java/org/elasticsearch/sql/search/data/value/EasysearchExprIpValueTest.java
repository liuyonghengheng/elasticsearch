


package org.elasticsearch.sql.search.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_IP;

import org.junit.jupiter.api.Test;

public class ElasticsearchExprIpValueTest {

  private ElasticsearchExprIpValue ipValue = new ElasticsearchExprIpValue("192.168.0.1");

  @Test
  void value() {
    assertEquals("192.168.0.1", ipValue.value());
  }

  @Test
  void type() {
    assertEquals(EASYSEARCH_IP, ipValue.type());
  }

  @Test
  void compare() {
    assertEquals(0, ipValue.compareTo(new ElasticsearchExprIpValue("192.168.0.1")));
  }

  @Test
  void equal() {
    assertTrue(ipValue.equal(new ElasticsearchExprIpValue("192.168.0.1")));
  }

  @Test
  void testHashCode() {
    assertNotNull(ipValue.hashCode());
  }
}
