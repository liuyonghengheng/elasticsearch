


package org.elasticsearch.sql.search.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_GEO_POINT;

import org.junit.jupiter.api.Test;

class ElasticsearchExprGeoPointValueTest {

  private ElasticsearchExprGeoPointValue geoPointValue = new ElasticsearchExprGeoPointValue(1.0,
      1.0);

  @Test
  void value() {
    assertEquals(new ElasticsearchExprGeoPointValue.GeoPoint(1.0, 1.0), geoPointValue.value());
  }

  @Test
  void type() {
    assertEquals(EASYSEARCH_GEO_POINT, geoPointValue.type());
  }

  @Test
  void compare() {
    assertEquals(0, geoPointValue.compareTo(new ElasticsearchExprGeoPointValue(1.0, 1.0)));
  }

  @Test
  void equal() {
    assertTrue(geoPointValue.equal(new ElasticsearchExprGeoPointValue(1.0,
        1.0)));
  }

  @Test
  void testHashCode() {
    assertNotNull(geoPointValue.hashCode());
  }
}
