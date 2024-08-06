

package org.elasticsearch.sql.search.data.value;

import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_GEO_POINT;

import java.util.Objects;
import lombok.Data;
import org.elasticsearch.sql.data.model.AbstractExprValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Elasticsearch GeoPointValue.
 * Todo, add this to avoid the unknown value type exception, the implementation will be changed.
 */
public class ElasticsearchExprGeoPointValue extends AbstractExprValue {

  private final GeoPoint geoPoint;

  public ElasticsearchExprGeoPointValue(Double lat, Double lon) {
    this.geoPoint = new GeoPoint(lat, lon);
  }

  @Override
  public Object value() {
    return geoPoint;
  }

  @Override
  public ExprType type() {
    return EASYSEARCH_GEO_POINT;
  }

  @Override
  public int compare(ExprValue other) {
    return geoPoint.toString()
        .compareTo((((ElasticsearchExprGeoPointValue) other).geoPoint).toString());
  }

  @Override
  public boolean equal(ExprValue other) {
    return geoPoint.equals(((ElasticsearchExprGeoPointValue) other).geoPoint);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(geoPoint);
  }

  @Data
  public static class GeoPoint {

    private final Double lat;

    private final Double lon;
  }
}
