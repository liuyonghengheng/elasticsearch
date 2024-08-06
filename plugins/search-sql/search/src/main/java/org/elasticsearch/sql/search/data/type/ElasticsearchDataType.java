

package org.elasticsearch.sql.search.data.type;

import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.elasticsearch.sql.data.type.ExprCoreType.UNKNOWN;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * The extension of ExprType in Elasticsearch.
 */
@RequiredArgsConstructor
public enum ElasticsearchDataType implements ExprType {
  /**
   * Elasticsearch Text. Rather than cast text to other types (STRING), leave it alone to prevent
   * cast_to_string(EASYSEARCH_TEXT).
   * Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html
   */
  EASYSEARCH_TEXT(Collections.singletonList(STRING), "string") {
    @Override
    public boolean shouldCast(ExprType other) {
      return false;
    }
  },

  /**
   * Elasticsearch multi-fields which has text and keyword.
   * Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html
   */
  EASYSEARCH_TEXT_KEYWORD(Arrays.asList(STRING, EASYSEARCH_TEXT), "string") {
    @Override
    public boolean shouldCast(ExprType other) {
      return false;
    }
  },


  EASYSEARCH_IP(Arrays.asList(UNKNOWN), "ip"),

  EASYSEARCH_GEO_POINT(Arrays.asList(UNKNOWN), "geo_point"),

  EASYSEARCH_BINARY(Arrays.asList(UNKNOWN), "binary");

  /**
   * The mapping between Type and legacy JDBC type name.
   */
  private static final Map<ExprType, String> LEGACY_TYPE_NAME_MAPPING =
      new ImmutableMap.Builder<ExprType, String>()
          .put(EASYSEARCH_TEXT, "text")
          .put(EASYSEARCH_TEXT_KEYWORD, "text")
          .build();

  /**
   * Parent of current type.
   */
  private final List<ExprType> parents;
  /**
   * JDBC type name.
   */
  private final String jdbcType;

  @Override
  public List<ExprType> getParent() {
    return parents;
  }

  @Override
  public String typeName() {
    return jdbcType;
  }

  @Override
  public String legacyTypeName() {
    return LEGACY_TYPE_NAME_MAPPING.getOrDefault(this, typeName());
  }
}
