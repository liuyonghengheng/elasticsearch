

package org.elasticsearch.sql.search.data.value;

import static org.elasticsearch.sql.data.type.ExprCoreType.ARRAY;
import static org.elasticsearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.elasticsearch.sql.data.type.ExprCoreType.BYTE;
import static org.elasticsearch.sql.data.type.ExprCoreType.DATE;
import static org.elasticsearch.sql.data.type.ExprCoreType.DATETIME;
import static org.elasticsearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.elasticsearch.sql.data.type.ExprCoreType.FLOAT;
import static org.elasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static org.elasticsearch.sql.data.type.ExprCoreType.LONG;
import static org.elasticsearch.sql.data.type.ExprCoreType.SHORT;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRUCT;
import static org.elasticsearch.sql.data.type.ExprCoreType.TIME;
import static org.elasticsearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_BINARY;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_GEO_POINT;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_IP;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_TEXT;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_TEXT_KEYWORD;
import static org.elasticsearch.sql.search.data.value.ElasticsearchDateFormatters.SQL_LITERAL_DATE_TIME_FORMAT;
import static org.elasticsearch.sql.search.data.value.ElasticsearchDateFormatters.STRICT_DATE_OPTIONAL_TIME_FORMATTER;
import static org.elasticsearch.sql.search.data.value.ElasticsearchDateFormatters.STRICT_HOUR_MINUTE_SECOND_FORMATTER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.sql.data.model.ExprBooleanValue;
import org.elasticsearch.sql.data.model.ExprByteValue;
import org.elasticsearch.sql.data.model.ExprCollectionValue;
import org.elasticsearch.sql.data.model.ExprDateValue;
import org.elasticsearch.sql.data.model.ExprDatetimeValue;
import org.elasticsearch.sql.data.model.ExprDoubleValue;
import org.elasticsearch.sql.data.model.ExprFloatValue;
import org.elasticsearch.sql.data.model.ExprIntegerValue;
import org.elasticsearch.sql.data.model.ExprLongValue;
import org.elasticsearch.sql.data.model.ExprNullValue;
import org.elasticsearch.sql.data.model.ExprShortValue;
import org.elasticsearch.sql.data.model.ExprStringValue;
import org.elasticsearch.sql.data.model.ExprTimeValue;
import org.elasticsearch.sql.data.model.ExprTimestampValue;
import org.elasticsearch.sql.data.model.ExprTupleValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.search.data.utils.Content;
import org.elasticsearch.sql.search.data.utils.ObjectContent;
import org.elasticsearch.sql.search.data.utils.ElasticsearchJsonContent;
import org.elasticsearch.sql.search.response.agg.ElasticsearchAggregationResponseParser;

/**
 * Construct ExprValue from Elasticsearch response.
 */
public class ElasticsearchExprValueFactory {
  /**
   * The Mapping of Field and ExprType.
   */
  @Setter
  private Map<String, ExprType> typeMapping;

  @Getter
  @Setter
  private ElasticsearchAggregationResponseParser parser;

  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendOptional(SQL_LITERAL_DATE_TIME_FORMAT)
          .appendOptional(STRICT_DATE_OPTIONAL_TIME_FORMATTER)
          .appendOptional(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
          .toFormatter();

  private static final String TOP_PATH = "";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<ExprType, Function<Content, ExprValue>> typeActionMap =
      new ImmutableMap.Builder<ExprType, Function<Content, ExprValue>>()
          .put(INTEGER, c -> new ExprIntegerValue(c.intValue()))
          .put(LONG, c -> new ExprLongValue(c.longValue()))
          .put(SHORT, c -> new ExprShortValue(c.shortValue()))
          .put(BYTE, c -> new ExprByteValue(c.byteValue()))
          .put(FLOAT, c -> new ExprFloatValue(c.floatValue()))
          .put(DOUBLE, c -> new ExprDoubleValue(c.doubleValue()))
          .put(STRING, c -> new ExprStringValue(c.stringValue()))
          .put(BOOLEAN, c -> ExprBooleanValue.of(c.booleanValue()))
          .put(TIMESTAMP, this::parseTimestamp)
          .put(DATE, c -> new ExprDateValue(parseTimestamp(c).dateValue().toString()))
          .put(TIME, c -> new ExprTimeValue(parseTimestamp(c).timeValue().toString()))
          .put(DATETIME, c -> new ExprDatetimeValue(parseTimestamp(c).datetimeValue()))
          .put(EASYSEARCH_TEXT, c -> new ElasticsearchExprTextValue(c.stringValue()))
          .put(EASYSEARCH_TEXT_KEYWORD, c -> new ElasticsearchExprTextKeywordValue(c.stringValue()))
          .put(EASYSEARCH_IP, c -> new ElasticsearchExprIpValue(c.stringValue()))
          .put(EASYSEARCH_GEO_POINT, c -> new ElasticsearchExprGeoPointValue(c.geoValue().getLeft(),
              c.geoValue().getRight()))
          .put(EASYSEARCH_BINARY, c -> new ElasticsearchExprBinaryValue(c.stringValue()))
          .build();

  /**
   * Constructor of ElasticsearchExprValueFactory.
   */
  public ElasticsearchExprValueFactory(
      Map<String, ExprType> typeMapping) {
    this.typeMapping = typeMapping;
  }

  /**
   * The struct construction has the following assumption. 1. The field has Elasticsearch Object
   * data type. https://www.elastic.co/guide/en/elasticsearch/reference/current/object.html 2. The
   * deeper field is flattened in the typeMapping. e.g. {"employ", "STRUCT"} {"employ.id",
   * "INTEGER"} {"employ.state", "STRING"}
   */
  public ExprValue construct(String jsonString) {
    try {
      return parse(new ElasticsearchJsonContent(OBJECT_MAPPER.readTree(jsonString)), TOP_PATH,
          Optional.of(STRUCT));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(String.format("invalid json: %s.", jsonString), e);
    }
  }

  /**
   * Construct ExprValue from field and its value object. Throw exception if trying
   * to construct from field of unsupported type.
   * Todo, add IP, GeoPoint support after we have function implementation around it.
   *
   * @param field field name
   * @param value value object
   * @return ExprValue
   */
  public ExprValue construct(String field, Object value) {
    return parse(new ObjectContent(value), field, type(field));
  }

  private ExprValue parse(Content content, String field, Optional<ExprType> fieldType) {
    if (content.isNull() || !fieldType.isPresent()) {
      return ExprNullValue.of();
    }

    ExprType type = fieldType.get();
    if (type == STRUCT) {
      return parseStruct(content, field);
    } else if (type == ARRAY) {
      return parseArray(content, field);
    } else {
      if (typeActionMap.containsKey(type)) {
        return typeActionMap.get(type).apply(content);
      } else {
        throw new IllegalStateException(
            String.format(
                "Unsupported type: %s for value: %s.", type.typeName(), content.objectValue()));
      }
    }
  }

  /**
   * In Elasticsearch, it is possible field doesn't have type definition in mapping.
   * but has empty value. For example, {"empty_field": []}.
   */
  private Optional<ExprType> type(String field) {
    return Optional.ofNullable(typeMapping.get(field));
  }

  /**
   * Only default strict_date_optional_time||epoch_millis is supported,
   * strict_date_optional_time_nanos||epoch_millis if field is date_nanos.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/date_nanos.html
   * The customized date_format is not supported.
   */
  private ExprValue constructTimestamp(String value) {
    try {
      return new ExprTimestampValue(
          // Using Elasticsearch DateFormatters for now.
          DateFormatters.from(DATE_TIME_FORMATTER.parse(value)).toInstant());
    } catch (DateTimeParseException e) {
      throw new IllegalStateException(
          String.format(
              "Construct ExprTimestampValue from \"%s\" failed, unsupported date format.", value),
          e);
    }
  }

  private ExprValue parseTimestamp(Content value) {
    if (value.isNumber()) {
      return new ExprTimestampValue(Instant.ofEpochMilli(value.longValue()));
    } else if (value.isString()) {
      return constructTimestamp(value.stringValue());
    } else {
      return new ExprTimestampValue((Instant) value.objectValue());
    }
  }

  private ExprValue parseStruct(Content content, String prefix) {
    LinkedHashMap<String, ExprValue> result = new LinkedHashMap<>();
    content.map().forEachRemaining(entry -> result.put(entry.getKey(),
        parse(entry.getValue(),
            makeField(prefix, entry.getKey()),
            type(makeField(prefix, entry.getKey())))));
    return new ExprTupleValue(result);
  }

  /**
   * Todo. ARRAY is not support now. In Elasticsearch, there is no dedicated array data type.
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html. The similar data
   * type is nested, but it can only allow a list of objects.
   */
  private ExprValue parseArray(Content content, String prefix) {
    List<ExprValue> result = new ArrayList<>();
    content.array().forEachRemaining(v -> result.add(parse(v, prefix, Optional.of(STRUCT))));
    return new ExprCollectionValue(result);
  }

  private String makeField(String path, String field) {
    return path.equalsIgnoreCase(TOP_PATH) ? field : String.join(".", path, field);
  }
}
