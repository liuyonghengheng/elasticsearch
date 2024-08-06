

package org.elasticsearch.sql.search.mapping;

import static java.util.Collections.emptyMap;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.ToString;
import org.elasticsearch.cluster.metadata.MappingMetadata;

/**
 * Elasticsearch index mapping. Because there is no specific behavior for different field types,
 * string is used to represent field types.
 */
@ToString
public class IndexMapping {

  /** Field mappings from field name to field type in Elasticsearch date type system. */
  private final Map<String, String> fieldMappings;

  public IndexMapping(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings;
  }

  public IndexMapping(MappingMetadata metaData) {
    this.fieldMappings = flatMappings(metaData.getSourceAsMap());
  }

  /**
   * How many fields in the index (after flatten).
   *
   * @return field size
   */
  public int size() {
    return fieldMappings.size();
  }

  /**
   * Return field type by its name.
   *
   * @param fieldName field name
   * @return field type in string. Or null if not exist.
   */
  public String getFieldType(String fieldName) {
    return fieldMappings.get(fieldName);
  }

  /**
   * Get all field types and transform raw string type to expected type.
   *
   * @param transform transform function to transform field type in string to another type
   * @param <T> expected field type class
   * @return mapping from field name to field type
   */
  public <T> Map<String, T> getAllFieldTypes(Function<String, T> transform) {
    return fieldMappings.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> transform.apply(e.getValue())));
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> flatMappings(Map<String, Object> indexMapping) {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();

    flatMappings(
        ((Map<String, Object>) indexMapping.getOrDefault("properties", emptyMap())),
        "",
        builder::put);
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private void flatMappings(
      Map<String, Object> mappings, String path, BiConsumer<String, String> func) {
    mappings.forEach(
        (fieldName, mappingObject) -> {
          Map<String, Object> mapping = (Map<String, Object>) mappingObject;
          String fullFieldName = path.isEmpty() ? fieldName : path + "." + fieldName;

          if (isMultiField(mapping)) {
            func.accept(fullFieldName, "text_keyword");
          } else {
            String type = (String) mapping.getOrDefault("type", "object");
            func.accept(fullFieldName, type);
          }

          if (mapping.containsKey("properties")) { // Nested field
            flatMappings((Map<String, Object>) mapping.get("properties"), fullFieldName, func);
          }
        });
  }

  private boolean isMultiField(Map<String, Object> mapping) {
    return mapping.containsKey("fields");
  }

}
