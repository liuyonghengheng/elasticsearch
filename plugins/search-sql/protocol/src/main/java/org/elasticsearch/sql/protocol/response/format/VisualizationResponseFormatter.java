

package org.elasticsearch.sql.protocol.response.format;

import com.google.common.collect.ImmutableList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.common.antlr.SyntaxCheckException;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.exception.QueryEngineException;
import org.elasticsearch.sql.executor.ExecutionEngine;
import org.elasticsearch.sql.search.response.error.ErrorMessage;
import org.elasticsearch.sql.search.response.error.ErrorMessageFactory;
import org.elasticsearch.sql.protocol.response.QueryResult;

/**
 * Visualization response formats the data in columns. For example:
 *
 * <pre>
 *  {
 *    "data": {
 *      "name": [
 *        "John",
 *        "Amber"
 *      ],
 *      "age": [
 *        26,
 *        28
 *      ]
 *    },
 *    "metadata": {
 *      "fields": [
 *        {
 *          "name": "name",
 *          "type": "string"
 *        },
 *        {
 *          "name": "age",
 *          "type": "integer"
 *        }
 *      ]
 *    },
 *    "size": 2,
 *    "status": 200
 *  }
 * </pre>
 */
public class VisualizationResponseFormatter extends JsonResponseFormatter<QueryResult> {
  public VisualizationResponseFormatter(Style style) {
    super(style);
  }

  @Override
  protected Object buildJsonObject(QueryResult response) {
    return VisualizationResponse.builder()
        .data(fetchData(response))
        .metadata(constructMetadata(response))
        .size(response.size())
        .status(200)
        .build();
  }

  @Override
  public String format(Throwable t) {
    int status = getStatus(t);
    ErrorMessage message = ErrorMessageFactory.createErrorMessage(t, status);
    VisualizationResponseFormatter.Error error = new Error(
        message.getType(),
        message.getReason(),
        message.getDetails());
    return jsonify(new VisualizationErrorResponse(error, status));
  }

  private int getStatus(Throwable t) {
    return (t instanceof SyntaxCheckException
        || t instanceof QueryEngineException) ? 400 : 503;
  }

  private Map<String, List<Object>> fetchData(QueryResult response) {
    Map<String, List<Object>> columnMap = new LinkedHashMap<>();
    response.getSchema().getColumns()
        .forEach(column -> columnMap.put(column.getName(), new LinkedList<>()));

    for (Object[] dataRow : response) {
      int column = 0;
      for (Map.Entry<String, List<Object>> entry : columnMap.entrySet()) {
        List<Object> dataColumn = entry.getValue();
        dataColumn.add(dataRow[column++]);
      }
    }

    return columnMap;
  }

  private Metadata constructMetadata(QueryResult response) {
    return new Metadata(fetchFields(response));
  }

  private List<Field> fetchFields(QueryResult response) {
    List<ExecutionEngine.Schema.Column> columns = response.getSchema().getColumns();
    ImmutableList.Builder<Field> fields = ImmutableList.builder();
    columns.forEach(column -> {
      Field field = new Field(column.getName(), convertToLegacyType(column.getExprType()));
      fields.add(field);
    });
    return fields.build();
  }

  /**
   * Convert type that exists in both legacy and new engine but has different name.
   * Return old type name to avoid breaking impact on client-side.
   */
  private String convertToLegacyType(ExprType type) {
    return type.legacyTypeName().toLowerCase();
  }

  @RequiredArgsConstructor
  @Getter
  public static class VisualizationErrorResponse {
    private final Error error;
    private final int status;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Error {
    private final String type;
    private final String reason;
    private final String details;
  }

  @Builder
  @Getter
  public static class VisualizationResponse {
    private final Map<String, List<Object>> data;
    private final Metadata metadata;
    private final long size;
    private final int status;
  }

  @RequiredArgsConstructor
  public static class Metadata {
    private final List<Field> fields;
  }

  @RequiredArgsConstructor
  public static class Field {
    private final String name;
    private final String type;
  }
}
