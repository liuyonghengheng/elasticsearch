

package org.elasticsearch.sql.search.storage.system;

import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.data.type.ExprType;

/**
 * Definition of the system table schema.
 */
@Getter
@RequiredArgsConstructor
public enum ElasticsearchSystemIndexSchema {

  SYS_TABLE_TABLES(new LinkedHashMap<String, ExprType>() {{
      put("TABLE_CAT", STRING);
      put("TABLE_SCHEM", STRING);
      put("TABLE_NAME", STRING);
      put("TABLE_TYPE", STRING);
      put("REMARKS", STRING);
      put("TYPE_CAT", STRING);
      put("TYPE_SCHEM", STRING);
      put("TYPE_NAME", STRING);
      put("SELF_REFERENCING_COL_NAME", STRING);
      put("REF_GENERATION", STRING);
    }
  }
  ),
  SYS_TABLE_MAPPINGS(new ImmutableMap.Builder<String, ExprType>()
      .put("TABLE_CAT", STRING)
      .put("TABLE_SCHEM", STRING)
      .put("TABLE_NAME", STRING)
      .put("COLUMN_NAME", STRING)
      .put("DATA_TYPE", STRING)
      .put("TYPE_NAME", STRING)
      .put("COLUMN_SIZE", STRING)
      .put("BUFFER_LENGTH", STRING)
      .put("DECIMAL_DIGITS", STRING)
      .put("NUM_PREC_RADIX", STRING)
      .put("NULLABLE", STRING)
      .put("REMARKS", STRING)
      .put("COLUMN_DEF", STRING)
      .put("SQL_DATA_TYPE", STRING)
      .put("SQL_DATETIME_SUB", STRING)
      .put("CHAR_OCTET_LENGTH", STRING)
      .put("ORDINAL_POSITION", STRING)
      .put("IS_NULLABLE", STRING)
      .put("SCOPE_CATALOG", STRING)
      .put("SCOPE_SCHEMA", STRING)
      .put("SCOPE_TABLE", STRING)
      .put("SOURCE_DATA_TYPE", STRING)
      .put("IS_AUTOINCREMENT", STRING)
      .put("IS_GENERATEDCOLUMN", STRING)
      .build());

  private final Map<String, ExprType> mapping;
}
