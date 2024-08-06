


package org.elasticsearch.sql.config;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.elasticsearch.sql.analysis.symbol.Namespace;
import org.elasticsearch.sql.analysis.symbol.Symbol;
import org.elasticsearch.sql.analysis.symbol.SymbolTable;
import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.exception.ExpressionEvaluationException;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.ReferenceExpression;
import org.elasticsearch.sql.expression.env.Environment;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.storage.StorageEngine;
import org.elasticsearch.sql.storage.Table;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration will be used for UT.
 */
@Configuration
public class TestConfig {
  public static final String INT_TYPE_NULL_VALUE_FIELD = "int_null_value";
  public static final String INT_TYPE_MISSING_VALUE_FIELD = "int_missing_value";
  public static final String DOUBLE_TYPE_NULL_VALUE_FIELD = "double_null_value";
  public static final String DOUBLE_TYPE_MISSING_VALUE_FIELD = "double_missing_value";
  public static final String BOOL_TYPE_NULL_VALUE_FIELD = "null_value_boolean";
  public static final String BOOL_TYPE_MISSING_VALUE_FIELD = "missing_value_boolean";
  public static final String STRING_TYPE_NULL_VALUE_FIELD = "string_null_value";
  public static final String STRING_TYPE_MISSING_VALUE_FIELD = "string_missing_value";

  public static Map<String, ExprType> typeMapping = new ImmutableMap.Builder<String, ExprType>()
      .put("integer_value", ExprCoreType.INTEGER)
      .put(INT_TYPE_NULL_VALUE_FIELD, ExprCoreType.INTEGER)
      .put(INT_TYPE_MISSING_VALUE_FIELD, ExprCoreType.INTEGER)
      .put("long_value", ExprCoreType.LONG)
      .put("float_value", ExprCoreType.FLOAT)
      .put("double_value", ExprCoreType.DOUBLE)
      .put(DOUBLE_TYPE_NULL_VALUE_FIELD, ExprCoreType.DOUBLE)
      .put(DOUBLE_TYPE_MISSING_VALUE_FIELD, ExprCoreType.DOUBLE)
      .put("boolean_value", ExprCoreType.BOOLEAN)
      .put(BOOL_TYPE_NULL_VALUE_FIELD, ExprCoreType.BOOLEAN)
      .put(BOOL_TYPE_MISSING_VALUE_FIELD, ExprCoreType.BOOLEAN)
      .put("string_value", ExprCoreType.STRING)
      .put(STRING_TYPE_NULL_VALUE_FIELD, ExprCoreType.STRING)
      .put(STRING_TYPE_MISSING_VALUE_FIELD, ExprCoreType.STRING)
      .put("struct_value", ExprCoreType.STRUCT)
      .put("array_value", ExprCoreType.ARRAY)
      .build();

  @Bean
  protected StorageEngine storageEngine() {
    return new StorageEngine() {
      @Override
      public Table getTable(String name) {
        return new Table() {
          @Override
          public Map<String, ExprType> getFieldTypes() {
            return typeMapping;
          }

          @Override
          public PhysicalPlan implement(LogicalPlan plan) {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }


  @Bean
  protected SymbolTable symbolTable() {
    SymbolTable symbolTable = new SymbolTable();
    typeMapping.entrySet()
        .forEach(
            entry -> symbolTable
                .store(new Symbol(Namespace.FIELD_NAME, entry.getKey()), entry.getValue()));
    return symbolTable;
  }

  @Bean
  protected Environment<Expression, ExprType> typeEnv() {
    return var -> {
      if (var instanceof ReferenceExpression) {
        ReferenceExpression refExpr = (ReferenceExpression) var;
        if (typeMapping.containsKey(refExpr.getAttr())) {
          return typeMapping.get(refExpr.getAttr());
        }
      }
      throw new ExpressionEvaluationException("type resolved failed");
    };
  }
}
