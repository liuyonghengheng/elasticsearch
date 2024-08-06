

package org.elasticsearch.sql.analysis.symbol;

/**
 * Namespace of symbol to avoid naming conflict.
 */
public enum Namespace {

  INDEX_NAME("Index"),
  FIELD_NAME("Field"),
  FUNCTION_NAME("Function");

  private final String name;

  Namespace(String name) {
    this.name = name;
  }

}
