

package org.elasticsearch.sql.common.antlr;

public class SyntaxCheckException extends RuntimeException {
  public SyntaxCheckException(String message) {
    super(message);
  }
}
