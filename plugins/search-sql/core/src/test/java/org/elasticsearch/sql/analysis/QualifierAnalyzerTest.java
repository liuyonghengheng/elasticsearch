


package org.elasticsearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.elasticsearch.sql.data.type.ExprCoreType.ARRAY;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRUCT;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.elasticsearch.sql.analysis.symbol.Namespace;
import org.elasticsearch.sql.analysis.symbol.Symbol;
import org.elasticsearch.sql.common.antlr.SyntaxCheckException;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.expression.config.ExpressionConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ExpressionConfig.class, AnalyzerTestBase.class})
class QualifierAnalyzerTest extends AnalyzerTestBase {

  private QualifierAnalyzer qualifierAnalyzer;

  @BeforeEach
  void setUp() {
    qualifierAnalyzer = new QualifierAnalyzer(analysisContext);
  }

  @Test
  void should_return_original_name_if_no_qualifier() {
    assertEquals("integer_value", qualifierAnalyzer.unqualified("integer_value"));
  }

  @Test
  void should_report_error_if_qualifier_is_not_index() {
    runInScope(new Symbol(Namespace.FIELD_NAME, "aIndex"), ARRAY, () -> {
      SyntaxCheckException error = assertThrows(SyntaxCheckException.class,
          () -> qualifierAnalyzer.unqualified("a", "integer_value"));
      assertEquals("The qualifier [a] of qualified name [a.integer_value] "
              + "must be an field name, index name or its alias", error.getMessage());
    });
  }

  @Test
  void should_report_error_if_qualifier_is_not_exist() {
    SyntaxCheckException error = assertThrows(SyntaxCheckException.class,
        () -> qualifierAnalyzer.unqualified("a", "integer_value"));
    assertEquals(
        "The qualifier [a] of qualified name [a.integer_value] must be an field name, index name "
            + "or its alias",
        error.getMessage());
  }

  @Test
  void should_return_qualified_name_if_qualifier_is_index() {
    runInScope(new Symbol(Namespace.INDEX_NAME, "a"), STRUCT, () ->
        assertEquals("integer_value", qualifierAnalyzer.unqualified("a", "integer_value"))
    );
  }

  @Test
  void should_return_qualified_name_if_qualifier_is_field() {
    runInScope(new Symbol(Namespace.FIELD_NAME, "a"), STRUCT, () ->
        assertEquals("a.integer_value", qualifierAnalyzer.unqualified("a", "integer_value"))
    );
  }

  @Test
  void should_report_error_if_more_parts_in_qualified_name() {
    runInScope(new Symbol(Namespace.INDEX_NAME, "a"), STRUCT, () ->
        qualifierAnalyzer.unqualified("a", "integer_value", "invalid")
    );
  }

  private void runInScope(Symbol symbol, ExprType type, Runnable test) {
    analysisContext.push();
    analysisContext.peek().define(symbol, type);
    try {
      test.run();
    } finally {
      analysisContext.pop();
    }
  }

}
