


package org.elasticsearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.elasticsearch.sql.data.type.ExprCoreType.FLOAT;
import static org.elasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRUCT;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.analysis.symbol.Namespace;
import org.elasticsearch.sql.analysis.symbol.Symbol;
import org.elasticsearch.sql.ast.dsl.AstDSL;
import org.elasticsearch.sql.ast.expression.UnresolvedExpression;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.expression.config.ExpressionConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@ContextConfiguration(classes = {ExpressionConfig.class, SelectExpressionAnalyzerTest.class})
public class SelectExpressionAnalyzerTest extends AnalyzerTestBase {

  @Mock
  private ExpressionReferenceOptimizer optimizer;

  @Test
  public void named_expression() {
    assertAnalyzeEqual(
        DSL.named("integer_value", DSL.ref("integer_value", INTEGER)),
        AstDSL.alias("integer_value", AstDSL.qualifiedName("integer_value"))
    );
  }

  @Test
  public void named_expression_with_alias() {
    assertAnalyzeEqual(
        DSL.named("integer_value", DSL.ref("integer_value", INTEGER), "int"),
        AstDSL.alias("integer_value", AstDSL.qualifiedName("integer_value"), "int")
    );
  }

  @Disabled("we didn't define the aggregator symbol any more")
  @Test
  public void named_expression_with_delegated_expression_defined_in_symbol_table() {
    analysisContext.push();
    analysisContext.peek().define(new Symbol(Namespace.FIELD_NAME, "AVG(integer_value)"), FLOAT);

    assertAnalyzeEqual(
        DSL.named("AVG(integer_value)", DSL.ref("AVG(integer_value)", FLOAT)),
        AstDSL.alias("AVG(integer_value)",
            AstDSL.aggregate("AVG", AstDSL.qualifiedName("integer_value")))
    );
  }

  @Test
  public void field_name_with_qualifier() {
    analysisContext.peek().define(new Symbol(Namespace.INDEX_NAME, "index_alias"), STRUCT);
    assertAnalyzeEqual(
        DSL.named("integer_value", DSL.ref("integer_value", INTEGER)),
        AstDSL.alias("integer_alias.integer_value",
            AstDSL.qualifiedName("index_alias", "integer_value"))
    );
  }

  @Test
  public void field_name_with_qualifier_quoted() {
    analysisContext.peek().define(new Symbol(Namespace.INDEX_NAME, "index_alias"), STRUCT);
    assertAnalyzeEqual(
        DSL.named("integer_value", DSL.ref("integer_value", INTEGER)),
        AstDSL.alias("`integer_alias`.integer_value", // qualifier in SELECT is quoted originally
            AstDSL.qualifiedName("index_alias", "integer_value"))
    );
  }

  @Test
  public void field_name_in_expression_with_qualifier() {
    analysisContext.peek().define(new Symbol(Namespace.INDEX_NAME, "index_alias"), STRUCT);
    assertAnalyzeEqual(
        DSL.named("abs(index_alias.integer_value)", dsl.abs(DSL.ref("integer_value", INTEGER))),
        AstDSL.alias("abs(index_alias.integer_value)",
            AstDSL.function("abs", AstDSL.qualifiedName("index_alias", "integer_value")))
    );
  }

  protected List<NamedExpression> analyze(UnresolvedExpression unresolvedExpression) {
    doAnswer(invocation -> ((NamedExpression) invocation.getArgument(0))
        .getDelegated()).when(optimizer).optimize(any(), any());
    return new SelectExpressionAnalyzer(expressionAnalyzer)
        .analyze(Arrays.asList(unresolvedExpression),
            analysisContext, optimizer);
  }

  protected void assertAnalyzeEqual(NamedExpression expected,
                                    UnresolvedExpression unresolvedExpression) {
    assertEquals(Arrays.asList(expected), analyze(unresolvedExpression));
  }
}
