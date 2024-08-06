


package org.elasticsearch.sql.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.RuleNode;
import org.junit.jupiter.api.Test;
import org.elasticsearch.sql.ast.dsl.AstDSL;
import org.elasticsearch.sql.ast.expression.UnresolvedExpression;
import org.elasticsearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.elasticsearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLLexer;
import org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser;

public class AstQualifiedNameBuilderTest {

  @Test
  public void canBuildRegularIdentifierForSQLStandard() {
    buildFromIdentifier("test").expectQualifiedName("test");
    buildFromIdentifier("test123").expectQualifiedName("test123");
    buildFromIdentifier("test_123").expectQualifiedName("test_123");
  }

  @Test
  public void canBuildRegularIdentifierForElasticsearch() {
    buildFromTableName(".search_dashboards").expectQualifiedName(".search_dashboards");
    buildFromIdentifier("@timestamp").expectQualifiedName("@timestamp");
    buildFromIdentifier("logs-2020-01").expectQualifiedName("logs-2020-01");
    buildFromIdentifier("*logs*").expectQualifiedName("*logs*");
  }

  @Test
  public void canBuildDelimitedIdentifier() {
    buildFromIdentifier("`logs.2020.01`").expectQualifiedName("logs.2020.01");
  }

  @Test
  public void canBuildQualifiedIdentifier() {
    buildFromQualifiers("account.location.city").expectQualifiedName("account", "location", "city");
  }

  private AstExpressionBuilderAssertion buildFromIdentifier(String expr) {
    return new AstExpressionBuilderAssertion(ElasticsearchSQLParser::ident, expr);
  }

  private AstExpressionBuilderAssertion buildFromQualifiers(String expr) {
    return new AstExpressionBuilderAssertion(ElasticsearchSQLParser::qualifiedName, expr);
  }

  private AstExpressionBuilderAssertion buildFromTableName(String expr) {
    return new AstExpressionBuilderAssertion(ElasticsearchSQLParser::tableName, expr);
  }

  @RequiredArgsConstructor
  private static class AstExpressionBuilderAssertion {
    private final AstExpressionBuilder astExprBuilder = new AstExpressionBuilder();
    private final Function<ElasticsearchSQLParser, RuleNode> build;
    private final String actual;

    public void expectQualifiedName(String... expected) {
      assertEquals(AstDSL.qualifiedName(expected), buildExpression(actual));
    }

    private UnresolvedExpression buildExpression(String expr) {
      return build.apply(createParser(expr)).accept(astExprBuilder);
    }

    private ElasticsearchSQLParser createParser(String expr) {
      ElasticsearchSQLLexer lexer = new ElasticsearchSQLLexer(new CaseInsensitiveCharStream(expr));
      ElasticsearchSQLParser parser = new ElasticsearchSQLParser(new CommonTokenStream(lexer));
      parser.addErrorListener(new SyntaxAnalysisErrorListener());
      return parser;
    }
  }

}
