


package org.elasticsearch.sql.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.elasticsearch.sql.ast.dsl.AstDSL.aggregate;
import static org.elasticsearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.IdentContext;
import static org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.QualifiedNameContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.ast.expression.UnresolvedExpression;
import org.elasticsearch.sql.sql.parser.context.QuerySpecification;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class AstHavingFilterBuilderTest {

  @Mock
  private QuerySpecification querySpec;

  private AstHavingFilterBuilder builder;

  @BeforeEach
  void setup() {
    builder = new AstHavingFilterBuilder(querySpec);
  }

  @Test
  void should_replace_alias_with_select_expression() {
    QualifiedNameContext qualifiedName = mock(QualifiedNameContext.class);
    IdentContext identifier = mock(IdentContext.class);
    UnresolvedExpression expression = aggregate("AVG", qualifiedName("age"));

    when(identifier.getText()).thenReturn("a");
    when(qualifiedName.ident()).thenReturn(ImmutableList.of(identifier));
    when(querySpec.isSelectAlias(any())).thenReturn(true);
    when(querySpec.getSelectItemByAlias(any())).thenReturn(expression);
    assertEquals(expression, builder.visitQualifiedName(qualifiedName));
  }
}
