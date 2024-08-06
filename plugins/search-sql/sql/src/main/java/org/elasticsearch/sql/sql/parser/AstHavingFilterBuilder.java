


package org.elasticsearch.sql.sql.parser;

import static org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.QualifiedNameContext;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.ast.expression.UnresolvedExpression;
import org.elasticsearch.sql.sql.parser.context.QuerySpecification;

/**
 * AST Having filter builder that builds HAVING clause condition expressions
 * and replace alias by original expression in SELECT clause.
 * The reason for this is it's hard to replace afterwards since UnresolvedExpression
 * is immutable.
 */
@RequiredArgsConstructor
public class AstHavingFilterBuilder extends AstExpressionBuilder {

  private final QuerySpecification querySpec;

  @Override
  public UnresolvedExpression visitQualifiedName(QualifiedNameContext ctx) {
    return replaceAlias(super.visitQualifiedName(ctx));
  }

  private UnresolvedExpression replaceAlias(UnresolvedExpression expr) {
    if (querySpec.isSelectAlias(expr)) {
      return querySpec.getSelectItemByAlias(expr);
    }
    return expr;
  }

}
