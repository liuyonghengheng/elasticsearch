

package org.elasticsearch.sql.analysis;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.ast.AbstractNodeVisitor;
import org.elasticsearch.sql.ast.expression.Alias;
import org.elasticsearch.sql.ast.expression.QualifiedName;
import org.elasticsearch.sql.ast.expression.Span;
import org.elasticsearch.sql.ast.expression.UnresolvedExpression;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.expression.span.SpanExpression;

/**
 * Analyze the Alias node in the {@link AnalysisContext} to construct the list of
 * {@link NamedExpression}.
 */
@RequiredArgsConstructor
public class NamedExpressionAnalyzer extends
    AbstractNodeVisitor<NamedExpression, AnalysisContext> {
  private final ExpressionAnalyzer expressionAnalyzer;

  /**
   * Analyze Select fields.
   */
  public NamedExpression analyze(UnresolvedExpression expression,
                                       AnalysisContext analysisContext) {
    return expression.accept(this, analysisContext);
  }

  @Override
  public NamedExpression visitAlias(Alias node, AnalysisContext context) {
    return DSL.named(
        unqualifiedNameIfFieldOnly(node, context),
        node.getDelegated().accept(expressionAnalyzer, context),
        node.getAlias());
  }

  private String unqualifiedNameIfFieldOnly(Alias node, AnalysisContext context) {
    UnresolvedExpression selectItem = node.getDelegated();
    if (selectItem instanceof QualifiedName) {
      QualifierAnalyzer qualifierAnalyzer = new QualifierAnalyzer(context);
      return qualifierAnalyzer.unqualified((QualifiedName) selectItem);
    }
    return node.getName();
  }
}
