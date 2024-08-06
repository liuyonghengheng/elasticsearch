

package org.elasticsearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.sql.ast.AbstractNodeVisitor;
import org.elasticsearch.sql.ast.Node;

/**
 * AST node that represents WHEN clause.
 */
@EqualsAndHashCode(callSuper = false)
@Getter
@RequiredArgsConstructor
@ToString
public class When extends UnresolvedExpression {

  /**
   * WHEN condition, either a search condition or compare value if case value present.
   */
  private final UnresolvedExpression condition;

  /**
   * Result to return if condition matched.
   */
  private final UnresolvedExpression result;

  @Override
  public List<? extends Node> getChild() {
    return ImmutableList.of(condition, result);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitWhen(this, context);
  }

}
