

package org.elasticsearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.elasticsearch.sql.ast.AbstractNodeVisitor;
import org.elasticsearch.sql.ast.expression.Literal;
import org.elasticsearch.sql.ast.expression.UnresolvedExpression;

/**
 * AST node represent Parse operation.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class Parse extends UnresolvedPlan {
  /**
   * Field.
   */
  private final UnresolvedExpression expression;

  /**
   * Pattern.
   */
  private final Literal pattern;

  /**
   * Child Plan.
   */
  private UnresolvedPlan child;

  @Override
  public Parse attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitParse(this, context);
  }
}
