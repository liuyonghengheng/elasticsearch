

package org.elasticsearch.sql.ast.tree;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.elasticsearch.sql.ast.AbstractNodeVisitor;
import org.elasticsearch.sql.ast.Node;

/**
 * Abstract unresolved plan.
 */
@EqualsAndHashCode(callSuper = false)
@ToString
public abstract class UnresolvedPlan extends Node {
  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitChildren(this, context);
  }

  public abstract UnresolvedPlan attach(UnresolvedPlan child);
}
