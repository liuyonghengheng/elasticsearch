

package org.elasticsearch.sql.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.elasticsearch.sql.ast.AbstractNodeVisitor;
import org.elasticsearch.sql.ast.Node;

@EqualsAndHashCode(callSuper = false)
@ToString
public abstract class UnresolvedExpression extends Node {
  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitChildren(this, context);
  }
}
