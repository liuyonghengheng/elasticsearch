

package org.elasticsearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.sql.ast.AbstractNodeVisitor;
import org.elasticsearch.sql.ast.Node;
import org.elasticsearch.sql.ast.expression.Literal;

/**
 * AST node class for a sequence of literal values.
 */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Values extends UnresolvedPlan {

  private final List<List<Literal>> values;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    throw new UnsupportedOperationException("Values node is supposed to have no child node");
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitValues(this, context);
  }

  @Override
  public List<? extends Node> getChild() {
    return ImmutableList.of();
  }

}
