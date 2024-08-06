

package org.elasticsearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.elasticsearch.sql.ast.AbstractNodeVisitor;
import org.elasticsearch.sql.ast.expression.UnresolvedExpression;

/**
 * Logical plan node of Filter, the interface for building filters in queries.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@Getter
public class Filter extends UnresolvedPlan {
  private UnresolvedExpression condition;
  private UnresolvedPlan child;

  public Filter(UnresolvedExpression condition) {
    this.condition = condition;
  }

  @Override
  public Filter attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitFilter(this, context);
  }
}
