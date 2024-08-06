

package org.elasticsearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.sql.ast.AbstractNodeVisitor;

/**
 * Logical plan node of RelationSubquery.
 */
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@ToString
public class RelationSubquery extends UnresolvedPlan {
  private UnresolvedPlan query;
  private String alias;

  /**
   * Take subquery alias as table name.
   */
  public String getAliasAsTableName() {
    return alias;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(query);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRelationSubquery(this, context);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    return this;
  }
}
