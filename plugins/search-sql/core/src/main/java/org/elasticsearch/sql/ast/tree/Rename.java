

package org.elasticsearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.sql.ast.AbstractNodeVisitor;
import org.elasticsearch.sql.ast.expression.Map;

@ToString
@EqualsAndHashCode(callSuper = false)
@Getter
@RequiredArgsConstructor
public class Rename extends UnresolvedPlan {
  private final List<Map> renameList;
  private UnresolvedPlan child;

  public Rename(List<Map> renameList, UnresolvedPlan child) {
    this.renameList = renameList;
    this.child = child;
  }

  @Override
  public Rename attach(UnresolvedPlan child) {
    if (null == this.child) {
      this.child = child;
    } else {
      this.child.attach(child);
    }
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRename(this, context);
  }
}
