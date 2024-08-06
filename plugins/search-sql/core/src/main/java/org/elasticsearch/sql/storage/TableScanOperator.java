

package org.elasticsearch.sql.storage;

import java.util.Collections;
import java.util.List;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * Abstract table scan class for different storage to implement.
 * This is also to avoid "polluting" physical plan visitor by concrete table scan implementation.
 */
public abstract class TableScanOperator extends PhysicalPlan {

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitTableScan(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.emptyList();
  }

  /**
   * Explain the execution plan.
   *
   * @return execution plan.
   */
  public abstract String explain();
}
