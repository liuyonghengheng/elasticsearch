

package org.elasticsearch.sql.search.executor.protector;

import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * Execution Plan Protector.
 */
public abstract class ExecutionProtector extends PhysicalPlanNodeVisitor<PhysicalPlan, Object> {

  /**
   * Decorated the PhysicalPlan to run in resource sensitive mode.
   */
  public abstract PhysicalPlan protect(PhysicalPlan physicalPlan);
}
