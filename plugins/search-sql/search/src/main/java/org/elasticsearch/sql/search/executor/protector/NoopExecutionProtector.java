

package org.elasticsearch.sql.search.executor.protector;

import org.elasticsearch.sql.planner.physical.PhysicalPlan;

/**
 * No operation execution protector.
 */
public class NoopExecutionProtector extends ExecutionProtector {

  @Override
  public PhysicalPlan protect(PhysicalPlan physicalPlan) {
    return physicalPlan;
  }
}
