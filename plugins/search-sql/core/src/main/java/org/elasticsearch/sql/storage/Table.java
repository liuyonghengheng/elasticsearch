

package org.elasticsearch.sql.storage;

import java.util.Map;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;

/**
 * Table.
 */
public interface Table {

  /**
   * Get the {@link ExprType} for each field in the table.
   */
  Map<String, ExprType> getFieldTypes();

  /**
   * Implement a {@link LogicalPlan} by {@link PhysicalPlan} in storage engine.
   *
   * @param plan logical plan
   * @return physical plan
   */
  PhysicalPlan implement(LogicalPlan plan);

  /**
   * Optimize the {@link LogicalPlan} by storage engine rule.
   * The default optimize solution is no optimization.
   *
   * @param plan logical plan.
   * @return logical plan.
   */
  default LogicalPlan optimize(LogicalPlan plan) {
    return plan;
  }

}
