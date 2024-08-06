

package org.elasticsearch.sql.planner;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.elasticsearch.sql.planner.logical.LogicalRelation;
import org.elasticsearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.storage.StorageEngine;
import org.elasticsearch.sql.storage.Table;

/**
 * Planner that plans and chooses the optimal physical plan.
 */
@RequiredArgsConstructor
public class Planner {

  /**
   * Storage engine.
   */
  private final StorageEngine storageEngine;

  private final LogicalPlanOptimizer logicalOptimizer;

  /**
   * Generate optimal physical plan for logical plan. If no table involved,
   * translate logical plan to physical by default implementor.
   * TODO: for now just delegate entire logical plan to storage engine.
   *
   * @param plan logical plan
   * @return optimal physical plan
   */
  public PhysicalPlan plan(LogicalPlan plan) {
    String tableName = findTableName(plan);
    if (isNullOrEmpty(tableName)) {
      return plan.accept(new DefaultImplementor<>(), null);
    }

    Table table = storageEngine.getTable(tableName);
    return table.implement(
        table.optimize(optimize(plan)));
  }

  private String findTableName(LogicalPlan plan) {
    return plan.accept(new LogicalPlanNodeVisitor<String, Object>() {

      @Override
      public String visitNode(LogicalPlan node, Object context) {
        List<LogicalPlan> children = node.getChild();
        if (children.isEmpty()) {
          return "";
        }
        return children.get(0).accept(this, context);
      }

      @Override
      public String visitRelation(LogicalRelation node, Object context) {
        return node.getRelationName();
      }
    }, null);
  }

  private LogicalPlan optimize(LogicalPlan plan) {
    return logicalOptimizer.optimize(plan);
  }
}
