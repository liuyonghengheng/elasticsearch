

package org.elasticsearch.sql.planner.physical;

import java.util.Iterator;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.executor.ExecutionEngine;
import org.elasticsearch.sql.planner.PlanNode;

/**
 * Physical plan.
 */
public abstract class PhysicalPlan implements PlanNode<PhysicalPlan>,
    Iterator<ExprValue>,
    AutoCloseable {
  /**
   * Accept the {@link PhysicalPlanNodeVisitor}.
   *
   * @param visitor visitor.
   * @param context visitor context.
   * @param <R>     returned object type.
   * @param <C>     context type.
   * @return returned object.
   */
  public abstract <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context);

  public void open() {
    getChild().forEach(PhysicalPlan::open);
  }

  public void close() {
    getChild().forEach(PhysicalPlan::close);
  }

  public ExecutionEngine.Schema schema() {
    throw new IllegalStateException(String.format("[BUG] schema can been only applied to "
        + "ProjectOperator, instead of %s", toString()));
  }
}
