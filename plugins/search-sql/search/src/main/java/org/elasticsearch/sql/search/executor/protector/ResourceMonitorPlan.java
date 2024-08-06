

package org.elasticsearch.sql.search.executor.protector;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.monitor.ResourceMonitor;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * A PhysicalPlan which will run the delegate plan in resource protection manner.
 */
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode
public class ResourceMonitorPlan extends PhysicalPlan {

  /**
   * How many method calls to delegate's next() to perform resource check once.
   */
  public static final long NUMBER_OF_NEXT_CALL_TO_CHECK = 1000;

  /**
   * Delegated PhysicalPlan.
   */
  private final PhysicalPlan delegate;

  /**
   * ResourceMonitor.
   */
  @ToString.Exclude
  private final ResourceMonitor monitor;

  /**
   * Count how many calls to delegate's next() already.
   */
  @EqualsAndHashCode.Exclude
  private long nextCallCount = 0L;


  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return delegate.accept(visitor, context);
  }

  @Override
  public void open() {
    if (!this.monitor.isHealthy()) {
      throw new IllegalStateException("resource is not enough to run the query, quit.");
    }
    delegate.open();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return delegate.getChild();
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public ExprValue next() {
    boolean shouldCheck = (++nextCallCount % NUMBER_OF_NEXT_CALL_TO_CHECK == 0);
    if (shouldCheck && !this.monitor.isHealthy()) {
      throw new IllegalStateException("resource is not enough to load next row, quit.");
    }
    return delegate.next();
  }
}
