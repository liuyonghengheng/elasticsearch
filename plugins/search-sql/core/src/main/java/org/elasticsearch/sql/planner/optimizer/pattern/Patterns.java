

package org.elasticsearch.sql.planner.optimizer.pattern;

import com.facebook.presto.matching.Property;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.elasticsearch.sql.planner.logical.LogicalPlan;

/**
 * Pattern helper class.
 */
@UtilityClass
public class Patterns {

  /**
   * LogicalPlan source {@link Property}.
   */
  public static Property<LogicalPlan, LogicalPlan> source() {
    return Property.optionalProperty("source", plan -> plan.getChild().size() == 1
        ? Optional.of(plan.getChild().get(0))
        : Optional.empty());
  }
}
