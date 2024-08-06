

package org.elasticsearch.sql.planner;

import java.util.List;

/**
 * The definition of Plan Node.
 */
public interface PlanNode<T extends PlanNode> {

  /**
   * Return the child nodes.
   *
   * @return child nodes.
   */
  List<T> getChild();
}
