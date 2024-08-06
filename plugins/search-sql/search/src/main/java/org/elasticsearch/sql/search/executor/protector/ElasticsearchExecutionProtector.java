

package org.elasticsearch.sql.search.executor.protector;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.monitor.ResourceMonitor;
import org.elasticsearch.sql.search.planner.physical.ADOperator;
import org.elasticsearch.sql.search.planner.physical.MLCommonsOperator;
import org.elasticsearch.sql.planner.physical.AggregationOperator;
import org.elasticsearch.sql.planner.physical.DedupeOperator;
import org.elasticsearch.sql.planner.physical.EvalOperator;
import org.elasticsearch.sql.planner.physical.FilterOperator;
import org.elasticsearch.sql.planner.physical.LimitOperator;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.planner.physical.ProjectOperator;
import org.elasticsearch.sql.planner.physical.RareTopNOperator;
import org.elasticsearch.sql.planner.physical.RemoveOperator;
import org.elasticsearch.sql.planner.physical.RenameOperator;
import org.elasticsearch.sql.planner.physical.SortOperator;
import org.elasticsearch.sql.planner.physical.ValuesOperator;
import org.elasticsearch.sql.planner.physical.WindowOperator;
import org.elasticsearch.sql.storage.TableScanOperator;

/**
 * Elasticsearch Execution Protector.
 */
@RequiredArgsConstructor
public class ElasticsearchExecutionProtector extends ExecutionProtector {

  /**
   * Elasticsearch resource monitor.
   */
  private final ResourceMonitor resourceMonitor;

  public PhysicalPlan protect(PhysicalPlan physicalPlan) {
    return physicalPlan.accept(this, null);
  }

  @Override
  public PhysicalPlan visitFilter(FilterOperator node, Object context) {
    return new FilterOperator(visitInput(node.getInput(), context), node.getConditions());
  }

  @Override
  public PhysicalPlan visitAggregation(AggregationOperator node, Object context) {
    return new AggregationOperator(visitInput(node.getInput(), context), node.getAggregatorList(),
        node.getGroupByExprList());
  }

  @Override
  public PhysicalPlan visitRareTopN(RareTopNOperator node, Object context) {
    return new RareTopNOperator(visitInput(node.getInput(), context), node.getCommandType(),
        node.getNoOfResults(), node.getFieldExprList(), node.getGroupByExprList());
  }

  @Override
  public PhysicalPlan visitRename(RenameOperator node, Object context) {
    return new RenameOperator(visitInput(node.getInput(), context), node.getMapping());
  }

  /**
   * Decorate with {@link ResourceMonitorPlan}.
   */
  @Override
  public PhysicalPlan visitTableScan(TableScanOperator node, Object context) {
    return doProtect(node);
  }

  @Override
  public PhysicalPlan visitProject(ProjectOperator node, Object context) {
    return new ProjectOperator(visitInput(node.getInput(), context), node.getProjectList(),
        node.getNamedParseExpressions());
  }

  @Override
  public PhysicalPlan visitRemove(RemoveOperator node, Object context) {
    return new RemoveOperator(visitInput(node.getInput(), context), node.getRemoveList());
  }

  @Override
  public PhysicalPlan visitEval(EvalOperator node, Object context) {
    return new EvalOperator(visitInput(node.getInput(), context), node.getExpressionList());
  }

  @Override
  public PhysicalPlan visitDedupe(DedupeOperator node, Object context) {
    return new DedupeOperator(visitInput(node.getInput(), context), node.getDedupeList(),
        node.getAllowedDuplication(), node.getKeepEmpty(), node.getConsecutive());
  }

  @Override
  public PhysicalPlan visitWindow(WindowOperator node, Object context) {
    return new WindowOperator(
        doProtect(visitInput(node.getInput(), context)),
        node.getWindowFunction(),
        node.getWindowDefinition());
  }

  /**
   * Decorate with {@link ResourceMonitorPlan}.
   */
  @Override
  public PhysicalPlan visitSort(SortOperator node, Object context) {
    return doProtect(
        new SortOperator(
            visitInput(node.getInput(), context),
            node.getSortList()));
  }

  /**
   * Values are a sequence of rows of literal value in memory
   * which doesn't need memory protection.
   */
  @Override
  public PhysicalPlan visitValues(ValuesOperator node, Object context) {
    return node;
  }

  @Override
  public PhysicalPlan visitLimit(LimitOperator node, Object context) {
    return new LimitOperator(
        visitInput(node.getInput(), context),
        node.getLimit(),
        node.getOffset());
  }

  @Override
  public PhysicalPlan visitMLCommons(PhysicalPlan node, Object context) {
    MLCommonsOperator mlCommonsOperator = (MLCommonsOperator) node;
    return doProtect(
            new MLCommonsOperator(visitInput(mlCommonsOperator.getInput(), context),
                    mlCommonsOperator.getAlgorithm(),
                    mlCommonsOperator.getArguments(),
                    mlCommonsOperator.getNodeClient())
    );
  }

  @Override
  public PhysicalPlan visitAD(PhysicalPlan node, Object context) {
    ADOperator adOperator = (ADOperator) node;
    return doProtect(
            new ADOperator(visitInput(adOperator.getInput(), context),
                    adOperator.getArguments(),
                    adOperator.getNodeClient()
                    )
    );
  }

  PhysicalPlan visitInput(PhysicalPlan node, Object context) {
    if (null == node) {
      return node;
    } else {
      return node.accept(this, context);
    }
  }

  protected PhysicalPlan doProtect(PhysicalPlan node) {
    if (isProtected(node)) {
      return node;
    }
    return new ResourceMonitorPlan(node, resourceMonitor);
  }

  private boolean isProtected(PhysicalPlan node) {
    return (node instanceof ResourceMonitorPlan);
  }

}
