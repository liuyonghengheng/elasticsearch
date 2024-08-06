


package org.elasticsearch.sql.planner;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.elasticsearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.elasticsearch.sql.data.type.ExprCoreType.INTEGER;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.planner.logical.LogicalAggregation;
import org.elasticsearch.sql.planner.logical.LogicalFilter;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalPlanDSL;
import org.elasticsearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.elasticsearch.sql.planner.logical.LogicalRelation;
import org.elasticsearch.sql.planner.logical.LogicalRename;
import org.elasticsearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.sql.planner.physical.AggregationOperator;
import org.elasticsearch.sql.planner.physical.FilterOperator;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.planner.physical.PhysicalPlanDSL;
import org.elasticsearch.sql.planner.physical.PhysicalPlanTestBase;
import org.elasticsearch.sql.planner.physical.RenameOperator;
import org.elasticsearch.sql.storage.StorageEngine;
import org.elasticsearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
public class PlannerTest extends PhysicalPlanTestBase {
  @Mock
  private PhysicalPlan scan;

  @Mock
  private StorageEngine storageEngine;

  @Mock
  private LogicalPlanOptimizer optimizer;

  @BeforeEach
  public void setUp() {
    when(storageEngine.getTable(any())).thenReturn(new MockTable());
  }

  @Test
  public void planner_test() {
    doAnswer(returnsFirstArg()).when(optimizer).optimize(any());
    assertPhysicalPlan(
        PhysicalPlanDSL.rename(
            PhysicalPlanDSL.agg(
                PhysicalPlanDSL.filter(
                    scan,
                    dsl.equal(DSL.ref("response", INTEGER), DSL.literal(10))
                ),
                ImmutableList.of(DSL.named("avg(response)", dsl.avg(DSL.ref("response", INTEGER)))),
                ImmutableList.of()
            ),
            ImmutableMap.of(DSL.ref("ivalue", INTEGER), DSL.ref("avg(response)", DOUBLE))
        ),
        LogicalPlanDSL.rename(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.filter(
                    LogicalPlanDSL.relation("schema"),
                    dsl.equal(DSL.ref("response", INTEGER), DSL.literal(10))
                ),
                ImmutableList.of(DSL.named("avg(response)", dsl.avg(DSL.ref("response", INTEGER)))),
                ImmutableList.of()
            ),
            ImmutableMap.of(DSL.ref("ivalue", INTEGER), DSL.ref("avg(response)", DOUBLE))
        )
    );
  }

  @Test
  public void plan_a_query_without_relation_involved() {
    // Storage engine mock is not needed here since no relation involved.
    Mockito.reset(storageEngine);

    assertPhysicalPlan(
        PhysicalPlanDSL.project(
            PhysicalPlanDSL.values(emptyList()),
            DSL.named("123", DSL.literal(123)),
            DSL.named("hello", DSL.literal("hello")),
            DSL.named("false", DSL.literal(false))
        ),
        LogicalPlanDSL.project(
            LogicalPlanDSL.values(emptyList()),
            DSL.named("123", DSL.literal(123)),
            DSL.named("hello", DSL.literal("hello")),
            DSL.named("false", DSL.literal(false))
        )
    );
  }

  protected void assertPhysicalPlan(PhysicalPlan expected, LogicalPlan logicalPlan) {
    assertEquals(expected, analyze(logicalPlan));
  }

  protected PhysicalPlan analyze(LogicalPlan logicalPlan) {
    return new Planner(storageEngine, optimizer).plan(logicalPlan);
  }

  protected class MockTable extends LogicalPlanNodeVisitor<PhysicalPlan, Object> implements Table {

    @Override
    public Map<String, ExprType> getFieldTypes() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PhysicalPlan implement(LogicalPlan plan) {
      return plan.accept(this, null);
    }

    @Override
    public PhysicalPlan visitRelation(LogicalRelation plan, Object context) {
      return scan;
    }

    @Override
    public PhysicalPlan visitFilter(LogicalFilter plan, Object context) {
      return new FilterOperator(plan.getChild().get(0).accept(this, context), plan.getCondition());
    }

    @Override
    public PhysicalPlan visitAggregation(LogicalAggregation plan, Object context) {
      return new AggregationOperator(plan.getChild().get(0).accept(this, context),
          plan.getAggregatorList(), plan.getGroupByList()
      );
    }

    @Override
    public PhysicalPlan visitRename(LogicalRename plan, Object context) {
      return new RenameOperator(plan.getChild().get(0).accept(this, context),
          plan.getRenameMap());
    }
  }
}
