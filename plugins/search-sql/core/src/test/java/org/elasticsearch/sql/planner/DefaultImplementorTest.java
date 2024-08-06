


package org.elasticsearch.sql.planner;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.elasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.elasticsearch.sql.expression.DSL.literal;
import static org.elasticsearch.sql.expression.DSL.named;
import static org.elasticsearch.sql.expression.DSL.ref;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.aggregation;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.eval;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.filter;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.limit;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.rareTopN;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.remove;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.rename;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.sort;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.values;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.window;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.ast.tree.RareTopN.CommandType;
import org.elasticsearch.sql.ast.tree.Sort;
import org.elasticsearch.sql.data.model.ExprBooleanValue;
import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.expression.ReferenceExpression;
import org.elasticsearch.sql.expression.aggregation.AvgAggregator;
import org.elasticsearch.sql.expression.aggregation.NamedAggregator;
import org.elasticsearch.sql.expression.window.WindowDefinition;
import org.elasticsearch.sql.expression.window.ranking.RowNumberFunction;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalPlanDSL;
import org.elasticsearch.sql.planner.logical.LogicalRelation;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.planner.physical.PhysicalPlanDSL;

@ExtendWith(MockitoExtension.class)
class DefaultImplementorTest {

  @Mock
  private Expression filter;

  @Mock
  private NamedAggregator aggregator;

  @Mock
  private NamedExpression groupBy;

  private final DefaultImplementor<Object> implementor = new DefaultImplementor<>();

  @Test
  public void visitShouldReturnDefaultPhysicalOperator() {
    String indexName = "test";
    NamedExpression include = named("age", ref("age", INTEGER));
    ReferenceExpression exclude = ref("name", STRING);
    ReferenceExpression dedupeField = ref("name", STRING);
    Expression filterExpr = literal(ExprBooleanValue.of(true));
    List<NamedExpression> groupByExprs = Arrays.asList(DSL.named("age", ref("age", INTEGER)));
    List<Expression> aggExprs = Arrays.asList(ref("age", INTEGER));
    ReferenceExpression rareTopNField = ref("age", INTEGER);
    List<Expression> topByExprs = Arrays.asList(ref("age", INTEGER));
    List<NamedAggregator> aggregators =
        Arrays.asList(DSL.named("avg(age)", new AvgAggregator(aggExprs, ExprCoreType.DOUBLE)));
    Map<ReferenceExpression, ReferenceExpression> mappings =
        ImmutableMap.of(ref("name", STRING), ref("lastname", STRING));
    Pair<ReferenceExpression, Expression> newEvalField =
        ImmutablePair.of(ref("name1", STRING), ref("name", STRING));
    Pair<Sort.SortOption, Expression> sortField =
        ImmutablePair.of(Sort.SortOption.DEFAULT_ASC, ref("name1", STRING));
    Integer limit = 1;
    Integer offset = 1;

    LogicalPlan plan =
        project(
            limit(
                LogicalPlanDSL.dedupe(
                    rareTopN(
                        sort(
                            eval(
                                remove(
                                    rename(
                                        aggregation(
                                            filter(values(emptyList()), filterExpr),
                                            aggregators,
                                            groupByExprs),
                                        mappings),
                                    exclude),
                                newEvalField),
                            sortField),
                        CommandType.TOP,
                        topByExprs,
                        rareTopNField),
                    dedupeField),
                limit,
                offset),
            include);

    PhysicalPlan actual = plan.accept(implementor, null);

    assertEquals(
        PhysicalPlanDSL.project(
            PhysicalPlanDSL.limit(
                PhysicalPlanDSL.dedupe(
                    PhysicalPlanDSL.rareTopN(
                        PhysicalPlanDSL.sort(
                            PhysicalPlanDSL.eval(
                                PhysicalPlanDSL.remove(
                                    PhysicalPlanDSL.rename(
                                        PhysicalPlanDSL.agg(
                                            PhysicalPlanDSL.filter(
                                                PhysicalPlanDSL.values(emptyList()),
                                                filterExpr),
                                            aggregators,
                                            groupByExprs),
                                        mappings),
                                    exclude),
                                newEvalField),
                            sortField),
                        CommandType.TOP,
                        topByExprs,
                        rareTopNField),
                    dedupeField),
                limit,
                offset),
            include),
        actual);
  }

  @Test
  public void visitRelationShouldThrowException() {
    assertThrows(UnsupportedOperationException.class,
        () -> new LogicalRelation("test").accept(implementor, null));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void visitWindowOperatorShouldReturnPhysicalWindowOperator() {
    NamedExpression windowFunction = named(new RowNumberFunction());
    WindowDefinition windowDefinition = new WindowDefinition(
        Collections.singletonList(ref("state", STRING)),
        Collections.singletonList(
            ImmutablePair.of(Sort.SortOption.DEFAULT_DESC, ref("age", INTEGER))));

    NamedExpression[] projectList = {
        named("state", ref("state", STRING)),
        named("row_number", ref("row_number", INTEGER))
    };
    Pair[] sortList = {
        ImmutablePair.of(Sort.SortOption.DEFAULT_ASC, ref("state", STRING)),
        ImmutablePair.of(Sort.SortOption.DEFAULT_DESC, ref("age", STRING))
    };

    LogicalPlan logicalPlan =
        project(
            window(
                sort(
                    values(),
                    sortList),
                windowFunction,
                windowDefinition),
            projectList);

    PhysicalPlan physicalPlan =
        PhysicalPlanDSL.project(
            PhysicalPlanDSL.window(
                PhysicalPlanDSL.sort(
                    PhysicalPlanDSL.values(),
                    sortList),
                windowFunction,
                windowDefinition),
            projectList);

    assertEquals(physicalPlan, logicalPlan.accept(implementor, null));
  }
}
