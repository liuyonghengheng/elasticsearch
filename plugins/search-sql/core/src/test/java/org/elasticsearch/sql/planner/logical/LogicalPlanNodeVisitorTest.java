


package org.elasticsearch.sql.planner.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.elasticsearch.sql.expression.DSL.named;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.ast.dsl.AstDSL;
import org.elasticsearch.sql.ast.expression.DataType;
import org.elasticsearch.sql.ast.expression.Literal;
import org.elasticsearch.sql.ast.tree.RareTopN.CommandType;
import org.elasticsearch.sql.ast.tree.Sort.SortOption;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.ReferenceExpression;
import org.elasticsearch.sql.expression.aggregation.Aggregator;
import org.elasticsearch.sql.expression.window.WindowDefinition;

/**
 * Todo. Temporary added for UT coverage, Will be removed.
 */
@ExtendWith(MockitoExtension.class)
class LogicalPlanNodeVisitorTest {

  @Mock
  Expression expression;
  @Mock
  ReferenceExpression ref;
  @Mock
  Aggregator aggregator;

  @Test
  public void logicalPlanShouldTraversable() {
    LogicalPlan logicalPlan =
        LogicalPlanDSL.rename(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.rareTopN(
                    LogicalPlanDSL.filter(LogicalPlanDSL.relation("schema"), expression),
                    CommandType.TOP,
                    ImmutableList.of(expression),
                    expression),
                ImmutableList.of(DSL.named("avg", aggregator)),
                ImmutableList.of(DSL.named("group", expression))),
            ImmutableMap.of(ref, ref));

    Integer result = logicalPlan.accept(new NodesCount(), null);
    assertEquals(5, result);
  }

  @Test
  public void testAbstractPlanNodeVisitorShouldReturnNull() {
    LogicalPlan relation = LogicalPlanDSL.relation("schema");
    assertNull(relation.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan filter = LogicalPlanDSL.filter(relation, expression);
    assertNull(filter.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan aggregation =
        LogicalPlanDSL.aggregation(
            filter, ImmutableList.of(DSL.named("avg", aggregator)), ImmutableList.of(DSL.named(
                "group", expression)));
    assertNull(aggregation.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan rename = LogicalPlanDSL.rename(aggregation, ImmutableMap.of(ref, ref));
    assertNull(rename.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan project = LogicalPlanDSL.project(relation, named("ref", ref));
    assertNull(project.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan remove = LogicalPlanDSL.remove(relation, ref);
    assertNull(remove.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan eval = LogicalPlanDSL.eval(relation, Pair.of(ref, expression));
    assertNull(eval.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan sort = LogicalPlanDSL.sort(relation,
        Pair.of(SortOption.DEFAULT_ASC, expression));
    assertNull(sort.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan dedup = LogicalPlanDSL.dedupe(relation, 1, false, false, expression);
    assertNull(dedup.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan window = LogicalPlanDSL.window(relation, named(expression), new WindowDefinition(
        ImmutableList.of(ref), ImmutableList.of(Pair.of(SortOption.DEFAULT_ASC, expression))));
    assertNull(window.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan rareTopN = LogicalPlanDSL.rareTopN(
        relation, CommandType.TOP, ImmutableList.of(expression), expression);
    assertNull(rareTopN.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan mlCommons = new LogicalMLCommons(LogicalPlanDSL.relation("schema"),
            "kmeans",
            AstDSL.exprList(AstDSL.argument("k", AstDSL.intLiteral(3))));
    assertNull(mlCommons.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));

    LogicalPlan ad = new LogicalAD(LogicalPlanDSL.relation("schema"),
            new HashMap<String, Literal>() {{
              put("shingle_size", new Literal(8, DataType.INTEGER));
              put("time_decay", new Literal(0.0001, DataType.DOUBLE));
              put("time_field", new Literal(null, DataType.STRING));
            }
        });
    assertNull(ad.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));
  }

  private static class NodesCount extends LogicalPlanNodeVisitor<Integer, Object> {
    @Override
    public Integer visitRelation(LogicalRelation plan, Object context) {
      return 1;
    }

    @Override
    public Integer visitFilter(LogicalFilter plan, Object context) {
      return 1
          + plan.getChild().stream()
          .map(child -> child.accept(this, context))
          .collect(Collectors.summingInt(Integer::intValue));
    }

    @Override
    public Integer visitAggregation(LogicalAggregation plan, Object context) {
      return 1
          + plan.getChild().stream()
          .map(child -> child.accept(this, context))
          .collect(Collectors.summingInt(Integer::intValue));
    }

    @Override
    public Integer visitRename(LogicalRename plan, Object context) {
      return 1
          + plan.getChild().stream()
          .map(child -> child.accept(this, context))
          .collect(Collectors.summingInt(Integer::intValue));
    }

    @Override
    public Integer visitRareTopN(LogicalRareTopN plan, Object context) {
      return 1
          + plan.getChild().stream()
          .map(child -> child.accept(this, context))
          .collect(Collectors.summingInt(Integer::intValue));
    }
  }
}
