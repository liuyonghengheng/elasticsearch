


package org.elasticsearch.sql.search.executor.protector;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.elasticsearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC;
import static org.elasticsearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.elasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.elasticsearch.sql.expression.DSL.literal;
import static org.elasticsearch.sql.expression.DSL.named;
import static org.elasticsearch.sql.expression.DSL.ref;
import static org.elasticsearch.sql.planner.physical.PhysicalPlanDSL.filter;
import static org.elasticsearch.sql.planner.physical.PhysicalPlanDSL.sort;
import static org.elasticsearch.sql.planner.physical.PhysicalPlanDSL.values;
import static org.elasticsearch.sql.planner.physical.PhysicalPlanDSL.window;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.sql.ast.dsl.AstDSL;
import org.elasticsearch.sql.ast.expression.DataType;
import org.elasticsearch.sql.ast.expression.Literal;
import org.elasticsearch.sql.ast.tree.RareTopN.CommandType;
import org.elasticsearch.sql.ast.tree.Sort;
import org.elasticsearch.sql.common.setting.Settings;
import org.elasticsearch.sql.data.model.ExprBooleanValue;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.expression.ReferenceExpression;
import org.elasticsearch.sql.expression.aggregation.AvgAggregator;
import org.elasticsearch.sql.expression.aggregation.NamedAggregator;
import org.elasticsearch.sql.expression.window.WindowDefinition;
import org.elasticsearch.sql.expression.window.aggregation.AggregateWindowFunction;
import org.elasticsearch.sql.expression.window.ranking.RankFunction;
import org.elasticsearch.sql.monitor.ResourceMonitor;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.search.data.value.ElasticsearchExprValueFactory;
import org.elasticsearch.sql.search.planner.physical.ADOperator;
import org.elasticsearch.sql.search.planner.physical.MLCommonsOperator;
import org.elasticsearch.sql.search.setting.SearchSettings;
import org.elasticsearch.sql.search.storage.ElasticsearchIndexScan;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.planner.physical.PhysicalPlanDSL;

@ExtendWith(MockitoExtension.class)
class ElasticsearchExecutionProtectorTest {

  @Mock
  private ElasticsearchClient client;

  @Mock
  private ResourceMonitor resourceMonitor;

  @Mock
  private ElasticsearchExprValueFactory exprValueFactory;

  @Mock
  private SearchSettings settings;

  private ElasticsearchExecutionProtector executionProtector;

  @BeforeEach
  public void setup() {
    executionProtector = new ElasticsearchExecutionProtector(resourceMonitor);
  }

  @Test
  public void testProtectIndexScan() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    String indexName = "test";
    NamedExpression include = named("age", ref("age", INTEGER));
    ReferenceExpression exclude = ref("name", STRING);
    ReferenceExpression dedupeField = ref("name", STRING);
    ReferenceExpression topField = ref("name", STRING);
    List<Expression> topExprs = Arrays.asList(ref("age", INTEGER));
    Expression filterExpr = literal(ExprBooleanValue.of(true));
    List<NamedExpression> groupByExprs = Arrays.asList(named("age", ref("age", INTEGER)));
    List<NamedAggregator> aggregators =
        Arrays.asList(named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)),
            DOUBLE)));
    Map<ReferenceExpression, ReferenceExpression> mappings =
        ImmutableMap.of(ref("name", STRING), ref("lastname", STRING));
    Pair<ReferenceExpression, Expression> newEvalField =
        ImmutablePair.of(ref("name1", STRING), ref("name", STRING));
    Integer sortCount = 100;
    Pair<Sort.SortOption, Expression> sortField =
        ImmutablePair.of(DEFAULT_ASC, ref("name1", STRING));
    Integer size = 200;
    Integer limit = 10;
    Integer offset = 10;

    assertEquals(
        PhysicalPlanDSL.project(
            PhysicalPlanDSL.limit(
                PhysicalPlanDSL.dedupe(
                    PhysicalPlanDSL.rareTopN(
                        resourceMonitor(
                            PhysicalPlanDSL.sort(
                                PhysicalPlanDSL.eval(
                                    PhysicalPlanDSL.remove(
                                        PhysicalPlanDSL.rename(
                                            PhysicalPlanDSL.agg(
                                                filter(
                                                    resourceMonitor(
                                                        new ElasticsearchIndexScan(
                                                            client, settings, indexName,
                                                            exprValueFactory)),
                                                    filterExpr),
                                                aggregators,
                                                groupByExprs),
                                            mappings),
                                        exclude),
                                    newEvalField),
                                sortField)),
                        CommandType.TOP,
                        topExprs,
                        topField),
                    dedupeField),
                limit,
                offset),
            include),
        executionProtector.protect(
            PhysicalPlanDSL.project(
                PhysicalPlanDSL.limit(
                    PhysicalPlanDSL.dedupe(
                        PhysicalPlanDSL.rareTopN(
                            PhysicalPlanDSL.sort(
                                PhysicalPlanDSL.eval(
                                    PhysicalPlanDSL.remove(
                                        PhysicalPlanDSL.rename(
                                            PhysicalPlanDSL.agg(
                                                filter(
                                                    new ElasticsearchIndexScan(
                                                        client, settings, indexName,
                                                        exprValueFactory),
                                                    filterExpr),
                                                aggregators,
                                                groupByExprs),
                                            mappings),
                                        exclude),
                                    newEvalField),
                                sortField),
                            CommandType.TOP,
                            topExprs,
                            topField),
                        dedupeField),
                    limit,
                    offset),
                include)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testProtectSortForWindowOperator() {
    NamedExpression rank = named(mock(RankFunction.class));
    Pair<Sort.SortOption, Expression> sortItem =
        ImmutablePair.of(DEFAULT_ASC, DSL.ref("age", INTEGER));
    WindowDefinition windowDefinition =
        new WindowDefinition(emptyList(), ImmutableList.of(sortItem));

    assertEquals(
        window(
            resourceMonitor(
                sort(
                    values(emptyList()),
                    sortItem)),
            rank,
            windowDefinition),
        executionProtector.protect(
            window(
                sort(
                    values(emptyList()),
                    sortItem
                ),
                rank,
                windowDefinition)));
  }

  @Test
  public void testProtectWindowOperatorInput() {
    NamedExpression avg = named(mock(AggregateWindowFunction.class));
    WindowDefinition windowDefinition = mock(WindowDefinition.class);

    assertEquals(
        window(
            resourceMonitor(
                values()),
            avg,
            windowDefinition),
        executionProtector.protect(
            window(
                values(),
                avg,
                windowDefinition)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNotProtectWindowOperatorInputIfAlreadyProtected() {
    NamedExpression avg = named(mock(AggregateWindowFunction.class));
    Pair<Sort.SortOption, Expression> sortItem =
        ImmutablePair.of(DEFAULT_ASC, DSL.ref("age", INTEGER));
    WindowDefinition windowDefinition =
        new WindowDefinition(emptyList(), ImmutableList.of(sortItem));

    assertEquals(
        window(
            resourceMonitor(
                sort(
                    values(emptyList()),
                    sortItem)),
            avg,
            windowDefinition),
        executionProtector.protect(
            window(
                sort(
                    values(emptyList()),
                    sortItem),
                avg,
                windowDefinition)));
  }

  @Test
  public void testWithoutProtection() {
    Expression filterExpr = literal(ExprBooleanValue.of(true));

    assertEquals(
        filter(
            filter(null, filterExpr),
            filterExpr),
        executionProtector.protect(
            filter(
                filter(null, filterExpr),
                filterExpr)
        )
    );
  }

  @Test
  public void testVisitMlCommons() {
    NodeClient nodeClient = mock(NodeClient.class);
    MLCommonsOperator mlCommonsOperator =
            new MLCommonsOperator(
              values(emptyList()),
              "kmeans",
              AstDSL.exprList(AstDSL.argument("k1", AstDSL.intLiteral(3))),
                nodeClient
            );

    assertEquals(executionProtector.doProtect(mlCommonsOperator),
            executionProtector.visitMLCommons(mlCommonsOperator, null));
  }

  @Test
  public void testVisitAD() {
    NodeClient nodeClient = mock(NodeClient.class);
    ADOperator adOperator =
            new ADOperator(
                values(emptyList()),
                new HashMap<String, Literal>() {{
                    put("shingle_size", new Literal(8, DataType.INTEGER));
                    put("time_decay", new Literal(0.0001, DataType.DOUBLE));
                    put("time_field", new Literal(null, DataType.STRING));
                }},
                nodeClient
            );

    assertEquals(executionProtector.doProtect(adOperator),
            executionProtector.visitAD(adOperator, null));
  }

  PhysicalPlan resourceMonitor(PhysicalPlan input) {
    return new ResourceMonitorPlan(input, resourceMonitor);
  }
}
