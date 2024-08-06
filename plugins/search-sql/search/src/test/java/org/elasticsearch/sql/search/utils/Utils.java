


package org.elasticsearch.sql.search.utils;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.sql.ast.tree.Sort;
import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.expression.ReferenceExpression;
import org.elasticsearch.sql.expression.aggregation.AvgAggregator;
import org.elasticsearch.sql.expression.aggregation.NamedAggregator;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexAgg;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexScan;
import org.elasticsearch.sql.planner.logical.LogicalPlan;

@UtilityClass
public class Utils {

  /**
   * Build ElasticsearchLogicalIndexScan.
   */
  public static LogicalPlan indexScan(String tableName, Expression filter) {
    return ElasticsearchLogicalIndexScan.builder().relationName(tableName).filter(filter).build();
  }

  /**
   * Build ElasticsearchLogicalIndexScan.
   */
  public static LogicalPlan indexScan(String tableName,
                                      Pair<Sort.SortOption, Expression>... sorts) {
    return ElasticsearchLogicalIndexScan.builder().relationName(tableName)
        .sortList(Arrays.asList(sorts))
        .build();
  }

  /**
   * Build ElasticsearchLogicalIndexScan.
   */
  public static LogicalPlan indexScan(String tableName,
                                      Expression filter,
                                      Pair<Sort.SortOption, Expression>... sorts) {
    return ElasticsearchLogicalIndexScan.builder().relationName(tableName)
        .filter(filter)
        .sortList(Arrays.asList(sorts))
        .build();
  }

  /**
   * Build ElasticsearchLogicalIndexScan.
   */
  public static LogicalPlan indexScan(String tableName, Integer offset, Integer limit,
                                      Set<ReferenceExpression> projectList) {
    return ElasticsearchLogicalIndexScan.builder().relationName(tableName)
        .offset(offset)
        .limit(limit)
        .projectList(projectList)
        .build();
  }

  /**
   * Build ElasticsearchLogicalIndexScan.
   */
  public static LogicalPlan indexScan(String tableName,
                                      Expression filter,
                                      Integer offset, Integer limit,
                                      Set<ReferenceExpression> projectList) {
    return ElasticsearchLogicalIndexScan.builder().relationName(tableName)
        .filter(filter)
        .offset(offset)
        .limit(limit)
        .projectList(projectList)
        .build();
  }

  /**
   * Build ElasticsearchLogicalIndexScan.
   */
  public static LogicalPlan indexScan(String tableName,
                                      Expression filter,
                                      Integer offset, Integer limit,
                                      List<Pair<Sort.SortOption, Expression>> sorts,
                                      Set<ReferenceExpression> projectList) {
    return ElasticsearchLogicalIndexScan.builder().relationName(tableName)
        .filter(filter)
        .sortList(sorts)
        .offset(offset)
        .limit(limit)
        .projectList(projectList)
        .build();
  }

  /**
   * Build ElasticsearchLogicalIndexScan.
   */
  public static LogicalPlan indexScan(String tableName,
                                      Set<ReferenceExpression> projects) {
    return ElasticsearchLogicalIndexScan.builder()
        .relationName(tableName)
        .projectList(projects)
        .build();
  }

  /**
   * Build ElasticsearchLogicalIndexScan.
   */
  public static LogicalPlan indexScan(String tableName, Expression filter,
                                      Set<ReferenceExpression> projects) {
    return ElasticsearchLogicalIndexScan.builder()
        .relationName(tableName)
        .filter(filter)
        .projectList(projects)
        .build();
  }

  /**
   * Build ElasticsearchLogicalIndexAgg.
   */
  public static LogicalPlan indexScanAgg(String tableName, List<NamedAggregator> aggregators,
                                         List<NamedExpression> groupByList) {
    return ElasticsearchLogicalIndexAgg.builder().relationName(tableName)
        .aggregatorList(aggregators).groupByList(groupByList).build();
  }

  /**
   * Build ElasticsearchLogicalIndexAgg.
   */
  public static LogicalPlan indexScanAgg(String tableName, List<NamedAggregator> aggregators,
                                         List<NamedExpression> groupByList,
                                         List<Pair<Sort.SortOption, Expression>> sortList) {
    return ElasticsearchLogicalIndexAgg.builder().relationName(tableName)
        .aggregatorList(aggregators).groupByList(groupByList).sortList(sortList).build();
  }

  /**
   * Build ElasticsearchLogicalIndexAgg.
   */
  public static LogicalPlan indexScanAgg(String tableName,
                                         Expression filter,
                                         List<NamedAggregator> aggregators,
                                         List<NamedExpression> groupByList) {
    return ElasticsearchLogicalIndexAgg.builder().relationName(tableName).filter(filter)
        .aggregatorList(aggregators).groupByList(groupByList).build();
  }

  public static AvgAggregator avg(Expression expr, ExprCoreType type) {
    return new AvgAggregator(Arrays.asList(expr), type);
  }

  public static List<NamedAggregator> agg(NamedAggregator... exprs) {
    return Arrays.asList(exprs);
  }

  public static List<NamedExpression> group(NamedExpression... exprs) {
    return Arrays.asList(exprs);
  }

  public static List<Pair<Sort.SortOption, Expression>> sort(Expression expr1,
                                                             Sort.SortOption option1) {
    return Collections.singletonList(Pair.of(option1, expr1));
  }

  public static List<Pair<Sort.SortOption, Expression>> sort(Expression expr1,
                                                             Sort.SortOption option1,
                                                             Expression expr2,
                                                             Sort.SortOption option2) {
    return Arrays.asList(Pair.of(option1, expr1), Pair.of(option2, expr2));
  }

  public static Set<ReferenceExpression> projects(ReferenceExpression... expressions) {
    return ImmutableSet.copyOf(expressions);
  }

  public static Set<ReferenceExpression> noProjects() {
    return null;
  }
}
