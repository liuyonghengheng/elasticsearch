


package org.elasticsearch.sql.search.storage.script.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.elasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.elasticsearch.sql.expression.DSL.named;
import static org.elasticsearch.sql.expression.DSL.ref;
import static org.elasticsearch.sql.search.utils.Utils.sort;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.search.aggregations.bucket.missing.MissingOrder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.sql.ast.tree.Sort;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.expression.ReferenceExpression;

@ExtendWith(MockitoExtension.class)
class GroupSortOrderTest {

  private final AggregationQueryBuilder.GroupSortOrder groupSortOrder =
      new AggregationQueryBuilder.GroupSortOrder(
          sort(ref("name", STRING), Sort.SortOption.DEFAULT_DESC,
              ref("age", INTEGER), Sort.SortOption.DEFAULT_ASC));
  @Mock
  private ReferenceExpression ref;

  @Test
  void both_expression_in_sort_list() {
    assertEquals(-1, compare(named("name", ref), named("age", ref)));
    assertEquals(1, compare(named("age", ref), named("name", ref)));
    assertEquals(SortOrder.DESC, sortOrder(named("name", ref)));
    assertEquals(MissingOrder.LAST, missingOrder(named("name", ref)));
    assertEquals(SortOrder.ASC, sortOrder(named("age", ref)));
    assertEquals(MissingOrder.FIRST, missingOrder(named("age", ref)));
  }

  @Test
  void only_one_expression_in_sort_list() {
    assertEquals(-1, compare(named("name", ref), named("noSort", ref)));
    assertEquals(1, compare(named("noSort", ref), named("name", ref)));
    assertEquals(SortOrder.DESC, sortOrder(named("name", ref)));
    assertEquals(MissingOrder.LAST, missingOrder(named("name", ref)));
    assertEquals(SortOrder.ASC, sortOrder(named("noSort", ref)));
    assertEquals(MissingOrder.FIRST, missingOrder(named("noSort", ref)));
  }

  @Test
  void no_expression_in_sort_list() {
    assertEquals(0, compare(named("noSort1", ref), named("noSort2", ref)));
    assertEquals(0, compare(named("noSort2", ref), named("noSort1", ref)));
    assertEquals(SortOrder.ASC, sortOrder(named("noSort1", ref)));
    assertEquals(MissingOrder.FIRST, missingOrder(named("noSort1", ref)));
    assertEquals(SortOrder.ASC, sortOrder(named("noSort2", ref)));
    assertEquals(MissingOrder.FIRST, missingOrder(named("noSort2", ref)));
  }

  private int compare(NamedExpression e1, NamedExpression e2) {
    return groupSortOrder.compare(e1, e2);
  }

  private SortOrder sortOrder(NamedExpression expr) {
    return groupSortOrder.sortOrder(expr);
  }

  private MissingOrder missingOrder(NamedExpression expr) {
    return groupSortOrder.missingOrder(expr);
  }
}
