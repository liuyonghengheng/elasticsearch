


package org.elasticsearch.sql.search.storage.script.sort;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.elasticsearch.sql.data.type.ExprCoreType.INTEGER;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.elasticsearch.sql.ast.tree.Sort;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.config.ExpressionConfig;

class SortQueryBuilderTest {

  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());

  private SortQueryBuilder sortQueryBuilder = new SortQueryBuilder();

  @Test
  void build_sortbuilder_from_reference() {
    assertNotNull(sortQueryBuilder.build(DSL.ref("intV", INTEGER), Sort.SortOption.DEFAULT_ASC));
  }

  @Test
  void build_sortbuilder_from_function_should_throw_exception() {
    final IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> sortQueryBuilder.build(dsl.equal(DSL.ref(
            "intV", INTEGER), DSL.literal(1)), Sort.SortOption.DEFAULT_ASC));
    assertThat(exception.getMessage(), Matchers.containsString("unsupported expression"));
  }
}
