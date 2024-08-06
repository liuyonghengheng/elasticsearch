


package org.elasticsearch.sql.search.storage.script.filter.lucene;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.elasticsearch.sql.data.model.ExprValueUtils;
import org.elasticsearch.sql.search.storage.script.filter.lucene.RangeQuery.Comparison;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RangeQueryTest {

  @Test
  void should_throw_exception_for_unsupported_comparison() {
    // Note that since we do switch check on enum comparison, this should'be impossible
    assertThrows(IllegalStateException.class, () ->
        new RangeQuery(Comparison.BETWEEN)
            .doBuild("name", STRING, ExprValueUtils.stringValue("John")));
  }

}
