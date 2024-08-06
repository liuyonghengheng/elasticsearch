


package org.elasticsearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.elasticsearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.elasticsearch.sql.expression.DSL.literal;
import static org.elasticsearch.sql.planner.physical.PhysicalPlanDSL.values;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.elasticsearch.sql.data.model.ExprValue;

class ValuesOperatorTest {

  @Test
  public void shouldHaveNoChild() {
    ValuesOperator values = values(ImmutableList.of(literal(1)));
    assertThat(
        values.getChild(),
        is(empty())
    );
  }

  @Test
  public void iterateSingleRow() {
    ValuesOperator values = values(ImmutableList.of(literal(1), literal("abc")));
    List<ExprValue> results = new ArrayList<>();
    while (values.hasNext()) {
      results.add(values.next());
    }

    assertThat(
        results,
        contains(collectionValue(Arrays.asList(1, "abc")))
    );
  }

}
