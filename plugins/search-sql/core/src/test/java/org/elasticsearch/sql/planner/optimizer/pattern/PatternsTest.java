


package org.elasticsearch.sql.planner.optimizer.pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.planner.logical.LogicalPlan;

@ExtendWith(MockitoExtension.class)
class PatternsTest {

  @Mock
  LogicalPlan plan;

  @Test
  void source_is_empty() {
    when(plan.getChild()).thenReturn(Collections.emptyList());
    assertFalse(Patterns.source().getFunction().apply(plan).isPresent());
  }
}
