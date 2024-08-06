


package org.elasticsearch.sql.search.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.relation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.planner.logical.LogicalAD;
import org.elasticsearch.sql.planner.logical.LogicalMLCommons;
import org.elasticsearch.sql.planner.logical.LogicalPlan;

@ExtendWith(MockitoExtension.class)
public class ElasticsearchDefaultImplementorTest {

  @Mock
  ElasticsearchIndexScan indexScan;
  @Mock
  ElasticsearchClient client;

  /**
   * For test coverage.
   */
  @Test
  public void visitInvalidTypeShouldThrowException() {
    final ElasticsearchIndex.ElasticsearchDefaultImplementor implementor =
        new ElasticsearchIndex.ElasticsearchDefaultImplementor(indexScan, client);

    final IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> implementor.visitNode(relation("index"),
            indexScan));
    ;
    assertEquals(
        "unexpected plan node type "
            + "class org.elasticsearch.sql.planner.logical.LogicalRelation",
        exception.getMessage());
  }

  @Test
  public void visitMachineLearning() {
    LogicalMLCommons node = Mockito.mock(LogicalMLCommons.class,
            Answers.RETURNS_DEEP_STUBS);
    Mockito.when(node.getChild().get(0)).thenReturn(Mockito.mock(LogicalPlan.class));
    ElasticsearchIndex.ElasticsearchDefaultImplementor implementor =
            new ElasticsearchIndex.ElasticsearchDefaultImplementor(indexScan, client);
    assertNotNull(implementor.visitMLCommons(node, indexScan));
  }

  @Test
  public void visitAD() {
    LogicalAD node = Mockito.mock(LogicalAD.class,
            Answers.RETURNS_DEEP_STUBS);
    Mockito.when(node.getChild().get(0)).thenReturn(Mockito.mock(LogicalPlan.class));
    ElasticsearchIndex.ElasticsearchDefaultImplementor implementor =
            new ElasticsearchIndex.ElasticsearchDefaultImplementor(indexScan, client);
    assertNotNull(implementor.visitAD(node, indexScan));
  }
}
