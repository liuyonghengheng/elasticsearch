


package org.elasticsearch.sql.search.storage.system;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.elasticsearch.sql.expression.DSL.named;
import static org.elasticsearch.sql.expression.DSL.ref;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.elasticsearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.elasticsearch.sql.utils.SystemIndexUtils.TABLE_INFO;
import static org.elasticsearch.sql.utils.SystemIndexUtils.mappingTable;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.planner.physical.ProjectOperator;

@ExtendWith(MockitoExtension.class)
class ElasticsearchSystemIndexTest {

  @Mock
  private ElasticsearchClient client;

  @Test
  void testGetFieldTypesOfMetaTable() {
    ElasticsearchSystemIndex systemIndex = new ElasticsearchSystemIndex(client, TABLE_INFO);
    final Map<String, ExprType> fieldTypes = systemIndex.getFieldTypes();
    assertThat(fieldTypes, anyOf(
        hasEntry("TABLE_CAT", STRING)
    ));
  }

  @Test
  void testGetFieldTypesOfMappingTable() {
    ElasticsearchSystemIndex systemIndex = new ElasticsearchSystemIndex(client, mappingTable(
        "test_index"));
    final Map<String, ExprType> fieldTypes = systemIndex.getFieldTypes();
    assertThat(fieldTypes, anyOf(
        hasEntry("COLUMN_NAME", STRING)
    ));
  }

  @Test
  void implement() {
    ElasticsearchSystemIndex systemIndex = new ElasticsearchSystemIndex(client, TABLE_INFO);
    NamedExpression projectExpr = named("TABLE_NAME", ref("TABLE_NAME", STRING));

    final PhysicalPlan plan = systemIndex.implement(
        project(
            relation(TABLE_INFO),
            projectExpr
        ));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(plan.getChild().get(0) instanceof ElasticsearchSystemIndexScan);
  }
}
