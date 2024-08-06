

package org.elasticsearch.sql.search.executor;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.common.response.ResponseListener;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.executor.ExecutionEngine;
import org.elasticsearch.sql.executor.Explain;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.search.executor.protector.ExecutionProtector;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.storage.TableScanOperator;

/** Elasticsearch execution engine implementation. */
@RequiredArgsConstructor
public class ElasticsearchExecutionEngine implements ExecutionEngine {

  private final ElasticsearchClient client;

  private final ExecutionProtector executionProtector;

  @Override
  public void execute(PhysicalPlan physicalPlan, ResponseListener<QueryResponse> listener) {
    PhysicalPlan plan = executionProtector.protect(physicalPlan);
    client.schedule(
        () -> {
          try {
            List<ExprValue> result = new ArrayList<>();
            plan.open();

            while (plan.hasNext()) {
              result.add(plan.next());
            }

            QueryResponse response = new QueryResponse(physicalPlan.schema(), result);
            listener.onResponse(response);
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            plan.close();
          }
        });
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    client.schedule(() -> {
      try {
        Explain searchExplain = new Explain() {
          @Override
          public ExplainResponseNode visitTableScan(TableScanOperator node, Object context) {
            return explain(node, context, explainNode -> {
              explainNode.setDescription(ImmutableMap.of("request", node.explain()));
            });
          }
        };

        listener.onResponse(searchExplain.apply(plan));
      } catch (Exception e) {
        listener.onFailure(e);
      }
    });
  }

}
