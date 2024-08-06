


package org.elasticsearch.sql.legacy.executor.adapter;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.sql.legacy.expression.domain.BindingTuple;
import org.elasticsearch.sql.legacy.query.SqlElasticRequestBuilder;
import org.elasticsearch.sql.legacy.query.planner.core.BindingTupleQueryPlanner;
import org.elasticsearch.sql.legacy.query.planner.core.ColumnNode;

/**
 * The definition of QueryPlan SqlElasticRequestBuilder.
 */
@RequiredArgsConstructor
public class QueryPlanRequestBuilder implements SqlElasticRequestBuilder {
    private final BindingTupleQueryPlanner queryPlanner;

    public List<BindingTuple> execute() {
        return queryPlanner.execute();
    }

    public List<ColumnNode> outputColumns() {
        return queryPlanner.getColumnNodes();
    }

    @Override
    public String explain() {
        return queryPlanner.explain();
    }

    @Override
    public ActionRequest request() {
        throw new RuntimeException("unsupported operation");
    }

    @Override
    public ActionResponse get() {
        throw new RuntimeException("unsupported operation");
    }

    @Override
    public ActionRequestBuilder getBuilder() {
        throw new RuntimeException("unsupported operation");
    }
}
