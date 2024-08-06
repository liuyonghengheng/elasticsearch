


package org.elasticsearch.sql.legacy.query.planner.physical.node.scroll;

import java.util.Iterator;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.sql.legacy.exception.SqlParseException;
import org.elasticsearch.sql.legacy.expression.domain.BindingTuple;
import org.elasticsearch.sql.legacy.query.AggregationQueryAction;
import org.elasticsearch.sql.legacy.query.QueryAction;
import org.elasticsearch.sql.legacy.query.planner.core.ExecuteParams;
import org.elasticsearch.sql.legacy.query.planner.core.PlanNode;
import org.elasticsearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.elasticsearch.sql.legacy.query.planner.physical.Row;
import org.elasticsearch.sql.legacy.query.planner.physical.estimation.Cost;

/**
 * The definition of Scroll Operator.
 */
@RequiredArgsConstructor
public class PhysicalScroll implements PhysicalOperator<BindingTuple> {
    private final QueryAction queryAction;

    private Iterator<BindingTupleRow> rowIterator;

    @Override
    public Cost estimate() {
        return null;
    }

    @Override
    public PlanNode[] children() {
        return new PlanNode[0];
    }

    @Override
    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    @Override
    public Row<BindingTuple> next() {
        return rowIterator.next();
    }

    @Override
    public void open(ExecuteParams params) {
        try {
            ActionResponse response = queryAction.explain().get();
            if (queryAction instanceof AggregationQueryAction) {
                rowIterator = SearchAggregationResponseHelper
                        .populateSearchAggregationResponse(((SearchResponse) response).getAggregations())
                        .iterator();
            } else {
                throw new IllegalStateException("Not support QueryAction type: " + queryAction.getClass());
            }
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    @SneakyThrows
    @Override
    public String toString() {
        return queryAction.explain().toString();
    }
}
