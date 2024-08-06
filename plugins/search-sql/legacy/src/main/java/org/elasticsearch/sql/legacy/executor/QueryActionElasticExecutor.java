


package org.elasticsearch.sql.legacy.executor;

import java.io.IOException;
import java.util.List;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.sql.legacy.exception.SqlParseException;
import org.elasticsearch.sql.legacy.executor.adapter.QueryPlanQueryAction;
import org.elasticsearch.sql.legacy.executor.adapter.QueryPlanRequestBuilder;
import org.elasticsearch.sql.legacy.executor.join.ElasticJoinExecutor;
import org.elasticsearch.sql.legacy.executor.multi.MultiRequestExecutorFactory;
import org.elasticsearch.sql.legacy.expression.domain.BindingTuple;
import org.elasticsearch.sql.legacy.query.AggregationQueryAction;
import org.elasticsearch.sql.legacy.query.DefaultQueryAction;
import org.elasticsearch.sql.legacy.query.DeleteQueryAction;
import org.elasticsearch.sql.legacy.query.DescribeQueryAction;
import org.elasticsearch.sql.legacy.query.QueryAction;
import org.elasticsearch.sql.legacy.query.ShowQueryAction;
import org.elasticsearch.sql.legacy.query.SqlElasticRequestBuilder;
import org.elasticsearch.sql.legacy.query.SqlElasticsearchRequestBuilder;
import org.elasticsearch.sql.legacy.query.join.ElasticsearchJoinQueryAction;
import org.elasticsearch.sql.legacy.query.multi.MultiQueryAction;
import org.elasticsearch.sql.legacy.query.multi.MultiQueryRequestBuilder;

/**
 * Created by Eliran on 3/10/2015.
 */
public class QueryActionElasticExecutor {
    public static SearchHits executeSearchAction(DefaultQueryAction searchQueryAction) throws SqlParseException {
        SqlElasticsearchRequestBuilder builder = searchQueryAction.explain();
        return ((SearchResponse) builder.get()).getHits();
    }

    public static SearchHits executeJoinSearchAction(Client client, ElasticsearchJoinQueryAction joinQueryAction)
            throws IOException, SqlParseException {
        SqlElasticRequestBuilder joinRequestBuilder = joinQueryAction.explain();
        ElasticJoinExecutor executor = ElasticJoinExecutor.createJoinExecutor(client, joinRequestBuilder);
        executor.run();
        return executor.getHits();
    }

    public static Aggregations executeAggregationAction(AggregationQueryAction aggregationQueryAction)
            throws SqlParseException {
        SqlElasticsearchRequestBuilder select = aggregationQueryAction.explain();
        return ((SearchResponse) select.get()).getAggregations();
    }

    public static List<BindingTuple> executeQueryPlanQueryAction(QueryPlanQueryAction queryPlanQueryAction) {
        QueryPlanRequestBuilder select = (QueryPlanRequestBuilder) queryPlanQueryAction.explain();
        return select.execute();
    }

    public static ActionResponse executeShowQueryAction(ShowQueryAction showQueryAction) {
        return showQueryAction.explain().get();
    }

    public static ActionResponse executeDescribeQueryAction(DescribeQueryAction describeQueryAction) {
        return describeQueryAction.explain().get();
    }

    public static ActionResponse executeDeleteAction(DeleteQueryAction deleteQueryAction) throws SqlParseException {
        return deleteQueryAction.explain().get();
    }

    public static SearchHits executeMultiQueryAction(Client client, MultiQueryAction queryAction)
            throws SqlParseException, IOException {
        SqlElasticRequestBuilder multiRequestBuilder = queryAction.explain();
        ElasticHitsExecutor executor = MultiRequestExecutorFactory.createExecutor(client,
                (MultiQueryRequestBuilder) multiRequestBuilder);
        executor.run();
        return executor.getHits();
    }

    public static Object executeAnyAction(Client client, QueryAction queryAction)
            throws SqlParseException, IOException {
        if (queryAction instanceof DefaultQueryAction) {
            return executeSearchAction((DefaultQueryAction) queryAction);
        }
        if (queryAction instanceof AggregationQueryAction) {
            return executeAggregationAction((AggregationQueryAction) queryAction);
        }
        if (queryAction instanceof QueryPlanQueryAction) {
            return executeQueryPlanQueryAction((QueryPlanQueryAction) queryAction);
        }
        if (queryAction instanceof ShowQueryAction) {
            return executeShowQueryAction((ShowQueryAction) queryAction);
        }
        if (queryAction instanceof DescribeQueryAction) {
            return executeDescribeQueryAction((DescribeQueryAction) queryAction);
        }
        if (queryAction instanceof ElasticsearchJoinQueryAction) {
            return executeJoinSearchAction(client, (ElasticsearchJoinQueryAction) queryAction);
        }
        if (queryAction instanceof MultiQueryAction) {
            return executeMultiQueryAction(client, (MultiQueryAction) queryAction);
        }
        if (queryAction instanceof DeleteQueryAction) {
            return executeDeleteAction((DeleteQueryAction) queryAction);
        }
        return null;
    }
}
