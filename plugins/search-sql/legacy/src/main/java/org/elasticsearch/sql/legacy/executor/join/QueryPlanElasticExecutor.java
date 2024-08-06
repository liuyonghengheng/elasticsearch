


package org.elasticsearch.sql.legacy.executor.join;

import java.util.List;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.sql.legacy.query.planner.HashJoinQueryPlanRequestBuilder;
import org.elasticsearch.sql.legacy.query.planner.core.QueryPlanner;

/**
 * Executor for generic QueryPlanner execution. This executor is just acting as adaptor to integrate with
 * existing framework. In future, QueryPlanner should be executed by itself and leave the response sent back
 * or other post-processing logic to ElasticDefaultRestExecutor.
 */
class QueryPlanElasticExecutor extends ElasticJoinExecutor {

    private final QueryPlanner queryPlanner;

    QueryPlanElasticExecutor(HashJoinQueryPlanRequestBuilder request) {
        super(request);
        this.queryPlanner = request.plan();
    }

    @Override
    protected List<SearchHit> innerRun() {
        List<SearchHit> result = queryPlanner.execute();
        populateMetaResult();
        return result;
    }

    private void populateMetaResult() {
        metaResults.addTotalNumOfShards(queryPlanner.getMetaResult().getTotalNumOfShards());
        metaResults.addSuccessfulShards(queryPlanner.getMetaResult().getSuccessfulShards());
        metaResults.addFailedShards(queryPlanner.getMetaResult().getFailedShards());
        metaResults.updateTimeOut(queryPlanner.getMetaResult().isTimedOut());
    }

}
