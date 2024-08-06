


package org.elasticsearch.sql.legacy.query.planner;

import org.elasticsearch.client.Client;
import org.elasticsearch.sql.legacy.query.join.HashJoinElasticRequestBuilder;
import org.elasticsearch.sql.legacy.query.planner.core.Config;
import org.elasticsearch.sql.legacy.query.planner.core.QueryParams;
import org.elasticsearch.sql.legacy.query.planner.core.QueryPlanner;
import org.elasticsearch.sql.legacy.request.SqlRequest;

/**
 * QueryPlanner builder for Hash Join query. In future, different queries could have its own builders to generate
 * QueryPlanner. QueryPlanner would run all stages in its pipeline no matter how it be assembled.
 */
public class HashJoinQueryPlanRequestBuilder extends HashJoinElasticRequestBuilder {

    /**
     * Client connection to Elasticsearch cluster
     */
    private final Client client;

    /**
     * Query request
     */
    private final SqlRequest request;

    /**
     * Query planner configuration
     */
    private final Config config;


    public HashJoinQueryPlanRequestBuilder(Client client, SqlRequest request) {
        this.client = client;
        this.request = request;
        this.config = new Config();
    }

    @Override
    public String explain() {
        return plan().explain();
    }

    /**
     * Planning for the query and create planner for explain/execute later.
     *
     * @return query planner
     */
    public QueryPlanner plan() {
        config.configureLimit(
                getTotalLimit(),
                getFirstTable().getHintLimit(),
                getSecondTable().getHintLimit()
        );
        config.configureTermsFilterOptimization(isUseTermFiltersOptimization());

        return new QueryPlanner(
                client,
                config,
                new QueryParams(
                        getFirstTable(),
                        getSecondTable(),
                        getJoinType(),
                        getT1ToT2FieldsComparison()
                )
        );
    }

    public Config getConfig() {
        return config;
    }
}
