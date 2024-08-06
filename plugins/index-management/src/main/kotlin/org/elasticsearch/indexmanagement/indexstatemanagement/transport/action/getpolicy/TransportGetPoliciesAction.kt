/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.index.query.Operator
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.elasticsearch.indexmanagement.opensearchapi.parseFromSearchResponse
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.addUserFilter
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportGetPoliciesAction::class.java)

class TransportGetPoliciesAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetPoliciesRequest, GetPoliciesResponse>(
    GetPoliciesAction.NAME, transportService, actionFilters, ::GetPoliciesRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(
        task: Task,
        getPoliciesRequest: GetPoliciesRequest,
        actionListener: ActionListener<GetPoliciesResponse>
    ) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val params = getPoliciesRequest.searchParams
        val user = buildUser(client.threadPool().threadContext)

        val sortBuilder = params.getSortBuilder()

        val queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.existsQuery("policy"))

        // Add user filter if enabled
        addUserFilter(user, queryBuilder, filterByEnabled, "policy.user")

        queryBuilder.must(
            QueryBuilders
                .queryStringQuery(params.queryString)
                .defaultOperator(Operator.AND)
                .field("policy.policy_id.keyword")
        )

        val searchSourceBuilder = SearchSourceBuilder()
            .query(queryBuilder)
            .sort(sortBuilder)
            .from(params.from)
            .size(params.size)
            .seqNoAndPrimaryTerm(true)

        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(INDEX_MANAGEMENT_INDEX)

        client.threadPool().threadContext.stashContext().use {
            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        val totalPolicies = response.hits.totalHits?.value ?: 0
                        val policies = parseFromSearchResponse(response, xContentRegistry, Policy.Companion::parse)
                        actionListener.onResponse(GetPoliciesResponse(policies, totalPolicies.toInt()))
                    }

                    override fun onFailure(t: Exception) {
                        if (t is IndexNotFoundException) {
                            // config index hasn't been initialized, catch this here and show empty result on Kibana
                            actionListener.onResponse(GetPoliciesResponse(emptyList(), 0))
                            return
                        }
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
            )
        }
    }
}
