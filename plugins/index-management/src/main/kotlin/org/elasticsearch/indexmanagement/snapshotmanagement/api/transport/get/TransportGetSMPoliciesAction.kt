/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.get

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.ExistsQueryBuilder
import org.elasticsearch.index.query.Operator
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.indexmanagement.IndexManagementPlugin
import org.elasticsearch.indexmanagement.common.model.rest.SearchParams
import org.elasticsearch.indexmanagement.opensearchapi.contentParser
import org.elasticsearch.indexmanagement.opensearchapi.parseWithType
import org.elasticsearch.indexmanagement.opensearchapi.suspendUntil
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICIES_ACTION_NAME
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.elasticsearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.elasticsearch.indexmanagement.snapshotmanagement.util.SM_POLICY_NAME_KEYWORD
import org.elasticsearch.indexmanagement.util.SecurityUtils
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.transport.TransportService

class TransportGetSMPoliciesAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<GetSMPoliciesRequest, GetSMPoliciesResponse>(
    GET_SM_POLICIES_ACTION_NAME, transportService, client, actionFilters, ::GetSMPoliciesRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: GetSMPoliciesRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): GetSMPoliciesResponse {
        val searchParams = request.searchParams
        val (policies, totalPoliciesCount) = getAllPolicies(searchParams, user)

        return GetSMPoliciesResponse(policies, totalPoliciesCount)
    }

    private suspend fun getAllPolicies(searchParams: SearchParams, user: User?): Pair<List<SMPolicy>, Long> {
        val searchRequest = getAllPoliciesRequest(searchParams, user)
        val searchResponse: SearchResponse = try {
            client.suspendUntil { search(searchRequest, it) }
        } catch (e: IndexNotFoundException) {
            throw ElasticsearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        }
        return parseGetAllPoliciesResponse(searchResponse)
    }

    private fun getAllPoliciesRequest(searchParams: SearchParams, user: User?): SearchRequest {
        val sortBuilder = searchParams.getSortBuilder()

        val queryBuilder = BoolQueryBuilder()
            .filter(ExistsQueryBuilder(SMPolicy.SM_TYPE))
            .must(
                QueryBuilders.queryStringQuery(searchParams.queryString)
                    .defaultOperator(Operator.AND)
                    .field(SM_POLICY_NAME_KEYWORD)
            )

        // Add user filter if enabled
        SecurityUtils.addUserFilter(user, queryBuilder, filterByEnabled, "sm_policy.user")

        val searchSourceBuilder = SearchSourceBuilder()
            .size(searchParams.size)
            .from(searchParams.from)
            .sort(sortBuilder)
            .query(queryBuilder)
            .seqNoAndPrimaryTerm(true)
        return SearchRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
    }

    private fun parseGetAllPoliciesResponse(searchResponse: SearchResponse): Pair<List<SMPolicy>, Long> {
        return try {
            val totalPolicies = searchResponse.hits.totalHits?.value ?: 0L
            searchResponse.hits.hits.map {
                contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, SMPolicy.Companion::parse)
            } to totalPolicies
        } catch (e: Exception) {
            log.error("Failed to parse snapshot management policy in search response", e)
            throw ElasticsearchStatusException("Failed to parse snapshot management policy", RestStatus.NOT_FOUND)
        }
    }
}
