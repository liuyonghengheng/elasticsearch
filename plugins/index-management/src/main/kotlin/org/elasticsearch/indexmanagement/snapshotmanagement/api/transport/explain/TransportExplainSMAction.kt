/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.explain

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
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.ExistsQueryBuilder
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.index.query.WildcardQueryBuilder
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import org.elasticsearch.indexmanagement.opensearchapi.contentParser
import org.elasticsearch.indexmanagement.opensearchapi.parseWithType
import org.elasticsearch.indexmanagement.opensearchapi.suspendUntil
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.elasticsearch.indexmanagement.snapshotmanagement.model.ExplainSMPolicy
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.SM_METADATA_TYPE
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.ENABLED_FIELD
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.NAME_FIELD
import org.elasticsearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.elasticsearch.indexmanagement.snapshotmanagement.smMetadataDocIdToPolicyName
import org.elasticsearch.indexmanagement.util.SecurityUtils
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import org.elasticsearch.transport.TransportService

class TransportExplainSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<ExplainSMPolicyRequest, ExplainSMPolicyResponse>(
    SMActions.EXPLAIN_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::ExplainSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: ExplainSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): ExplainSMPolicyResponse {
        val policyNames = request.policyNames.toSet()

        val namesToEnabled = getPolicyEnabledStatus(policyNames, user)
        val namesToMetadata = getSMMetadata(namesToEnabled.keys)
        return buildExplainResponse(namesToEnabled, namesToMetadata)
    }

    @Suppress("ThrowsCount")
    private suspend fun getPolicyEnabledStatus(policyNames: Set<String>, user: User?): Map<String, Boolean> {
        // Search the config index for SM policies
        val searchRequest = getPolicyEnabledSearchRequest(policyNames, user)
        val searchResponse: SearchResponse = try {
            client.suspendUntil { search(searchRequest, it) }
        } catch (e: IndexNotFoundException) {
            throw ElasticsearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        } catch (e: Exception) {
            log.error("Failed to search for snapshot management policy", e)
            throw ElasticsearchStatusException("Failed to search for snapshot management policy", RestStatus.INTERNAL_SERVER_ERROR)
        }

        // Parse each returned policy to get the job enabled status
        return try {
            searchResponse.hits.hits.associate {
                parseNameToEnabled(contentParser(it.sourceRef))
            }
        } catch (e: Exception) {
            log.error("Failed to parse snapshot management policy in search response", e)
            throw ElasticsearchStatusException("Failed to parse snapshot management policy", RestStatus.NOT_FOUND)
        }
    }

    private fun getPolicyEnabledSearchRequest(policyNames: Set<String>, user: User?): SearchRequest {
        val queryBuilder = getPolicyQuery(policyNames)

        // Add user filter if enabled
        SecurityUtils.addUserFilter(user, queryBuilder, filterByEnabled, "sm_policy.user")

        // Only return the name and enabled field
        val includes = arrayOf(
            "${SMPolicy.SM_TYPE}.$NAME_FIELD",
            "${SMPolicy.SM_TYPE}.$ENABLED_FIELD"
        )
        val fetchSourceContext = FetchSourceContext(true, includes, arrayOf())
        val searchSourceBuilder = SearchSourceBuilder().size(MAX_HITS).query(queryBuilder).fetchSource(fetchSourceContext)
        return SearchRequest(INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
    }

    private fun getPolicyQuery(policyNames: Set<String>): BoolQueryBuilder {
        // Search for all SM Policy documents which match at least one of the given names
        val queryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(SMPolicy.SM_TYPE))
        queryBuilder.minimumShouldMatch(1).apply {
            policyNames.forEach { policyName ->
                if (policyName.contains('*') || policyName.contains('?')) {
                    this.should(WildcardQueryBuilder("${SMPolicy.SM_TYPE}.$NAME_FIELD", policyName))
                } else {
                    this.should(TermQueryBuilder("${SMPolicy.SM_TYPE}.$NAME_FIELD", policyName))
                }
            }
        }
        return queryBuilder
    }

    private suspend fun getSMMetadata(policyNames: Set<String>): Map<String, SMMetadata> {
        val searchRequest = getSMMetadataSearchRequest(policyNames)
        val searchResponse: SearchResponse = try {
            client.suspendUntil { search(searchRequest, it) }
        } catch (e: IndexNotFoundException) {
            throw ElasticsearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        }

        return try {
            searchResponse.hits.hits.associate {
                val smMetadata = contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, SMMetadata.Companion::parse)
                smMetadataDocIdToPolicyName(smMetadata.id) to smMetadata
            }
        } catch (e: Exception) {
            log.error("Failed to parse snapshot management metadata in search response", e)
            throw ElasticsearchStatusException("Failed to parse snapshot management metadata", RestStatus.NOT_FOUND)
        }
    }

    private fun getSMMetadataSearchRequest(policyNames: Set<String>): SearchRequest {
        // Search for all SM Metadata documents which match at least one of the given names
        val queryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(SM_METADATA_TYPE))
        queryBuilder.minimumShouldMatch(1).apply {
            policyNames.forEach {
                this.should(TermQueryBuilder("$SM_METADATA_TYPE.$NAME_FIELD", it))
            }
        }

        // Search the config index for SM Metadata
        return SearchRequest(INDEX_MANAGEMENT_INDEX).source(SearchSourceBuilder().size(MAX_HITS).query(queryBuilder))
    }

    private fun parseNameToEnabled(xcp: XContentParser): Pair<String, Boolean> {
        var name: String? = null
        var enabled: Boolean? = null

        if (xcp.currentToken() == null) xcp.nextToken()
        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                NAME_FIELD -> name = xcp.text()
                ENABLED_FIELD -> enabled = xcp.booleanValue()
            }
        }
        return requireNotNull(name) { "The name field of SMPolicy must not be null." } to
            requireNotNull(enabled) { "The enabled field of SMPolicy must not be null." }
    }

    private fun buildExplainResponse(namesToEnabled: Map<String, Boolean>, namesToMetadata: Map<String, SMMetadata>): ExplainSMPolicyResponse {
        val policiesToExplain = namesToEnabled.entries.associate { (policyName, enabled) ->
            policyName to ExplainSMPolicy(namesToMetadata[policyName], enabled)
        }
        log.debug("Explain response: $policiesToExplain")
        return ExplainSMPolicyResponse(policiesToExplain)
    }
}
