/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.action.explain

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.IdsQueryBuilder
import org.elasticsearch.index.query.WildcardQueryBuilder
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import org.elasticsearch.indexmanagement.opensearchapi.contentParser
import org.elasticsearch.indexmanagement.opensearchapi.parseWithType
import org.elasticsearch.indexmanagement.rollup.model.ExplainRollup
import org.elasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.indexmanagement.rollup.model.RollupMetadata
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.addUserFilter
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.RemoteTransportException
import org.elasticsearch.transport.TransportService
import kotlin.Exception

class TransportExplainRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    actionFilters: ActionFilters
) : HandledTransportAction<ExplainRollupRequest, ExplainRollupResponse>(
    ExplainRollupAction.NAME, transportService, actionFilters, ::ExplainRollupRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    @Suppress("SpreadOperator")
    override fun doExecute(task: Task, request: ExplainRollupRequest, actionListener: ActionListener<ExplainRollupResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val ids = request.rollupIDs
        // Instantiate concrete ids to metadata map by removing wildcard matches
        val idsToExplain: MutableMap<String, ExplainRollup?> = ids.filter { !it.contains("*") }.map { it to null }.toMap(mutableMapOf())
        // First search is for all rollup documents that match at least one of the given rollupIDs
        val queryBuilder = BoolQueryBuilder().minimumShouldMatch(1).apply {
            ids.forEach {
                this.should(WildcardQueryBuilder("${Rollup.ROLLUP_TYPE}.${Rollup.ROLLUP_ID_FIELD}.keyword", "*$it*"))
            }
        }
        val user = buildUser(client.threadPool().threadContext)
        addUserFilter(user, queryBuilder, filterByEnabled, "rollup.user")

        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(SearchSourceBuilder().size(MAX_HITS).query(queryBuilder))
        client.threadPool().threadContext.stashContext().use {
            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        try {
                            response.hits.hits.forEach {
                                val rollup = contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, Rollup.Companion::parse)
                                idsToExplain[rollup.id] = ExplainRollup(metadataID = rollup.metadataID)
                            }
                        } catch (e: Exception) {
                            log.error("Failed to parse explain response", e)
                            actionListener.onFailure(e)
                            return
                        }

                        val metadataIds = idsToExplain.values.mapNotNull { it?.metadataID }
                        val metadataSearchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX)
                            .source(SearchSourceBuilder().size(MAX_HITS).query(IdsQueryBuilder().addIds(*metadataIds.toTypedArray())))
                        client.search(
                            metadataSearchRequest,
                            object : ActionListener<SearchResponse> {
                                override fun onResponse(response: SearchResponse) {
                                    try {
                                        response.hits.hits.forEach {
                                            val metadata = contentParser(it.sourceRef)
                                                .parseWithType(it.id, it.seqNo, it.primaryTerm, RollupMetadata.Companion::parse)
                                            idsToExplain.computeIfPresent(metadata.rollupID) { _,
                                                explainRollup ->
                                                explainRollup.copy(metadata = metadata)
                                            }
                                        }
                                        actionListener.onResponse(ExplainRollupResponse(idsToExplain.toMap()))
                                    } catch (e: Exception) {
                                        log.error("Failed to parse rollup metadata", e)
                                        actionListener.onFailure(e)
                                        return
                                    }
                                }

                                override fun onFailure(e: Exception) {
                                    log.error("Failed to search rollup metadata", e)
                                    when (e) {
                                        is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                                        else -> actionListener.onFailure(e)
                                    }
                                }
                            }
                        )
                    }

                    override fun onFailure(e: Exception) {
                        log.error("Failed to search for rollups", e)
                        when (e) {
                            is ResourceNotFoundException -> {
                                val nonWildcardIds = ids.filter { !it.contains("*") }.map { it to null }.toMap(mutableMapOf())
                                actionListener.onResponse(ExplainRollupResponse(nonWildcardIds))
                            }
                            is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                            else -> actionListener.onFailure(e)
                        }
                    }
                }
            )
        }
    }
}
