/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.explain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
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
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.IdsQueryBuilder
import org.elasticsearch.index.query.WildcardQueryBuilder
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.opensearchapi.parseWithType
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.transform.model.ExplainTransform
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.indexmanagement.transform.model.TransformMetadata
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.addUserFilter
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.RemoteTransportException
import org.elasticsearch.transport.TransportService

class TransportExplainTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ExplainTransformRequest, ExplainTransformResponse>(
    ExplainTransformAction.NAME, transportService, actionFilters, ::ExplainTransformRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    @Suppress("SpreadOperator", "NestedBlockDepth", "LongMethod")
    override fun doExecute(task: Task, request: ExplainTransformRequest, actionListener: ActionListener<ExplainTransformResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val ids = request.transformIDs
        // Instantiate concrete ids to metadata map by removing wildcard matches
        val idsToExplain: MutableMap<String, ExplainTransform?> = ids.filter { !it.contains("*") }
            .map { it to null }.toMap(mutableMapOf())
        val failedToExplain: MutableMap<String, String> = mutableMapOf()
        val queryBuilder = BoolQueryBuilder().minimumShouldMatch(1).apply {
            ids.forEach {
                this.should(WildcardQueryBuilder("${ Transform.TRANSFORM_TYPE}.${Transform.TRANSFORM_ID_FIELD}.keyword", "*$it*"))
            }
        }
        val user = buildUser(client.threadPool().threadContext)
        addUserFilter(user, queryBuilder, filterByEnabled, "transform.user")

        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(SearchSourceBuilder().seqNoAndPrimaryTerm(true).query(queryBuilder))

        client.threadPool().threadContext.stashContext().use {
            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        val metadataIdToTransform: MutableMap<String, Transform> = HashMap()
                        try {
                            response.hits.hits.forEach {
                                val transform = contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, Transform.Companion::parse)
                                idsToExplain[transform.id] = ExplainTransform(metadataID = transform.metadataId)
                                if (transform.metadataId != null) metadataIdToTransform[transform.metadataId] = transform
                            }
                        } catch (e: Exception) {
                            log.error("Failed to parse explain response", e)
                            actionListener.onFailure(e)
                            return
                        }

                        val metadataIds = idsToExplain.values.mapNotNull { it?.metadataID }
                        val metadataSearchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX)
                            .source(SearchSourceBuilder().query(IdsQueryBuilder().addIds(*metadataIds.toTypedArray())))
                        client.search(
                            metadataSearchRequest,
                            object : ActionListener<SearchResponse> {
                                override fun onResponse(response: SearchResponse) {
                                    CoroutineScope(Dispatchers.IO).launch {
                                        response.hits.hits.forEach {
                                            try {
                                                val metadata = contentParser(it.sourceRef)
                                                    .parseWithType(it.id, it.seqNo, it.primaryTerm, TransformMetadata.Companion::parse)

                                                val transform = metadataIdToTransform[metadata.id]
                                                // Only add continuous stats for continuous transforms which have not failed
                                                if (transform?.continuous == true && metadata.status != TransformMetadata.Status.FAILED) {
                                                    addContinuousStats(transform, metadata)
                                                } else {
                                                    idsToExplain.computeIfPresent(metadata.transformId) { _, explainTransform ->
                                                        // Don't provide shardIDToGlobalCheckpoint for a failed or non-continuous transform
                                                        explainTransform.copy(metadata = metadata.copy(shardIDToGlobalCheckpoint = null))
                                                    }
                                                }
                                            } catch (e: Exception) {
                                                log.error("Failed to parse transform [${it.id}] metadata", e)
                                                idsToExplain.remove(it.id)
                                                failedToExplain[it.id] =
                                                    "Failed to parse transform metadata - ${e.message}"
                                            }
                                        }
                                        actionListener.onResponse(ExplainTransformResponse(idsToExplain.toMap(), failedToExplain))
                                    }
                                }

                                override fun onFailure(e: Exception) {
                                    log.error("Failed to search transform metadata", e)
                                    when (e) {
                                        is RemoteTransportException ->
                                            actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as java.lang.Exception)
                                        else -> actionListener.onFailure(e)
                                    }
                                }

                                private suspend fun addContinuousStats(transform: Transform, metadata: TransformMetadata) {
                                    val continuousStats = transform.getContinuousStats(client, metadata)
                                    if (continuousStats == null) {
                                        log.error("Failed to get continuous transform stats for transform [${transform.id}]")
                                        idsToExplain.remove(transform.id)
                                        failedToExplain[transform.id] =
                                            "Failed to get continuous transform stats"
                                    } else {
                                        idsToExplain.computeIfPresent(metadata.transformId) { _, explainTransform ->
                                            explainTransform.copy(
                                                metadata = metadata.copy(
                                                    shardIDToGlobalCheckpoint = null,
                                                    continuousStats = continuousStats
                                                )
                                            )
                                        }
                                    }
                                }
                            }
                        )
                    }

                    override fun onFailure(e: Exception) {
                        log.error("Failed to search for transforms", e)
                        when (e) {
                            is ResourceNotFoundException -> {
                                val failureReason = "Failed to search transform metadata"
                                val nonWildcardIds = ids.filter { !it.contains("*") }.map { it to failureReason }.toMap(mutableMapOf())
                                actionListener.onResponse(ExplainTransformResponse(mapOf(), nonWildcardIds))
                            }
                            is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as java.lang.Exception)
                            else -> actionListener.onFailure(e)
                        }
                    }
                }
            )
        }
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(
            xContentRegistry,
            LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON
        )
    }
}
