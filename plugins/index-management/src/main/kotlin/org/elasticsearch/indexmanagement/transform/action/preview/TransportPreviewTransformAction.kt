/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.preview

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.indexmanagement.transform.TransformSearchService
import org.elasticsearch.indexmanagement.transform.TransformValidator
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportPreviewTransformAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    private val client: Client,
    private val clusterService: ClusterService,
    private val indexNameExpressionResolver: IndexNameExpressionResolver
) : HandledTransportAction<PreviewTransformRequest, PreviewTransformResponse>(
    PreviewTransformAction.NAME, transportService, actionFilters, ::PreviewTransformRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Suppress("SpreadOperator")
    override fun doExecute(task: Task, request: PreviewTransformRequest, listener: ActionListener<PreviewTransformResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val transform = request.transform

        val concreteIndices =
            indexNameExpressionResolver.concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpand(), true, transform.sourceIndex)
        if (concreteIndices.isEmpty()) {
            listener.onFailure(ElasticsearchStatusException("No specified source index exist in the cluster", RestStatus.NOT_FOUND))
            return
        }

        val mappingRequest = GetMappingsRequest().indices(*concreteIndices)
        client.execute(
            GetMappingsAction.INSTANCE, mappingRequest,
            object : ActionListener<GetMappingsResponse> {
                override fun onResponse(response: GetMappingsResponse) {
                    val issues = validateMappings(concreteIndices.toList(), response, transform)
                    if (issues.isNotEmpty()) {
                        val errorMessage = issues.joinToString(" ")
                        listener.onFailure(ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST))
                        return
                    }
                    val searchRequest = TransformSearchService.getSearchServiceRequest(transform = transform, pageSize = 10)
                    executeSearch(searchRequest, transform, listener)
                }

                override fun onFailure(e: Exception) {
                    listener.onFailure(e)
                }
            }
        )
    }

    fun validateMappings(indices: List<String>, response: GetMappingsResponse, transform: Transform): List<String> {
        val issues = mutableListOf<String>()
        indices.forEach { index ->
            issues.addAll(TransformValidator.validateMappingsResponse(index, response, transform))
        }

        return issues
    }

    fun executeSearch(searchRequest: SearchRequest, transform: Transform, listener: ActionListener<PreviewTransformResponse>) {
        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    try {
                        val transformSearchResult = TransformSearchService.convertResponse(
                            transform = transform, searchResponse = response, waterMarkDocuments = false
                        )
                        val formattedResult = transformSearchResult.docsToIndex.map {
                            it.sourceAsMap()
                        }
                        listener.onResponse(PreviewTransformResponse(formattedResult, RestStatus.OK))
                    } catch (e: Exception) {
                        listener.onFailure(
                            ElasticsearchStatusException(
                                "Failed to parse the transformed results", RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.unwrapCause(e)
                            )
                        )
                    }
                }

                override fun onFailure(e: Exception) = listener.onFailure(e)
            }
        )
    }
}
