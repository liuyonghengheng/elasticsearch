/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.get

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportGetTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetTransformRequest, GetTransformResponse> (
    GetTransformAction.NAME, transportService, actionFilters, ::GetTransformRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    @Suppress("ReturnCount")
    override fun doExecute(task: Task, request: GetTransformRequest, listener: ActionListener<GetTransformResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val user = buildUser(client.threadPool().threadContext)
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id).preference(request.preference)
        client.threadPool().threadContext.stashContext().use {
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            listener.onFailure(ElasticsearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                            return
                        }

                        try {
                            val transform: Transform?
                            try {
                                transform = parseFromGetResponse(response, xContentRegistry, Transform.Companion::parse)
                            } catch (e: IllegalArgumentException) {
                                listener.onFailure(ElasticsearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                                return
                            }
                            if (!userHasPermissionForResource(user, transform.user, filterByEnabled, "transform", request.id, listener)) {
                                return
                            }

                            // if HEAD request don't return the transform
                            val transformResponse = if (request.srcContext != null && !request.srcContext.fetchSource()) {
                                GetTransformResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, null)
                            } else {
                                GetTransformResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, transform)
                            }
                            listener.onResponse(transformResponse)
                        } catch (e: Exception) {
                            listener.onFailure(
                                ElasticsearchStatusException(
                                    "Failed to parse transform",
                                    RestStatus.INTERNAL_SERVER_ERROR,
                                    ExceptionsHelper.unwrapCause(e)
                                )
                            )
                        }
                    }

                    override fun onFailure(e: Exception) {
                        listener.onFailure(e)
                    }
                }
            )
        }
    }
}
