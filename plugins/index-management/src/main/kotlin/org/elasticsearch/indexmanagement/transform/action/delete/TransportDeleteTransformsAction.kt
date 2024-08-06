/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.delete

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.MultiGetRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

@Suppress("ReturnCount")
class TransportDeleteTransformsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry,
    actionFilters: ActionFilters
) : HandledTransportAction<DeleteTransformsRequest, BulkResponse>(
    DeleteTransformsAction.NAME, transportService, actionFilters, ::DeleteTransformsRequest
) {

    private val log = LogManager.getLogger(javaClass)
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: DeleteTransformsRequest, actionListener: ActionListener<BulkResponse>) {
        // TODO: if metadata id exists delete the metadata doc else just delete transform
        DeleteTransformHandler(client, request, actionListener).start()
    }

    inner class DeleteTransformHandler(
        val client: Client,
        val request: DeleteTransformsRequest,
        val actionListener: ActionListener<BulkResponse>,
        val user: User? = buildUser(client.threadPool().threadContext)
    ) {

        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            // Use Multi-Get Request
            val getRequest = MultiGetRequest()
            val fetchSourceContext = FetchSourceContext(true)
            request.ids.forEach { id ->
                getRequest.add(MultiGetRequest.Item(INDEX_MANAGEMENT_INDEX, id).fetchSourceContext(fetchSourceContext))
            }

            client.threadPool().threadContext.stashContext().use {
                client.multiGet(
                    getRequest,
                    object : ActionListener<MultiGetResponse> {
                        override fun onResponse(response: MultiGetResponse) {
                            try {
                                // response is failed only if managed index is not present
                                if (response.responses.first().isFailed) {
                                    actionListener.onFailure(
                                        ElasticsearchStatusException(
                                            "Cluster missing system index $INDEX_MANAGEMENT_INDEX, cannot execute the request", RestStatus.BAD_REQUEST
                                        )
                                    )
                                    return
                                }

                                bulkDelete(response, request.ids, request.force, actionListener)
                            } catch (e: Exception) {
                                actionListener.onFailure(e)
                            }
                        }

                        override fun onFailure(e: Exception) = actionListener.onFailure(e)
                    }
                )
            }
        }

        @Suppress("LongMethod")
        private fun bulkDelete(response: MultiGetResponse, ids: List<String>, forceDelete: Boolean, actionListener: ActionListener<BulkResponse>) {
            val enabledIDs = mutableListOf<String>()
            val notTransform = mutableListOf<String>()
            val noPermission = mutableListOf<String>()

            response.responses.forEach {
                if (it.response.isExists) {
                    try {
                        val transform = parseFromGetResponse(it.response, xContentRegistry, Transform.Companion::parse)
                        val enabled = transform.enabled
                        if (enabled && !forceDelete) {
                            enabledIDs.add(it.id)
                        }
                        if (!userHasPermissionForResource(user, transform.user, filterByEnabled)) {
                            noPermission.add(it.id)
                        }
                    } catch (e: Exception) {
                        // if cannot parse considering not a transform
                        notTransform.add(it.id)
                    }
                }
            }

            if (noPermission.isNotEmpty()) {
                actionListener.onFailure(
                    ElasticsearchStatusException(
                        "Don't have permission to delete some/all transforms in [${request.ids}]", RestStatus.FORBIDDEN
                    )
                )
                return
            }

            if (notTransform.isNotEmpty()) {
                actionListener.onFailure(
                    ElasticsearchStatusException(
                        "Cannot find transforms $notTransform", RestStatus.BAD_REQUEST
                    )
                )
                return
            }

            if (enabledIDs.isNotEmpty()) {
                actionListener.onFailure(
                    ElasticsearchStatusException(
                        "$enabledIDs transform(s) are enabled, please disable them before deleting them or set force flag", RestStatus.CONFLICT
                    )
                )
                return
            }

            val bulkDeleteRequest = BulkRequest()
            bulkDeleteRequest.refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
            for (id in ids) {
                bulkDeleteRequest.add(DeleteRequest(INDEX_MANAGEMENT_INDEX, id))
            }

            client.bulk(
                bulkDeleteRequest,
                object : ActionListener<BulkResponse> {
                    override fun onResponse(response: BulkResponse) {
                        actionListener.onResponse(response)
                    }

                    override fun onFailure(e: Exception) = actionListener.onFailure(e)
                }
            )
        }
    }
}
