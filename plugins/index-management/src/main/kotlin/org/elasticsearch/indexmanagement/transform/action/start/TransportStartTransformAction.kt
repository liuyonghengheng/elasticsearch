/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.start

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.elasticsearch.indexmanagement.opensearchapi.parseWithType
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.indexmanagement.transform.model.TransformMetadata
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import java.time.Instant

@Suppress("ReturnCount")
class TransportStartTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<StartTransformRequest, AcknowledgedResponse>(
    StartTransformAction.NAME, transportService, actionFilters, ::StartTransformRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: StartTransformRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id())
        val user = buildUser(client.threadPool().threadContext)
        client.threadPool().threadContext.stashContext().use {
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(ElasticsearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                            return
                        }

                        val transform: Transform?
                        try {
                            transform = parseFromGetResponse(response, xContentRegistry, Transform.Companion::parse)
                        } catch (e: IllegalArgumentException) {
                            actionListener.onFailure(ElasticsearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                            return
                        }

                        if (!userHasPermissionForResource(user, transform.user, filterByEnabled, "transform", transform.id, actionListener)) {
                            return
                        }
                        if (transform.enabled) {
                            log.debug("Transform job is already enabled, checking if metadata needs to be updated")
                            return if (transform.metadataId == null) {
                                actionListener.onResponse(AcknowledgedResponse(true))
                            } else {
                                retrieveAndUpdateTransformMetadata(transform, actionListener)
                            }
                        }

                        updateTransformJob(transform, request, actionListener)
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    }
                }
            )
        }
    }

    private fun updateTransformJob(
        transform: Transform,
        request: StartTransformRequest,
        actionListener: ActionListener<AcknowledgedResponse>
    ) {
        val now = Instant.now().toEpochMilli()
        request.index(INDEX_MANAGEMENT_INDEX).doc(
            mapOf(
                Transform.TRANSFORM_TYPE to mapOf(
                    Transform.ENABLED_FIELD to true,
                    Transform.ENABLED_AT_FIELD to now, Transform.UPDATED_AT_FIELD to now
                )
            )
        )
        client.update(
            request,
            object : ActionListener<UpdateResponse> {
                override fun onResponse(response: UpdateResponse) {
                    if (response.result == DocWriteResponse.Result.UPDATED) {
                        // If there is a metadata ID on transform then we need to set it back to STARTED or RETRY
                        if (transform.metadataId != null) {
                            retrieveAndUpdateTransformMetadata(transform, actionListener)
                        } else {
                            actionListener.onResponse(AcknowledgedResponse(true))
                        }
                    } else {
                        actionListener.onResponse(AcknowledgedResponse(false))
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            }
        )
    }

    private fun retrieveAndUpdateTransformMetadata(transform: Transform, actionListener: ActionListener<AcknowledgedResponse>) {
        val req = GetRequest(INDEX_MANAGEMENT_INDEX, transform.metadataId).routing(transform.id)
        client.get(
            req,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists || response.isSourceEmpty) {
                        actionListener.onFailure(ElasticsearchStatusException("Metadata doc missing for transform [${req.id()}]", RestStatus.NOT_FOUND))
                    } else {
                        val metadata = response.sourceAsBytesRef?.let {
                            val xcp = XContentHelper.createParser(
                                NamedXContentRegistry.EMPTY,
                                LoggingDeprecationHandler.INSTANCE, it, XContentType.JSON
                            )
                            xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, TransformMetadata.Companion::parse)
                        }
                        if (metadata == null) {
                            actionListener.onFailure(
                                ElasticsearchStatusException(
                                    "Metadata doc missing for transform [${req.id()}]", RestStatus.NOT_FOUND
                                )
                            )
                        } else {
                            updateTransformMetadata(transform, metadata, actionListener)
                        }
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            }
        )
    }

    private fun updateTransformMetadata(transform: Transform, metadata: TransformMetadata, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        val updatedStatus = when (metadata.status) {
            TransformMetadata.Status.FINISHED, TransformMetadata.Status.STOPPED -> TransformMetadata.Status.STARTED
            TransformMetadata.Status.STARTED, TransformMetadata.Status.INIT ->
                return actionListener.onResponse(AcknowledgedResponse(true))
            TransformMetadata.Status.FAILED -> TransformMetadata.Status.STARTED
        }
        val updateRequest = UpdateRequest(INDEX_MANAGEMENT_INDEX, transform.metadataId)
            .doc(
                mapOf(
                    TransformMetadata.TRANSFORM_METADATA_TYPE to mapOf(
                        TransformMetadata.STATUS_FIELD to updatedStatus.type,
                        TransformMetadata.FAILURE_REASON to null, TransformMetadata.LAST_UPDATED_AT_FIELD to now
                    )
                )
            )
            .routing(transform.id)
        client.update(
            updateRequest,
            object : ActionListener<UpdateResponse> {
                override fun onResponse(response: UpdateResponse) {
                    actionListener.onResponse(AcknowledgedResponse(response.result == DocWriteResponse.Result.UPDATED))
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            }
        )
    }
}
