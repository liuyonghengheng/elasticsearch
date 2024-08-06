/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.action.start

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
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.opensearchapi.parseWithType
import org.elasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.indexmanagement.rollup.model.RollupMetadata
import org.elasticsearch.indexmanagement.rollup.util.parseRollup
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import java.lang.IllegalArgumentException
import java.time.Instant

@Suppress("ReturnCount")
class TransportStartRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val clusterService: ClusterService,
    val settings: Settings,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<StartRollupRequest, AcknowledgedResponse>(
    StartRollupAction.NAME, transportService, actionFilters, ::StartRollupRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: StartRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val getReq = GetRequest(INDEX_MANAGEMENT_INDEX, request.id())
        val user: User? = buildUser(client.threadPool().threadContext)
        client.threadPool().threadContext.stashContext().use {
            client.get(
                getReq,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(ElasticsearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                            return
                        }

                        val rollup: Rollup?
                        try {
                            rollup = parseRollup(response, xContentRegistry)
                        } catch (e: IllegalArgumentException) {
                            actionListener.onFailure(ElasticsearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                            return
                        }
                        if (!userHasPermissionForResource(user, rollup.user, filterByEnabled, "rollup", rollup.id, actionListener)) {
                            return
                        }
                        if (rollup.enabled) {
                            log.debug("Rollup job is already enabled, checking if metadata needs to be updated")
                            return if (rollup.metadataID == null) {
                                actionListener.onResponse(AcknowledgedResponse(true))
                            } else {
                                getRollupMetadata(rollup, actionListener)
                            }
                        }

                        updateRollupJob(rollup, request, actionListener)
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    }
                }
            )
        }
    }

    // TODO: Should create a transport action to update metadata
    private fun updateRollupJob(rollup: Rollup, request: StartRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        request.index(INDEX_MANAGEMENT_INDEX).doc(
            mapOf(
                Rollup.ROLLUP_TYPE to mapOf(
                    Rollup.ENABLED_FIELD to true,
                    Rollup.ENABLED_TIME_FIELD to now, Rollup.LAST_UPDATED_TIME_FIELD to now
                )
            )
        )
        client.update(
            request,
            object : ActionListener<UpdateResponse> {
                override fun onResponse(response: UpdateResponse) {
                    if (response.result == DocWriteResponse.Result.UPDATED) {
                        // If there is a metadata ID on rollup then we need to set it back to STARTED or RETRY
                        if (rollup.metadataID != null) {
                            getRollupMetadata(rollup, actionListener)
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

    private fun getRollupMetadata(rollup: Rollup, actionListener: ActionListener<AcknowledgedResponse>) {
        val req = GetRequest(INDEX_MANAGEMENT_INDEX, rollup.metadataID).routing(rollup.id)
        client.get(
            req,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists || response.isSourceEmpty) {
                        // If there is no metadata doc then the runner will instantiate a new one
                        // in FAILED status which the user will need to retry from
                        actionListener.onResponse(AcknowledgedResponse(true))
                    } else {
                        val metadata = response.sourceAsBytesRef?.let {
                            val xcp = XContentHelper.createParser(
                                NamedXContentRegistry.EMPTY,
                                LoggingDeprecationHandler.INSTANCE, it, XContentType.JSON
                            )
                            xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, RollupMetadata.Companion::parse)
                        }
                        if (metadata == null) {
                            // If there is no metadata doc then the runner will instantiate a new one
                            // in FAILED status which the user will need to retry from
                            actionListener.onResponse(AcknowledgedResponse(true))
                        } else {
                            updateRollupMetadata(rollup, metadata, actionListener)
                        }
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            }
        )
    }

    private fun updateRollupMetadata(rollup: Rollup, metadata: RollupMetadata, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        val updatedStatus = when (metadata.status) {
            RollupMetadata.Status.FINISHED, RollupMetadata.Status.STOPPED -> RollupMetadata.Status.STARTED
            RollupMetadata.Status.STARTED, RollupMetadata.Status.INIT, RollupMetadata.Status.RETRY ->
                return actionListener.onResponse(AcknowledgedResponse(true))
            RollupMetadata.Status.FAILED -> RollupMetadata.Status.RETRY
        }
        val updateRequest = UpdateRequest(INDEX_MANAGEMENT_INDEX, rollup.metadataID)
            .doc(
                mapOf(
                    RollupMetadata.ROLLUP_METADATA_TYPE to mapOf(
                        RollupMetadata.STATUS_FIELD to updatedStatus.type,
                        RollupMetadata.FAILURE_REASON to null, RollupMetadata.LAST_UPDATED_FIELD to now
                    )
                )
            )
            .routing(rollup.id)
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
