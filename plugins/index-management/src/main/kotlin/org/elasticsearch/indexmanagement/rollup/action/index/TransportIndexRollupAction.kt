/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.action.index

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.indexmanagement.IndexManagementIndices
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.indexmanagement.rollup.util.RollupFieldValueExpressionResolver
import org.elasticsearch.indexmanagement.rollup.util.parseRollup
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.util.IndexUtils
import org.elasticsearch.indexmanagement.util.SecurityUtils
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.validateUserConfiguration
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

// TODO: Field and mappings validations of source and target index, i.e. reject a histogram agg on example_field if its not possible
@Suppress("LongParameterList")
class TransportIndexRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val indexManagementIndices: IndexManagementIndices,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<IndexRollupRequest, IndexRollupResponse>(
    IndexRollupAction.NAME, transportService, actionFilters, ::IndexRollupRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: IndexRollupRequest, listener: ActionListener<IndexRollupResponse>) {
        IndexRollupHandler(client, listener, request).start()
    }

    inner class IndexRollupHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexRollupResponse>,
        private val request: IndexRollupRequest,
        private val user: User? = buildUser(client.threadPool().threadContext, request.rollup.user)
    ) {

        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            client.threadPool().threadContext.stashContext().use {
                if (!validateUserConfiguration(user, filterByEnabled, actionListener)) {
                    return
                }
                indexManagementIndices.checkAndUpdateIMConfigIndex(ActionListener.wrap(::onCreateMappingsResponse, actionListener::onFailure))
            }
        }

        private fun onCreateMappingsResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
                if (request.opType() == DocWriteRequest.OpType.CREATE) {
                    if (!validateTargetIndexName()) {
                        return actionListener.onFailure(
                            ElasticsearchStatusException(
                                "target_index value is invalid: ${request.rollup.targetIndex}",
                                RestStatus.BAD_REQUEST
                            )
                        )
                    }
                    putRollup()
                } else {
                    getRollup()
                }
            } else {
                val message = "Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mapping."
                log.error(message)
                actionListener.onFailure(ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun getRollup() {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.rollup.id)
            client.get(getRequest, ActionListener.wrap(::onGetRollup, actionListener::onFailure))
        }

        @Suppress("ReturnCount")
        private fun onGetRollup(response: GetResponse) {
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
            if (!SecurityUtils.userHasPermissionForResource(user, rollup.user, filterByEnabled, "rollup", rollup.id, actionListener)) {
                return
            }
            val modified = modifiedImmutableProperties(rollup, request.rollup)
            if (modified.isNotEmpty()) {
                return actionListener.onFailure(ElasticsearchStatusException("Not allowed to modify $modified", RestStatus.BAD_REQUEST))
            }
            if (!validateTargetIndexName()) {
                return actionListener.onFailure(
                    ElasticsearchStatusException(
                        "target_index value is invalid: ${request.rollup.targetIndex}",
                        RestStatus.BAD_REQUEST
                    )
                )
            }
            putRollup()
        }

        private fun modifiedImmutableProperties(rollup: Rollup, newRollup: Rollup): List<String> {
            val modified = mutableListOf<String>()
            if (rollup.continuous != newRollup.continuous) modified.add(Rollup.CONTINUOUS_FIELD)
            if (rollup.dimensions != newRollup.dimensions) modified.add(Rollup.DIMENSIONS_FIELD)
            if (rollup.metrics != newRollup.metrics) modified.add(Rollup.METRICS_FIELD)
            if (rollup.sourceIndex != newRollup.sourceIndex) modified.add(Rollup.SOURCE_INDEX_FIELD)
            if (rollup.targetIndex != newRollup.targetIndex) modified.add(Rollup.TARGET_INDEX_FIELD)
            if (rollup.roles != newRollup.roles) modified.add(Rollup.ROLES_FIELD)
            return modified.toList()
        }

        private fun putRollup() {
            val rollup = request.rollup.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion, user = this.user)
            request.index(INDEX_MANAGEMENT_INDEX)
                .id(request.rollup.id)
                .source(rollup.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .timeout(IndexRequest.DEFAULT_TIMEOUT)
            client.index(
                request,
                object : ActionListener<IndexResponse> {
                    override fun onResponse(response: IndexResponse) {
                        if (response.shardInfo.failed > 0) {
                            val failureReasons = response.shardInfo.failures.joinToString(", ") { it.reason() }
                            actionListener.onFailure(ElasticsearchStatusException(failureReasons, response.status()))
                        } else {
                            val status = if (request.opType() == DocWriteRequest.OpType.CREATE) RestStatus.CREATED else RestStatus.OK
                            actionListener.onResponse(
                                IndexRollupResponse(
                                    response.id, response.version, response.seqNo, response.primaryTerm, status,
                                    rollup.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm)
                                )
                            )
                        }
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(e)
                    }
                }
            )
        }

        private fun validateTargetIndexName(): Boolean {
            val targetIndexResolvedName = RollupFieldValueExpressionResolver.resolve(request.rollup, request.rollup.targetIndex)
            return targetIndexResolvedName.contains("*") == false && targetIndexResolvedName.contains("?") == false
        }
    }
}
