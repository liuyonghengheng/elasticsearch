/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.index

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
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
import org.elasticsearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.transform.TransformValidator
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.indexmanagement.util.IndexUtils
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.validateUserConfiguration
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

@Suppress("SpreadOperator", "LongParameterList")
class TransportIndexTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val indexManagementIndices: IndexManagementIndices,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<IndexTransformRequest, IndexTransformResponse>(
    IndexTransformAction.NAME, transportService, actionFilters, ::IndexTransformRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: IndexTransformRequest, listener: ActionListener<IndexTransformResponse>) {
        IndexTransformHandler(client, listener, request).start()
    }

    inner class IndexTransformHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexTransformResponse>,
        private val request: IndexTransformRequest,
        private val user: User? = buildUser(client.threadPool().threadContext, request.transform.user)
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
                indexManagementIndices.checkAndUpdateIMConfigIndex(
                    ActionListener.wrap(::onConfigIndexAcknowledgedResponse, actionListener::onFailure)
                )
            }
        }

        private fun onConfigIndexAcknowledgedResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
                if (request.opType() == DocWriteRequest.OpType.CREATE) {
                    validateAndPutTransform()
                } else {
                    updateTransform()
                }
            } else {
                val message = "Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mappings."
                log.error(message)
                actionListener.onFailure(ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun updateTransform() {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.transform.id)
            client.get(getRequest, ActionListener.wrap(::onGetTransform, actionListener::onFailure))
        }

        @Suppress("ReturnCount")
        private fun onGetTransform(response: GetResponse) {
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
            val modified = modifiedImmutableProperties(transform, request.transform)
            if (modified.isNotEmpty()) {
                return actionListener.onFailure(ElasticsearchStatusException("Not allowed to modify $modified", RestStatus.BAD_REQUEST))
            }
            putTransform()
        }

        private fun modifiedImmutableProperties(transform: Transform, newTransform: Transform): List<String> {
            val modified = mutableListOf<String>()
            if (transform.sourceIndex != newTransform.sourceIndex) modified.add(Transform.SOURCE_INDEX_FIELD)
            if (transform.targetIndex != newTransform.targetIndex) modified.add(Transform.TARGET_INDEX_FIELD)
            if (transform.dataSelectionQuery != newTransform.dataSelectionQuery) modified.add(Transform.DATA_SELECTION_QUERY_FIELD)
            if (transform.groups != newTransform.groups) modified.add(Transform.GROUPS_FIELD)
            if (transform.aggregations != newTransform.aggregations) modified.add(Transform.AGGREGATIONS_FIELD)
            if (transform.roles != newTransform.roles) modified.add(Transform.ROLES_FIELD)
            if (transform.continuous != newTransform.continuous) modified.add(Transform.CONTINUOUS_FIELD)
            return modified.toList()
        }

        private fun putTransform() {
            val transform = request.transform.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion, user = this.user)
            request.index(INDEX_MANAGEMENT_INDEX)
                .id(request.transform.id)
                .source(transform.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .timeout(IndexRequest.DEFAULT_TIMEOUT)
            client.index(
                request,
                object : ActionListener<IndexResponse> {
                    override fun onResponse(response: IndexResponse) {
                        if (response.shardInfo.failed > 0) {
                            val failureReasons = response.shardInfo.failures.joinToString(",") { it.reason() }
                            actionListener.onFailure(ElasticsearchStatusException(failureReasons, response.status()))
                        } else {
                            val status = if (request.opType() == DocWriteRequest.OpType.CREATE) RestStatus.CREATED else RestStatus.OK
                            actionListener.onResponse(
                                IndexTransformResponse(
                                    response.id, response.version, response.seqNo, response.primaryTerm, status,
                                    transform.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm)
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

        private fun validateAndPutTransform() {
            val concreteIndices =
                indexNameExpressionResolver.concreteIndexNames(
                    clusterService.state(), IndicesOptions.lenientExpand(), true,
                    request.transform
                        .sourceIndex
                )
            if (concreteIndices.isEmpty()) {
                actionListener.onFailure(ElasticsearchStatusException("No specified source index exist in the cluster", RestStatus.NOT_FOUND))
                return
            }

            val mappingRequest = GetMappingsRequest().indices(*concreteIndices)
            client.execute(
                GetMappingsAction.INSTANCE, mappingRequest,
                object : ActionListener<GetMappingsResponse> {
                    override fun onResponse(response: GetMappingsResponse) {
                        val issues = validateMappings(concreteIndices.toList(), response, request.transform)
                        if (issues.isNotEmpty()) {
                            val errorMessage = issues.joinToString(" ")
                            actionListener.onFailure(ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST))
                            return
                        }

                        putTransform()
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(e)
                    }
                }
            )
        }

        private fun validateMappings(indices: List<String>, response: GetMappingsResponse, transform: Transform): List<String> {
            val issues = mutableListOf<String>()
            indices.forEach { index ->
                issues.addAll(TransformValidator.validateMappingsResponse(index, response, transform))
            }

            return issues
        }
    }
}
