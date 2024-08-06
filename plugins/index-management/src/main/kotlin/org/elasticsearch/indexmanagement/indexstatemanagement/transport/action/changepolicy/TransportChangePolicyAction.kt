/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.ElasticsearchSecurityException
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.get.MultiGetRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.index.Index
import org.elasticsearch.indexmanagement.IndexManagementPlugin
import org.elasticsearch.indexmanagement.indexstatemanagement.DefaultIndexMetadataService
import org.elasticsearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.elasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.elasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.elasticsearch.indexmanagement.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import org.elasticsearch.indexmanagement.indexstatemanagement.opensearchapi.buildMgetMetadataRequest
import org.elasticsearch.indexmanagement.indexstatemanagement.opensearchapi.getManagedIndexMetadata
import org.elasticsearch.indexmanagement.indexstatemanagement.opensearchapi.mgetResponseToMap
import org.elasticsearch.indexmanagement.indexstatemanagement.resthandler.RestChangePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexRequest
import org.elasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.elasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.elasticsearch.indexmanagement.indexstatemanagement.util.isSafeToChange
import org.elasticsearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.elasticsearch.indexmanagement.indexstatemanagement.util.updateManagedIndexRequest
import org.elasticsearch.indexmanagement.opensearchapi.contentParser
import org.elasticsearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.elasticsearch.indexmanagement.opensearchapi.parseWithType
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.ISMIndexMetadata
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.indexmanagement.util.IndexManagementException
import org.elasticsearch.indexmanagement.util.IndexUtils
import org.elasticsearch.indexmanagement.util.NO_ID
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.validateUserConfiguration
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import java.lang.IllegalArgumentException

private val log = LogManager.getLogger(TransportChangePolicyAction::class.java)

@Suppress("SpreadOperator", "TooManyFunctions", "LongParameterList")
class TransportChangePolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val indexMetadataProvider: IndexMetadataProvider
) : HandledTransportAction<ChangePolicyRequest, ISMStatusResponse>(
    ChangePolicyAction.NAME, transportService, actionFilters, ::ChangePolicyRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: ChangePolicyRequest, listener: ActionListener<ISMStatusResponse>) {
        ChangePolicyHandler(client, listener, request).start()
    }

    inner class ChangePolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ISMStatusResponse>,
        private val request: ChangePolicyRequest,
        private val user: User? = buildUser(client.threadPool().threadContext)
    ) {

        private val failedIndices = mutableListOf<FailedIndex>()
        private val managedIndicesToUpdate = mutableListOf<Pair<String, String>>()
        private val indexUuidToCurrentState = mutableMapOf<String, String>()
        private val indicesToUpdate = mutableMapOf<String, String>() // uuid -> name
        private val indexUuidToIndexMetadata = mutableMapOf<String, IndexMetadata>() // uuid -> indexmetadata
        private val changePolicy = request.changePolicy
        private lateinit var policy: Policy
        private var updated: Int = 0

        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            if (user == null) {
                getPolicy()
            } else {
                validateAndGetPolicy()
            }
        }

        private fun validateAndGetPolicy() {
            val request = ManagedIndexRequest().indices(*request.indices.toTypedArray())
            client.execute(
                ManagedIndexAction.INSTANCE,
                request,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) {
                        getPolicy()
                    }

                    override fun onFailure(e: java.lang.Exception) {
                        actionListener.onFailure(
                            IndexManagementException.wrap(
                                when (e is ElasticsearchSecurityException) {
                                    true -> ElasticsearchStatusException(
                                        "User doesn't have required index permissions on one or more requested indices: ${e.localizedMessage}",
                                        RestStatus.FORBIDDEN
                                    )
                                    false -> e
                                }
                            )
                        )
                    }
                }
            )
        }

        private fun getPolicy() {
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, changePolicy.policyID)

            client.threadPool().threadContext.stashContext().use {
                if (!validateUserConfiguration(user, filterByEnabled, actionListener)) {
                    return
                }
                client.get(getRequest, ActionListener.wrap(::onGetPolicyResponse, ::onFailure))
            }
        }

        @Suppress("ReturnCount")
        private fun onGetPolicyResponse(response: GetResponse) {
            if (!response.isExists || response.isSourceEmpty) {
                actionListener.onFailure(ElasticsearchStatusException("Could not find policy=${request.changePolicy.policyID}", RestStatus.NOT_FOUND))
                return
            }
            try {
                policy = parseFromGetResponse(response, xContentRegistry, Policy.Companion::parse)
            } catch (e: IllegalArgumentException) {
                actionListener.onFailure(ElasticsearchStatusException("Could not find policy=${request.changePolicy.policyID}", RestStatus.NOT_FOUND))
                return
            }
            if (!userHasPermissionForResource(user, policy.user, filterByEnabled, "policy", request.changePolicy.policyID, actionListener)) {
                return
            }

            IndexUtils.checkAndUpdateConfigIndexMapping(
                clusterService.state(),
                client.admin().indices(),
                ActionListener.wrap(::onUpdateMapping, ::onFailure)
            )
        }

        private fun onUpdateMapping(acknowledgedResponse: AcknowledgedResponse) {
            if (!acknowledgedResponse.isAcknowledged) {
                actionListener.onFailure(
                    ElasticsearchStatusException(
                        "Could not update ${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX} with new mapping.",
                        RestStatus.FAILED_DEPENDENCY
                    )
                )
                return
            }

            getIndicesToUpdate()
        }

        private fun getIndicesToUpdate() {
            CoroutineScope(Dispatchers.IO).launch {
                val indexNameToMetadata: MutableMap<String, ISMIndexMetadata> = HashMap()
                try {
                    indexNameToMetadata.putAll(indexMetadataProvider.getISMIndexMetadataByType(request.indexType, request.indices))
                } catch (e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    return@launch
                }
                indexNameToMetadata.forEach { (indexName, indexMetadata) ->
                    indicesToUpdate.putIfAbsent(indexMetadata.indexUuid, indexName)
                }
                if (request.indexType == DEFAULT_INDEX_TYPE) {
                    getClusterState()
                } else {
                    getManagedIndexMetadata()
                }
            }
        }

        @Suppress("SpreadOperator")
        private fun getClusterState() {
            val strictExpandOptions = IndicesOptions.strictExpand()
            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(*request.indices.toTypedArray())
                .metadata(true)
                .local(false)
                .indicesOptions(strictExpandOptions)
            client.admin()
                .cluster()
                .state(
                    clusterStateRequest,
                    object : ActionListener<ClusterStateResponse> {
                        override fun onResponse(response: ClusterStateResponse) {
                            val clusterState = response.state
                            val defaultIndexMetadataService = indexMetadataProvider.services[DEFAULT_INDEX_TYPE] as DefaultIndexMetadataService
                            clusterState.metadata.indices.forEach {
                                val indexUUID = defaultIndexMetadataService.getCustomIndexUUID(it.value)
                                indexUuidToIndexMetadata[indexUUID] = it.value
                            }
                            // ISMIndexMetadata from the default index metadata service uses lenient expand, we want to use strict expand, filter
                            // out the indices which are not also in the strict expand response
                            indicesToUpdate.filter { indexUuidToIndexMetadata.containsKey(it.key) }
                            getManagedIndexMetadata()
                        }

                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                        }
                    }
                )
        }

        private fun getManagedIndexMetadata() {
            client.multiGet(
                buildMgetMetadataRequest(indicesToUpdate.toList().map { it.first }),
                ActionListener.wrap(::onMgetMetadataResponse, ::onFailure)
            )
        }

        @Suppress("ComplexMethod")
        private fun onMgetMetadataResponse(mgetResponse: MultiGetResponse) {
            val metadataMap = mgetResponseToMap(mgetResponse)
            val includedStates = changePolicy.include.map { it.state }.toSet()

            indicesToUpdate.forEach { (indexUuid, indexName) ->
                // indexMetaData and clusterStateMetadata will be null for non-default index types
                val indexMetaData = indexUuidToIndexMetadata[indexUuid]
                val clusterStateMetadata = indexMetaData?.getManagedIndexMetadata()
                val mgetFailure = metadataMap[indexUuid]?.second
                val managedIndexMetadata: ManagedIndexMetaData? = metadataMap[managedIndexMetadataID(indexUuid)]?.first

                val currentState = managedIndexMetadata?.stateMetaData?.name
                if (currentState != null) {
                    indexUuidToCurrentState[indexUuid] = currentState
                }

                when {
                    mgetFailure != null ->
                        failedIndices.add(
                            FailedIndex(
                                indexName, indexUuid,
                                "Failed to get managed index metadata, $mgetFailure"
                            )
                        )
                    // if there exists a transitionTo on the ManagedIndexMetaData then we will
                    // fail as they might not of meant to add a ChangePolicy when its on the next state
                    managedIndexMetadata?.transitionTo != null ->
                        failedIndices.add(
                            FailedIndex(
                                indexName, indexUuid,
                                RestChangePolicyAction.INDEX_IN_TRANSITION
                            )
                        )
                    // else if there is no ManagedIndexMetaData yet then the managed index has not initialized and we can change the policy safely
                    managedIndexMetadata == null -> {
                        if (clusterStateMetadata != null) {
                            failedIndices.add(
                                FailedIndex(
                                    indexName, indexUuid,
                                    "Cannot change policy until metadata has finished migrating"
                                )
                            )
                        } else {
                            managedIndicesToUpdate.add(indexName to indexUuid)
                        }
                    }
                    // else if the includedStates is empty (i.e. not being used) then we will always try to update the managed index
                    includedStates.isEmpty() -> managedIndicesToUpdate.add(indexName to indexUuid)
                    // else only update the managed index if its currently in one of the included states
                    includedStates.contains(managedIndexMetadata.stateMetaData?.name) ->
                        managedIndicesToUpdate.add(indexName to indexUuid)
                    // else the managed index did not match any of the included state filters and we will not update it
                    else -> log.debug("Skipping $indexName as it does not match any of the include state filters")
                }
            }

            if (managedIndicesToUpdate.isEmpty()) {
                updated = 0
                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
                return
            } else {
                client.multiGet(
                    mgetManagedIndexConfigRequest(managedIndicesToUpdate.map { (_, indexUuid) -> indexUuid }.toTypedArray()),
                    ActionListener.wrap(::onMultiGetResponse, ::onFailure)
                )
            }
        }

        private fun onMultiGetResponse(response: MultiGetResponse) {
            val foundManagedIndices = mutableSetOf<String>()
            val sweptConfigs = response.responses.mapNotNull {
                // The id is the index uuid
                if (!it.response.isExists) { // meaning this index is not managed
                    val indexUuid = it.response.id
                    val indexName = managedIndicesToUpdate.find { (_, second) -> second == indexUuid }?.first
                    if (indexName != null) {
                        failedIndices.add(FailedIndex(indexName, indexUuid, RestChangePolicyAction.INDEX_NOT_MANAGED))
                    }
                }
                if (!it.isFailed && !it.response.isSourceEmpty) {
                    foundManagedIndices.add(it.response.id)
                    contentParser(it.response.sourceAsBytesRef).parseWithType(
                        NO_ID, it.response.seqNo,
                        it.response.primaryTerm, SweptManagedIndexConfig.Companion::parse
                    )
                } else {
                    null
                }
            }

            if (sweptConfigs.isEmpty()) {
                updated = 0
                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
                return
            } else {
                updateManagedIndexConfig(sweptConfigs)
            }
        }

        private fun updateManagedIndexConfig(sweptConfigs: List<SweptManagedIndexConfig>) {
            val mapOfItemIdToIndex = mutableMapOf<Int, Index>()
            val bulkUpdateManagedIndexRequest = BulkRequest()
            sweptConfigs.forEachIndexed { id, sweptConfig ->
                // compare the sweptConfig policy to the get policy here and update changePolicy
                val currentStateName = indexUuidToCurrentState[sweptConfig.uuid]
                val updatedChangePolicy = changePolicy
                    .copy(isSafe = sweptConfig.policy?.isSafeToChange(currentStateName, policy, changePolicy) == true, user = this.user)
                bulkUpdateManagedIndexRequest.add(updateManagedIndexRequest(sweptConfig.copy(changePolicy = updatedChangePolicy)))
                mapOfItemIdToIndex[id] = Index(sweptConfig.index, sweptConfig.uuid)
            }
            client.bulk(
                bulkUpdateManagedIndexRequest,
                object : ActionListener<BulkResponse> {
                    override fun onResponse(response: BulkResponse) {
                        onBulkResponse(response, mapOfItemIdToIndex)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
            )
        }

        private fun onBulkResponse(bulkResponse: BulkResponse, mapOfItemIdToIndex: Map<Int, Index>) {
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            failedResponses.forEach {
                val indexPair = mapOfItemIdToIndex[it.itemId]
                if (indexPair != null) {
                    failedIndices.add(FailedIndex(indexPair.name, indexPair.uuid, it.failureMessage))
                }
            }

            updated = (bulkResponse.items ?: arrayOf()).size - failedResponses.size
            actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
        }

        @Suppress("SpreadOperator")
        private fun mgetManagedIndexConfigRequest(managedIndexUuids: Array<String>): MultiGetRequest {
            val request = MultiGetRequest()
            val includes = arrayOf(
                "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_FIELD}",
                "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_UUID_FIELD}",
                "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.POLICY_ID_FIELD}",
                "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.POLICY_FIELD}",
                "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.CHANGE_POLICY_FIELD}"
            )
            val excludes = emptyArray<String>()
            val fetchSourceContext = FetchSourceContext(true, includes, excludes)
            managedIndexUuids.forEach {
                request.add(
                    MultiGetRequest.Item(
                        IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, it
                    ).fetchSourceContext(fetchSourceContext).routing(it)
                )
            }
            return request
        }

        private fun onFailure(t: Exception) {
            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
        }
    }
}
