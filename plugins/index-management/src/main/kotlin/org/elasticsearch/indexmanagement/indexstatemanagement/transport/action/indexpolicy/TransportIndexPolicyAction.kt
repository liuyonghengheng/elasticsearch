/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.metadata.AutoExpandReplicas
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.ValidationException
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.indexmanagement.IndexManagementIndices
import org.elasticsearch.indexmanagement.IndexManagementPlugin
import org.elasticsearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import org.elasticsearch.indexmanagement.indexstatemanagement.action.ReplicaCountAction
import org.elasticsearch.indexmanagement.indexstatemanagement.findConflictingPolicyTemplates
import org.elasticsearch.indexmanagement.indexstatemanagement.findSelfConflictingTemplates
import org.elasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.elasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.elasticsearch.indexmanagement.indexstatemanagement.opensearchapi.filterNotNullValues
import org.elasticsearch.indexmanagement.indexstatemanagement.util.ISM_TEMPLATE_FIELD
import org.elasticsearch.indexmanagement.indexstatemanagement.validateFormat
import org.elasticsearch.indexmanagement.opensearchapi.parseFromSearchResponse
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.util.IndexManagementException
import org.elasticsearch.indexmanagement.util.IndexUtils
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.validateUserConfiguration
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportIndexPolicyAction::class.java)

@Suppress("LongParameterList")
class TransportIndexPolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val ismIndices: IndexManagementIndices,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    //var awarenessReplicaBalance: AwarenessReplicaBalance,
) : HandledTransportAction<IndexPolicyRequest, IndexPolicyResponse>(
    IndexPolicyAction.NAME, transportService, actionFilters, ::IndexPolicyRequest
) {

    @Volatile
    private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: IndexPolicyRequest, listener: ActionListener<IndexPolicyResponse>) {
        IndexPolicyHandler(client, listener, request).start()
    }

    inner class IndexPolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<IndexPolicyResponse>,
        private val request: IndexPolicyRequest,
        private val user: User? = buildUser(client.threadPool().threadContext)
    ) {
        fun start() {
            validate()
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            client.threadPool().threadContext.stashContext().use {
                if (!validateUserConfiguration(user, filterByEnabled, actionListener)) {
                    return
                }
                ismIndices.checkAndUpdateIMConfigIndex(object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) {
                        onCreateMappingsResponse(response)
                    }

                    override fun onFailure(t: Exception) {
                        if (t is ResourceAlreadyExistsException) {
                            actionListener.onFailure(ElasticsearchStatusException(t.localizedMessage, RestStatus.CONFLICT))
                        } else {
                            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                        }
                    }
                })
            }
        }

        @Suppress("ComplexMethod", "LongMethod", "NestedBlockDepth")
        private fun validate() {
            // ISM doesn't support update auto expand replica setting, so initiate this as false
            val autoExpandReplica = AutoExpandReplicas.SETTING[Settings.EMPTY]

//            request.policy.states.forEach { state ->
//                state.actions.forEach { action ->
//                    if (action is ReplicaCountAction) {
//                        val error = awarenessReplicaBalance.validate(action.numOfReplicas, autoExpandReplica)
//                        if (error.isPresent) {
//                            val ex = ValidationException()
//                            ex.addValidationError(error.get())
//                            actionListener.onFailure(ex)
//                        }
//                    }
//                }
//            }
        }

        private fun onCreateMappingsResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated ${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX} with newest mappings.")

                // if there is template field, we will check
                val reqTemplates = request.policy.ismTemplate
                if (reqTemplates != null) {
                    validateISMTemplates(reqTemplates)
                } else putPolicy()
            } else {
                log.error("Unable to create or update ${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX} with newest mapping.")

                actionListener.onFailure(
                    ElasticsearchStatusException(
                        "Unable to create or update ${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX} with newest mapping.",
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                )
            }
        }

        private fun validateISMTemplates(ismTemplateList: List<ISMTemplate>) {
            val possibleEx = validateFormat(ismTemplateList.map { it.indexPatterns }.flatten())
            if (possibleEx != null) {
                actionListener.onFailure(possibleEx)
                return
            }

            // check self overlapping
            val selfOverlap = ismTemplateList.findSelfConflictingTemplates()
            if (selfOverlap != null) {
                val errorMessage =
                    "New policy ${request.policyID} has an ISM template with index pattern ${selfOverlap.first} " +
                        "matching this policy's other ISM templates with index patterns ${selfOverlap.second}," +
                        " please use different priority"
                actionListener.onFailure(IndexManagementException.wrap(IllegalArgumentException(errorMessage)))
                return
            }

            val searchRequest = SearchRequest()
                .source(
                    SearchSourceBuilder().query(
                        QueryBuilders.existsQuery(ISM_TEMPLATE_FIELD)
                    ).size(MAX_HITS)
                )
                .indices(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)

            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        val policies = parseFromSearchResponse(response, xContentRegistry, Policy.Companion::parse)
                        val policyToTemplateMap: Map<String, List<ISMTemplate>> =
                            policies.map { it.id to it.ismTemplate }.toMap().filterNotNullValues()
                        ismTemplateList.forEach {
                            val conflictingPolicyTemplates = policyToTemplateMap
                                .findConflictingPolicyTemplates(request.policyID, it.indexPatterns, it.priority)
                            if (conflictingPolicyTemplates.isNotEmpty()) {
                                val errorMessage =
                                    "New policy ${request.policyID} has an ISM template with index pattern ${it.indexPatterns} " +
                                        "matching existing policy templates," +
                                        " please use a different priority than ${it.priority}"
                                actionListener.onFailure(
                                    IndexManagementException.wrap(
                                        IllegalArgumentException(
                                            errorMessage
                                        )
                                    )
                                )
                                return
                            }
                        }

                        putPolicy()
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
            )
        }

        private fun putPolicy() {
            val policy = request.policy.copy(
                schemaVersion = IndexUtils.indexManagementConfigSchemaVersion, user = this.user
            )

            val indexRequest = IndexRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
                .setRefreshPolicy(request.refreshPolicy)
                .source(policy.toXContent(XContentFactory.jsonBuilder()))
                .id(request.policyID)
                .timeout(IndexRequest.DEFAULT_TIMEOUT)

            if (request.seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || request.primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            } else {
                indexRequest.setIfSeqNo(request.seqNo)
                    .setIfPrimaryTerm(request.primaryTerm)
            }

            client.index(
                indexRequest,
                object : ActionListener<IndexResponse> {
                    override fun onResponse(response: IndexResponse) {
                        val failureReasons = checkShardsFailure(response)
                        if (failureReasons != null) {
                            actionListener.onFailure(
                                ElasticsearchStatusException(
                                    failureReasons.toString(),
                                    response.status()
                                )
                            )
                            return
                        }
                        actionListener.onResponse(
                            IndexPolicyResponse(
                                response.id,
                                response.version,
                                response.primaryTerm,
                                response.seqNo,
                                request.policy,
                                response.status()
                            )
                        )
                    }

                    override fun onFailure(t: Exception) {
                        // TODO should wrap document already exists exception
                        //  provide a direct message asking user to use seqNo and primaryTerm
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
            )
        }

        private fun checkShardsFailure(response: IndexResponse): String? {
            val failureReasons = StringBuilder()
            if (response.shardInfo.failed > 0) {
                response.shardInfo.failures.forEach { entry ->
                    failureReasons.append(entry.reason())
                }
                return failureReasons.toString()
            }
            return null
        }
    }
}
