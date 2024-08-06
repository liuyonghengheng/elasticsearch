/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.indexmanagement.IndexManagementPlugin
import org.elasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.elasticsearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import java.lang.IllegalArgumentException

@Suppress("ReturnCount")
class TransportGetPolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetPolicyRequest, GetPolicyResponse>(
    GetPolicyAction.NAME, transportService, actionFilters, ::GetPolicyRequest
) {

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: GetPolicyRequest, listener: ActionListener<GetPolicyResponse>) {
        GetPolicyHandler(client, listener, request).start()
    }

    inner class GetPolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<GetPolicyResponse>,
        private val request: GetPolicyRequest,
        private val user: User? = buildUser(client.threadPool().threadContext)
    ) {
        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
                .version(request.version)

            client.threadPool().threadContext.stashContext().use {
                client.get(
                    getRequest,
                    object : ActionListener<GetResponse> {
                        override fun onResponse(response: GetResponse) {
                            onGetResponse(response)
                        }

                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                        }
                    }
                )
            }
        }

        fun onGetResponse(response: GetResponse) {
            if (!response.isExists) {
                actionListener.onFailure(ElasticsearchStatusException("Policy not found", RestStatus.NOT_FOUND))
                return
            }

            val policy: Policy?
            try {
                policy = parseFromGetResponse(response, xContentRegistry, Policy.Companion::parse)
            } catch (e: IllegalArgumentException) {
                actionListener.onFailure(ElasticsearchStatusException("Policy not found", RestStatus.NOT_FOUND))
                return
            }
            if (!userHasPermissionForResource(user, policy.user, filterByEnabled, "policy", request.policyID, actionListener)) {
                return
            } else {
                // if HEAD request don't return the policy
                val policyResponse = if (!request.fetchSrcContext.fetchSource()) {
                    GetPolicyResponse(response.id, response.version, response.seqNo, response.primaryTerm, null)
                } else {
                    GetPolicyResponse(response.id, response.version, response.seqNo, response.primaryTerm, policy)
                }
                actionListener.onResponse(policyResponse)
            }
        }
    }
}
