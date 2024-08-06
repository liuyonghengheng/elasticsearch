/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.get

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.indexmanagement.IndexManagementPlugin
import org.elasticsearch.indexmanagement.opensearchapi.suspendUntil
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICY_ACTION_NAME
import org.elasticsearch.indexmanagement.snapshotmanagement.parseSMPolicy
import org.elasticsearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.verifyUserHasPermissionForResource
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.TransportService

class TransportGetSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<GetSMPolicyRequest, GetSMPolicyResponse>(
    GET_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::GetSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: GetSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): GetSMPolicyResponse {
        val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
        val getResponse: GetResponse = try {
            client.suspendUntil { get(getRequest, it) }
        } catch (e: IndexNotFoundException) {
            throw ElasticsearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        }
        if (!getResponse.isExists) {
            throw ElasticsearchStatusException("Snapshot management policy not found", RestStatus.NOT_FOUND)
        }
        val smPolicy = try {
            parseSMPolicy(getResponse)
        } catch (e: IllegalArgumentException) {
            log.error("Error while parsing snapshot management policy ${request.policyID}", e)
            throw ElasticsearchStatusException("Snapshot management policy not found", RestStatus.INTERNAL_SERVER_ERROR)
        }

        // Check if the requested user has permission on the resource, throwing an exception if the user does not
        verifyUserHasPermissionForResource(user, smPolicy.user, filterByEnabled, "snapshot management policy", smPolicy.policyName)

        log.debug("Get SM policy: $smPolicy")
        return GetSMPolicyResponse(getResponse.id, getResponse.version, getResponse.seqNo, getResponse.primaryTerm, smPolicy)
    }
}
