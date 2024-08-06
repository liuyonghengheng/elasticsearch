/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.index

import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.indexmanagement.IndexManagementIndices
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.opensearchapi.suspendUntil
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions.INDEX_SM_POLICY_ACTION_NAME
import org.elasticsearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.elasticsearch.indexmanagement.util.IndexUtils
import org.elasticsearch.indexmanagement.util.SecurityUtils
import org.elasticsearch.transport.TransportService

class TransportIndexSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    private val indexManagementIndices: IndexManagementIndices,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<IndexSMPolicyRequest, IndexSMPolicyResponse>(
    INDEX_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::IndexSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: IndexSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): IndexSMPolicyResponse {
        // If filterBy is enabled and security is disabled or if filter by is enabled and backend role are empty an exception will be thrown
        SecurityUtils.validateUserConfiguration(user, filterByEnabled)

        if (indexManagementIndices.checkAndUpdateIMConfigIndex(log)) {
            log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
        }
        return indexSMPolicy(request, user)
    }

    private suspend fun indexSMPolicy(request: IndexSMPolicyRequest, user: User?): IndexSMPolicyResponse {
        val policy = request.policy.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion, user = user)
        val indexReq = request.index(INDEX_MANAGEMENT_INDEX)
            .source(policy.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            .id(policy.id)
            .routing(policy.id) // by default routed by id
        val indexRes: IndexResponse = client.suspendUntil { index(indexReq, it) }

        return IndexSMPolicyResponse(indexRes.id, indexRes.version, indexRes.seqNo, indexRes.primaryTerm, policy, indexRes.status())
    }
}
