/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.start

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.index.engine.VersionConflictEngineException
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.opensearchapi.suspendUntil
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.elasticsearch.indexmanagement.snapshotmanagement.getSMPolicy
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.elasticsearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.verifyUserHasPermissionForResource
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.TransportService
import java.time.Instant

class TransportStartSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<StartSMRequest, AcknowledgedResponse>(
    SMActions.START_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::StartSMRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: StartSMRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): AcknowledgedResponse {
        val smPolicy = client.getSMPolicy(request.id())

        // Check if the requested user has permission on the resource, throwing an exception if the user does not
        verifyUserHasPermissionForResource(user, smPolicy.user, filterByEnabled, "snapshot management policy", smPolicy.policyName)

        if (smPolicy.jobEnabled) {
            log.debug("Snapshot management policy is already enabled")
            return AcknowledgedResponse(true)
        }
        return AcknowledgedResponse(enableSMPolicy(request))
    }

    private suspend fun enableSMPolicy(updateRequest: StartSMRequest): Boolean {
        val now = Instant.now().toEpochMilli()
        updateRequest.index(INDEX_MANAGEMENT_INDEX).doc(
            mapOf(
                SMPolicy.SM_TYPE to mapOf(
                    SMPolicy.ENABLED_FIELD to true,
                    SMPolicy.ENABLED_TIME_FIELD to now,
                    SMPolicy.LAST_UPDATED_TIME_FIELD to now
                )
            )
        )
        val updateResponse: UpdateResponse = try {
            client.suspendUntil { update(updateRequest, it) }
        } catch (e: VersionConflictEngineException) {
            log.error("VersionConflictEngineException while trying to enable snapshot management policy id [${updateRequest.id()}]: $e")
            throw ElasticsearchStatusException(conflictExceptionMessage, RestStatus.INTERNAL_SERVER_ERROR)
        } catch (e: Exception) {
            log.error("Failed trying to enable snapshot management policy id [${updateRequest.id()}]: $e")
            throw ElasticsearchStatusException("Failed while trying to enable SM Policy", RestStatus.INTERNAL_SERVER_ERROR)
        }
        return updateResponse.result == DocWriteResponse.Result.UPDATED
    }

    companion object {
        private const val conflictExceptionMessage = "Failed while trying to enable SM Policy due to a concurrent update, please try again"
    }
}
