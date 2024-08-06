/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.elasticsearch.replication.action.setup

import org.elasticsearch.replication.metadata.store.ReplicationContext
import org.elasticsearch.replication.util.SecurityContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchSecurityException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.StepListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.replication.util.stackTraceToString
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.ActionNotFoundTransportException
import org.elasticsearch.transport.RemoteTransportException
import org.elasticsearch.transport.TransportService

class TransportSetupChecksAction @Inject constructor(transportService: TransportService,
                                                    val threadPool: ThreadPool,
                                                    actionFilters: ActionFilters,
                                                    private val client : Client,
                                                    private val clusterService: ClusterService) :
        HandledTransportAction<SetupChecksRequest, AcknowledgedResponse>(SetupChecksAction.NAME,
                transportService, actionFilters, ::SetupChecksRequest) {

    companion object {
        private val log = LogManager.getLogger(TransportSetupChecksAction::class.java)
        fun unwrapSecurityExceptionIfPresent(e: Exception): Exception {
            val ex = ExceptionsHelper.unwrapCausesAndSuppressed<Exception>(e) { cause ->
                cause is ElasticsearchSecurityException
            }
            if(!ex.isPresent) {
                return e
            }
            val securityException = ex.get()
            return ElasticsearchSecurityException(securityException.message, RestStatus.FORBIDDEN, securityException.cause)
        }
    }

    override fun doExecute(task: Task, request: SetupChecksRequest, listener: ActionListener<AcknowledgedResponse>) {
        var leaderClusterClient: Client?
        val followerClusterName = clusterService.clusterName.value()
        try {
            leaderClusterClient = client.getRemoteClusterClient(request.connectionName)
        } catch (e: Exception) {
            // Logging it as info as this check is to see if leader cluster is added or not
            log.info("Failed to connect to remote cluster $request.connectionName with error $e")
            listener.onFailure(e)
            return
        }

        // If user obj is present, security plugin is enabled. Roles are mandatory
        if(request.followerContext.user != null && request.followerContext.user!!.roles.isEmpty()) {
            log.info("User roles are empty for follower_resource:${request.followerContext.resource}")
            listener.onFailure(ElasticsearchSecurityException("Follower roles are mandatory for replication", RestStatus.FORBIDDEN))
            return
        }

        if(request.leaderContext.user != null && request.leaderContext.user!!.roles.isEmpty()) {
            log.info("User roles are empty for leader_resource:${request.leaderContext.resource}")
            listener.onFailure(ElasticsearchSecurityException("Leader roles are mandatory for replication", RestStatus.FORBIDDEN))
            return
        }

        val userPermissionsValidationAtLocal = StepListener<AcknowledgedResponse>()
        val userPermissionsValidationAtRemote = StepListener<AcknowledgedResponse>()
        val rolePermissionsValidationAtLocal = StepListener<AcknowledgedResponse>()
        val rolePermissionsValidationAtRemote = StepListener<AcknowledgedResponse>()

        rolePermissionsValidationAtRemote.whenComplete(
                { r ->
                    log.info("Permissions validation successful for role [connection:${request.connectionName}, " +
                            "resource:${request.leaderContext.resource}]")
                    listener.onResponse(r)
                },
                { e ->
                    var exceptionToThrow = e
                    if ((e is RemoteTransportException) && (e.cause is ActionNotFoundTransportException)) {
                        exceptionToThrow = UnsupportedOperationException("Replication is not enabled on the remote domain")
                    }
                    log.error("Permissions validation failed for role [connection:${request.connectionName}, " +
                            "resource:${request.leaderContext.resource}] with ${exceptionToThrow.stackTraceToString()}")
                    listener.onFailure(unwrapSecurityExceptionIfPresent(exceptionToThrow))
                }
        )

        rolePermissionsValidationAtLocal.whenComplete(
                {
                    log.info("Permissions validation successful for User [connection:${request.connectionName}, " +
                            "resource:${request.leaderContext.resource}]")
                    triggerPermissionsValidation(leaderClusterClient!!, request.connectionName, request.leaderContext, true, rolePermissionsValidationAtRemote)
                },
                { e ->
                    log.error("Permissions validation failed for role [local:$followerClusterName, " +
                            "resource:${request.followerContext.resource}] with ${e.stackTraceToString()}")
                    listener.onFailure(unwrapSecurityExceptionIfPresent(e))
                }
        )

        userPermissionsValidationAtRemote.whenComplete(
                {
                    log.info("Permissions validation successful for User [connection:${request.connectionName}, " +
                            "resource:${request.leaderContext.resource}]")
                    triggerPermissionsValidation(client, followerClusterName, request.followerContext, true, rolePermissionsValidationAtLocal)
                },
                { e ->
                    var exceptionToThrow = e
                    if ((e is RemoteTransportException) && (e.cause is ActionNotFoundTransportException)) {
                        exceptionToThrow = UnsupportedOperationException("Replication is not enabled on the remote domain")
                    }
                    log.error("Permissions validation failed for User [connection:${request.connectionName}, " +
                            "resource:${request.leaderContext.resource}] with ${exceptionToThrow.stackTraceToString()}")
                    listener.onFailure(unwrapSecurityExceptionIfPresent(exceptionToThrow))
                }
        )

        userPermissionsValidationAtLocal.whenComplete(
                {
                    log.info("Permissions validation successful for User [local:$followerClusterName, " +
                            "resource:${request.followerContext.resource}]")
                    triggerPermissionsValidation(leaderClusterClient!!, request.connectionName, request.leaderContext, false, userPermissionsValidationAtRemote)
                },
                { e ->
                    log.error("Permissions validation failed for User [local:$followerClusterName, " +
                            "resource:${request.followerContext.resource}] with ${e.stackTraceToString()}")
                    listener.onFailure(unwrapSecurityExceptionIfPresent(e))
                }
        )

        triggerPermissionsValidation(client, followerClusterName, request.followerContext, false, userPermissionsValidationAtLocal)

    }

    private fun triggerPermissionsValidation(client: Client,
                                             cluster: String,
                                             replContext: ReplicationContext,
                                             shouldValidateRole: Boolean,
                                             permissionListener: ActionListener<AcknowledgedResponse>) {

        var storedContext: ThreadContext.StoredContext? = null
        try {
            // Remove the assume roles transient from the previous call
            storedContext = client.threadPool().threadContext.newStoredContext(false,
                    listOf(SecurityContext.SECURITY_INJECTED_ROLES_VALIDATION))
            val validateRole = replContext.user?.roles?.get(0)
            val inThreadContextRole = client.threadPool().threadContext.getTransient<String?>(SecurityContext.SECURITY_INJECTED_ROLES_VALIDATION)
            log.debug("Validation role in threadcontect is $inThreadContextRole for $cluster")
            if(shouldValidateRole) {
                client.threadPool().threadContext.putTransient(SecurityContext.SECURITY_INJECTED_ROLES_VALIDATION, validateRole)
            }
            val validateReq = ValidatePermissionsRequest(cluster, replContext.resource, validateRole)
            client.execute(ValidatePermissionsAction.INSTANCE, validateReq, permissionListener)
        } finally {
            storedContext?.close()
        }
    }
}
