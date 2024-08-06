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

package org.elasticsearch.replication.action.autofollow

import org.elasticsearch.replication.action.index.ReplicateIndexRequest
import org.elasticsearch.replication.action.setup.SetupChecksAction
import org.elasticsearch.replication.action.setup.SetupChecksRequest
import org.elasticsearch.replication.metadata.store.ReplicationContext
import org.elasticsearch.replication.util.SecurityContext
import org.elasticsearch.replication.util.completeWith
import org.elasticsearch.replication.util.coroutineContext
import org.elasticsearch.replication.util.overrideFgacRole
import org.elasticsearch.replication.util.suspendExecute
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

class TransportUpdateAutoFollowPatternAction @Inject constructor(transportService: TransportService,
                                                                 val threadPool: ThreadPool,
                                                                 actionFilters: ActionFilters,
                                                                 private val client: Client) :
        HandledTransportAction<UpdateAutoFollowPatternRequest, AcknowledgedResponse>(UpdateAutoFollowPatternAction.NAME,
                transportService, actionFilters, ::UpdateAutoFollowPatternRequest), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportUpdateAutoFollowPatternAction::class.java)
    }

    override fun doExecute(task: Task, request: UpdateAutoFollowPatternRequest, listener: ActionListener<AcknowledgedResponse>) {
        log.info("Setting-up autofollow for ${request.connection}:${request.patternName} -> " +
                "${request.pattern}")
        val user = SecurityContext.fromSecurityThreadContext(threadPool.threadContext)
        launch(threadPool.coroutineContext()) {
            listener.completeWith {
                if (request.action == UpdateAutoFollowPatternRequest.Action.ADD) {
                    // Pattern is same for leader and follower
                    val followerClusterRole = request.useRoles?.get(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE)
                    val leaderClusterRole = request.useRoles?.get(ReplicateIndexRequest.LEADER_CLUSTER_ROLE)
                    val setupChecksRequest = SetupChecksRequest(ReplicationContext(request.pattern!!, user?.overrideFgacRole(followerClusterRole)),
                            ReplicationContext(request.pattern!!, user?.overrideFgacRole(leaderClusterRole)),
                            request.connection)
                    val setupChecksRes = client.suspendExecute(SetupChecksAction.INSTANCE, setupChecksRequest)
                    if(!setupChecksRes.isAcknowledged) {
                        throw org.elasticsearch.replication.ReplicationException("Setup checks failed while setting-up auto follow pattern")
                    }
                }
                val masterNodeReq = AutoFollowMasterNodeRequest(user, request)
                client.suspendExecute(AutoFollowMasterNodeAction.INSTANCE, masterNodeReq)
            }
        }
    }
}
