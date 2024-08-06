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

package org.elasticsearch.replication.action.pause

import org.elasticsearch.replication.metadata.*
import org.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.elasticsearch.replication.util.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.RestoreInProgress
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException

class TransportPauseIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                               clusterService: ClusterService,
                                                               threadPool: ThreadPool,
                                                               actionFilters: ActionFilters,
                                                               indexNameExpressionResolver:
                                                               IndexNameExpressionResolver,
                                                               val client: Client,
                                                               val replicationMetadataManager: ReplicationMetadataManager) :
    TransportMasterNodeAction<PauseIndexReplicationRequest, AcknowledgedResponse> (PauseIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::PauseIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportPauseIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: PauseIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: PauseIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith {
                log.info("Pausing index replication on index:" + request.indexName)
                validatePauseReplicationRequest(request)

                // Restoring Index can't be paused
                val restoring = clusterService.state().custom<RestoreInProgress>(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).any { entry ->
                    entry.indices().any { it == request.indexName }
                }

                if (restoring) {
                    throw ElasticsearchException("Index is in restore phase currently for index: ${request.indexName}. You can pause after restore completes." )
                }

                // If the index is not in bootstrap phase, bring down the tasks and persist the info
                replicationMetadataManager.updateIndexReplicationState(request.indexName, ReplicationOverallState.PAUSED, request.reason)

                AcknowledgedResponse(true)
            }
        }
    }

    private fun validatePauseReplicationRequest(request: PauseIndexReplicationRequest) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
            throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]
        if (replicationOverallState == ReplicationOverallState.PAUSED.name)
            throw ResourceAlreadyExistsException("Index ${request.indexName} is already paused")
        else if (replicationOverallState != ReplicationOverallState.RUNNING.name)
            throw IllegalStateException("Cannot pause when in $replicationOverallState state")
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }
}
