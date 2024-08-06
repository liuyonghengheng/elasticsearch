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

package org.elasticsearch.replication.action.update

import org.elasticsearch.replication.metadata.ReplicationMetadataManager
import org.elasticsearch.replication.metadata.ReplicationOverallState
import org.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.elasticsearch.replication.util.completeWith
import org.elasticsearch.replication.util.coroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.settings.IndexScopedSettings
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException

class TransportUpdateIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                              clusterService: ClusterService,
                                                              threadPool: ThreadPool,
                                                              actionFilters: ActionFilters,
                                                              indexNameExpressionResolver:
                                                              IndexNameExpressionResolver,
                                                              val indexScopedSettings: IndexScopedSettings,
                                                              val client: Client,
                                                              val replicationMetadataManager: ReplicationMetadataManager) :
    TransportMasterNodeAction<UpdateIndexReplicationRequest, AcknowledgedResponse> (UpdateIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::UpdateIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportUpdateIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: UpdateIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: UpdateIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith {
                log.info("Updating index replication on index:" + request.indexName)
                validateUpdateReplicationRequest(request)

                replicationMetadataManager.updateSettings(request.indexName, request.settings)

                AcknowledgedResponse(true)
            }
        }
    }

    private fun validateUpdateReplicationRequest(request: UpdateIndexReplicationRequest) {
        indexScopedSettings.validate(request.settings,
                false,
                false)

        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
                throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]
        if (replicationOverallState == ReplicationOverallState.RUNNING.name || replicationOverallState == ReplicationOverallState.PAUSED.name)
            return

        throw IllegalStateException("Cannot update settings when in $replicationOverallState state")
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }


    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }
}
