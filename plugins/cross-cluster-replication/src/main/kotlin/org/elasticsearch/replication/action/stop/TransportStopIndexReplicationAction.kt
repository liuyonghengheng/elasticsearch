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

package org.elasticsearch.replication.action.stop

import org.elasticsearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.elasticsearch.replication.action.index.block.IndexBlockUpdateType
import org.elasticsearch.replication.action.index.block.UpdateIndexBlockAction
import org.elasticsearch.replication.action.index.block.UpdateIndexBlockRequest
import org.elasticsearch.replication.metadata.INDEX_REPLICATION_BLOCK
import org.elasticsearch.replication.metadata.ReplicationMetadataManager
import org.elasticsearch.replication.metadata.ReplicationOverallState
import org.elasticsearch.replication.metadata.UpdateMetadataAction
import org.elasticsearch.replication.metadata.UpdateMetadataRequest
import org.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.elasticsearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.elasticsearch.replication.util.coroutineContext
import org.elasticsearch.replication.util.suspendExecute
import org.elasticsearch.replication.util.suspending
import org.elasticsearch.replication.util.waitForClusterStateUpdate
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.client.Requests
import org.elasticsearch.cluster.AckedClusterStateUpdateTask
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.RestoreInProgress
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.replication.util.stackTraceToString
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException

class TransportStopIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                              clusterService: ClusterService,
                                                              threadPool: ThreadPool,
                                                              actionFilters: ActionFilters,
                                                              indexNameExpressionResolver:
                                                              IndexNameExpressionResolver,
                                                              val client: Client,
                                                              val replicationMetadataManager: ReplicationMetadataManager) :
    TransportMasterNodeAction<StopIndexReplicationRequest, AcknowledgedResponse> (StopIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::StopIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportStopIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: StopIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: StopIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            try {
                log.info("Stopping index replication on index:" + request.indexName)

                // NOTE: We remove the block first before validation since it is harmless idempotent operations and
                //       gives back control of the index even if any failure happens in one of the steps post this.
                val updateIndexBlockRequest = UpdateIndexBlockRequest(request.indexName,IndexBlockUpdateType.REMOVE_BLOCK)
                val updateIndexBlockResponse = client.suspendExecute(UpdateIndexBlockAction.INSTANCE, updateIndexBlockRequest, injectSecurityContext = true)
                if(!updateIndexBlockResponse.isAcknowledged) {
                    throw ElasticsearchException("Failed to remove index block on ${request.indexName}")
                }

                validateReplicationStateOfIndex(request)

                // Index will be deleted if replication is stopped while it is restoring.  So no need to close/reopen
                val restoring = clusterService.state().custom<RestoreInProgress>(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).any { entry ->
                    entry.indices().any { it == request.indexName }
                }
                if(restoring) {
                    log.info("Index[${request.indexName}] is in restoring stage")
                }
                if (!restoring &&
                        state.routingTable.hasIndex(request.indexName)) {

                    var updateRequest = UpdateMetadataRequest(request.indexName, UpdateMetadataRequest.Type.CLOSE, Requests.closeIndexRequest(request.indexName))
                    var closeResponse = client.suspendExecute(UpdateMetadataAction.INSTANCE, updateRequest, injectSecurityContext = true)
                    if (!closeResponse.isAcknowledged) {
                        throw ElasticsearchException("Unable to close index: ${request.indexName}")
                    }
                }

                try {
                    val replMetadata = replicationMetadataManager.getIndexReplicationMetadata(request.indexName)
                    val remoteClient = client.getRemoteClusterClient(replMetadata.connectionName)
                    val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
                    retentionLeaseHelper.attemptRemoveRetentionLease(clusterService, replMetadata, request.indexName)
                } catch(e: Exception) {
                    log.error("Failed to remove retention lease from the leader cluster", e)
                }

                val clusterStateUpdateResponse : AcknowledgedResponse =
                        clusterService.waitForClusterStateUpdate("stop_replication") { l -> StopReplicationTask(request, l)}
                if (!clusterStateUpdateResponse.isAcknowledged) {
                    throw ElasticsearchException("Failed to update cluster state")
                }

                // Index will be deleted if stop is called while it is restoring. So no need to reopen
                if (!restoring &&
                        state.routingTable.hasIndex(request.indexName)) {
                    val reopenResponse = client.suspending(client.admin().indices()::open, injectSecurityContext = true)(OpenIndexRequest(request.indexName))
                    if (!reopenResponse.isAcknowledged) {
                        throw ElasticsearchException("Failed to reopen index: ${request.indexName}")
                    }
                }
                replicationMetadataManager.deleteIndexReplicationMetadata(request.indexName)
                listener.onResponse(AcknowledgedResponse(true))
            } catch (e: Exception) {
                log.error("Stop replication failed for index[${request.indexName}] with error ${e.stackTraceToString()}")
                listener.onFailure(e)
            }
        }
    }

    private fun validateReplicationStateOfIndex(request: StopIndexReplicationRequest) {
        // If replication blocks/settings are present, Stop action should proceed with the clean-up
        // This can happen during settings of follower index are carried over in the snapshot and the restore is
        // performed using this snapshot.
        if (clusterService.state().blocks.hasIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
                || clusterService.state().metadata.index(request.indexName)?.settings?.get(REPLICATED_INDEX_SETTING.key) != null) {
            return
        }

        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
            throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]
        if (replicationOverallState == ReplicationOverallState.RUNNING.name ||
            replicationOverallState == ReplicationOverallState.STOPPED.name ||
            replicationOverallState == ReplicationOverallState.FAILED.name ||
            replicationOverallState == ReplicationOverallState.PAUSED.name)
            return
        throw IllegalStateException("Unknown value of replication state:$replicationOverallState")
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }

    class StopReplicationTask(val request: StopIndexReplicationRequest, listener: ActionListener<AcknowledgedResponse>) :
        AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

        override fun execute(currentState: ClusterState): ClusterState {
            val newState = ClusterState.builder(currentState)

            // remove index block
            if (currentState.blocks.hasIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)) {
                val newBlocks = ClusterBlocks.builder().blocks(currentState.blocks)
                    .removeIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
                newState.blocks(newBlocks)
            }

            val mdBuilder = Metadata.builder(currentState.metadata)
            // remove replicated index setting
            val currentIndexMetadata = currentState.metadata.index(request.indexName)
            if (currentIndexMetadata != null &&
                    currentIndexMetadata.settings[REPLICATED_INDEX_SETTING.key] != null) {
                val newIndexMetadata = IndexMetadata.builder(currentIndexMetadata)
                        .settings(Settings.builder().put(currentIndexMetadata.settings).putNull(REPLICATED_INDEX_SETTING.key))
                        .settingsVersion(1 + currentIndexMetadata.settingsVersion)
                mdBuilder.put(newIndexMetadata)
            }
            newState.metadata(mdBuilder)

            return newState.build()
        }

        override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
    }
}
