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

package org.elasticsearch.replication.action.resume

import org.elasticsearch.replication.action.index.ReplicateIndexResponse
import org.elasticsearch.replication.metadata.ReplicationMetadataManager
import org.elasticsearch.replication.metadata.ReplicationOverallState
import org.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.elasticsearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.elasticsearch.replication.task.ReplicationState
import org.elasticsearch.replication.task.index.IndexReplicationExecutor
import org.elasticsearch.replication.task.index.IndexReplicationParams
import org.elasticsearch.replication.task.index.IndexReplicationState
import org.elasticsearch.replication.util.ValidationUtil
import org.elasticsearch.replication.util.completeWith
import org.elasticsearch.replication.util.coroutineContext
import org.elasticsearch.replication.util.persistentTasksService
import org.elasticsearch.replication.util.startTask
import org.elasticsearch.replication.util.suspending
import org.elasticsearch.replication.util.waitForTaskCondition
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.env.Environment
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException
import java.lang.IllegalStateException

class TransportResumeIndexReplicationAction @Inject constructor(transportService: TransportService,
                                                                clusterService: ClusterService,
                                                                threadPool: ThreadPool,
                                                                actionFilters: ActionFilters,
                                                                indexNameExpressionResolver: IndexNameExpressionResolver,
                                                                val client: Client,
                                                                val replicationMetadataManager: ReplicationMetadataManager,
                                                                private val environment: Environment) :
    TransportMasterNodeAction<ResumeIndexReplicationRequest, AcknowledgedResponse> (ResumeIndexReplicationAction.NAME,
            transportService, clusterService, threadPool, actionFilters, ::ResumeIndexReplicationRequest,
            indexNameExpressionResolver), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportResumeIndexReplicationAction::class.java)
    }

    override fun checkBlock(request: ResumeIndexReplicationRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun masterOperation(request: ResumeIndexReplicationRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith {
                log.info("Resuming index replication on index:" + request.indexName)
                validateResumeReplicationRequest(request)
                val replMetdata = replicationMetadataManager.getIndexReplicationMetadata(request.indexName)
                val remoteMetadata = getLeaderIndexMetadata(replMetdata.connectionName, replMetdata.leaderContext.resource)
                val params = IndexReplicationParams(replMetdata.connectionName, remoteMetadata.index, request.indexName)
                if (!isResumable(params)) {
                    throw ResourceNotFoundException("Retention lease doesn't exist. Replication can't be resumed for ${request.indexName}")
                }

                val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
                val getSettingsRequest = GetSettingsRequest().includeDefaults(false).indices(params.leaderIndex.name)
                val settingsResponse = remoteClient.suspending(
                    remoteClient.admin().indices()::getSettings,
                    injectSecurityContext = true
                )(getSettingsRequest)

                val leaderSettings = settingsResponse.indexToSettings.get(params.leaderIndex.name) ?: throw IndexNotFoundException(params.leaderIndex.name)

                // k-NN Setting is a static setting. In case the setting is changed at the leader index before resume,
                // block the resume.
//                if(leaderSettings.getAsBoolean(KNN_INDEX_SETTING, false)) {
//                    throw IllegalStateException("Cannot resume replication for k-NN enabled index ${params.leaderIndex.name}.")
//                }

                ValidationUtil.validateAnalyzerSettings(environment, leaderSettings, replMetdata.settings)

                replicationMetadataManager.updateIndexReplicationState(request.indexName, ReplicationOverallState.RUNNING)
                val task = persistentTasksService.startTask("replication:index:${request.indexName}",
                        IndexReplicationExecutor.TASK_NAME, params)

                if (!task.isAssigned) {
                    log.error("Failed to assign task")
                    listener.onResponse(ReplicateIndexResponse(false))
                }

                // Now wait for the replication to start and the follower index to get created before returning
                persistentTasksService.waitForTaskCondition(task.id, request.timeout()) { t ->
                    val replicationState = (t.state as IndexReplicationState?)?.state
                    replicationState == ReplicationState.FOLLOWING
                }

                AcknowledgedResponse(true)
            }
        }
    }

    private suspend fun isResumable(params :IndexReplicationParams): Boolean {
        var isResumable = true
        val remoteClient = client.getRemoteClusterClient(params.leaderAlias)
        val shards = clusterService.state().routingTable.indicesRouting().get(params.followerIndexName).shards()
        val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(clusterService.clusterName.value(), remoteClient)
        shards.forEach {
            val followerShardId = it.value.shardId
            if  (!retentionLeaseHelper.verifyRetentionLeaseExist(ShardId(params.leaderIndex, followerShardId.id), followerShardId)) {
                isResumable = false
            }
        }

        if (isResumable) {
            return true
        }

        // clean up all retention leases we may have accidentally took while doing verifyRetentionLeaseExist .
        // Idempotent Op which does no harm
        shards.forEach {
            val followerShardId = it.value.shardId
            log.debug("Removing lease for $followerShardId.id ")
            retentionLeaseHelper.attemptRetentionLeaseRemoval(ShardId(params.leaderIndex, followerShardId.id), followerShardId)
        }

        return false
    }

    private suspend fun getLeaderIndexMetadata(leaderAlias: String, leaderIndex: String): IndexMetadata {
        val leaderClusterClient = client.getRemoteClusterClient(leaderAlias)
        val clusterStateRequest = leaderClusterClient.admin().cluster().prepareState()
                .clear()
                .setIndices(leaderIndex)
                .setMetadata(true)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()
        val remoteState = leaderClusterClient.suspending(leaderClusterClient.admin().cluster()::state)(clusterStateRequest).state
        return remoteState.metadata.index(leaderIndex) ?: throw IndexNotFoundException("${leaderAlias}:${leaderIndex}")
    }

    private fun validateResumeReplicationRequest(request: ResumeIndexReplicationRequest) {
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, request.indexName)
                ?:
            throw IllegalArgumentException("No replication in progress for index:${request.indexName}")
        val replicationOverallState = replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]

        if (replicationOverallState != ReplicationOverallState.PAUSED.name)
            throw ResourceAlreadyExistsException("Replication on Index ${request.indexName} is already running")
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }
}
