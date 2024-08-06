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

package org.elasticsearch.replication.task.shard

import org.elasticsearch.replication.ReplicationSettings
import org.elasticsearch.replication.metadata.ReplicationMetadataManager
import org.elasticsearch.replication.metadata.ReplicationOverallState
import org.elasticsearch.replication.metadata.state.REPLICATION_LAST_KNOWN_OVERALL_STATE
import org.elasticsearch.replication.metadata.state.getReplicationStateParamsForIndex
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.persistent.AllocatedPersistentTask
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.elasticsearch.persistent.PersistentTasksExecutor
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool

class ShardReplicationExecutor(executor: String, private val clusterService : ClusterService,
                               private val threadPool: ThreadPool, private val client: Client,
                               private val replicationMetadataManager: ReplicationMetadataManager,
                               private val replicationSettings: ReplicationSettings,
                               private val stats: FollowerClusterStats) :
    PersistentTasksExecutor<ShardReplicationParams>(TASK_NAME, executor) {

    companion object {
        const val TASK_NAME = "cluster:indices/shards/replication"
        val SHARD_NOT_ACTIVE = Assignment(null, "No active shard found")
        val log = LogManager.getLogger(ShardReplicationExecutor::class.java)
    }

    override fun validate(params: ShardReplicationParams, clusterState: ClusterState) {
        // Checks that there is a primary shard. Side-effect will check that the index and shard exists.
        clusterState.routingTable.shardRoutingTable(params.followerShardId)
            .primaryShard() ?: throw ElasticsearchException("no primary shard available for ${params.followerShardId}")
        val replicationStateParams = getReplicationStateParamsForIndex(clusterService, params.followerShardId.indexName)
                ?:
            throw IllegalStateException("Cant find replication details metadata for followIndex:${params.followerShardId.indexName}. " +
                    "Seems like replication is not in progress, so not starting shard task for shardId:${params.followerShardId}")
        if (replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] == ReplicationOverallState.FAILED.name
            || replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE] == ReplicationOverallState.STOPPED.name)
            throw IllegalStateException("Unknown replication state metadata:${replicationStateParams[REPLICATION_LAST_KNOWN_OVERALL_STATE]} " +
                    " followIndex:${params.followerShardId.indexName}")
    }

    override fun getAssignment(params: ShardReplicationParams, clusterState: ClusterState) : Assignment {
        val primaryShard = clusterState.routingTable().shardRoutingTable(params.followerShardId).primaryShard()
        if (!primaryShard.active()) return SHARD_NOT_ACTIVE
        return Assignment(primaryShard.currentNodeId(), "node with primary shard")
    }

    override fun nodeOperation(task: AllocatedPersistentTask, params: ShardReplicationParams, state: PersistentTaskState?) {
        if (task is ShardReplicationTask) {
            log.info("starting persistent replication task: $params, $state, ${task.allocationId}, ${task.status}")
            task.run()
        } else {
            task.markAsFailed(IllegalArgumentException("Unknown task class ${task::class.java}"))
        }
    }

    override fun createTask(id: Long, type: String, action: String, parentTaskId: TaskId,
                            taskInProgress: PersistentTask<ShardReplicationParams>,
                            headers: Map<String, String>): AllocatedPersistentTask {
        return ShardReplicationTask(id, type, action, getDescription(taskInProgress), parentTaskId,
                                    taskInProgress.params!!, executor, clusterService, threadPool,
                                    client, replicationMetadataManager, replicationSettings, stats)
    }

    override fun getDescription(taskInProgress: PersistentTask<ShardReplicationParams>): String {
        val params = requireNotNull(taskInProgress.params)
        return "replication:${params.leaderAlias}:${params.leaderShardId} -> ${params.followerShardId}"
    }
}
