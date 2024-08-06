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

package org.elasticsearch.replication.task

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.IndexService
import org.elasticsearch.index.shard.IndexEventListener
import org.elasticsearch.index.shard.IndexShard
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.indices.cluster.IndicesClusterStateService
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

object IndexCloseListener : IndexEventListener {

    private val tasks = ConcurrentHashMap<Any, MutableSet<CrossClusterReplicationTask>>()

    fun addCloseListener(indexOrShardId: Any, task: CrossClusterReplicationTask) {
        require(indexOrShardId is String || indexOrShardId is ShardId) {
            "Can't register a close listener for ${indexOrShardId}. Only Index or ShardIds are allowed."
        }
        tasks.computeIfAbsent(indexOrShardId) { Collections.synchronizedSet(mutableSetOf()) }.add(task)
    }

    fun removeCloseListener(indexOrShardId: Any, task: CrossClusterReplicationTask) {
        tasks.computeIfPresent(indexOrShardId) { _, v ->
            v.remove(task)
            if (v.isEmpty()) null else v
        }
    }

    override fun beforeIndexShardClosed(shardId: ShardId, indexShard: IndexShard?, indexSettings: Settings) {
        super.beforeIndexShardClosed(shardId, indexShard, indexSettings)
        val tasksToCancel = tasks.remove(shardId)
        if (tasksToCancel != null) {
            for (task in tasksToCancel) {
                task.onIndexShardClosed(shardId, indexShard, indexSettings)
            }
        }
    }

    override fun beforeIndexRemoved(indexService: IndexService,
                                    reason: IndicesClusterStateService.AllocatedIndices.IndexRemovalReason) {
        super.beforeIndexRemoved(indexService, reason)
        val tasksToCancel = tasks.remove(indexService.index().name)
        if (tasksToCancel != null) {
            for (task in tasksToCancel) {
                task.onIndexRemoved(indexService, reason)
            }
        }
    }
}
