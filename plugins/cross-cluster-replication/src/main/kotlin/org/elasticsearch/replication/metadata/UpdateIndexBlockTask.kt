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

package org.elasticsearch.replication.metadata

import org.elasticsearch.replication.action.index.block.IndexBlockUpdateType
import org.elasticsearch.replication.action.index.block.UpdateIndexBlockRequest
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.cluster.AckedClusterStateUpdateTask
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlock
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.block.ClusterBlocks
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.collect.ImmutableOpenMap
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.rest.RestStatus
import java.util.*


/* This is our custom index block to prevent changes to follower
        index while replication is in progress.
         */
val INDEX_REPLICATION_BLOCK = ClusterBlock(
        1000,
        "index read-only(cross-cluster-replication)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE))

/* This function checks the local cluster state to see if given
    index is blocked with given level with any block other than
    our own INDEX_REPLICATION_BLOCK
*/
fun checkIfIndexBlockedWithLevel(clusterService: ClusterService,
                                 indexName: String,
                                 clusterBlockLevel: ClusterBlockLevel) {
    clusterService.state().routingTable.index(indexName) ?:
    throw IndexNotFoundException("Index with name:$indexName doesn't exist")
    val writeIndexBlockMap : ImmutableOpenMap<String, Set<ClusterBlock>> = clusterService.state().blocks()
            .indices(clusterBlockLevel)
    if (!writeIndexBlockMap.containsKey(indexName))
        return
    val clusterBlocksSet : Set<ClusterBlock> = writeIndexBlockMap.get(indexName)
    if (clusterBlocksSet.contains(INDEX_REPLICATION_BLOCK)
            && clusterBlocksSet.size > 1)
        throw ClusterBlockException(clusterBlocksSet)
}

class UpdateIndexBlockTask(val request: UpdateIndexBlockRequest, listener: ActionListener<AcknowledgedResponse>) :
        AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener)
{
    override fun execute(currentState: ClusterState): ClusterState {
        val newState = ClusterState.builder(currentState)
        when(request.updateType) {
            IndexBlockUpdateType.ADD_BLOCK -> {
                if (!currentState.blocks.hasIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)) {
                    val newBlocks = ClusterBlocks.builder().blocks(currentState.blocks)
                        .addIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
                    newState.blocks(newBlocks)
                }
            }
            IndexBlockUpdateType.REMOVE_BLOCK -> {
                val newBlocks = ClusterBlocks.builder().blocks(currentState.blocks)
                    .removeIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
                newState.blocks(newBlocks)
            }
        }
        return newState.build()
    }

    override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
}
