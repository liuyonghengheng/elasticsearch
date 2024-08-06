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

package org.elasticsearch.replication.action.repository

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.store.StoreFileMetadata

class GetFileChunkRequest : RemoteClusterRepositoryRequest<GetFileChunkRequest> {
    val storeFileMetadata: StoreFileMetadata
    val offset: Long
    val length: Int

    constructor(restoreUUID: String, node: DiscoveryNode, leaderShardId: ShardId, storeFileMetaData: StoreFileMetadata,
                offset: Long, length: Int, followerCluster: String, followerShardId: ShardId):
            super(restoreUUID, node, leaderShardId, followerCluster, followerShardId) {
        this.storeFileMetadata = storeFileMetaData
        this.offset = offset
        this.length = length
    }

    constructor(input : StreamInput): super(input) {
        storeFileMetadata = StoreFileMetadata(input)
        offset = input.readLong()
        length = input.readInt()
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        storeFileMetadata.writeTo(out)
        out.writeLong(offset)
        out.writeInt(length)
    }
}
