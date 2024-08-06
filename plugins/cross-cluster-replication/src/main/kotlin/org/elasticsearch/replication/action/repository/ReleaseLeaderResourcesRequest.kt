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
import org.elasticsearch.index.shard.ShardId

class ReleaseLeaderResourcesRequest: RemoteClusterRepositoryRequest<ReleaseLeaderResourcesRequest> {

    constructor(restoreUUID: String, node: DiscoveryNode, leaderShardId: ShardId,
                followerCluster: String, followerShardId: ShardId):
            super(restoreUUID, node, leaderShardId, followerCluster, followerShardId)

    constructor(input : StreamInput): super(input)

    override fun validate(): ActionRequestValidationException? {
        return null
    }
}
