/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.refreshanalyzer

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.shard.ShardId
import java.io.IOException

class RefreshSearchAnalyzerShardResponse : BroadcastShardResponse {
    var reloadedAnalyzers: List<String>

    constructor(si: StreamInput) : super(si) {
        reloadedAnalyzers = si.readStringArray().toList()
    }

    constructor(shardId: ShardId, reloadedAnalyzers: List<String>) : super(shardId) {
        this.reloadedAnalyzers = reloadedAnalyzers
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeStringArray(reloadedAnalyzers.toTypedArray())
    }
}
