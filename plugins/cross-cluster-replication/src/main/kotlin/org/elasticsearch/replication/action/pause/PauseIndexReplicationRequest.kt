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

import org.elasticsearch.replication.metadata.ReplicationMetadataManager
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.IndicesRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser

class PauseIndexReplicationRequest : AcknowledgedRequest<PauseIndexReplicationRequest>, IndicesRequest.Replaceable, ToXContentObject {

    lateinit var indexName: String
    var reason = ReplicationMetadataManager.CUSTOMER_INITIATED_ACTION

    constructor(indexName: String, reason: String) {
        this.indexName = indexName
        this.reason = reason
    }

    private constructor() {
    }

    constructor(inp: StreamInput): super(inp) {
        indexName = inp.readString()
        reason = inp.readString()
    }

    companion object {
        private val PARSER = ObjectParser<PauseIndexReplicationRequest, Void>("PauseReplicationRequestParser") {
            PauseIndexReplicationRequest()
        }

        init {
            PARSER.declareString(PauseIndexReplicationRequest::reason::set, ParseField("reason"))
        }

        fun fromXContent(parser: XContentParser, followerIndex: String): PauseIndexReplicationRequest {
            val PauseIndexReplicationRequest = PARSER.parse(parser, null)
            PauseIndexReplicationRequest.indexName = followerIndex
            return PauseIndexReplicationRequest
        }
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    override fun indices(vararg indices: String?): IndicesRequest {
        return this
    }

    override fun indices(): Array<String> {
        return arrayOf(indexName)
    }

    override fun indicesOptions(): IndicesOptions {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed()
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("indexName", indexName)
        builder.endObject()
        return builder
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(indexName)
        out.writeString(reason)
    }

}
