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

package org.elasticsearch.replication.action.status

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.broadcast.BroadcastRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder

class ShardInfoRequest : BroadcastRequest<ShardInfoRequest> , ToXContentObject {

    var indexName: String
    var verbose: Boolean = false

    constructor(indexName: String) {
        this.indexName = indexName
    }

    constructor(indexName: String,verbose: Boolean) {
        this.indexName = indexName
        this.verbose = verbose
    }

    constructor(inp: StreamInput): super(inp) {
        indexName = inp.readString()
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException = ActionRequestValidationException()
        if(indexName.isEmpty()) {
            validationException.addValidationError("Index name must be specified to obtain replication status")
        }
        return if(validationException.validationErrors().isEmpty()) return null else validationException
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
    }

}
