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

package org.elasticsearch.replication.action.index.block

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.IndicesRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder

enum class IndexBlockUpdateType {
    ADD_BLOCK, REMOVE_BLOCK
}

class UpdateIndexBlockRequest :  AcknowledgedRequest<UpdateIndexBlockRequest>, IndicesRequest, ToXContentObject {

    var indexName: String
    var updateType: IndexBlockUpdateType

    constructor(index: String, updateType: IndexBlockUpdateType): super() {
        this.indexName = index
        this.updateType = updateType
    }

    constructor(inp: StreamInput): super(inp) {
        indexName = inp.readString()
        updateType = inp.readEnum(IndexBlockUpdateType::class.java)
    }

    override fun validate(): ActionRequestValidationException? {
        /* No validation for now. Null checks are implicit as constructor doesn't
        allow nulls to be passed into the request.
         */
        return null;
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
        builder.field("updateType", updateType)
        builder.endObject()
        return builder
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(indexName)
        out.writeEnum(updateType)
    }
}
