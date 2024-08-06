/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.elasticsearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import org.elasticsearch.indexmanagement.indexstatemanagement.util.buildInvalidIndexResponse
import java.io.IOException

open class ISMStatusResponse : ActionResponse, ToXContentObject {

    val updated: Int
    val failedIndices: List<FailedIndex>

    constructor(
        updated: Int,
        failedIndices: List<FailedIndex>
    ) : super() {
        this.updated = updated
        this.failedIndices = failedIndices
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        updated = sin.readInt(),
        failedIndices = sin.readList(::FailedIndex)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeInt(updated)
        out.writeCollection(failedIndices)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field(UPDATED_INDICES, updated)
        buildInvalidIndexResponse(builder, failedIndices)
        return builder.endObject()
    }
}
