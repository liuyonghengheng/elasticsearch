/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.get

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.indexmanagement.transform.model.Transform.Companion.TRANSFORM_TYPE
import org.elasticsearch.indexmanagement.util._ID
import org.elasticsearch.indexmanagement.util._PRIMARY_TERM
import org.elasticsearch.indexmanagement.util._SEQ_NO
import org.elasticsearch.indexmanagement.util._VERSION
import org.elasticsearch.rest.RestStatus
import java.io.IOException

class GetTransformResponse(
    val id: String,
    val version: Long,
    val seqNo: Long,
    val primaryTerm: Long,
    val status: RestStatus,
    val transform: Transform?
) : ActionResponse(), ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        status = sin.readEnum(RestStatus::class.java),
        transform = if (sin.readBoolean()) Transform(sin) else null
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeEnum(status)
        if (transform == null) {
            out.writeBoolean(false)
        } else {
            out.writeBoolean(true)
            transform.writeTo(out)
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_SEQ_NO, seqNo)
            .field(_PRIMARY_TERM, primaryTerm)
        if (transform != null) builder.field(TRANSFORM_TYPE, transform, XCONTENT_WITHOUT_TYPE_AND_USER)
        return builder.endObject()
    }
}
