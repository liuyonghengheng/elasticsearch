/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.action.get

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.elasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.indexmanagement.rollup.model.Rollup.Companion.ROLLUP_TYPE
import org.elasticsearch.indexmanagement.util._ID
import org.elasticsearch.indexmanagement.util._PRIMARY_TERM
import org.elasticsearch.indexmanagement.util._SEQ_NO
import org.elasticsearch.indexmanagement.util._VERSION
import org.elasticsearch.rest.RestStatus
import java.io.IOException

class GetRollupResponse : ActionResponse, ToXContentObject {
    var id: String
    var version: Long
    var seqNo: Long
    var primaryTerm: Long
    val status: RestStatus
    val rollup: Rollup?

    constructor(
        id: String,
        version: Long,
        seqNo: Long,
        primaryTerm: Long,
        status: RestStatus,
        rollup: Rollup?
    ) : super() {
        this.id = id
        this.version = version
        this.seqNo = seqNo
        this.primaryTerm = primaryTerm
        this.status = status
        this.rollup = rollup
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        status = sin.readEnum(RestStatus::class.java),
        rollup = if (sin.readBoolean()) Rollup(sin) else null
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeEnum(status)
        if (rollup == null) {
            out.writeBoolean(false)
        } else {
            out.writeBoolean(true)
            rollup.writeTo(out)
        }
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_SEQ_NO, seqNo)
            .field(_PRIMARY_TERM, primaryTerm)
        if (rollup != null) builder.field(ROLLUP_TYPE, rollup, XCONTENT_WITHOUT_TYPE_AND_USER)
        return builder.endObject()
    }
}
