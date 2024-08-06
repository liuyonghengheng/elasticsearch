/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.preview

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.RestStatus

class PreviewTransformResponse(
    val documents: List<Map<String, Any>>,
    val status: RestStatus
) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        documents = sin.let {
            val documentList = mutableListOf<Map<String, Any>>()
            val size = it.readVInt()
            repeat(size) { _ ->
                documentList.add(sin.readMap()!!)
            }
            documentList.toList()
        },
        status = sin.readEnum(RestStatus::class.java)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeVInt(documents.size)
        for (document in documents) {
            out.writeMap(document)
        }
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("documents", documents)
            .endObject()
    }
}
