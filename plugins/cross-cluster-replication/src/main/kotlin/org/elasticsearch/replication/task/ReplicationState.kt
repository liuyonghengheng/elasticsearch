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

package org.elasticsearch.replication.task

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder

/**
 * Enum that represents the state of replication of either shards or indices.
 */
enum class ReplicationState : Writeable, ToXContentFragment {

    INIT, RESTORING, INIT_FOLLOW, FOLLOWING, MONITORING, FAILED, COMPLETED;

    override fun writeTo(out: StreamOutput) {
        out.writeEnum(this)
        fun readState(inp : StreamInput) : ReplicationState = inp.readEnum(ReplicationState::class.java)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        return builder.value(toString())
    }
}
