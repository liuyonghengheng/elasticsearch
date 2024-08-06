/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.spi.indexstatemanagement.model

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import java.io.IOException

data class ActionTimeout(val timeout: TimeValue) : ToXContentFragment, Writeable {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.field(TIMEOUT_FIELD, timeout.stringRep)
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        timeout = sin.readTimeValue()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeTimeValue(timeout)
    }

    companion object {
        const val TIMEOUT_FIELD = "timeout"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ActionTimeout {
            if (xcp.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ActionTimeout(TimeValue.parseTimeValue(xcp.text(), TIMEOUT_FIELD))
            } else {
                throw IllegalArgumentException("Invalid token: [${xcp.currentToken()}] for ActionTimeout")
            }
        }
    }
}
