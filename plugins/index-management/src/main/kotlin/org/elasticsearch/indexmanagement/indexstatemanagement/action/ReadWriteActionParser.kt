/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.ActionParser

class ReadWriteActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val index = sin.readInt()
        return ReadWriteAction(index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        ensureExpectedToken(XContentParser.Token.END_OBJECT, xcp.nextToken(), xcp)

        return ReadWriteAction(index)
    }

    override fun getActionType(): String {
        return ReadWriteAction.name
    }
}
