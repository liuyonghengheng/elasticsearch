/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.apache.logging.log4j.LogManager
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.ActionParser

private val logger = LogManager.getLogger(DeleteActionParser::class.java)

class DeleteActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val index = sin.readInt()
        return DeleteAction(index)
    }

//    override fun fromXContent(xcp: XContentParser, index: Int): Action {
//        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
//        ensureExpectedToken(XContentParser.Token.END_OBJECT, xcp.nextToken(), xcp)
//
//        return DeleteAction(index)
//    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var deleteSearchableSnapshot: Boolean? = null

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            if (xcp.currentToken() == XContentParser.Token.FIELD_NAME) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    "delete_searchable_snapshot" -> deleteSearchableSnapshot = xcp.booleanValue()
                    else -> throw IllegalArgumentException("Unknown field: $fieldName")
                }
            }
        }


        return DeleteAction(index)
    }

    override fun getActionType(): String {
        return DeleteAction.name
    }
}
