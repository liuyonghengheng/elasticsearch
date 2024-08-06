/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.indexmanagement.indexstatemanagement.action.ForceMergeAction.Companion.MAX_NUM_SEGMENTS_FIELD
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.ActionParser

class ForceMergeActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val maxNumSegments = sin.readInt()
        val index = sin.readInt()
        return ForceMergeAction(maxNumSegments, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var maxNumSegments: Int? = null

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                MAX_NUM_SEGMENTS_FIELD -> maxNumSegments = xcp.intValue()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ForceMergeActionConfig.")
            }
        }

        return ForceMergeAction(
            requireNotNull(maxNumSegments) { "ForceMergeActionConfig maxNumSegments is null" },
            index
        )
    }

    override fun getActionType(): String {
        return ForceMergeAction.name
    }
}
