/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.indexmanagement.rollup.model.ISMRollup
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.ActionParser

class RollupActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val ismRollup = ISMRollup(sin)
        val index = sin.readInt()
        return RollupAction(ismRollup, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var ismRollup: ISMRollup? = null

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                RollupAction.ISM_ROLLUP_FIELD -> ismRollup = ISMRollup.parse(xcp)
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in RollupAction.")
            }
        }

        return RollupAction(ismRollup = requireNotNull(ismRollup) { "RollupAction rollup is null" }, index)
    }

    override fun getActionType(): String {
        return RollupAction.name
    }
}
