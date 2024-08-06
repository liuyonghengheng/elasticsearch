/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.indexmanagement.indexstatemanagement.action.RolloverAction.Companion.MAX_AGE_FIELD
import org.elasticsearch.indexmanagement.indexstatemanagement.action.RolloverAction.Companion.MAX_DOC_COUNT_FIELD
import org.elasticsearch.indexmanagement.indexstatemanagement.action.RolloverAction.Companion.MAX_PRIMARY_SHARD_SIZE_FIELD
import org.elasticsearch.indexmanagement.indexstatemanagement.action.RolloverAction.Companion.MAX_SIZE_FIELD
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.ActionParser

class RolloverActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val minSize = sin.readOptionalWriteable(::ByteSizeValue)
        val minDocs = sin.readOptionalLong()
        val minAge = sin.readOptionalTimeValue()
        val minPrimaryShardSize = sin.readOptionalWriteable(::ByteSizeValue)
        val index = sin.readInt()

        return RolloverAction(minSize, minDocs, minAge, minPrimaryShardSize, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var minSize: ByteSizeValue? = null
        var minDocs: Long? = null
        var minAge: TimeValue? = null
        var minPrimaryShardSize: ByteSizeValue? = null

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                RolloverAction.MIN_SIZE_FIELD, MAX_SIZE_FIELD  -> minSize = ByteSizeValue.parseBytesSizeValue(xcp.text(), RolloverAction.MIN_SIZE_FIELD)
                RolloverAction.MIN_DOC_COUNT_FIELD, MAX_DOC_COUNT_FIELD -> minDocs = xcp.longValue()
                RolloverAction.MIN_INDEX_AGE_FIELD, MAX_AGE_FIELD -> minAge = TimeValue.parseTimeValue(xcp.text(), RolloverAction.MIN_INDEX_AGE_FIELD)
                RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD, MAX_PRIMARY_SHARD_SIZE_FIELD -> minPrimaryShardSize = ByteSizeValue.parseBytesSizeValue(
                    xcp.text(),
                    RolloverAction
                        .MIN_PRIMARY_SHARD_SIZE_FIELD
                )
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in RolloverAction.")
            }
        }

        return RolloverAction(minSize, minDocs, minAge, minPrimaryShardSize, index)
    }

    override fun getActionType(): String {
        return RolloverAction.name
    }
}
