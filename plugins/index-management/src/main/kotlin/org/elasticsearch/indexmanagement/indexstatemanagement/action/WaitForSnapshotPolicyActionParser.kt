package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.indexmanagement.indexstatemanagement.action.WaitForSnapshotPolicyAction.Companion.POLICY_FIELD
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.ActionParser

class WaitForSnapshotPolicyActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val policy = sin.readString()
        val index = sin.readInt()

        return WaitForSnapshotPolicyAction(policy, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var policy: String? = null

        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                POLICY_FIELD -> policy = xcp.text()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in WaitForSnapshotPolicyAction.")
            }
        }

        return WaitForSnapshotPolicyAction(
            policy = requireNotNull(policy) { "WaitForSnapshotPolicyAction policy must be specified" },
            index = index
        )
    }

    override fun getActionType(): String {
        return WaitForSnapshotPolicyAction.name
    }
}
