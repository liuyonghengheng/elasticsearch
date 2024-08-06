/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import org.elasticsearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyRequest
import org.elasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.elasticsearch.indexmanagement.indexstatemanagement.util.TYPE_PARAM_KEY
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestChangePolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, CHANGE_POLICY_BASE_URI,
                POST, LEGACY_CHANGE_POLICY_BASE_URI
            ),
            ReplacedRoute(
                POST, "$CHANGE_POLICY_BASE_URI/{index}",
                POST, "$LEGACY_CHANGE_POLICY_BASE_URI/{index}"
            )
        )
    }

    override fun getName(): String = "change_policy_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing index")
        }

        val indexType = request.param(TYPE_PARAM_KEY, DEFAULT_INDEX_TYPE)

        val xcp = request.contentParser()
        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
        val changePolicy = ChangePolicy.parse(xcp)

        val changePolicyRequest = ChangePolicyRequest(indices.toList(), changePolicy, indexType)

        return RestChannelConsumer { channel ->
            client.execute(ChangePolicyAction.INSTANCE, changePolicyRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val CHANGE_POLICY_BASE_URI = "$ISM_BASE_URI/change_policy"
        const val LEGACY_CHANGE_POLICY_BASE_URI = "$LEGACY_ISM_BASE_URI/change_policy"
        const val INDEX_NOT_MANAGED = "This index is not being managed"
        const val INDEX_IN_TRANSITION = "Cannot change policy while transitioning to new state"
    }
}
