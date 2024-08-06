/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ES_POLICY_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyRequest
import org.elasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.elasticsearch.indexmanagement.indexstatemanagement.util.TYPE_PARAM_KEY
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestRemovePolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, ES_REMOVE_POLICY_BASE_URI,
                POST, LEGACY_REMOVE_POLICY_BASE_URI
            ),
            ReplacedRoute(
                POST, "$ES_REMOVE_POLICY_BASE_URI/{index}",
                POST, "$LEGACY_REMOVE_POLICY_BASE_URI/{index}"
            )
        )
    }

    override fun getName(): String = "remove_policy_action"

    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String> = Strings.splitStringByCommaToArray(request.param("index"))

        if (indices.isNullOrEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val indexType = request.param(TYPE_PARAM_KEY, DEFAULT_INDEX_TYPE)

        val removePolicyRequest = RemovePolicyRequest(indices.toList(), indexType)

        return RestChannelConsumer { channel ->
            client.execute(RemovePolicyAction.INSTANCE, removePolicyRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val ES_REMOVE_POLICY_BASE_URI = "$ES_POLICY_BASE_URI/remove"
        // const val REMOVE_POLICY_BASE_URI = "$ISM_BASE_URI/remove"
        const val LEGACY_REMOVE_POLICY_BASE_URI = "$LEGACY_ISM_BASE_URI/remove"


    }
}
