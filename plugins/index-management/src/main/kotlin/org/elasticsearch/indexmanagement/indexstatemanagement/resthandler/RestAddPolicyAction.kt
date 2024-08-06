/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.indexmanagement.IndexManagementPlugin
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ES_POLICY_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyRequest
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

class RestAddPolicyAction : BaseRestHandler() {

    override fun getName(): String = "add_policy_action"

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, ADD_POLICY_BASE_URI,
                POST, LEGACY_ADD_POLICY_BASE_URI
            ),
            ReplacedRoute(
                POST, "$ADD_POLICY_BASE_URI/{index}",
                POST, "$LEGACY_ADD_POLICY_BASE_URI/{index}"
            )
        )
    }

    @Throws(IOException::class)
    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))

        if (indices.isNullOrEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val body = if (request.hasContent()) {
            XContentHelper.convertToMap(request.requiredContent(), false, request.xContentType).v2()
        } else {
            mapOf()
        }

        val indexType = request.param(TYPE_PARAM_KEY, DEFAULT_INDEX_TYPE)

        val policyID = requireNotNull(body.getOrDefault("policy_id", null)) { "Missing policy_id" }

        val addPolicyRequest = AddPolicyRequest(indices.toList(), policyID as String, indexType)

        return RestChannelConsumer { channel ->
            client.execute(AddPolicyAction.INSTANCE, addPolicyRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        // const val ADD_POLICY_BASE_URI = "$ISM_BASE_URI/add"
        const val ADD_POLICY_BASE_URI = "${ES_POLICY_BASE_URI}/add"
        const val LEGACY_ADD_POLICY_BASE_URI = "$LEGACY_ISM_BASE_URI/add"
    }
}
