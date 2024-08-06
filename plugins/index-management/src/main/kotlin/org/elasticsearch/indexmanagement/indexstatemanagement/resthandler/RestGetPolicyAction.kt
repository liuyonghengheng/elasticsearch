/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.resthandler

import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ES_POLICY_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_POLICY_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesRequest
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyRequest
import org.elasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_POLICY_SORT_FIELD
import org.elasticsearch.indexmanagement.util.getSearchParams
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.RestRequest.Method.HEAD
import org.elasticsearch.rest.action.RestActions
import org.elasticsearch.rest.action.RestToXContentListener
import org.elasticsearch.search.fetch.subphase.FetchSourceContext

private val log = LogManager.getLogger(RestGetPolicyAction::class.java)

class RestGetPolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                // GET, POLICY_BASE_URI,
                GET, ES_POLICY_BASE_URI,
                GET, LEGACY_POLICY_BASE_URI
            ),
            ReplacedRoute(
                GET, "$ES_POLICY_BASE_URI/{policyID}",
                GET, "$LEGACY_POLICY_BASE_URI/{policyID}"
            ),
            ReplacedRoute(
                HEAD, "$ES_POLICY_BASE_URI/{policyID}",
                HEAD, "$LEGACY_POLICY_BASE_URI/{policyID}"
            )
        )
    }

    override fun getName(): String {
        return "get_policy_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${request.path()}")

        val policyId = request.param("policyID")

        var fetchSrcContext: FetchSourceContext = FetchSourceContext.FETCH_SOURCE
        if (request.method() == HEAD) {
            fetchSrcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }

        val searchParams = request.getSearchParams(DEFAULT_POLICY_SORT_FIELD)

        return RestChannelConsumer { channel ->
            if (policyId == null || policyId.isEmpty()) {
                val getPoliciesRequest = GetPoliciesRequest(searchParams)
                client.execute(GetPoliciesAction.INSTANCE, getPoliciesRequest, RestToXContentListener(channel))
            } else {
                val getPolicyRequest = GetPolicyRequest(policyId, RestActions.parseVersion(request), fetchSrcContext)
                client.execute(GetPolicyAction.INSTANCE, getPolicyRequest, RestToXContentListener(channel))
            }
        }
    }
}
