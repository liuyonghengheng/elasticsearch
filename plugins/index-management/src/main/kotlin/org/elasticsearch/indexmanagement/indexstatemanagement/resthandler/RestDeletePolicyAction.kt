/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.resthandler

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ES_POLICY_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_POLICY_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyRequest
import org.elasticsearch.indexmanagement.util.REFRESH
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.DELETE
import org.elasticsearch.rest.action.RestStatusToXContentListener
import java.io.IOException

class RestDeletePolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                // DELETE, "$POLICY_BASE_URI/{policyID}",
                DELETE, "$ES_POLICY_BASE_URI/{policyID}",
                DELETE, "$LEGACY_POLICY_BASE_URI/{policyID}"
            )
        )
    }

    override fun getName(): String = "delete_policy_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyId = request.param("policyID")
        if (policyId == null || policyId.isEmpty()) {
            throw IllegalArgumentException("Missing policy ID")
        }

        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))
        val deletePolicyRequest = DeletePolicyRequest(policyId, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(DeletePolicyAction.INSTANCE, deletePolicyRequest, RestStatusToXContentListener(channel))
        }
    }
}
