/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICY_ACTION_TYPE
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICIES_ACTION_TYPE
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPoliciesRequest
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPolicyRequest
import org.elasticsearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.elasticsearch.indexmanagement.snapshotmanagement.util.DEFAULT_SM_POLICY_SORT_FIELD
import org.elasticsearch.indexmanagement.util.getSearchParams
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.action.RestToXContentListener

class RestGetSMPolicyHandler : BaseRestHandler() {

    override fun getName(): String {
        return "snapshot_management_get_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, "$SM_POLICIES_URI/{policyName}"),
            Route(GET, "$SM_POLICIES_URI/")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        return if (policyName.isEmpty()) {
            getAllPolicies(request, client)
        } else {
            getSMPolicyByName(client, policyName)
        }
    }

    private fun getSMPolicyByName(client: NodeClient, policyName: String): RestChannelConsumer {
        return RestChannelConsumer {
            client.execute(GET_SM_POLICY_ACTION_TYPE, GetSMPolicyRequest(smPolicyNameToDocId(policyName)), RestToXContentListener(it))
        }
    }

    private fun getAllPolicies(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val searchParams = request.getSearchParams(DEFAULT_SM_POLICY_SORT_FIELD)

        return RestChannelConsumer {
            client.execute(GET_SM_POLICIES_ACTION_TYPE, GetSMPoliciesRequest(searchParams), RestToXContentListener(it))
        }
    }
}
