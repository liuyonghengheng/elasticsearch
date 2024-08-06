/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler

import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.delete.DeleteSMPolicyRequest
import org.elasticsearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.elasticsearch.indexmanagement.util.REFRESH
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestToXContentListener

class RestDeleteSMPolicyHandler : BaseRestHandler() {

    override fun getName(): String {
        return "snapshot_management_delete_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(RestRequest.Method.DELETE, "$SM_POLICIES_URI/{policyName}")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        if (policyName == "") {
            throw IllegalArgumentException("Missing policy name")
        }

        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        return RestChannelConsumer {
            client.execute(
                SMActions.DELETE_SM_POLICY_ACTION_TYPE,
                DeleteSMPolicyRequest(smPolicyNameToDocId(policyName)).setRefreshPolicy(refreshPolicy),
                RestToXContentListener(it)
            )
        }
    }
}
