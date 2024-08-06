/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler

import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.explain.ExplainSMPolicyRequest
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.action.RestToXContentListener

class RestExplainSMPolicyHandler : BaseRestHandler() {

    private val log = LogManager.getLogger(RestExplainSMPolicyHandler::class.java)

    override fun getName(): String {
        return "snapshot_management_explain_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, "$SM_POLICIES_URI/{policyName}/_explain")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        var policyNames: Array<String> = Strings.splitStringByCommaToArray(request.param("policyName", ""))
        if (policyNames.isEmpty()) policyNames = arrayOf("*")
        log.debug("Explain snapshot management policy request received with policy name(s) [$policyNames]")

        return RestChannelConsumer {
            client.execute(
                SMActions.EXPLAIN_SM_POLICY_ACTION_TYPE,
                ExplainSMPolicyRequest(policyNames),
                RestToXContentListener(it)
            )
        }
    }
}
