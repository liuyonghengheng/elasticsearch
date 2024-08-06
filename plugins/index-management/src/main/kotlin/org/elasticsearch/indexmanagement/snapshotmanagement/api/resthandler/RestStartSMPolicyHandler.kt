/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler

import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.start.StartSMRequest
import org.elasticsearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.elasticsearch.indexmanagement.snapshotmanagement.util.getValidSMPolicyName
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestToXContentListener

class RestStartSMPolicyHandler : BaseRestHandler() {

    private val log = LogManager.getLogger(RestStartSMPolicyHandler::class.java)

    override fun getName(): String {
        return "snapshot_management_start_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(RestRequest.Method.POST, "$SM_POLICIES_URI/{policyName}/_start")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.getValidSMPolicyName()
        log.debug("Start snapshot management policy request received with policy name [$policyName]")

        val indexReq = StartSMRequest(smPolicyNameToDocId(policyName))
        return RestChannelConsumer {
            client.execute(
                SMActions.START_SM_POLICY_ACTION_TYPE,
                indexReq, RestToXContentListener(it)
            )
        }
    }
}
