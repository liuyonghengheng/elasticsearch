/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.resthandler

import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ES_POLICY_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_POLICY_BASE_URI
import org.elasticsearch.indexmanagement.indexstatemanagement.Response
import org.elasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.elasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.ALLOW_LIST
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyRequest
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyResponse
import org.elasticsearch.indexmanagement.opensearchapi.parseWithType
import org.elasticsearch.indexmanagement.util.IF_PRIMARY_TERM
import org.elasticsearch.indexmanagement.util.IF_SEQ_NO
import org.elasticsearch.indexmanagement.util.NO_ID
import org.elasticsearch.indexmanagement.util.REFRESH
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.PUT
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

class RestIndexPolicyAction(
    settings: Settings,
    val clusterService: ClusterService
) : BaseRestHandler() {

    @Volatile private var allowList = ALLOW_LIST.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
    }

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                // PUT, POLICY_BASE_URI,
                PUT, ES_POLICY_BASE_URI,
                PUT, LEGACY_POLICY_BASE_URI
            ),
            ReplacedRoute(
                // PUT, "$POLICY_BASE_URI/{policyID}",
                PUT, "$ES_POLICY_BASE_URI/{policyID}",
                PUT, "$LEGACY_POLICY_BASE_URI/{policyID}"
            )
        )
    }

    override fun getName(): String {
        return "index_policy_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("policyID", NO_ID)
        if (NO_ID == id) {
            throw IllegalArgumentException("Missing policy ID")
        }

        val xcp = request.contentParser()
        val policy = xcp.parseWithType(id = id, parse = Policy.Companion::parse2).copy(lastUpdatedTime = Instant.now())
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)

        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        val disallowedActions = policy.getDisallowedActions(allowList)
        if (disallowedActions.isNotEmpty()) {
            return RestChannelConsumer { channel ->
                channel.sendResponse(
                    BytesRestResponse(
                        RestStatus.FORBIDDEN,
                        "You have actions that are not allowed in your policy $disallowedActions"
                    )
                )
            }
        }

        val indexPolicyRequest = IndexPolicyRequest(id, policy, seqNo, primaryTerm, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(
                IndexPolicyAction.INSTANCE, indexPolicyRequest,
                object : RestResponseListener<IndexPolicyResponse>(channel) {
                    override fun buildResponse(response: IndexPolicyResponse): RestResponse {

                        if (response.status == RestStatus.CREATED) {
                            val res = Response(true)
                            return BytesRestResponse(
                                RestStatus.OK,
                                res.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS)
                            )
                        }
                        return BytesRestResponse(
                            response.status,
                            response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS)
                        )

                    }
                }
            )
        }
    }
}
