/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.resthandler

import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.elasticsearch.indexmanagement.opensearchapi.parseWithType
import org.elasticsearch.indexmanagement.rollup.action.index.IndexRollupAction
import org.elasticsearch.indexmanagement.rollup.action.index.IndexRollupRequest
import org.elasticsearch.indexmanagement.rollup.action.index.IndexRollupResponse
import org.elasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.indexmanagement.util.IF_PRIMARY_TERM
import org.elasticsearch.indexmanagement.util.IF_SEQ_NO
import org.elasticsearch.indexmanagement.util.NO_ID
import org.elasticsearch.indexmanagement.util.REFRESH
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.PUT
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

class RestIndexRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                PUT, ROLLUP_JOBS_BASE_URI,
                PUT, LEGACY_ROLLUP_JOBS_BASE_URI
            ),
            ReplacedRoute(
                PUT, "$ROLLUP_JOBS_BASE_URI/{rollupID}",
                PUT, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}"
            )
        )
    }

    override fun getName(): String {
        return "opendistro_index_rollup_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("rollupID", NO_ID)
        if (NO_ID == id) {
            throw IllegalArgumentException("Missing rollup ID")
        }

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val xcp = request.contentParser()
        val rollup = xcp.parseWithType(id = id, seqNo = seqNo, primaryTerm = primaryTerm, parse = Rollup.Companion::parse)
            .copy(jobLastUpdatedTime = Instant.now())
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }
        val indexRollupRequest = IndexRollupRequest(rollup, refreshPolicy)
        return RestChannelConsumer { channel ->
            client.execute(IndexRollupAction.INSTANCE, indexRollupRequest, indexRollupResponse(channel))
        }
    }

    private fun indexRollupResponse(channel: RestChannel):
        RestResponseListener<IndexRollupResponse> {
        return object : RestResponseListener<IndexRollupResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexRollupResponse): RestResponse {
                val restResponse =
                    BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (response.status == RestStatus.CREATED) {
                    val location = "$ROLLUP_JOBS_BASE_URI/${response.id}"
                    restResponse.addHeader("Location", location)
                }
                return restResponse
            }
        }
    }
}
