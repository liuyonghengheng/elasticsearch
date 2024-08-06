/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.resthandler

import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.elasticsearch.indexmanagement.opensearchapi.parseWithType
import org.elasticsearch.indexmanagement.transform.action.index.IndexTransformAction
import org.elasticsearch.indexmanagement.transform.action.index.IndexTransformRequest
import org.elasticsearch.indexmanagement.transform.action.index.IndexTransformResponse
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.indexmanagement.util.IF_PRIMARY_TERM
import org.elasticsearch.indexmanagement.util.IF_SEQ_NO
import org.elasticsearch.indexmanagement.util.NO_ID
import org.elasticsearch.indexmanagement.util.REFRESH
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.PUT
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

class RestIndexTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(PUT, TRANSFORM_BASE_URI),
            RestHandler.Route(PUT, "$TRANSFORM_BASE_URI/{transformID}")
        )
    }

    override fun getName(): String {
        return "opendistro_index_transform_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("transformID", NO_ID)
        if (NO_ID == id) {
            throw IllegalArgumentException("Missing transform ID")
        }

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val xcp = request.contentParser()
        val transform = xcp.parseWithType(id = id, seqNo = seqNo, primaryTerm = primaryTerm, parse = Transform.Companion::parse)
            .copy(updatedAt = Instant.now())
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }
        val indexTransformRequest = IndexTransformRequest(transform, refreshPolicy)
        return RestChannelConsumer { channel ->
            client.execute(IndexTransformAction.INSTANCE, indexTransformRequest, indexTransformResponse(channel))
        }
    }

    private fun indexTransformResponse(channel: RestChannel):
        RestResponseListener<IndexTransformResponse> {
        return object : RestResponseListener<IndexTransformResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexTransformResponse): RestResponse {
                val restResponse =
                    BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (response.status == RestStatus.CREATED) {
                    val location = "$TRANSFORM_BASE_URI/${response.id}"
                    restResponse.addHeader("Location", location)
                }
                return restResponse
            }
        }
    }
}
