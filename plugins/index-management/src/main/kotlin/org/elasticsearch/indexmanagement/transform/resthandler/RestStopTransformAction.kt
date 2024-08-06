/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.elasticsearch.indexmanagement.transform.action.stop.StopTransformAction
import org.elasticsearch.indexmanagement.transform.action.stop.StopTransformRequest
import org.elasticsearch.indexmanagement.util.NO_ID
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestStopTransformAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, "$TRANSFORM_BASE_URI/{transformID}/_stop")
        )
    }

    override fun getName(): String {
        return "opendistro_stop_transform_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("transformID", NO_ID)
        if (NO_ID == id) {
            throw IllegalArgumentException("Missing transform ID")
        }

        val stopRequest = StopTransformRequest(id)
        return RestChannelConsumer { channel ->
            client.execute(StopTransformAction.INSTANCE, stopRequest, RestToXContentListener(channel))
        }
    }
}
