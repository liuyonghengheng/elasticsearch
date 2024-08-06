/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.elasticsearch.indexmanagement.transform.action.delete.DeleteTransformsAction
import org.elasticsearch.indexmanagement.transform.action.delete.DeleteTransformsRequest
import org.elasticsearch.indexmanagement.transform.action.delete.DeleteTransformsRequest.Companion.DEFAULT_FORCE_DELETE
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.DELETE
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestDeleteTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            Route(DELETE, "$TRANSFORM_BASE_URI/{transformID}")
        )
    }

    override fun getName(): String = "opendistro_delete_transform_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val transformID = request.param("transformID")
        val force = request.paramAsBoolean("force", DEFAULT_FORCE_DELETE)
        return RestChannelConsumer { channel ->
            channel.newBuilder()
            val deleteTransformsRequest = DeleteTransformsRequest(transformID.split(","), force)
            client.execute(DeleteTransformsAction.INSTANCE, deleteTransformsRequest, RestToXContentListener(channel))
        }
    }
}
