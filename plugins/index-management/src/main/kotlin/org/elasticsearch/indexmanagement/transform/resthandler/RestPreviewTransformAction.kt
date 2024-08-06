/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.elasticsearch.indexmanagement.opensearchapi.parseWithType
import org.elasticsearch.indexmanagement.transform.action.preview.PreviewTransformAction
import org.elasticsearch.indexmanagement.transform.action.preview.PreviewTransformRequest
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener

class RestPreviewTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(POST, TRANSFORM_BASE_URI),
            RestHandler.Route(POST, "$TRANSFORM_BASE_URI/_preview")
        )
    }

    override fun getName(): String {
        return "opendistro_preview_transform_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val xcp = request.contentParser()
        val transform = xcp.parseWithType(parse = Transform.Companion::parse)
        val previewTransformRequest = PreviewTransformRequest(transform)
        return RestChannelConsumer { channel ->
            client.execute(PreviewTransformAction.INSTANCE, previewTransformRequest, RestToXContentListener(channel))
        }
    }
}
