/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.elasticsearch.indexmanagement.transform.action.explain.ExplainTransformAction
import org.elasticsearch.indexmanagement.transform.action.explain.ExplainTransformRequest
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.action.RestToXContentListener

class RestExplainTransformAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(Route(GET, "$TRANSFORM_BASE_URI/{transformID}/_explain"))
    }

    override fun getName(): String = "opendistro_explain_transform_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val transformIDs: List<String> = Strings.splitStringByCommaToArray(request.param("transformID")).toList()
        if (transformIDs.isEmpty()) {
            throw IllegalArgumentException("Missing transformID")
        }
        val explainRequest = ExplainTransformRequest(transformIDs)
        return RestChannelConsumer { channel ->
            client.execute(ExplainTransformAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }
}
