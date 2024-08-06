/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.elasticsearch.indexmanagement.transform.action.get.GetTransformAction
import org.elasticsearch.indexmanagement.transform.action.get.GetTransformRequest
import org.elasticsearch.indexmanagement.transform.action.get.GetTransformsAction
import org.elasticsearch.indexmanagement.transform.action.get.GetTransformsRequest
import org.elasticsearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_FROM
import org.elasticsearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SEARCH_STRING
import org.elasticsearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SIZE
import org.elasticsearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SORT_DIRECTION
import org.elasticsearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SORT_FIELD
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.RestRequest.Method.HEAD
import org.elasticsearch.rest.action.RestToXContentListener
import org.elasticsearch.search.fetch.subphase.FetchSourceContext

class RestGetTransformAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, TRANSFORM_BASE_URI),
            Route(GET, "$TRANSFORM_BASE_URI/{transformID}"),
            Route(HEAD, "$TRANSFORM_BASE_URI/{transformID}")
        )
    }

    override fun getName(): String {
        return "opendistro_get_transform_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val transformID = request.param("transformID")
        val searchString = request.param("search", DEFAULT_SEARCH_STRING)
        val from = request.paramAsInt("from", DEFAULT_FROM)
        val size = request.paramAsInt("size", DEFAULT_SIZE)
        val sortField = request.param("sortField", DEFAULT_SORT_FIELD)
        val sortDirection = request.param("sortDirection", DEFAULT_SORT_DIRECTION)
        return RestChannelConsumer { channel ->
            if (transformID == null || transformID.isEmpty()) {
                val req = GetTransformsRequest(
                    searchString,
                    from,
                    size,
                    sortField,
                    sortDirection
                )
                client.execute(GetTransformsAction.INSTANCE, req, RestToXContentListener(channel))
            } else {
                val req = GetTransformRequest(transformID, if (request.method() == HEAD) FetchSourceContext.DO_NOT_FETCH_SOURCE else null)
                client.execute(GetTransformAction.INSTANCE, req, RestToXContentListener(channel))
            }
        }
    }
}
