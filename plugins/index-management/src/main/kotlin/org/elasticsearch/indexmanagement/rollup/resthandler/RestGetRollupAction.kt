/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.elasticsearch.indexmanagement.rollup.action.get.GetRollupAction
import org.elasticsearch.indexmanagement.rollup.action.get.GetRollupRequest
import org.elasticsearch.indexmanagement.rollup.action.get.GetRollupsAction
import org.elasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest
import org.elasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_FROM
import org.elasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SEARCH_STRING
import org.elasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SIZE
import org.elasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SORT_DIRECTION
import org.elasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SORT_FIELD
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.RestRequest.Method.HEAD
import org.elasticsearch.rest.action.RestToXContentListener
import org.elasticsearch.search.fetch.subphase.FetchSourceContext

class RestGetRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                GET, ROLLUP_JOBS_BASE_URI,
                GET, LEGACY_ROLLUP_JOBS_BASE_URI
            ),
            ReplacedRoute(
                GET, "$ROLLUP_JOBS_BASE_URI/{rollupID}",
                GET, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}"
            ),
            ReplacedRoute(
                HEAD, "$ROLLUP_JOBS_BASE_URI/{rollupID}",
                HEAD, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}"
            )
        )
    }

    override fun getName(): String {
        return "opendistro_get_rollup_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupID = request.param("rollupID")
        val searchString = request.param("search", DEFAULT_SEARCH_STRING)
        val from = request.paramAsInt("from", DEFAULT_FROM)
        val size = request.paramAsInt("size", DEFAULT_SIZE)
        val sortField = request.param("sortField", DEFAULT_SORT_FIELD)
        val sortDirection = request.param("sortDirection", DEFAULT_SORT_DIRECTION)
        return RestChannelConsumer { channel ->
            if (rollupID == null || rollupID.isEmpty()) {
                val req = GetRollupsRequest(
                    searchString,
                    from,
                    size,
                    sortField,
                    sortDirection
                )
                client.execute(GetRollupsAction.INSTANCE, req, RestToXContentListener(channel))
            } else {
                val req = GetRollupRequest(rollupID, if (request.method() == HEAD) FetchSourceContext.DO_NOT_FETCH_SOURCE else null)
                client.execute(GetRollupAction.INSTANCE, req, RestToXContentListener(channel))
            }
        }
    }
}
