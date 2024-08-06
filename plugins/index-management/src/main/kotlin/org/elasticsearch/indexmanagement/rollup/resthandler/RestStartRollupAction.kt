/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.elasticsearch.indexmanagement.rollup.action.start.StartRollupAction
import org.elasticsearch.indexmanagement.rollup.action.start.StartRollupRequest
import org.elasticsearch.indexmanagement.util.NO_ID
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestStartRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, "$ROLLUP_JOBS_BASE_URI/{rollupID}/_start",
                POST, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}/_start"
            )
        )
    }

    override fun getName(): String {
        return "opendistro_start_rollup_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("rollupID", NO_ID)
        val startRequest = StartRollupRequest(id)
        return RestChannelConsumer { channel ->
            client.execute(StartRollupAction.INSTANCE, startRequest, RestToXContentListener(channel))
        }
    }
}
