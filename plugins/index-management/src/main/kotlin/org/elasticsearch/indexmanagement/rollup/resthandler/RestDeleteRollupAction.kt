/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.resthandler

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.elasticsearch.indexmanagement.rollup.action.delete.DeleteRollupAction
import org.elasticsearch.indexmanagement.rollup.action.delete.DeleteRollupRequest
import org.elasticsearch.indexmanagement.util.REFRESH
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.DELETE
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestDeleteRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                DELETE, "$ROLLUP_JOBS_BASE_URI/{rollupID}",
                DELETE, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}"
            )
        )
    }

    override fun getName(): String = "opendistro_delete_rollup_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupID = request.param("rollupID")
        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))
        return RestChannelConsumer { channel ->
            channel.newBuilder()
            val deleteRollupRequest = DeleteRollupRequest(rollupID)
                .setRefreshPolicy(refreshPolicy)
            client.execute(DeleteRollupAction.INSTANCE, deleteRollupRequest, RestToXContentListener(channel))
        }
    }
}
