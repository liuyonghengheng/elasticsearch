/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.elasticsearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import org.elasticsearch.indexmanagement.rollup.action.explain.ExplainRollupRequest
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.action.RestToXContentListener

class RestExplainRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                GET, "$ROLLUP_JOBS_BASE_URI/{rollupID}/_explain",
                GET, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}/_explain"
            )
        )
    }

    override fun getName(): String = "opendistro_explain_rollup_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupIDs: List<String> = Strings.splitStringByCommaToArray(request.param("rollupID")).toList()
        if (rollupIDs.isEmpty()) {
            throw IllegalArgumentException("Missing rollupID")
        }
        val explainRequest = ExplainRollupRequest(rollupIDs)
        return RestChannelConsumer { channel ->
            client.execute(ExplainRollupAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }
}
