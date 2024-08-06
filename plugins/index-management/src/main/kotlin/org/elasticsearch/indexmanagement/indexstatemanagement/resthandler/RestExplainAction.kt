/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.resthandler

import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.common.logging.DeprecationLogger
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainRequest
import org.elasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_EXPLAIN_VALIDATE_ACTION
import org.elasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_EXPLAIN_SHOW_POLICY
import org.elasticsearch.indexmanagement.indexstatemanagement.util.SHOW_VALIDATE_ACTION
import org.elasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.elasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_JOB_SORT_FIELD
import org.elasticsearch.indexmanagement.indexstatemanagement.util.SHOW_POLICY_QUERY_PARAM
import org.elasticsearch.indexmanagement.indexstatemanagement.util.TYPE_PARAM_KEY
import org.elasticsearch.indexmanagement.indexstatemanagement.util.parseClusterManagerTimeout
import org.elasticsearch.indexmanagement.util.getSearchParams
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.action.RestToXContentListener

private val log = LogManager.getLogger(RestExplainAction::class.java)

class RestExplainAction : BaseRestHandler() {

    companion object {
        // const val EXPLAIN_BASE_URI = "$ISM_BASE_URI/explain"
        const val EXPLAIN_BASE_URI = "/_ilm/explain"
        const val LEGACY_EXPLAIN_BASE_URI = "$LEGACY_ISM_BASE_URI/explain"
    }

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                GET, EXPLAIN_BASE_URI,
                GET, LEGACY_EXPLAIN_BASE_URI
            ),
            ReplacedRoute(
                GET, "$EXPLAIN_BASE_URI/{index}",
                GET, "$LEGACY_EXPLAIN_BASE_URI/{index}"
            )
        )
    }

    override fun getName(): String {
        return "ism_explain_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${request.path()}")

        val indices: Array<String> = Strings.splitStringByCommaToArray(request.param("index"))

        val searchParams = request.getSearchParams(DEFAULT_JOB_SORT_FIELD)

        val indexType = request.param(TYPE_PARAM_KEY, DEFAULT_INDEX_TYPE)

        val clusterManagerTimeout = parseClusterManagerTimeout(
            request, DeprecationLogger.getLogger(RestExplainAction::class.java), name
        )

        val explainRequest = ExplainRequest(
            indices.toList(),
            request.paramAsBoolean("local", false),
            clusterManagerTimeout,
            searchParams,
            request.paramAsBoolean(SHOW_POLICY_QUERY_PARAM, DEFAULT_EXPLAIN_SHOW_POLICY),
            request.paramAsBoolean(SHOW_VALIDATE_ACTION, DEFAULT_EXPLAIN_VALIDATE_ACTION),
            indexType
        )

        return RestChannelConsumer { channel ->
            client.execute(ExplainAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }
}
