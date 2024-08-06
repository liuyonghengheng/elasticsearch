/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.resthandler

import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.common.logging.DeprecationLogger
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexRequest
import org.elasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.elasticsearch.indexmanagement.indexstatemanagement.util.TYPE_PARAM_KEY
import org.elasticsearch.indexmanagement.indexstatemanagement.util.parseClusterManagerTimeout
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.ReplacedRoute
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener

class RestRetryFailedManagedIndexAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, RETRY_BASE_URI,
                POST, LEGACY_RETRY_BASE_URI
            ),
            ReplacedRoute(
                POST, "$RETRY_BASE_URI/{index}",
                POST, "$LEGACY_RETRY_BASE_URI/{index}"
            )
        )
    }

    override fun getName(): String {
        return "retry_failed_managed_index"
    }

    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }
        val body = if (request.hasContent()) {
            XContentHelper.convertToMap(request.requiredContent(), false, request.xContentType).v2()
        } else {
            mapOf()
        }

        val indexType = request.param(TYPE_PARAM_KEY, DEFAULT_INDEX_TYPE)

        val clusterManagerTimeout = parseClusterManagerTimeout(
            request, DeprecationLogger.getLogger(RestRetryFailedManagedIndexAction::class.java), name
        )

        val retryFailedRequest = RetryFailedManagedIndexRequest(
            indices.toList(), body["state"] as String?,
            clusterManagerTimeout,
            indexType
        )

        return RestChannelConsumer { channel ->
            client.execute(RetryFailedManagedIndexAction.INSTANCE, retryFailedRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val RETRY_BASE_URI = "$ISM_BASE_URI/retry"
        const val LEGACY_RETRY_BASE_URI = "$LEGACY_ISM_BASE_URI/retry"
    }
}
