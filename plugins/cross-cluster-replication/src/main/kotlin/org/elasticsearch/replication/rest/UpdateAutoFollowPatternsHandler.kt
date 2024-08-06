/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.elasticsearch.replication.rest

import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.replication.action.autofollow.UpdateAutoFollowPatternAction
import org.elasticsearch.replication.action.autofollow.UpdateAutoFollowPatternRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.replication.rest.RestURL.DELETE_URL
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestToXContentListener

class UpdateAutoFollowPatternsHandler : BaseRestHandler() {

    companion object {
        const val PATH = DELETE_URL
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.POST, PATH),
            RestHandler.Route(RestRequest.Method.DELETE, PATH))
    }

    override fun getName() = "plugins_replication_autofollow_update"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val action = when {
            request.method() == RestRequest.Method.POST -> UpdateAutoFollowPatternRequest.Action.ADD
            request.method() == RestRequest.Method.DELETE -> UpdateAutoFollowPatternRequest.Action.REMOVE
            // Should not be reached unless someone updates the restController with a new method but forgets to add it here.
            else ->
                throw ElasticsearchStatusException("Unsupported method ", RestStatus.METHOD_NOT_ALLOWED, request.method())
        }

        val updateRequest = UpdateAutoFollowPatternRequest.fromXContent(request.contentParser(), action)
        return RestChannelConsumer { channel ->
            client.admin().cluster()
                .execute(UpdateAutoFollowPatternAction.INSTANCE, updateRequest, RestToXContentListener(channel))
        }
    }
}
