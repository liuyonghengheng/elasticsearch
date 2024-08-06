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

import org.elasticsearch.replication.action.stop.StopIndexReplicationAction
import org.elasticsearch.replication.action.stop.StopIndexReplicationRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.replication.rest.RestURL.STOP_URL
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class StopIndexReplicationHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.POST, STOP_URL))
    }

    override fun getName(): String {
        return "plugins_index_stop_replicate_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        request.contentOrSourceParamParser().use { parser ->
            val followIndex = request.param("index")
            val stopReplicationRequest = StopIndexReplicationRequest.fromXContent(parser, followIndex)
            return RestChannelConsumer { channel: RestChannel? ->
                client.admin().cluster()
                        .execute(StopIndexReplicationAction.INSTANCE, stopReplicationRequest, RestToXContentListener(channel))
            }
        }
    }
}
