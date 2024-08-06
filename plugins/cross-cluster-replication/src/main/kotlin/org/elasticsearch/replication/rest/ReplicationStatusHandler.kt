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


import org.elasticsearch.replication.action.status.ReplicationStatusAction
import org.elasticsearch.replication.action.status.ShardInfoRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.replication.rest.RestURL.STATUS_URL
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class ReplicationStatusHandler : BaseRestHandler() {

    companion object {
        private val log = LogManager.getLogger(ReplicationStatusHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.GET, STATUS_URL))
    }

    override fun getName(): String {
        return "plugins_replication_status"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val index = request.param("index", "")
        var isVerbose = (request.paramAsBoolean("verbose", false))
        val indexReplicationStatusRequest = ShardInfoRequest(index,isVerbose)
        return RestChannelConsumer {
            channel ->
            client.execute(ReplicationStatusAction.INSTANCE, indexReplicationStatusRequest, RestToXContentListener(channel))
        }
    }
}
