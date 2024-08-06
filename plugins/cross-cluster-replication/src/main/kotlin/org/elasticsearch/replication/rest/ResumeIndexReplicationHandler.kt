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

import org.elasticsearch.replication.action.resume.ResumeIndexReplicationAction
import org.elasticsearch.replication.action.resume.ResumeIndexReplicationRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.replication.rest.RestURL.RESUME_URL
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class ResumeIndexReplicationHandler : BaseRestHandler() {

    companion object {
        private val log = LogManager.getLogger(ResumeIndexReplicationHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.POST, RESUME_URL))
    }

    override fun getName(): String {
        return "plugins_index_resume_replicate_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        request.contentOrSourceParamParser().use { parser ->
            val followIndex = request.param("index")
            val resumeReplicationRequest = ResumeIndexReplicationRequest.fromXContent(parser, followIndex)
            return RestChannelConsumer { channel: RestChannel? ->
                client.admin().cluster()
                        .execute(ResumeIndexReplicationAction.INSTANCE, resumeReplicationRequest, RestToXContentListener(channel))
            }
        }
    }
}
