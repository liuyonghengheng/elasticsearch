package org.elasticsearch.replication.rest

import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.replication.action.stats.FollowerStatsAction
import org.elasticsearch.replication.action.stats.FollowerStatsRequest
import org.elasticsearch.replication.action.stats.FollowerStatsResponse
import org.elasticsearch.replication.rest.RestURL.FOLLOWER_STATS_URL
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener
import java.io.IOException

class FollowerStatsHandler : BaseRestHandler() {
    companion object {
        private val log = LogManager.getLogger(FollowerStatsHandler::class.java)
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.GET, FOLLOWER_STATS_URL))
    }

    override fun getName(): String {
        return "plugins_follower_replication_stats"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val statsRequest = FollowerStatsRequest()
        return RestChannelConsumer { channel: RestChannel? ->
            client.admin().cluster()
                    .execute(FollowerStatsAction.INSTANCE, statsRequest, object : RestResponseListener<FollowerStatsResponse>(channel) {
                        @Throws(Exception::class)
                        override fun buildResponse(nodesStatsResponse: FollowerStatsResponse): RestResponse? {
                            val builder: XContentBuilder = XContentFactory.jsonBuilder().prettyPrint()
                            return BytesRestResponse(RestStatus.OK, nodesStatsResponse.toXContent(builder, ToXContent.EMPTY_PARAMS))
                        }
                    })
        }
    }
}
