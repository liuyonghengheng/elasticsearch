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

import org.elasticsearch.replication.action.update.UpdateIndexReplicationAction
import org.elasticsearch.replication.action.update.UpdateIndexReplicationRequest
import org.elasticsearch.replication.task.index.IndexReplicationExecutor.Companion.log
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Requests
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.replication.rest.RestURL.INDEX_UPDATE_URL
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class UpdateIndexHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(RestHandler.Route(RestRequest.Method.PUT, INDEX_UPDATE_URL))
    }

    override fun getName(): String {
        return "plugins_index_update_replicate_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer? {
        val followIndex = request.param("index")
        log.info("Update Setting requested for $followIndex")
        val updateSettingsRequest = Requests.updateSettingsRequest(*Strings.splitStringByCommaToArray(request.param("index")))
        updateSettingsRequest.timeout(request.paramAsTime("timeout", updateSettingsRequest.timeout()))
        updateSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", updateSettingsRequest.masterNodeTimeout()))
        updateSettingsRequest.indicesOptions(IndicesOptions.fromRequest(request, updateSettingsRequest.indicesOptions()))
        updateSettingsRequest.fromXContent(request.contentParser())
        val updateIndexReplicationRequest = UpdateIndexReplicationRequest(followIndex, updateSettingsRequest.settings() )
        return RestChannelConsumer { channel: RestChannel? ->
            client.admin().cluster()
                    .execute(UpdateIndexReplicationAction.INSTANCE, updateIndexReplicationRequest, RestToXContentListener(channel))
        }
    }
}
