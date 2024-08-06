/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.common.model.notification

import org.elasticsearch.client.Client
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.commons.notifications.NotificationsPluginInterface
import org.elasticsearch.commons.notifications.action.SendNotificationResponse
import org.elasticsearch.commons.notifications.model.ChannelMessage
import org.elasticsearch.commons.notifications.model.EventSource
import org.elasticsearch.indexmanagement.opensearchapi.suspendUntil
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.generateUserString
import java.io.IOException

data class Channel(val id: String) : ToXContent, Writeable {

    init {
        require(id.isNotEmpty()) { "Channel ID cannot be empty" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(ID, id)
            .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
    }

    companion object {
        const val ID = "id"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Channel {
            var id: String? = null

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    ID -> id = xcp.text()
                    else -> {
                        error("Unexpected field: $fieldName, while parsing Channel destination")
                    }
                }
            }

            return Channel(requireNotNull(id) { "Channel ID is null" })
        }
    }

    /**
     * Extension function for publishing a notification to a channel in the Notification plugin.
     */
    suspend fun sendNotification(
        client: Client,
        eventSource: EventSource,
        message: String,
        user: User?
    ) {
        val channel = this
        client.threadPool().threadContext.stashContext().use {
            // We need to set the user context information in the thread context for notification plugin to correctly resolve the user object
            client.threadPool().threadContext.putTransient(ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT, generateUserString(user))
            val res: SendNotificationResponse = NotificationsPluginInterface.suspendUntil {
                this.sendNotification(
                    (client as NodeClient),
                    eventSource,
                    ChannelMessage(message, null, null),
                    listOf(channel.id),
                    it
                )
            }
            validateResponseStatus(res.getStatus(), res.notificationEvent.eventSource.referenceId)
        }
    }
}
