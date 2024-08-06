/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:JvmName("NotificationUtils")
package org.elasticsearch.indexmanagement.indexstatemanagement.util

import org.elasticsearch.client.Client
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.commons.destination.message.LegacyBaseMessage
import org.elasticsearch.commons.notifications.NotificationsPluginInterface
import org.elasticsearch.commons.notifications.action.LegacyPublishNotificationRequest
import org.elasticsearch.commons.notifications.action.LegacyPublishNotificationResponse
import org.elasticsearch.commons.notifications.model.EventSource
import org.elasticsearch.commons.notifications.model.SeverityType
import org.elasticsearch.indexmanagement.common.model.notification.Channel
import org.elasticsearch.indexmanagement.common.model.notification.validateResponseStatus
import org.elasticsearch.indexmanagement.opensearchapi.suspendUntil
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.rest.RestStatus

/**
 * Extension function for publishing a notification to a legacy destination.
 *
 * We now support the new channels from the Notification plugin. But, we still need to support
 * the old embedded legacy destinations that are directly on the policies in the error notifications
 * or notification actions. So we have a separate API in the NotificationsPluginInterface that allows
 * us to publish these old legacy ones directly.
 */
suspend fun LegacyBaseMessage.publishLegacyNotification(client: Client) {
    val baseMessage = this
    val res: LegacyPublishNotificationResponse = NotificationsPluginInterface.suspendUntil {
        this.publishLegacyNotification(
            (client as NodeClient),
            LegacyPublishNotificationRequest(baseMessage),
            it
        )
    }
    validateResponseStatus(RestStatus.fromCode(res.destinationResponse.statusCode), res.destinationResponse.responseContent)
}

/**
 * Extension function for publishing a notification to a channel in the Notification plugin. Builds the event source directly
 * from the managed index metadata.
 */
suspend fun Channel.sendNotification(
    client: Client,
    title: String,
    managedIndexMetaData: ManagedIndexMetaData,
    compiledMessage: String,
    user: User?
) {
    val eventSource = managedIndexMetaData.getEventSource(title)
    this.sendNotification(client, eventSource, compiledMessage, user)
}

fun ManagedIndexMetaData.getEventSource(title: String): EventSource {
    return EventSource(title, indexUuid, SeverityType.INFO)
}
