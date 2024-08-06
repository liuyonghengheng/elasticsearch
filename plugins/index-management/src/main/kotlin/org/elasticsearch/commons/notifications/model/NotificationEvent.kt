/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.elasticsearch.commons.notifications.model

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.commons.notifications.NotificationConstants.EVENT_SOURCE_TAG
import org.elasticsearch.commons.notifications.NotificationConstants.STATUS_LIST_TAG
import org.elasticsearch.commons.utils.logger
import org.elasticsearch.commons.utils.objectList
import java.io.IOException

/**
 * Data class representing Notification event.
 */
data class NotificationEvent(
    val eventSource: EventSource,
    val statusList: List<EventStatus> = listOf()
) : BaseModel {

    init {
        require(statusList.isNotEmpty()) { "statusList is null or empty" }
    }

    companion object {
        private val log by logger(NotificationEvent::class.java)

        /**
         * reader to create instance of class from writable.
         */
        val reader = Writeable.Reader { NotificationEvent(it) }

        /**
         * Creator used in REST communication.
         * @param parser XContentParser to deserialize data from.
         */
        @JvmStatic
        @Throws(IOException::class)
        fun parse(parser: XContentParser): NotificationEvent {
            var eventSource: EventSource? = null
            var statusList: List<EventStatus> = listOf()

            XContentParserUtils.ensureExpectedToken(
                XContentParser.Token.START_OBJECT,
                parser.currentToken(),
                parser
            )
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()
                when (fieldName) {
                    EVENT_SOURCE_TAG -> eventSource = EventSource.parse(parser)
                    STATUS_LIST_TAG -> statusList = parser.objectList { EventStatus.parse(it) }
                    else -> {
                        parser.skipChildren()
                        log.info("Unexpected field: $fieldName, while parsing notification event")
                    }
                }
            }
            eventSource ?: throw IllegalArgumentException("$EVENT_SOURCE_TAG field absent")
            if (statusList.isEmpty()) {
                throw IllegalArgumentException("$STATUS_LIST_TAG field absent or empty")
            }
            return NotificationEvent(
                eventSource,
                statusList
            )
        }
    }

    /**
     * Constructor used in transport action communication.
     * @param input StreamInput stream to deserialize data from.
     */
    constructor(input: StreamInput) : this(
        eventSource = EventSource.reader.read(input),
        statusList = input.readList(EventStatus.reader)
    )

    /**
     * {@inheritDoc}
     */
    override fun writeTo(output: StreamOutput) {
        eventSource.writeTo(output)
        output.writeList(statusList)
    }

    /**
     * {@inheritDoc}
     */
    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!
        return builder.startObject()
            .field(EVENT_SOURCE_TAG, eventSource)
            .field(STATUS_LIST_TAG, statusList)
            .endObject()
    }

    // Overriding toString so consuming plugins can log/output this from the  sendNotification response if needed
    override fun toString(): String {
        return try {
            XContentHelper.toXContent(this, XContentType.JSON, EMPTY_PARAMS, true).utf8ToString()
        } catch (e: IOException) {
            log.debug("Failed to convert NotificationEvent to string", e)
            super.toString() + " threw " + e.toString()
        }
    }
}
