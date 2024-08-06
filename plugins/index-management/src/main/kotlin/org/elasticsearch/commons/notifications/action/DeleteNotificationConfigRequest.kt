/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.elasticsearch.commons.notifications.action

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.commons.notifications.NotificationConstants.CONFIG_ID_LIST_TAG
import org.elasticsearch.commons.utils.logger
import org.elasticsearch.commons.utils.stringList
import java.io.IOException

/**
 * Action Response for creating new configuration.
 */
class DeleteNotificationConfigRequest : ActionRequest, ToXContentObject {
    val configIds: Set<String>

    companion object {
        private val log by logger(DeleteNotificationConfigRequest::class.java)

        /**
         * reader to create instance of class from writable.
         */
        val reader = Writeable.Reader { DeleteNotificationConfigRequest(it) }

        /**
         * Creator used in REST communication.
         * @param parser XContentParser to deserialize data from.
         */
        @JvmStatic
        @Throws(IOException::class)
        fun parse(parser: XContentParser): DeleteNotificationConfigRequest {
            var configIds: Set<String>? = null

            XContentParserUtils.ensureExpectedToken(
                XContentParser.Token.START_OBJECT,
                parser.currentToken(),
                parser
            )
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()
                when (fieldName) {
                    CONFIG_ID_LIST_TAG -> configIds = parser.stringList().toSet()
                    else -> {
                        parser.skipChildren()
                        log.info("Unexpected field: $fieldName, while parsing DeleteNotificationConfigRequest")
                    }
                }
            }
            configIds ?: throw IllegalArgumentException("$CONFIG_ID_LIST_TAG field absent")
            return DeleteNotificationConfigRequest(configIds)
        }
    }

    /**
     * constructor for creating the class
     * @param configIds the id of the notification configuration
     */
    constructor(configIds: Set<String>) {
        this.configIds = configIds
    }

    /**
     * {@inheritDoc}
     */
    @Throws(IOException::class)
    constructor(input: StreamInput) : super(input) {
        configIds = input.readStringList().toSet()
    }

    /**
     * {@inheritDoc}
     */
    @Throws(IOException::class)
    override fun writeTo(output: StreamOutput) {
        super.writeTo(output)
        output.writeStringCollection(configIds)
    }

    /**
     * {@inheritDoc}
     */
    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!
        return builder.startObject()
            .field(CONFIG_ID_LIST_TAG, configIds)
            .endObject()
    }

    /**
     * {@inheritDoc}
     */
    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (configIds.isNullOrEmpty()) {
            validationException = ValidateActions.addValidationError("configIds is null or empty", validationException)
        }
        return validationException
    }
}
