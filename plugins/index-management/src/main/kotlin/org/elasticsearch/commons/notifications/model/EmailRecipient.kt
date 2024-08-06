/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.elasticsearch.commons.notifications.model

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.commons.notifications.NotificationConstants.RECIPIENT_TAG
import org.elasticsearch.commons.utils.logger
import org.elasticsearch.commons.utils.validateEmail
import java.io.IOException

/**
 * Data class representing Email recipient.
 */
data class EmailRecipient(
    val recipient: String
) : BaseConfigData {

    init {
        validateEmail(recipient)
    }

    companion object {
        private val log by logger(EmailRecipient::class.java)

        /**
         * reader to create instance of class from writable.
         */
        val reader = Writeable.Reader { EmailRecipient(it) }

        /**
         * Parser to parse xContent
         */
        val xParser = XParser { parse(it) }

        /**
         * Creator used in REST communication.
         * @param parser XContentParser to deserialize data from.
         */
        @JvmStatic
        @Throws(IOException::class)
        fun parse(parser: XContentParser): EmailRecipient {
            var recipient: String? = null

            XContentParserUtils.ensureExpectedToken(
                XContentParser.Token.START_OBJECT,
                parser.currentToken(),
                parser
            )
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()
                when (fieldName) {
                    RECIPIENT_TAG -> recipient = parser.text()
                    else -> {
                        parser.skipChildren()
                        log.info("Unexpected field: $fieldName, while parsing EmailRecipient")
                    }
                }
            }
            recipient ?: throw IllegalArgumentException("$RECIPIENT_TAG field absent")
            return EmailRecipient(recipient)
        }
    }

    /**
     * Constructor used in transport action communication.
     * @param input StreamInput stream to deserialize data from.
     */
    constructor(input: StreamInput) : this(
        recipient = input.readString()
    )

    /**
     * {@inheritDoc}
     */
    override fun writeTo(output: StreamOutput) {
        output.writeString(recipient)
    }

    /**
     * {@inheritDoc}
     */
    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!
        return builder.startObject()
            .field(RECIPIENT_TAG, recipient)
            .endObject()
    }
}
