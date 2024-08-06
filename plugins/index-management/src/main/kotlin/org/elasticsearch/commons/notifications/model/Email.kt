/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.elasticsearch.commons.notifications.model

import org.elasticsearch.common.Strings
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.commons.notifications.NotificationConstants.EMAIL_ACCOUNT_ID_TAG
import org.elasticsearch.commons.notifications.NotificationConstants.EMAIL_GROUP_ID_LIST_TAG
import org.elasticsearch.commons.notifications.NotificationConstants.RECIPIENT_LIST_TAG
import org.elasticsearch.commons.utils.logger
import org.elasticsearch.commons.utils.objectList
import org.elasticsearch.commons.utils.stringList
import java.io.IOException

/**
 * Data class representing Email account and default recipients.
 */
data class Email(
    val emailAccountID: String,
    val recipients: List<EmailRecipient>,
    val emailGroupIds: List<String>
) : BaseConfigData {

    init {
        require(!Strings.isNullOrEmpty(emailAccountID)) { "emailAccountID is null or empty" }
    }

    companion object {
        private val log by logger(Email::class.java)

        /**
         * reader to create instance of class from writable.
         */
        val reader = Writeable.Reader { Email(it) }

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
        fun parse(parser: XContentParser): Email {
            var emailAccountID: String? = null
            var recipients: List<EmailRecipient> = listOf()
            var emailGroupIds: List<String> = listOf()

            XContentParserUtils.ensureExpectedToken(
                XContentParser.Token.START_OBJECT,
                parser.currentToken(),
                parser
            )
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()
                when (fieldName) {
                    EMAIL_ACCOUNT_ID_TAG -> emailAccountID = parser.text()
                    RECIPIENT_LIST_TAG -> recipients = parser.objectList { EmailRecipient.parse(it) }
                    EMAIL_GROUP_ID_LIST_TAG -> emailGroupIds = parser.stringList()
                    else -> {
                        parser.skipChildren()
                        log.info("Unexpected field: $fieldName, while parsing Email")
                    }
                }
            }
            emailAccountID ?: throw IllegalArgumentException("$EMAIL_ACCOUNT_ID_TAG field absent")
            return Email(emailAccountID, recipients, emailGroupIds)
        }
    }

    /**
     * Constructor used in transport action communication.
     * @param input StreamInput stream to deserialize data from.
     */
    constructor(input: StreamInput) : this(
        emailAccountID = input.readString(),
        recipients = input.readList(EmailRecipient.reader),
        emailGroupIds = input.readStringList()
    )

    /**
     * {@inheritDoc}
     */
    override fun writeTo(output: StreamOutput) {
        output.writeString(emailAccountID)
        output.writeList(recipients)
        output.writeStringCollection(emailGroupIds)
    }

    /**
     * {@inheritDoc}
     */
    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!
        return builder.startObject()
            .field(EMAIL_ACCOUNT_ID_TAG, emailAccountID)
            .field(RECIPIENT_LIST_TAG, recipients)
            .field(EMAIL_GROUP_ID_LIST_TAG, emailGroupIds)
            .endObject()
    }
}
