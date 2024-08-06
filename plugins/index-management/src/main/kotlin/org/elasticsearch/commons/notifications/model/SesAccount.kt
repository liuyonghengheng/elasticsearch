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
import org.elasticsearch.commons.notifications.NotificationConstants.FROM_ADDRESS_TAG
import org.elasticsearch.commons.notifications.NotificationConstants.REGION_TAG
import org.elasticsearch.commons.notifications.NotificationConstants.ROLE_ARN_TAG
import org.elasticsearch.commons.utils.fieldIfNotNull
import org.elasticsearch.commons.utils.logger
import org.elasticsearch.commons.utils.validateEmail
import org.elasticsearch.commons.utils.validateIamRoleArn
import java.io.IOException

/**
 * Data class representing SES account channel.
 */
data class SesAccount(
    val awsRegion: String,
    val roleArn: String?,
    val fromAddress: String
) : BaseConfigData {

    init {
        require(!Strings.isNullOrEmpty(awsRegion)) { "awsRegion is null or empty" }
        validateEmail(fromAddress)
        if (roleArn != null) {
            validateIamRoleArn(roleArn)
        }
    }

    companion object {
        private val log by logger(SesAccount::class.java)

        /**
         * reader to create instance of class from writable.
         */
        val reader = Writeable.Reader { SesAccount(it) }

        /**
         * Parser to parse xContent
         */
        val xParser = XParser { parse(it) }

        @JvmStatic
        @Throws(IOException::class)
        fun parse(parser: XContentParser): SesAccount {
            var awsRegion: String? = null
            var roleArn: String? = null
            var fromAddress: String? = null

            XContentParserUtils.ensureExpectedToken(
                XContentParser.Token.START_OBJECT,
                parser.currentToken(),
                parser
            )
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()
                when (fieldName) {
                    REGION_TAG -> awsRegion = parser.text()
                    ROLE_ARN_TAG -> roleArn = parser.textOrNull()
                    FROM_ADDRESS_TAG -> fromAddress = parser.text()
                    else -> {
                        parser.skipChildren()
                        log.info("Unexpected field: $fieldName, while parsing SesAccount")
                    }
                }
            }
            awsRegion ?: throw IllegalArgumentException("$REGION_TAG field absent")
            fromAddress ?: throw IllegalArgumentException("$FROM_ADDRESS_TAG field absent")
            return SesAccount(
                awsRegion,
                roleArn,
                fromAddress
            )
        }
    }

    /**
     * {@inheritDoc}
     */
    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        return builder!!.startObject()
            .field(REGION_TAG, awsRegion)
            .fieldIfNotNull(ROLE_ARN_TAG, roleArn)
            .field(FROM_ADDRESS_TAG, fromAddress)
            .endObject()
    }

    /**
     * Constructor used in transport action communication.
     * @param input StreamInput stream to deserialize data from.
     */
    constructor(input: StreamInput) : this(
        awsRegion = input.readString(),
        roleArn = input.readOptionalString(),
        fromAddress = input.readString()
    )

    /**
     * {@inheritDoc}
     */
    override fun writeTo(out: StreamOutput) {
        out.writeString(awsRegion)
        out.writeOptionalString(roleArn)
        out.writeString(fromAddress)
    }
}
