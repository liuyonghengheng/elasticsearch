/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.commons.notifications.action

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.commons.destination.response.LegacyDestinationResponse
import java.io.IOException

/**
 * Action Response for legacy publish notification.
 */
class LegacyPublishNotificationResponse : BaseResponse {
    val destinationResponse: LegacyDestinationResponse

    companion object {
        /**
         * reader to create instance of class from writable.
         */
        val reader = Writeable.Reader { LegacyPublishNotificationResponse(it) }
    }

    /**
     * constructor for creating the class
     * @param destinationResponse the response of the published notification
     */
    constructor(destinationResponse: LegacyDestinationResponse) {
        this.destinationResponse = destinationResponse
    }

    /**
     * {@inheritDoc}
     */
    @Throws(IOException::class)
    constructor(input: StreamInput) : super(input) {
        destinationResponse = LegacyDestinationResponse(input)
    }

    /**
     * {@inheritDoc}
     */
    @Throws(IOException::class)
    override fun writeTo(output: StreamOutput) {
        destinationResponse.writeTo(output)
    }

    // This class is only used across transport wire and does not need to implement toXContent
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        throw IllegalStateException("Legacy notification response is not intended for REST or persistence and does not support XContent.")
    }
}
