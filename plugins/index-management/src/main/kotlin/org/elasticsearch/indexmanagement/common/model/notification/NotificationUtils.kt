/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:JvmName("NotificationUtils")
package org.elasticsearch.indexmanagement.common.model.notification

import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.rest.RestStatus

/**
 * all valid response status
 */
private val VALID_RESPONSE_STATUS = setOf(
    RestStatus.OK.status, RestStatus.CREATED.status, RestStatus.ACCEPTED.status,
    RestStatus.NON_AUTHORITATIVE_INFORMATION.status, RestStatus.NO_CONTENT.status,
    RestStatus.RESET_CONTENT.status, RestStatus.PARTIAL_CONTENT.status,
    RestStatus.MULTI_STATUS.status
)

@Throws(ElasticsearchStatusException::class)
fun validateResponseStatus(restStatus: RestStatus, responseContent: String) {
    if (!VALID_RESPONSE_STATUS.contains(restStatus.status)) {
        throw ElasticsearchStatusException("Failed: $responseContent", restStatus)
    }
}
