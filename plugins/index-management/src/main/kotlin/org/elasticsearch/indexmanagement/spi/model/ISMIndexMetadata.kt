/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.spi.indexstatemanagement.model

data class ISMIndexMetadata(
    val indexUuid: String,
    val indexCreationDate: Long,
    val documentCount: Long,
)
