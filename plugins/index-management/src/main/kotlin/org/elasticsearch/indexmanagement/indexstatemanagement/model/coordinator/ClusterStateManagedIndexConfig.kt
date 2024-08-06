/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.model.coordinator

import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig

/**
 * Data class to hold index metadata from cluster state.
 *
 * This data class is used in the [org.elasticsearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator]
 * when reading in index metadata from cluster state and implements [ToXContentObject] for partial updates
 * of the [ManagedIndexConfig] job document.
 */
data class ClusterStateManagedIndexConfig(
    val index: String,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val uuid: String,
    val policyID: String
)
