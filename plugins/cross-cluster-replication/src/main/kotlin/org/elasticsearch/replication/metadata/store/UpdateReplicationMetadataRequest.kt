/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.elasticsearch.replication.metadata.store

import org.elasticsearch.index.seqno.SequenceNumbers

data class UpdateReplicationMetadataRequest(val replicationMetadata: ReplicationMetadata,
                                       val ifSeqno: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
                                       val ifPrimaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
}
