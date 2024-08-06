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

package org.elasticsearch.replication.action.changes

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.translog.Translog

class GetChangesResponse(val changes: List<Translog.Operation>,
                         val fromSeqNo: Long,
                         val maxSeqNoOfUpdatesOrDeletes: Long,
                         val lastSyncedGlobalCheckpoint: Long) : ActionResponse() {

    constructor(inp: StreamInput) : this(inp.readList(Translog.Operation::readOperation), inp.readVLong(),
        inp.readLong(), inp.readLong())

    override fun writeTo(out: StreamOutput) {
        out.writeCollection(changes, Translog.Operation::writeOperation)
        out.writeVLong(fromSeqNo)
        out.writeLong(maxSeqNoOfUpdatesOrDeletes)
        out.writeLong(lastSyncedGlobalCheckpoint)
    }
}
