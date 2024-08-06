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

package org.elasticsearch.replication.action.index

import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput

class ReplicateIndexResponse(val ack: Boolean) : AcknowledgedResponse(ack) {
    constructor(inp: StreamInput) : this(inp.readBoolean())

    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(ack)
    }
}
