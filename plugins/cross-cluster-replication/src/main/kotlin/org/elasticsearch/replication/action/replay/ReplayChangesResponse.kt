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

package org.elasticsearch.replication.action.replay

import org.elasticsearch.action.support.WriteResponse
import org.elasticsearch.action.support.replication.ReplicationResponse
import org.elasticsearch.common.io.stream.StreamInput

class ReplayChangesResponse : ReplicationResponse, WriteResponse {

    constructor(inp: StreamInput) : super(inp)

    constructor(): super()

    override fun setForcedRefresh(forcedRefresh: Boolean) {
        //no-op
    }


}
