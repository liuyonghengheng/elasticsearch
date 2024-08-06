/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.action.mapping

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.indexmanagement.rollup.model.Rollup

class UpdateRollupMappingRequest : AcknowledgedRequest<UpdateRollupMappingRequest> {
    val rollup: Rollup

    constructor(sin: StreamInput) : super(sin) {
        rollup = Rollup(sin)
    }

    constructor(rollup: Rollup) {
        this.rollup = rollup
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        rollup.writeTo(out)
    }
}
