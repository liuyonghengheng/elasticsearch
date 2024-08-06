/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.action.explain

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException

class ExplainRollupRequest : ActionRequest {

    val rollupIDs: List<String>

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(rollupIDs = sin.readStringArray().toList())

    constructor(rollupIDs: List<String>) {
        this.rollupIDs = rollupIDs
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (rollupIDs.isEmpty()) {
            validationException = addValidationError("Missing rollupID", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringArray(rollupIDs.toTypedArray())
    }
}
