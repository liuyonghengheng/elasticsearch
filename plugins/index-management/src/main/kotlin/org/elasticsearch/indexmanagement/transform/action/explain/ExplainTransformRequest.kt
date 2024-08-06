/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.explain

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException

class ExplainTransformRequest(val transformIDs: List<String>) : ActionRequest() {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(transformIDs = sin.readStringArray().toList())

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (transformIDs.isEmpty()) {
            validationException = addValidationError("Missing transformID", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringArray(transformIDs.toTypedArray())
    }
}
