/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.indexmanagement.common.model.rest.SearchParams
import org.elasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import java.io.IOException

class ExplainRequest : ActionRequest {

    val indices: List<String>
    val local: Boolean
    val clusterManagerTimeout: TimeValue
    val searchParams: SearchParams
    val showPolicy: Boolean
    val validateAction: Boolean
    val indexType: String

    @Suppress("LongParameterList")
    constructor(
        indices: List<String>,
        local: Boolean,
        clusterManagerTimeout: TimeValue,
        searchParams: SearchParams,
        showPolicy: Boolean,
        validateAction: Boolean,
        indexType: String
    ) : super() {
        this.indices = indices
        this.local = local
        this.clusterManagerTimeout = clusterManagerTimeout
        this.searchParams = searchParams
        this.showPolicy = showPolicy
        this.validateAction = validateAction
        this.indexType = indexType
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indices = sin.readStringList(),
        local = sin.readBoolean(),
        clusterManagerTimeout = sin.readTimeValue(),
        searchParams = SearchParams(sin),
        showPolicy = sin.readBoolean(),
        validateAction = sin.readBoolean(),
        indexType = sin.readString()
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (indexType != DEFAULT_INDEX_TYPE && indices.size > 1) {
            validationException = ValidateActions.addValidationError(
                MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR,
                validationException
            )
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indices)
        out.writeBoolean(local)
        out.writeTimeValue(clusterManagerTimeout)
        searchParams.writeTo(out)
        out.writeBoolean(showPolicy)
        out.writeBoolean(validateAction)
        out.writeString(indexType)
    }

    companion object {
        const val MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR =
            "Cannot call explain on more than one index name/pattern when using a custom index type"
    }
}
