/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.elasticsearch.indexmanagement.indexstatemanagement.opensearchapi.addObject
import org.elasticsearch.indexmanagement.indexstatemanagement.settings.LegacyOpenDistroManagedIndexSettings
import org.elasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.indexmanagement.indexstatemanagement.util.TOTAL_MANAGED_INDICES
import org.elasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.ValidationResult
import java.io.IOException

open class ExplainResponse : ActionResponse, ToXContentObject {

    // TODO refactor these lists usage to map
    val indexNames: List<String>
    val indexPolicyIDs: List<String?>
    val indexMetadatas: List<ManagedIndexMetaData?>
    val totalManagedIndices: Int
    val enabledState: Map<String, Boolean>
    val policies: Map<String, Policy>
    val validationResults: List<ValidationResult?>

    @Suppress("LongParameterList")
    constructor(
        indexNames: List<String>,
        indexPolicyIDs: List<String?>,
        indexMetadatas: List<ManagedIndexMetaData?>,
        totalManagedIndices: Int,
        enabledState: Map<String, Boolean>,
        policies: Map<String, Policy>,
        validationResults: List<ValidationResult?>
    ) : super() {
        this.indexNames = indexNames
        this.indexPolicyIDs = indexPolicyIDs
        this.indexMetadatas = indexMetadatas
        this.totalManagedIndices = totalManagedIndices
        this.enabledState = enabledState
        this.policies = policies
        this.validationResults = validationResults
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indexNames = sin.readStringList(),
        indexPolicyIDs = sin.readStringList(),
        indexMetadatas = sin.readList { ManagedIndexMetaData.fromStreamInput(it) },
        totalManagedIndices = sin.readInt(),
        enabledState = sin.readMap() as Map<String, Boolean>,
        policies = sin.readMap(StreamInput::readString, ::Policy),
        validationResults = sin.readList { ValidationResult.fromStreamInput(it) }
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indexNames)
        out.writeStringCollection(indexPolicyIDs)
        out.writeCollection(indexMetadatas)
        out.writeInt(totalManagedIndices)
        out.writeMap(enabledState)
        out.writeMap(
            policies,
            { _out, key -> _out.writeString(key) },
            { _out, policy -> policy.writeTo(_out) }
        )
        out.writeCollection(validationResults)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        indexNames.forEachIndexed { ind, name ->
            builder.startObject(name)
            builder.field(ManagedIndexSettings.POLICY_ID.key, indexPolicyIDs[ind])
            // builder.field(LegacyOpenDistroManagedIndexSettings.POLICY_ID.key, indexPolicyIDs[ind])
            indexMetadatas[ind]?.toXContent(builder, ToXContent.EMPTY_PARAMS)
            builder.field("enabled", enabledState[name])
            policies[name]?.let { builder.field(Policy.POLICY_TYPE, it, XCONTENT_WITHOUT_TYPE_AND_USER) }
            if (validationResults[ind] != null) {
                builder.addObject(ValidationResult.VALIDATE, validationResults[ind], params, true)
            }
            builder.endObject()
        }
        builder.field(TOTAL_MANAGED_INDICES, totalManagedIndices)
        return builder.endObject()
    }
}
