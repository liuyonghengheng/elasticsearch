/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.model

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.indexmanagement.opensearchapi.optionalField
import java.io.IOException

data class ExplainSMPolicy(
    val metadata: SMMetadata? = null,
    val enabled: Boolean? = null
) : ToXContentFragment, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        metadata = if (sin.readBoolean()) SMMetadata(sin) else null,
        enabled = sin.readOptionalBoolean()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(metadata != null)
        metadata?.writeTo(out)
        out.writeOptionalBoolean(enabled)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        metadata?.let {
            builder
                .field(SMMetadata.CREATION_FIELD, it.creation)
                .optionalField(SMMetadata.DELETION_FIELD, it.deletion)
                .field(SMMetadata.POLICY_SEQ_NO_FIELD, it.policySeqNo)
                .field(SMMetadata.POLICY_PRIMARY_TERM_FIELD, it.policyPrimaryTerm)
        }
        return builder.field(SMPolicy.ENABLED_FIELD, enabled)
    }
}
