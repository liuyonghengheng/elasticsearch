/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.get

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.elasticsearch.indexmanagement.util._ID
import org.elasticsearch.indexmanagement.util._PRIMARY_TERM
import org.elasticsearch.indexmanagement.util._SEQ_NO

// totalPolicies may differ from the length of the policies field if the size parameter is introduced
class GetSMPoliciesResponse(
    val policies: List<SMPolicy>,
    val totalPolicies: Long
) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        policies = sin.readList(::SMPolicy),
        totalPolicies = sin.readLong()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeList(policies)
        out.writeLong(totalPolicies)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startArray("policies")
            .apply {
                for (policy in policies) {
                    this.startObject()
                        .field(_ID, policy.id)
                        .field(_SEQ_NO, policy.seqNo)
                        .field(_PRIMARY_TERM, policy.primaryTerm)
                        .field(SMPolicy.SM_TYPE, policy, XCONTENT_WITHOUT_TYPE_AND_USER)
                        .endObject()
                }
            }
            .endArray()
            .field("total_policies", totalPolicies)
            .endObject()
    }
}
