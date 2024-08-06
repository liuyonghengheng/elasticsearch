/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler

import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyRequest
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyResponse
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.elasticsearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.elasticsearch.indexmanagement.snapshotmanagement.util.getValidSMPolicyName
import org.elasticsearch.indexmanagement.util.IF_PRIMARY_TERM
import org.elasticsearch.indexmanagement.util.IF_SEQ_NO
import org.elasticsearch.indexmanagement.util.REFRESH
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener
import java.time.Instant

abstract class RestBaseIndexSMPolicyHandler : BaseRestHandler() {

    protected fun prepareIndexRequest(request: RestRequest, client: NodeClient, create: Boolean): RestChannelConsumer {
        val policyName = request.getValidSMPolicyName()

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val xcp = request.contentParser()
        val policy = SMPolicy.parse(xcp, id = smPolicyNameToDocId(policyName), seqNo = seqNo, primaryTerm = primaryTerm)
            .copy(jobLastUpdateTime = Instant.now())

        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        return RestChannelConsumer {
            client.execute(
                SMActions.INDEX_SM_POLICY_ACTION_TYPE,
                IndexSMPolicyRequest(policy, create, refreshPolicy),
                object : RestResponseListener<IndexSMPolicyResponse>(it) {
                    override fun buildResponse(response: IndexSMPolicyResponse): RestResponse {
                        val restResponse = BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                        if (response.status == RestStatus.CREATED || response.status == RestStatus.OK) {
                            val location = "$SM_POLICIES_URI/${response.policy.policyName}"
                            restResponse.addHeader("Location", location)
                        }
                        return restResponse
                    }
                }
            )
        }
    }
}
