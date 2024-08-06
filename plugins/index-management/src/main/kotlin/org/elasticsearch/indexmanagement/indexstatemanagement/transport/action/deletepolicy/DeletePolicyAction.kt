/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.delete.DeleteResponse

class DeletePolicyAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeletePolicyAction()
        const val NAME = "cluster:admin/ilm/policy/delete"
    }
}
