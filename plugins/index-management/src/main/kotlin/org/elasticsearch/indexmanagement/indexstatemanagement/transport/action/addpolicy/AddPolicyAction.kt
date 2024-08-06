/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy

import org.elasticsearch.action.ActionType
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse

class AddPolicyAction private constructor() : ActionType<ISMStatusResponse>(NAME, ::ISMStatusResponse) {
    companion object {
        val INSTANCE = AddPolicyAction()
        const val NAME = "cluster:admin/ilm/managedindex/add"
    }
}
