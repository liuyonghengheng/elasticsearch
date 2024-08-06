/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import org.elasticsearch.action.ActionType
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse

class ChangePolicyAction private constructor() : ActionType<ISMStatusResponse>(NAME, ::ISMStatusResponse) {
    companion object {
        val INSTANCE = ChangePolicyAction()
        const val NAME = "cluster:admin/ilm/managedindex/change"
    }
}
