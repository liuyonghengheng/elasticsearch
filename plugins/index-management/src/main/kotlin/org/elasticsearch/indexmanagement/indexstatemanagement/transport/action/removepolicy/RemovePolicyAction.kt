/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import org.elasticsearch.action.ActionType
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse

class RemovePolicyAction private constructor() : ActionType<ISMStatusResponse>(NAME, ::ISMStatusResponse) {
    companion object {
        val INSTANCE = RemovePolicyAction()
        const val NAME = "cluster:admin/ilm/managedindex/remove"
    }
}
