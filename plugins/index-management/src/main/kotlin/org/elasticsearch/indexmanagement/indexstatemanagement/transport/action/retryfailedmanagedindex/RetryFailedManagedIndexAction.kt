/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import org.elasticsearch.action.ActionType
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse

class RetryFailedManagedIndexAction private constructor() : ActionType<ISMStatusResponse>(NAME, ::ISMStatusResponse) {
    companion object {
        val INSTANCE = RetryFailedManagedIndexAction()
        const val NAME = "cluster:admin/ilm/managedindex/retry"
    }
}
