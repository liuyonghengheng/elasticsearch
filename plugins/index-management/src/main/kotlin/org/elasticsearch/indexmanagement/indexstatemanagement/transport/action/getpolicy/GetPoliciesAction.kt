/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.elasticsearch.action.ActionType

class GetPoliciesAction private constructor() : ActionType<GetPoliciesResponse>(NAME, ::GetPoliciesResponse) {
    companion object {
        val INSTANCE = GetPoliciesAction()
        const val NAME = "cluster:admin/ilm/policy/search"
    }
}
