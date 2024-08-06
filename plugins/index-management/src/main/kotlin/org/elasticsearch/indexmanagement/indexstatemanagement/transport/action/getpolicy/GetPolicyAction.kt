/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.elasticsearch.action.ActionType

class GetPolicyAction private constructor() : ActionType<GetPolicyResponse>(NAME, ::GetPolicyResponse) {
    companion object {
        val INSTANCE = GetPolicyAction()
        const val NAME = "cluster:admin/ilm/policy/get"
    }
}
