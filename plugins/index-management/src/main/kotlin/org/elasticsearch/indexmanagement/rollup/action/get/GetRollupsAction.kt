/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.action.get

import org.elasticsearch.action.ActionType

class GetRollupsAction private constructor() : ActionType<GetRollupsResponse>(NAME, ::GetRollupsResponse) {
    companion object {
        val INSTANCE = GetRollupsAction()
        const val NAME = "cluster:admin/opendistro/rollup/search"
    }
}
