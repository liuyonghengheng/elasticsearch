/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.action.delete

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.delete.DeleteResponse

class DeleteRollupAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeleteRollupAction()
        const val NAME = "cluster:admin/opendistro/rollup/delete"
    }
}
