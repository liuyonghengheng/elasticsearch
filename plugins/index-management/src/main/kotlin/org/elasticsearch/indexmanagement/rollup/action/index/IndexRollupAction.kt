/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.action.index

import org.elasticsearch.action.ActionType

class IndexRollupAction private constructor() : ActionType<IndexRollupResponse>(NAME, ::IndexRollupResponse) {
    companion object {
        val INSTANCE = IndexRollupAction()
        const val NAME = "cluster:admin/opendistro/rollup/index"
    }
}
