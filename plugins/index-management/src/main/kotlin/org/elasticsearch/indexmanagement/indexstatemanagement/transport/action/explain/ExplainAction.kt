/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.elasticsearch.action.ActionType

class ExplainAction private constructor() : ActionType<ExplainResponse>(NAME, ::ExplainResponse) {
    companion object {
        val INSTANCE = ExplainAction()
        const val NAME = "cluster:admin/ilm/managedindex/explain"
    }
}
