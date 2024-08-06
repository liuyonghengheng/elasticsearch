/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.explain

import org.elasticsearch.action.ActionType

class ExplainTransformAction private constructor() : ActionType<ExplainTransformResponse>(NAME, ::ExplainTransformResponse) {
    companion object {
        val INSTANCE = ExplainTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/explain"
    }
}
