/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.preview

import org.elasticsearch.action.ActionType

class PreviewTransformAction private constructor() : ActionType<PreviewTransformResponse>(NAME, ::PreviewTransformResponse) {
    companion object {
        val INSTANCE = PreviewTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/preview"
    }
}
