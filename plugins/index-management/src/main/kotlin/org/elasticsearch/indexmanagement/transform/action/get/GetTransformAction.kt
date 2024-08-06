/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.get

import org.elasticsearch.action.ActionType

class GetTransformAction private constructor() : ActionType<GetTransformResponse>(NAME, ::GetTransformResponse) {
    companion object {
        val INSTANCE = GetTransformAction()
        const val NAME = "cluster:admin/opendistro/transform/get"
    }
}
