/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.get

import org.elasticsearch.action.ActionType

class GetTransformsAction private constructor() : ActionType<GetTransformsResponse>(NAME, ::GetTransformsResponse) {
    companion object {
        val INSTANCE = GetTransformsAction()
        const val NAME = "cluster:admin/opendistro/transform/get_transforms"
    }
}
