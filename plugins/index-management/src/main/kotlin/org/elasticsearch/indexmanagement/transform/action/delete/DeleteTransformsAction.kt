/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.action.delete

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.bulk.BulkResponse

class DeleteTransformsAction private constructor() : ActionType<BulkResponse>(NAME, ::BulkResponse) {
    companion object {
        val INSTANCE = DeleteTransformsAction()
        const val NAME = "cluster:admin/opendistro/transform/delete"
    }
}
