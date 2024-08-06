/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.elasticsearch.replication.action.repository

import org.elasticsearch.action.ActionType

class GetStoreMetadataAction private constructor() : ActionType<GetStoreMetadataResponse>(NAME, ::GetStoreMetadataResponse) {
    companion object {
        const val NAME = "indices:data/read/plugins/replication/file_metadata"
        val INSTANCE = GetStoreMetadataAction()
    }
}
