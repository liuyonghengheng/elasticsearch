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

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.store.Store

class GetStoreMetadataResponse : ActionResponse {

    val metadataSnapshot : Store.MetadataSnapshot

    constructor(metadataSnapshot: Store.MetadataSnapshot): super() {
        this.metadataSnapshot = metadataSnapshot
    }

    constructor(inp: StreamInput) : super(inp) {
        metadataSnapshot = Store.MetadataSnapshot(inp)
    }

    override fun writeTo(out: StreamOutput) {
        metadataSnapshot.writeTo(out)
    }
}
