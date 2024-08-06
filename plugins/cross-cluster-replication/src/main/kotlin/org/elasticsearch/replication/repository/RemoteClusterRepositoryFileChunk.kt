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

package org.elasticsearch.replication.repository

import org.elasticsearch.index.store.StoreFileMetadata
import org.elasticsearch.indices.recovery.MultiChunkTransfer.ChunkRequest

class RemoteClusterRepositoryFileChunk constructor(val storeFileMetadata: StoreFileMetadata,
                                                   val offset: Long,
                                                   val length: Int): ChunkRequest {

    override fun lastChunk(): Boolean {
        return storeFileMetadata.length() <= offset + length
    }
}
