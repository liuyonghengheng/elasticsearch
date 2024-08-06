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
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.store.StoreFileMetadata

class GetFileChunkResponse : ActionResponse {

    val storeFileMetadata: StoreFileMetadata
    val offset: Long
    val data: BytesReference

    constructor(storeFileMetadata: StoreFileMetadata, offset: Long, data: BytesReference): super() {
        this.storeFileMetadata = storeFileMetadata
        this.offset = offset
        this.data = data
    }

    constructor(inp: StreamInput): super(inp) {
        storeFileMetadata = StoreFileMetadata(inp)
        offset = inp.readLong()
        data = inp.readBytesReference()
    }

    override fun writeTo(out: StreamOutput) {
        storeFileMetadata.writeTo(out)
        out.writeLong(offset)
        out.writeBytesReference(data)
    }
}
