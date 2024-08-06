/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.common.model.rest

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.search.sort.FieldSortBuilder
import org.elasticsearch.search.sort.SortBuilders
import org.elasticsearch.search.sort.SortOrder
import java.io.IOException

const val DEFAULT_PAGINATION_SIZE = 20
const val DEFAULT_PAGINATION_FROM = 0
const val DEFAULT_SORT_ORDER = "asc"
const val DEFAULT_QUERY_STRING = "*"

data class SearchParams(
    val size: Int,
    val from: Int,
    val sortField: String,
    val sortOrder: String,
    val queryString: String
) : Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        size = sin.readInt(),
        from = sin.readInt(),
        sortField = sin.readString(),
        sortOrder = sin.readString(),
        queryString = sin.readString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeInt(size)
        out.writeInt(from)
        out.writeString(sortField)
        out.writeString(sortOrder)
        out.writeString(queryString)
    }

    fun getSortBuilder(): FieldSortBuilder {
        return SortBuilders
            .fieldSort(this.sortField)
            .order(SortOrder.fromString(this.sortOrder))
    }
}
