/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.refreshanalyzer

import org.elasticsearch.action.ActionType
import org.elasticsearch.common.io.stream.Writeable

class RefreshSearchAnalyzerAction : ActionType<RefreshSearchAnalyzerResponse>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/refresh_search_analyzers"
        val INSTANCE = RefreshSearchAnalyzerAction()
        val reader = Writeable.Reader { inp -> RefreshSearchAnalyzerResponse(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<RefreshSearchAnalyzerResponse> = reader
}
