/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.util

import org.elasticsearch.cluster.metadata.IndexAbstraction
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import org.elasticsearch.indexmanagement.opensearchapi.toMap
import org.elasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptService
import org.elasticsearch.script.ScriptType
import org.elasticsearch.script.TemplateScript

object RollupFieldValueExpressionResolver {

    private val validTopContextFields = setOf(Rollup.SOURCE_INDEX_FIELD)

    private lateinit var scriptService: ScriptService
    private lateinit var clusterService: ClusterService
    lateinit var indexAliasUtils: IndexAliasUtils
    fun resolve(rollup: Rollup, fieldValue: String): String {
        val script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, fieldValue, mapOf())

        val contextMap = rollup.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE)
            .toMap()
            .filterKeys { key -> key in validTopContextFields }

        var compiledValue = scriptService.compile(script, TemplateScript.CONTEXT)
            .newInstance(script.params + mapOf("ctx" to contextMap))
            .execute()

        if (indexAliasUtils.isAlias(compiledValue)) {
            compiledValue = indexAliasUtils.getWriteIndexNameForAlias(compiledValue)
        }

        return if (compiledValue.isNullOrBlank()) fieldValue else compiledValue
    }

    fun registerServices(scriptService: ScriptService, clusterService: ClusterService) {
        this.scriptService = scriptService
        this.clusterService = clusterService
        this.indexAliasUtils = IndexAliasUtils(clusterService)
    }

    fun registerServices(scriptService: ScriptService, clusterService: ClusterService, indexAliasUtils: IndexAliasUtils) {
        this.scriptService = scriptService
        this.clusterService = clusterService
        this.indexAliasUtils = indexAliasUtils
    }

    open class IndexAliasUtils(val clusterService: ClusterService) {

        open fun hasAlias(index: String): Boolean {
            val aliases = this.clusterService.state().metadata().indices.get(index)?.aliases
            if (aliases != null) {
                return aliases.size() > 0
            }
            return false
        }

        open fun isAlias(index: String): Boolean {
            return this.clusterService.state().metadata().indicesLookup?.get(index) is IndexAbstraction.Alias
        }

        open fun getWriteIndexNameForAlias(alias: String): String? {
            return this.clusterService.state().metadata().indicesLookup?.get(alias)?.writeIndex?.index?.name
        }

        open fun getBackingIndicesForAlias(alias: String): MutableList<IndexMetadata>? {
            return this.clusterService.state().metadata().indicesLookup?.get(alias)?.indices
        }
    }
}
