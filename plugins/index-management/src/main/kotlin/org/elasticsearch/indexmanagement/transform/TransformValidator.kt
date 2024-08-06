/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform

import org.elasticsearch.ElasticsearchSecurityException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.indexmanagement.opensearchapi.suspendUntil
import org.elasticsearch.indexmanagement.transform.exceptions.TransformValidationException
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.indexmanagement.transform.model.TransformValidationResult
import org.elasticsearch.indexmanagement.transform.settings.TransformSettings
import org.elasticsearch.monitor.jvm.JvmService
import org.elasticsearch.transport.RemoteTransportException

@Suppress("SpreadOperator", "ReturnCount", "ThrowsCount")
class TransformValidator(
    private val indexNameExpressionResolver: IndexNameExpressionResolver,
    private val clusterService: ClusterService,
    private val client: Client,
    val settings: Settings,
    private val jvmService: JvmService
) {

    @Volatile private var circuitBreakerEnabled = TransformSettings.TRANSFORM_CIRCUIT_BREAKER_ENABLED.get(settings)
    @Volatile private var circuitBreakerJvmThreshold = TransformSettings.TRANSFORM_CIRCUIT_BREAKER_JVM_THRESHOLD.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(TransformSettings.TRANSFORM_CIRCUIT_BREAKER_ENABLED) {
            circuitBreakerEnabled = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(TransformSettings.TRANSFORM_CIRCUIT_BREAKER_JVM_THRESHOLD) {
            circuitBreakerJvmThreshold = it
        }
    }
    /**
     * // TODO: When FGAC is supported in transform should check the user has the correct permissions
     * Validates the provided transform. Validation checks include the following:
     * 1. Source index/indices defined in transform exist
     * 2. Groupings defined in transform can be realized using source index/indices
     */
    suspend fun validate(transform: Transform): TransformValidationResult {
        val errorMessage = "Failed to validate the transform job"
        try {
            val issues = mutableListOf<String>()
            if (circuitBreakerEnabled && jvmService.stats().mem.heapUsedPercent > circuitBreakerJvmThreshold) {
                issues.add("The cluster is breaching the jvm usage threshold [$circuitBreakerJvmThreshold], cannot execute the transform")
                return TransformValidationResult(issues.isEmpty(), issues)
            }
            val concreteIndices =
                indexNameExpressionResolver.concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpand(), true, transform.sourceIndex)
            if (concreteIndices.isEmpty()) return TransformValidationResult(false, listOf("No specified source index exist in the cluster"))

            val request = ClusterHealthRequest()
                .indices(*concreteIndices)
                .waitForYellowStatus()
            val response: ClusterHealthResponse = client.suspendUntil { execute(ClusterHealthAction.INSTANCE, request, it) }
            if (response.isTimedOut) {
                issues.add("Cannot determine that the requested source indices are healthy")
                return TransformValidationResult(issues.isEmpty(), issues)
            }
            concreteIndices.forEach { index -> issues.addAll(validateIndex(index, transform)) }

            return TransformValidationResult(issues.isEmpty(), issues)
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            throw TransformValidationException(errorMessage, unwrappedException)
        } catch (e: ElasticsearchSecurityException) {
            throw TransformValidationException("$errorMessage - missing required index permissions: ${e.localizedMessage}")
        } catch (e: Exception) {
            throw TransformValidationException(errorMessage, e)
        }
    }

    /**
     * Internal method to validate grouping defined inside transform can be realized with the index provided.
     * 1. Checks that each field mentioned in transform groupings exist in source index
     * 2. Checks that type of field in index can be grouped as requested
     */
    private suspend fun validateIndex(index: String, transform: Transform): List<String> {
        val request = GetMappingsRequest().indices(index)
        val result: GetMappingsResponse =
            client.admin().indices().suspendUntil { getMappings(request, it) } ?: error("GetMappingResponse for [$index] was null")
        return validateMappingsResponse(index, result, transform)
    }

    companion object {
        fun validateMappingsResponse(index: String, response: GetMappingsResponse, transform: Transform): List<String> {
            val issues = mutableListOf<String>()
            val indexTypeMappings = response.mappings[index]
            if (indexTypeMappings == null) {
                issues.add("Source index [$index] mappings are empty, cannot validate the job.")
                return issues
            }

            val mappingMetadata = indexTypeMappings.iterator().next().value
            val indexMappingSource = mappingMetadata.sourceAsMap

            transform.groups.forEach { group ->
                if (!group.canBeRealizedInMappings(indexMappingSource)) {
                    issues.add("Cannot find field [${group.sourceField}] that can be grouped as [${group.type.type}] in [$index].")
                }
            }
            return issues
        }
    }
}
