/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.step.indexpriority

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_PRIORITY
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.indexmanagement.indexstatemanagement.action.IndexPriorityAction
import org.elasticsearch.indexmanagement.opensearchapi.suspendUntil
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Step
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.elasticsearch.transport.RemoteTransportException

class AttemptSetIndexPriorityStep(private val action: IndexPriorityAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val managedIndexMetaData = context.metadata
        try {
            val updateSettingsRequest = UpdateSettingsRequest()
                .indices(managedIndexMetaData.index)
                .settings(Settings.builder().put(SETTING_PRIORITY, action.indexPriority))
            val response: AcknowledgedResponse = context.client.admin().indices()
                .suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (response.isAcknowledged) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to getSuccessMessage(indexName, action.indexPriority))
            } else {
                val message = getFailedMessage(indexName, action.indexPriority)
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to message)
            }
        } catch (e: RemoteTransportException) {
            handleException(indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(indexName, e)
        }

        return this
    }

    private fun handleException(indexName: String, e: Exception) {
        val message = getFailedMessage(indexName, action.indexPriority)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent() = true

    companion object {
        const val name = "attempt_set_index_priority"
        fun getFailedMessage(index: String, indexPriority: Int) = "Failed to set index priority to $indexPriority [index=$index]"
        fun getSuccessMessage(index: String, indexPriority: Int) = "Successfully set index priority to $indexPriority [index=$index]"
    }
}
