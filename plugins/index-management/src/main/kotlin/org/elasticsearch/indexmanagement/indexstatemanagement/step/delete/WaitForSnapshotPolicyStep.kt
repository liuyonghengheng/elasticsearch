package org.elasticsearch.indexmanagement.indexstatemanagement.step.delete

import kotlinx.coroutines.CompletableDeferred
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.indexmanagement.indexstatemanagement.action.WaitForSnapshotPolicyAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.explain.ExplainSMPolicyRequest
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.explain.ExplainSMPolicyResponse
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Step
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData

class WaitForSnapshotPolicyStep(private val action: WaitForSnapshotPolicyAction) : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val policy = action.policy
        val policyNames: Array<String> = arrayOf(policy)
        val deferred = CompletableDeferred<Unit>()
        try {
            context.client.execute(
                SMActions.EXPLAIN_SM_POLICY_ACTION_TYPE,
                ExplainSMPolicyRequest(policyNames),
                object : ActionListener<ExplainSMPolicyResponse> {
                    override fun onResponse(explainSMPolicyResponse: ExplainSMPolicyResponse) {
                        val explainSMPolicy = explainSMPolicyResponse.policiesToExplain[policy]
                        if (explainSMPolicy == null) {
                            val message = getNotExistMessage(policy)
                            logger.warn(message)
                            stepStatus = StepStatus.FAILED
                            info = mapOf("message" to message)
                            deferred.complete(Unit)
                            return
                        }
                        val execution = explainSMPolicy?.metadata?.creation?.latestExecution
                        if (execution != null && execution.status == SMMetadata.LatestExecution.Status.SUCCESS) {
                            val message = getSuccessMessage(policy)
                            logger.info(message)
                            stepStatus = StepStatus.COMPLETED
                            info = mapOf("message" to message)
                        } else {
                            val message = getWaitingMessage(policy)
                            logger.info(message)
                            stepStatus = StepStatus.CONDITION_NOT_MET
                            info = mapOf("message" to message)
                        }
                        deferred.complete(Unit)
                    }

                    override fun onFailure(e: Exception) {
                        val message = getFailedMessage(policy)
                        logger.warn(message)
                        stepStatus = StepStatus.FAILED
                        info = mapOf("message" to message)
                        deferred.complete(Unit)
                    }
                })
        } catch (e: Exception) {
            val message = getFailedMessage(policy)
            logger.warn(message)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message)
            deferred.complete(Unit)
        }

        deferred.await()
        return this
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
        const val name = "wait_for_snapshot"
        fun getWaitingMessage(policy: String) = "Waiting for Policy to complete [policy=$policy]"
        fun getSuccessMessage(policy: String) = "Policy successfully executed [policy=$policy]"
        fun getFailedMessage(policy: String) = "Failed to wait policy execute [policy=$policy]"
        fun getNotExistMessage(policy: String) = "Policy does not exist [policy=$policy]"
    }
}
