/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.engine

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.indexmanagement.opensearchapi.IndexManagementSecurityContext
import org.elasticsearch.indexmanagement.opensearchapi.withClosableContext
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.indexmanagement.IndexManagementIndices
import org.elasticsearch.indexmanagement.opensearchapi.retry
import org.elasticsearch.indexmanagement.snapshotmanagement.SnapshotManagementException
import org.elasticsearch.indexmanagement.snapshotmanagement.SnapshotManagementException.ExceptionKey
import org.elasticsearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.elasticsearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.elasticsearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.elasticsearch.indexmanagement.snapshotmanagement.indexMetadata
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMMetadata.LatestExecution.Status.TIME_LIMIT_EXCEEDED
import org.elasticsearch.indexmanagement.util.OpenForTesting
import org.elasticsearch.threadpool.ThreadPool
import java.time.Instant.now

@OpenForTesting
class SMStateMachine(
    val client: Client,
    val job: SMPolicy,
    var metadata: SMMetadata,
    val settings: Settings,
    val threadPool: ThreadPool,
    val indicesManager: IndexManagementIndices,
) {

    val log: Logger = LogManager.getLogger("$javaClass [${job.policyName}]")

    lateinit var currentState: SMState
    fun currentState(currentState: SMState): SMStateMachine {
        this.currentState = currentState
        return this
    }

    @Suppress("LongMethod", "ReturnCount", "NestedBlockDepth")
    suspend fun next(transitions: Map<SMState, List<SMState>>): SMStateMachine {
        try {
            do {
                val nextStates = transitions[currentState]
                if (nextStates == null) {
                    // Unlikely to reach unless the currentState field of metadata is tampered
                    log.error("No next states for current state [$currentState].")
                    return this
                }

                lateinit var result: SMResult
                val prevState = currentState
                for (nextState in nextStates) {
                    currentState = nextState
                    log.debug("Start executing $currentState.")
                    log.debug(
                        "User and roles string from thread context: ${threadPool.threadContext.getTransient<String>(
                            ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
                        )}"
                    )
                    result = withClosableContext(
                        IndexManagementSecurityContext(
                            job.id, settings, threadPool.threadContext, job.user
                        )
                    ) {
                        log.debug(
                            "User and roles string from thread context: ${threadPool.threadContext.getTransient<String>(
                                ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
                            )}"
                        )
                        currentState.instance.execute(this@SMStateMachine) as SMResult
                    }
                    log.debug(
                        "User and roles string from thread context: ${threadPool.threadContext.getTransient<String>(
                            ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
                        )}"
                    )

                    when (result) {
                        is SMResult.Next -> {
                            log.info("State [$currentState] has finished.")
                            updateMetadata(
                                result.metadataToSave
                                    .setCurrentState(currentState)
                                    .resetRetry()
                                    .build()
                            )
                            // break the nextStates loop, to avoid executing other lateral states
                            break
                        }
                        is SMResult.Stay -> {
                            log.debug("State [$currentState] has not finished.")
                            updateMetadata(
                                result.metadataToSave
                                    .setCurrentState(prevState)
                                    .resetRetry()
                                    .build()
                            )
                            // can still execute other lateral states if exists
                        }
                        is SMResult.Fail -> {
                            handleFailureNotification(result)
                            // any error causing Fail state is logged in place
                            if (result.forceReset == true) {
                                updateMetadata(result.metadataToSave.resetWorkflow().build())
                            } else {
                                updateMetadata(handleRetry(result, prevState).build())
                            }
                        }
                    }
                }
            } while (currentState.instance.continuous && result is SMResult.Next)
        } catch (ex: Exception) {
            val message = "There was an exception at ${now()} while executing Snapshot Management policy ${job.policyName}, please check logs."
            job.notificationConfig?.sendFailureNotification(client, job.policyName, message, job.user, log)
            if (ex is SnapshotManagementException &&
                ex.exKey == ExceptionKey.METADATA_INDEXING_FAILURE
            ) {
                // update metadata exception is special, we don't want to retry update metadata here
                return this
            }
            log.error("Uncaught snapshot management runtime exception.", ex)
        }
        return this
    }

    private suspend fun handleFailureNotification(result: SMResult.Fail) {
        val message = result.metadataToSave.getWorkflowMetadata()?.latestExecution?.info?.message
        if (message != null) {
            // Time limit exceeded is a special failure case which needs a different notification
            if (result.metadataToSave.getWorkflowMetadata()?.latestExecution?.status == TIME_LIMIT_EXCEEDED) {
                job.notificationConfig?.sendTimeLimitExceededNotification(
                    client,
                    job.policyName,
                    message,
                    job.user,
                    log
                )
            } else {
                job.notificationConfig?.sendFailureNotification(client, job.policyName, message, job.user, log)
            }
        }
    }

    private fun handleRetry(result: SMResult.Fail, prevState: SMState): SMMetadata.Builder {
        val metadataBuilder = result.metadataToSave.setCurrentState(prevState)
        val metadata = result.metadataToSave.build()
        val retry = when (result.workflowType) {
            WorkflowType.CREATION -> {
                metadata.creation.retry
            }
            WorkflowType.DELETION -> {
                metadata.deletion?.retry
            }
        }
        val retryCount: Int
        if (retry == null) {
            log.warn("Starting to retry state [$currentState], remaining count 3.")
            metadataBuilder.setRetry(MAX_NUMBER_OF_RETRIES) // TODO SM 3 retry count could be customizable
        } else {
            retryCount = retry.count - 1
            if (retryCount > 0) {
                log.warn("Retrying state [$currentState], remaining count $retryCount.")
                metadataBuilder.setRetry(retryCount)
            } else {
                val errorMessage = "Retry count exhausted for state [$currentState], reset workflow ${result.workflowType}."
                log.warn(errorMessage)
                metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.FAILED,
                    endTime = now()
                ).resetWorkflow()
            }
        }

        return metadataBuilder
    }

    /**
     * Update job running status metadata
     *
     * In one lifecycle of this context object, there could be multiple
     *  metadata update operations, so we keep the seqNo and priTerm.
     *
     * If experienced with any exception during update metadata, state machine stuck
     * Indexing exception will be caught and logged in [SMStateMachine.next]
     */
    private var metadataSeqNo: Long = metadata.seqNo
    private var metadataPrimaryTerm: Long = metadata.primaryTerm
    suspend fun updateMetadata(md: SMMetadata) {
        indicesManager.checkAndUpdateIMConfigIndex(log)
        try {
            log.debug("Update metadata: $md")
            if (md == metadata) {
                log.debug("Metadata not change, so don't need to update")
                return
            }
            updateMetaDataRetryPolicy.retry(log) {
                val res = client.indexMetadata(md, job.id, seqNo = metadataSeqNo, primaryTerm = metadataPrimaryTerm)
                metadataSeqNo = res.seqNo
                metadataPrimaryTerm = res.primaryTerm
                metadata = md
            }
        } catch (ex: Exception) {
            val smEx = SnapshotManagementException(ExceptionKey.METADATA_INDEXING_FAILURE, ex)
            log.error(smEx.message, ex)
            throw smEx
        }

        // TODO SM save a copy to history
    }

    private val updateMetaDataRetryPolicy = BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(EXPONENTIAL_BACKOFF_MILLIS), MAX_NUMBER_OF_RETRIES)

    /**
     * Handle the policy change before job running
     *
     * Currently, only need to handle the schedule change in policy.
     */
    suspend fun handlePolicyChange(): SMStateMachine {
        if (job.seqNo > metadata.policySeqNo || job.primaryTerm > metadata.policyPrimaryTerm) {
            val now = now()
            val metadataToSave = SMMetadata.Builder(metadata)
                .setSeqNoPrimaryTerm(job.seqNo, job.primaryTerm)
                .setNextCreationTime(job.creation.schedule.getNextExecutionTime(now))

            val deletion = job.deletion
            deletion?.let {
                metadataToSave.setNextDeletionTime(deletion.schedule.getNextExecutionTime(now))
            }

            updateMetadata(metadataToSave.build())
        }
        return this
    }

    companion object {
        private const val MAX_NUMBER_OF_RETRIES = 3
        private const val EXPONENTIAL_BACKOFF_MILLIS = 250L
    }
}
