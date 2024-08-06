/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.util

import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.indexmanagement.opensearchapi.retry
import org.elasticsearch.indexmanagement.opensearchapi.suspendUntil
import org.elasticsearch.jobscheduler.spi.JobExecutionContext
import org.elasticsearch.jobscheduler.spi.LockModel
import org.elasticsearch.jobscheduler.spi.ScheduledJobParameter

private val logger = LogManager.getLogger("JobSchedulerUtils")

/**
 * Util method to attempt to get the lock on the requested scheduled job using the backoff policy.
 * If failed to acquire the lock using backoff policy will return a null lock otherwise returns acquired lock.
 */
suspend fun acquireLockForScheduledJob(scheduledJob: ScheduledJobParameter, context: JobExecutionContext, backoffPolicy: BackoffPolicy): LockModel? {
    var lock: LockModel? = null
    try {
        // acquireLock will attempt to create the lock index if needed and then read/create a lock. This is purely for internal purposes
        // and should not need the role's context to run
        backoffPolicy.retry(logger) {
            lock = context.lockService.suspendUntil { acquireLock(scheduledJob, context, it) }
        }
    } catch (e: Exception) {
        logger.error("Failed to acquireLock for job ${scheduledJob.name}", e)
    }
    return lock
}

/**
 * Util method to attempt to release the requested lock.
 * Returns a boolean of the success of the release lock
 */
suspend fun releaseLockForScheduledJob(context: JobExecutionContext, lock: LockModel): Boolean {
    var released = false
    try {
        released = context.lockService.suspendUntil { release(lock, it) }
        if (!released) {
            logger.warn("Could not release lock for job ${lock.jobId}")
        }
    } catch (e: Exception) {
        logger.error("Failed to release lock for job ${lock.jobId}", e)
    }
    return released
}

/**
 * Util method to attempt to renew the requested lock using the backoff policy.
 * If failed to renew the lock using backoff policy will return a null lock otherwise returns renewed lock.
 */
suspend fun renewLockForScheduledJob(context: JobExecutionContext, lock: LockModel, backoffPolicy: BackoffPolicy): LockModel? {
    var updatedLock: LockModel? = null
    try {
        backoffPolicy.retry(logger) {
            updatedLock = context.lockService.suspendUntil { renewLock(lock, it) }
        }
    } catch (e: Exception) {
        logger.warn("Failed trying to renew lock on $lock, releasing the existing lock", e)
    }

    return updatedLock
}
