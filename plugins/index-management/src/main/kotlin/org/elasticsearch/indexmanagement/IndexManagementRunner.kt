/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement

import org.apache.logging.log4j.LogManager
import org.elasticsearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.elasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.elasticsearch.indexmanagement.rollup.RollupRunner
import org.elasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.indexmanagement.snapshotmanagement.SMRunner
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.elasticsearch.indexmanagement.transform.TransformRunner
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.jobscheduler.spi.JobExecutionContext
import org.elasticsearch.jobscheduler.spi.ScheduledJobParameter
import org.elasticsearch.jobscheduler.spi.ScheduledJobRunner

object IndexManagementRunner : ScheduledJobRunner {

    private val logger = LogManager.getLogger(javaClass)

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        when (job) {
            is ManagedIndexConfig -> ManagedIndexRunner.runJob(job, context)
            is Rollup -> RollupRunner.runJob(job, context)
            is Transform -> TransformRunner.runJob(job, context)
            is SMPolicy -> SMRunner.runJob(job, context)
            else -> {
                val errorMessage = "Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}"
                logger.error(errorMessage)
                throw IllegalArgumentException(errorMessage)
            }
        }
    }
}
