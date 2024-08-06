/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ClusterStateListener
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.component.LifecycleListener
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.indexmanagement.util.OpenForTesting
import org.elasticsearch.threadpool.Scheduler
import org.elasticsearch.threadpool.ThreadPool

class PluginVersionSweepCoordinator(
    private val skipExecution: SkipExecution,
    settings: Settings,
    private val threadPool: ThreadPool,
    clusterService: ClusterService,
) : CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("ISMPluginSweepCoordinator")),
    LifecycleListener(),
    ClusterStateListener {
    private val logger = LogManager.getLogger(javaClass)

    private var scheduledSkipExecution: Scheduler.Cancellable? = null

    @Volatile
    private var sweepSkipPeriod = ManagedIndexSettings.SWEEP_SKIP_PERIOD.get(settings)

    @Volatile
    private var indexStateManagementEnabled = ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.get(settings)

    init {
        clusterService.addLifecycleListener(this)
        clusterService.addListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.SWEEP_SKIP_PERIOD) {
            sweepSkipPeriod = it
            initBackgroundSweepISMPluginVersionExecution()
        }
    }

    override fun afterStart() {
        initBackgroundSweepISMPluginVersionExecution()
    }

    override fun beforeStop() {
        scheduledSkipExecution?.cancel()
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        if (event.nodesChanged() || event.isNewCluster) {
            skipExecution.sweepISMPluginVersion()
            initBackgroundSweepISMPluginVersionExecution()
        }
    }

    @OpenForTesting
    fun initBackgroundSweepISMPluginVersionExecution() {
        // If ISM is disabled return early
        if (!isIndexStateManagementEnabled()) return
        // Cancel existing background sweep
        scheduledSkipExecution?.cancel()
        val scheduledJob = Runnable {
            launch {
                try {
                    if (!skipExecution.flag) {
                        logger.info("Canceling sweep ism plugin version job")
                        scheduledSkipExecution?.cancel()
                    } else {
                        skipExecution.sweepISMPluginVersion()
                    }
                } catch (e: Exception) {
                    logger.error("Failed to sweep ism plugin version", e)
                }
            }
        }
        scheduledSkipExecution =
            threadPool.scheduleWithFixedDelay(scheduledJob, sweepSkipPeriod, ThreadPool.Names.MANAGEMENT)
    }

    private fun isIndexStateManagementEnabled(): Boolean = indexStateManagementEnabled == true
}
