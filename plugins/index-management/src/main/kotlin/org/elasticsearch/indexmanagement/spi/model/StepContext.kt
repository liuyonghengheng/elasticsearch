/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.spi.indexstatemanagement.model

import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.jobscheduler.spi.utils.LockService
import org.elasticsearch.script.ScriptService

class StepContext(
    val metadata: ManagedIndexMetaData,
    val clusterService: ClusterService,
    val client: Client,
    val threadContext: ThreadContext?,
    val user: User?,
    val scriptService: ScriptService,
    val settings: Settings,
    val lockService: LockService
) {
    fun getUpdatedContext(metadata: ManagedIndexMetaData): StepContext {
        return StepContext(metadata, this.clusterService, this.client, this.threadContext, this.user, this.scriptService, this.settings, this.lockService)
    }
}
