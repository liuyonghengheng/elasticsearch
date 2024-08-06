/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.managedIndex

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

/**
 * This is a non operational transport action that is used by ISM to check if the user has required index permissions to manage index
 */
class TransportManagedIndexAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
) : HandledTransportAction<ManagedIndexRequest, AcknowledgedResponse>(
    ManagedIndexAction.NAME, transportService, actionFilters, ::ManagedIndexRequest
) {

    override fun doExecute(task: Task, request: ManagedIndexRequest, listener: ActionListener<AcknowledgedResponse>) {
        // Do nothing
        return listener.onResponse(AcknowledgedResponse(true))
    }
}
