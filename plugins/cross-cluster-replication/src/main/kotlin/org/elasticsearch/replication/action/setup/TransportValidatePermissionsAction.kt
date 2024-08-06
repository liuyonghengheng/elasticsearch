/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.elasticsearch.replication.action.setup

import org.elasticsearch.replication.util.completeWith
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

class TransportValidatePermissionsAction @Inject constructor(transportService: TransportService,
                                                             val threadPool: ThreadPool,
                                                             actionFilters: ActionFilters,
                                                             private val client : Client) :
        HandledTransportAction<ValidatePermissionsRequest, AcknowledgedResponse>(ValidatePermissionsAction.NAME,
                transportService, actionFilters, ::ValidatePermissionsRequest) {


    companion object {
        private val log = LogManager.getLogger(TransportValidatePermissionsAction::class.java)
    }

    override fun doExecute(task: Task, request: ValidatePermissionsRequest, listener: ActionListener<AcknowledgedResponse>) {
        log.info("Replication setup - Permissions validation successful for Index - ${request.index} and role ${request.clusterRole}")
        listener.completeWith { AcknowledgedResponse(true) }
    }

}
