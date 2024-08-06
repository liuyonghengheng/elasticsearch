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

package org.elasticsearch.replication.action.replicationstatedetails

import org.elasticsearch.replication.metadata.UpdateReplicationStateDetailsTaskExecutor
import org.elasticsearch.replication.util.completeWith
import org.elasticsearch.replication.util.coroutineContext
import org.elasticsearch.replication.util.submitClusterStateUpdateTask
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateTaskExecutor
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

class TransportUpdateReplicationStateDetails @Inject constructor(transportService: TransportService,
                                                                 clusterService: ClusterService,
                                                                 threadPool: ThreadPool,
                                                                 actionFilters: ActionFilters,
                                                                 indexNameExpressionResolver: IndexNameExpressionResolver) :
        TransportMasterNodeAction<UpdateReplicationStateDetailsRequest, AcknowledgedResponse>(UpdateReplicationStateAction.NAME,
                transportService, clusterService, threadPool, actionFilters, ::UpdateReplicationStateDetailsRequest, indexNameExpressionResolver),
        CoroutineScope by GlobalScope {

    override fun checkBlock(request: UpdateReplicationStateDetailsRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks.globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun masterOperation(request: UpdateReplicationStateDetailsRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {

        launch(threadPool.coroutineContext(ThreadPool.Names.MANAGEMENT)) {
            listener.completeWith {
                submitClusterStateUpdateTask(request, UpdateReplicationStateDetailsTaskExecutor.INSTANCE
                        as ClusterStateTaskExecutor<AcknowledgedRequest<UpdateReplicationStateDetailsRequest>>,
                        clusterService,
                        "update-replication-state-params")
                AcknowledgedResponse(true)
            }
        }
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    override fun read(inp: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(inp)
    }
}
