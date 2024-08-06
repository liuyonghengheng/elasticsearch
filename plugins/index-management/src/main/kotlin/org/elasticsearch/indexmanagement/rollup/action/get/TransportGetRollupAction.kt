/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.rollup.action.get

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.indexmanagement.rollup.util.parseRollup
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.util.SecurityUtils
import org.elasticsearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import java.lang.Exception

class TransportGetRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val settings: Settings,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetRollupRequest, GetRollupResponse> (
    GetRollupAction.NAME, transportService, actionFilters, ::GetRollupRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    @Suppress("ReturnCount")
    override fun doExecute(task: Task, request: GetRollupRequest, listener: ActionListener<GetRollupResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id).preference(request.preference)
        val user = buildUser(client.threadPool().threadContext)
        client.threadPool().threadContext.stashContext().use {
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            return listener.onFailure(ElasticsearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                        }

                        val rollup: Rollup?
                        try {
                            rollup = parseRollup(response, xContentRegistry)
                        } catch (e: IllegalArgumentException) {
                            listener.onFailure(ElasticsearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                            return
                        }
                        if (!SecurityUtils.userHasPermissionForResource(user, rollup.user, filterByEnabled, "rollup", request.id, listener)) {
                            return
                        } else {
                            // if HEAD request don't return the rollup
                            val rollupResponse = if (request.srcContext != null && !request.srcContext.fetchSource()) {
                                GetRollupResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, null)
                            } else {
                                GetRollupResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, rollup)
                            }
                            listener.onResponse(rollupResponse)
                        }
                    }

                    override fun onFailure(e: Exception) {
                        listener.onFailure(e)
                    }
                }
            )
        }
    }
}
