/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.api.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext
import org.elasticsearch.commons.ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.index.engine.VersionConflictEngineException
import org.elasticsearch.indexmanagement.util.IndexManagementException
import org.elasticsearch.indexmanagement.util.SecurityUtils
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

abstract class BaseTransportAction<Request : ActionRequest, Response : ActionResponse>(
    name: String,
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    requestReader: Writeable.Reader<Request>,
) : HandledTransportAction<Request, Response>(
    name, transportService, actionFilters, requestReader
) {

    private val log = LogManager.getLogger(javaClass)
    private val coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.IO)

    override fun doExecute(
        task: Task,
        request: Request,
        listener: ActionListener<Response>
    ) {
        log.debug(
            "user and roles string from thread context: " +
                client.threadPool().threadContext.getTransient<String>(_SECURITY_USER_INFO_THREAD_CONTEXT)
        )
        val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext)
        coroutineScope.launch {
            try {
                client.threadPool().threadContext.stashContext().use { threadContext ->
                    listener.onResponse(executeRequest(request, user, threadContext))
                }
            } catch (ex: IndexManagementException) {
                listener.onFailure(ex)
            } catch (ex: VersionConflictEngineException) {
                listener.onFailure(ex)
            } catch (ex: ElasticsearchStatusException) {
                listener.onFailure(ex)
            } catch (ex: Exception) {
                log.error("Uncaught exception:", ex)
                listener.onFailure(ElasticsearchStatusException(ex.message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }
    }

    abstract suspend fun executeRequest(request: Request, user: User?, threadContext: StoredContext): Response
}
