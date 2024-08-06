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

package org.elasticsearch.replication.repository

import org.elasticsearch.replication.action.repository.GetFileChunkAction
import org.elasticsearch.replication.action.repository.GetFileChunkRequest
import org.elasticsearch.replication.metadata.store.ReplicationMetadata
import org.elasticsearch.replication.util.coroutineContext
import org.elasticsearch.replication.util.suspendExecuteWithRetries
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.logging.log4j.Logger
import org.elasticsearch.action.ActionListener
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.index.store.Store
import org.elasticsearch.index.store.StoreFileMetadata
import org.elasticsearch.indices.recovery.MultiChunkTransfer
import org.elasticsearch.indices.recovery.MultiFileWriter
import org.elasticsearch.indices.recovery.RecoveryState

class RemoteClusterMultiChunkTransfer(val logger: Logger,
                                      val followerClusterName: String,
                                      threadContext: ThreadContext,
                                      val localStore: Store,
                                      maxConcurrentFileChunks: Int,
                                      val restoreUUID: String,
                                      val replMetadata: ReplicationMetadata,
                                      val leaderNode: DiscoveryNode,
                                      val leaderShardId: ShardId,
                                      val remoteFiles: List<StoreFileMetadata>,
                                      val leaderClusterClient: Client,
                                      val recoveryState: RecoveryState,
                                      val chunkSize: ByteSizeValue,
                                      listener: ActionListener<Void>) :
        MultiChunkTransfer<StoreFileMetadata, RemoteClusterRepositoryFileChunk>(logger,
                threadContext, listener, maxConcurrentFileChunks, remoteFiles), CoroutineScope by GlobalScope {

    private var offset = 0L
    private val tempFilePrefix = "${RESTORE_SHARD_TEMP_FILE_PREFIX}${restoreUUID}."
    private val multiFileWriter = MultiFileWriter(localStore, recoveryState.index, tempFilePrefix, logger) {}
    private val mutex = Mutex()

    init {
        // Add all the available files to show the recovery status
        for (fileMetadata in remoteFiles) {
            recoveryState.index.addFileDetail(fileMetadata.name(), fileMetadata.length(), false)
        }
        recoveryState.index.setFileDetailsComplete()
    }

    companion object {
        const val RESTORE_SHARD_TEMP_FILE_PREFIX = "CLUSTER_REPO_TEMP_"
    }

    override fun handleError(md: StoreFileMetadata, e: Exception) {
        logger.error("Error while transferring segments $e")
    }

    override fun onNewResource(md: StoreFileMetadata) {
        // Reset the values for the next file
        offset = 0L
    }

    override fun executeChunkRequest(request: RemoteClusterRepositoryFileChunk, listener: ActionListener<Void>) {
        val getFileChunkRequest = GetFileChunkRequest(restoreUUID, leaderNode, leaderShardId, request.storeFileMetadata,
                request.offset, request.length, followerClusterName, recoveryState.shardId)

        launch(Dispatchers.IO + leaderClusterClient.threadPool().coroutineContext()) {
            try {
                val response = leaderClusterClient.suspendExecuteWithRetries(replMetadata, GetFileChunkAction.INSTANCE,
                        getFileChunkRequest, log = logger)
                logger.debug("Filename: ${request.storeFileMetadata.name()}, " +
                        "response_size: ${response.data.length()}, response_offset: ${response.offset}")
                mutex.withLock {
                    multiFileWriter.writeFileChunk(response.storeFileMetadata, response.offset, response.data, request.lastChunk())
                    listener.onResponse(null)
                }
            } catch (e: Exception) {
                logger.error("Failed to fetch file chunk for ${request.storeFileMetadata.name()} with offset ${request.offset}: $e")
                listener.onFailure(e)
            }
        }

    }

    @Suppress("DEPRECATION")
    override fun nextChunkRequest(md: StoreFileMetadata): RemoteClusterRepositoryFileChunk {
        val chunkReq = RemoteClusterRepositoryFileChunk(md, offset, chunkSize.bytesAsInt())
        offset += chunkSize.bytesAsInt()
        return chunkReq
    }

    override fun close() {
        multiFileWriter.renameAllTempFiles()
        multiFileWriter.close()
    }
}
