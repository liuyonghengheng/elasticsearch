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

package org.elasticsearch.replication.task.autofollow

import org.elasticsearch.replication.ReplicationSettings
import org.elasticsearch.replication.metadata.ReplicationMetadataManager
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.persistent.AllocatedPersistentTask
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.elasticsearch.persistent.PersistentTasksExecutor
import org.elasticsearch.tasks.TaskId
import org.elasticsearch.threadpool.ThreadPool

class AutoFollowExecutor(executor: String, private val clusterService: ClusterService,
                         private val threadPool: ThreadPool, private val client: Client,
                         private val replicationMetadataManager: ReplicationMetadataManager,
                         private val replicationSettings: ReplicationSettings) :
    PersistentTasksExecutor<AutoFollowParams>(TASK_NAME, executor) {

    companion object {
        const val TASK_NAME = "cluster:admin/plugins/replication/autofollow"
    }

    override fun nodeOperation(task: AllocatedPersistentTask, params: AutoFollowParams, state: PersistentTaskState?) {
        if (task is AutoFollowTask) {
            task.run()
        } else {
            task.markAsFailed(IllegalArgumentException("unknown task type : ${task::class.java}"))
        }
    }

    override fun createTask(id: Long, type: String, action: String, parentTaskId: TaskId,
                            taskInProgress: PersistentTask<AutoFollowParams>,
                            headers: Map<String, String>): AllocatedPersistentTask {
        return AutoFollowTask(id, type, action, getDescription(taskInProgress),
                parentTaskId, headers, executor, clusterService, threadPool, client,
                replicationMetadataManager, taskInProgress.params!!, replicationSettings)
    }

    override fun getDescription(taskInProgress: PersistentTask<AutoFollowParams>): String {
        return "replication auto follow task for leader cluster: ${taskInProgress.params?.leaderCluster} with pattern " +
                "${taskInProgress.params?.patternName}"
    }
}
