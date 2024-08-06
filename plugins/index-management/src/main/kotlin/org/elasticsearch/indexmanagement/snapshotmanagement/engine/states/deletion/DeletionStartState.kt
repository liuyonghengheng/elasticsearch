/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.snapshotmanagement.engine.states.deletion

import org.elasticsearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.elasticsearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.elasticsearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.elasticsearch.indexmanagement.snapshotmanagement.engine.states.State
import org.elasticsearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMMetadata

object DeletionStartState : State {

    override val continuous: Boolean = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val metadataToSave = SMMetadata.Builder(context.metadata)
            .workflow(WorkflowType.DELETION)
            .setCurrentState(SMState.DELETION_START)

        return SMResult.Next(metadataToSave)
    }
}
