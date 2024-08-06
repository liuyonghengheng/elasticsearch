/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.indexmanagement.indexstatemanagement.step.replicacount.AttemptReplicaCountStep
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Step
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ReplicaCountAction(
    val numOfReplicas: Int,
    index: Int
) : Action(name, index) {

    init {
        require(numOfReplicas >= 0) { "ReplicaCountAction number_of_replicas value must be a non-negative number" }
    }

    private val attemptReplicaCountStep = AttemptReplicaCountStep(this)
    private val steps = listOf(attemptReplicaCountStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptReplicaCountStep
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(NUMBER_OF_REPLICAS_FIELD, numOfReplicas)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeInt(numOfReplicas)
        out.writeInt(actionIndex)
    }

    companion object {
        const val NUMBER_OF_REPLICAS_FIELD = "number_of_replicas"
        const val name = "replica_count"
    }
}
