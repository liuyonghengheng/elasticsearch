/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import org.elasticsearch.indexmanagement.indexstatemanagement.step.rollup.WaitForRollupCompletionStep
import org.elasticsearch.indexmanagement.rollup.model.ISMRollup
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Step
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class RollupAction(
    val ismRollup: ISMRollup,
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "rollup"
        const val ISM_ROLLUP_FIELD = "ism_rollup"
    }

    private val attemptCreateRollupJobStep = AttemptCreateRollupJobStep(this)
    private val waitForRollupCompletionStep = WaitForRollupCompletionStep()
    private val steps = listOf(attemptCreateRollupJobStep, waitForRollupCompletionStep)

    @Suppress("ReturnCount")
    override fun getStepToExecute(context: StepContext): Step {
        // If stepMetaData is null, return the first step
        val stepMetaData = context.metadata.stepMetaData ?: return attemptCreateRollupJobStep

        // If the current step has completed, return the next step
        if (stepMetaData.stepStatus == Step.StepStatus.COMPLETED) {
            return when (stepMetaData.name) {
                AttemptCreateRollupJobStep.name -> waitForRollupCompletionStep
                else -> attemptCreateRollupJobStep
            }
        }

        return when (stepMetaData.name) {
            AttemptCreateRollupJobStep.name -> attemptCreateRollupJobStep
            else -> waitForRollupCompletionStep
        }
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(ISM_ROLLUP_FIELD, ismRollup)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        ismRollup.writeTo(out)
        out.writeInt(actionIndex)
    }
}
