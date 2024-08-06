/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.indexmanagement.indexstatemanagement.step.delete.AttemptDeleteStep
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Step
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class DeleteAction(
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "delete"
    }

    private val attemptDeleteStep = AttemptDeleteStep()
    private val steps = listOf(attemptDeleteStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptDeleteStep
    }

    override fun getSteps(): List<Step> = steps
}
