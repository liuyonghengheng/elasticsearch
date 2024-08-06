/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.indexmanagement.indexstatemanagement.step.close.AttemptCloseStep
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Step
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class CloseAction(
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "close"
    }
    private val attemptCloseStep = AttemptCloseStep()

    private val steps = listOf(attemptCloseStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptCloseStep
    }

    override fun getSteps(): List<Step> = steps
}
