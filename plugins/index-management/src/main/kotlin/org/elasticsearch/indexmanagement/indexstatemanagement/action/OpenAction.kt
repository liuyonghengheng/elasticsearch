/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.indexmanagement.indexstatemanagement.step.open.AttemptOpenStep
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Step
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class OpenAction(
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "open"
    }
    private val attemptOpenStep = AttemptOpenStep()
    private val steps = listOf(attemptOpenStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptOpenStep
    }

    override fun getSteps(): List<Step> = steps
}
