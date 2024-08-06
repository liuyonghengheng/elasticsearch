/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.indexmanagement.indexstatemanagement.step.readonly.SetReadOnlyStep
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Step
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ReadOnlyAction(
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "read_only"
    }
    private val setReadOnlyStep = SetReadOnlyStep()
    private val steps = listOf(setReadOnlyStep)

    override fun getStepToExecute(context: StepContext): Step {
        return setReadOnlyStep
    }

    override fun getSteps(): List<Step> = steps
}
