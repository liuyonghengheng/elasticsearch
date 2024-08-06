/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.indexmanagement.indexstatemanagement.step.readwrite.SetReadWriteStep
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Step
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ReadWriteAction(
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "read_write"
    }

    private val setReadWriteStep = SetReadWriteStep()
    private val steps = listOf(setReadWriteStep)

    override fun getStepToExecute(context: StepContext): Step {
        return setReadWriteStep
    }

    override fun getSteps(): List<Step> = steps
}
