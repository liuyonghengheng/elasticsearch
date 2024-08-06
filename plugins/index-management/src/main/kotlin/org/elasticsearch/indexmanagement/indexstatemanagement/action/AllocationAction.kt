/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.indexmanagement.indexstatemanagement.step.allocation.AttemptAllocationStep
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Step
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class AllocationAction(
    val require: Map<String, String>,
    val include: Map<String, String>,
    val exclude: Map<String, String>,
    val waitFor: Boolean = false,
    index: Int
) : Action(name, index) {

    init {
        require(require.isNotEmpty() || include.isNotEmpty() || exclude.isNotEmpty()) { "At least one allocation parameter need to be specified." }
    }

    private val attemptAllocationStep = AttemptAllocationStep(this)

    private val steps = listOf(attemptAllocationStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptAllocationStep
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(REQUIRE, require)
        builder.field(INCLUDE, include)
        builder.field(EXCLUDE, exclude)
        builder.field(WAIT_FOR, waitFor)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeMap(require)
        out.writeMap(include)
        out.writeMap(exclude)
        out.writeBoolean(waitFor)
        out.writeInt(actionIndex)
    }

    companion object {
        const val name = "allocation"
        const val REQUIRE = "require"
        const val INCLUDE = "include"
        const val EXCLUDE = "exclude"
        const val WAIT_FOR = "wait_for"
    }
}
