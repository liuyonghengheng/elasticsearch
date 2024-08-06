package org.elasticsearch.indexmanagement.indexstatemanagement.action

import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.indexmanagement.indexstatemanagement.step.delete.WaitForSnapshotPolicyStep
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Step
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class WaitForSnapshotPolicyAction(
    val policy: String,
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "wait_for_snapshot"
        const val POLICY_FIELD = "policy"
    }

    private val waitForSnapshotPolicyStep = WaitForSnapshotPolicyStep(this)
    private val steps = listOf(waitForSnapshotPolicyStep)

    override fun getStepToExecute(context: StepContext): Step {
        return waitForSnapshotPolicyStep
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(POLICY_FIELD, policy)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeString(policy)
        out.writeInt(actionIndex)
    }
}
