/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.elasticsearch.replication.task.autofollow

import org.elasticsearch.Version
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.persistent.PersistentTaskParams
import java.io.IOException

class AutoFollowParams : PersistentTaskParams {

    lateinit var leaderCluster: String
    lateinit var patternName: String

    companion object {
        const val NAME = AutoFollowExecutor.TASK_NAME

        private val PARSER = ObjectParser<AutoFollowParams, Void>(NAME, true) { AutoFollowParams() }
        init {
            PARSER.declareString(AutoFollowParams::leaderCluster::set, ParseField("leader_cluster"))
            PARSER.declareString(AutoFollowParams::patternName::set, ParseField("pattern_name"))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser): AutoFollowParams {
            return PARSER.parse(parser, null)
        }
    }

    private constructor() {
    }

    constructor(leaderCluster: String, patternName: String) {
        this.leaderCluster = leaderCluster
        this.patternName = patternName
    }

    constructor(inp: StreamInput) : this(inp.readString(), inp.readString())

    override fun writeTo(out: StreamOutput) {
        out.writeString(leaderCluster)
        out.writeString(patternName)
    }

    override fun getWriteableName() = NAME

    override fun getMinimalSupportedVersion() = Version.V_1_0_0

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("leader_cluster", leaderCluster)
            .field("pattern_name", patternName)
            .endObject()
    }
}
