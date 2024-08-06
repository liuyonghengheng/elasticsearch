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

package org.elasticsearch.replication.action.update
import org.elasticsearch.replication.metadata.store.KEY_SETTINGS
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.IndicesRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.settings.Settings.readSettingsFromStream
import org.elasticsearch.common.xcontent.*
import java.io.IOException
import java.util.*


class UpdateIndexReplicationRequest : AcknowledgedRequest<UpdateIndexReplicationRequest>, IndicesRequest.Replaceable, ToXContentObject {

    lateinit var indexName: String
    lateinit var settings: Settings

    constructor(indexName: String, settings: Settings) {
        this.indexName = indexName
        this.settings = settings
    }

    constructor(inp: StreamInput): super(inp) {
        indexName = inp.readString()
        settings = readSettingsFromStream(inp)
    }

    private constructor() {
    }

    companion object {
        private val PARSER = ObjectParser<UpdateIndexReplicationRequest, Void>("FollowIndexRequestParser") { UpdateIndexReplicationRequest() }

        init {
            PARSER.declareString(UpdateIndexReplicationRequest::indexName::set, ParseField("indexName"))
        }

        @Suppress("UNUSED_PARAMETER")
        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser, followerIndex: String): UpdateIndexReplicationRequest {
            throw IOException("Not supported for $this")
        }
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    override fun indices(vararg indices: String?): IndicesRequest {
        return this
    }

    override fun indices(): Array<String> {
        return arrayOf(indexName)
    }

    override fun indicesOptions(): IndicesOptions {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed()
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("indexName", indexName)
        builder.endObject()

        builder.startObject(KEY_SETTINGS)
        settings.toXContent(builder, ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
        builder.endObject()

        return builder
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(indexName)
        Settings.writeSettingsToStream(settings, out);
    }
}
