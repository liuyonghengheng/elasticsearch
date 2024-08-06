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

package org.elasticsearch.replication.action.index

import org.elasticsearch.replication.metadata.store.KEY_SETTINGS
import org.elasticsearch.replication.util.ValidationUtil.validateName
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.IndicesRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import java.io.IOException
import java.util.Collections
import java.util.function.BiConsumer
import kotlin.collections.HashMap

class ReplicateIndexRequest : AcknowledgedRequest<ReplicateIndexRequest>, IndicesRequest.Replaceable, ToXContentObject {

    lateinit var followerIndex: String
    lateinit var leaderAlias: String
    lateinit var leaderIndex: String
    var useRoles: HashMap<String, String>? = null // roles to use - {leader_fgac_role: role1, follower_fgac_role: role2}
    // Used for integ tests to wait until the restore from leader cluster completes
    var waitForRestore: Boolean = false
    // Triggered from autofollow to skip permissions check based on user as this is already validated
    var isAutoFollowRequest: Boolean = false

    var settings :Settings = Settings.EMPTY

    private constructor() {
    }

    constructor(followerIndex: String, leaderAlias: String, leaderIndex: String, settings: Settings = Settings.EMPTY) : super() {
        this.followerIndex = followerIndex
        this.leaderAlias = leaderAlias
        this.leaderIndex = leaderIndex
        this.settings = settings
    }

    companion object {
        const val LEADER_CLUSTER_ROLE = "leader_cluster_role"
        const val FOLLOWER_CLUSTER_ROLE = "follower_cluster_role"
        private val INDEX_REQ_PARSER = ObjectParser<ReplicateIndexRequest, Void>("FollowIndexRequestParser") { ReplicateIndexRequest() }
        val FGAC_ROLES_PARSER = ObjectParser<HashMap<String, String>, Void>("UseRolesParser") { HashMap() }
        init {
            FGAC_ROLES_PARSER.declareStringOrNull({useRoles: HashMap<String, String>, role: String -> useRoles[LEADER_CLUSTER_ROLE] = role},
                    ParseField(LEADER_CLUSTER_ROLE))
            FGAC_ROLES_PARSER.declareStringOrNull({useRoles: HashMap<String, String>, role: String -> useRoles[FOLLOWER_CLUSTER_ROLE] = role},
                    ParseField(FOLLOWER_CLUSTER_ROLE))

            INDEX_REQ_PARSER.declareString(ReplicateIndexRequest::leaderAlias::set, ParseField("leader_alias"))
            INDEX_REQ_PARSER.declareString(ReplicateIndexRequest::leaderIndex::set, ParseField("leader_index"))
            INDEX_REQ_PARSER.declareObjectOrDefault(BiConsumer {reqParser: ReplicateIndexRequest, roles: HashMap<String, String> -> reqParser.useRoles = roles},
                    FGAC_ROLES_PARSER, null, ParseField("use_roles"))
            INDEX_REQ_PARSER.declareObjectOrDefault(
                { request: ReplicateIndexRequest, settings: Settings -> request.settings = settings},
                { p: XContentParser?, _: Void? -> Settings.fromXContent(p) },
                    null, ParseField(KEY_SETTINGS))
        }

        @Throws(IOException::class)
        fun fromXContent(parser: XContentParser, followerIndex: String): ReplicateIndexRequest {
            val followIndexRequest = INDEX_REQ_PARSER.parse(parser, null)
            followIndexRequest.followerIndex = followerIndex
            if(followIndexRequest.useRoles?.size == 0) {
                followIndexRequest.useRoles = null
            }

            return followIndexRequest
        }
    }

    override fun validate(): ActionRequestValidationException? {

        var validationException = ActionRequestValidationException()
        if (!this::leaderAlias.isInitialized ||
            !this::leaderIndex.isInitialized ||
            !this::followerIndex.isInitialized) {
            validationException.addValidationError("Mandatory params are missing for the request")
        }

        validateName(leaderIndex, validationException)
        validateName(followerIndex, validationException)

        if(useRoles != null && (useRoles!!.size < 2 || useRoles!![LEADER_CLUSTER_ROLE] == null ||
                useRoles!![FOLLOWER_CLUSTER_ROLE] == null)) {
            validationException.addValidationError("Need roles for $LEADER_CLUSTER_ROLE and $FOLLOWER_CLUSTER_ROLE")
        }
        return if(validationException.validationErrors().isEmpty()) return null else validationException
    }

    override fun indices(vararg indices: String?): IndicesRequest {
        return this
    }

    override fun indices(): Array<String?> {
        return arrayOf(followerIndex)
    }

    override fun indicesOptions(): IndicesOptions {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed()
    }

    constructor(inp: StreamInput) : super(inp) {
        leaderAlias = inp.readString()
        leaderIndex = inp.readString()
        followerIndex = inp.readString()

        var leaderClusterRole = inp.readOptionalString()
        var followerClusterRole = inp.readOptionalString()
        useRoles = HashMap()
        if(leaderClusterRole != null) useRoles!![LEADER_CLUSTER_ROLE] = leaderClusterRole
        if(followerClusterRole != null) useRoles!![FOLLOWER_CLUSTER_ROLE] = followerClusterRole

        waitForRestore = inp.readBoolean()
        isAutoFollowRequest = inp.readBoolean()
        settings = Settings.readSettingsFromStream(inp)

    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(leaderAlias)
        out.writeString(leaderIndex)
        out.writeString(followerIndex)
        out.writeOptionalString(useRoles?.get(LEADER_CLUSTER_ROLE))
        out.writeOptionalString(useRoles?.get(FOLLOWER_CLUSTER_ROLE))
        out.writeBoolean(waitForRestore)
        out.writeBoolean(isAutoFollowRequest)

        Settings.writeSettingsToStream(settings, out);
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params): XContentBuilder {
        builder.startObject()
        builder.field("leader_alias", leaderAlias)
        builder.field("leader_index", leaderIndex)
        builder.field("follower_index", followerIndex)
        if(useRoles != null && useRoles!!.size == 2) {
            builder.field("use_roles")
            builder.startObject()
            builder.field(LEADER_CLUSTER_ROLE, useRoles!![LEADER_CLUSTER_ROLE])
            builder.field(FOLLOWER_CLUSTER_ROLE, useRoles!![FOLLOWER_CLUSTER_ROLE])
            builder.endObject()
        }
        builder.field("wait_for_restore", waitForRestore)
        builder.field("is_autofollow_request", isAutoFollowRequest)

        builder.startObject(KEY_SETTINGS)
        settings.toXContent(builder, ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
        builder.endObject()

        builder.endObject()

        return builder
    }
}
