/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.model

import org.elasticsearch.cluster.ClusterModule
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.*
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.commons.authuser.User
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.elasticsearch.indexmanagement.indexstatemanagement.util.WITH_USER
import org.elasticsearch.indexmanagement.opensearchapi.instant
import org.elasticsearch.indexmanagement.opensearchapi.optionalISMTemplateField
import org.elasticsearch.indexmanagement.opensearchapi.optionalTimeField
import org.elasticsearch.indexmanagement.opensearchapi.optionalUserField
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.indexmanagement.util.IndexUtils
import org.elasticsearch.indexmanagement.util.NO_ID
import java.io.IOException
import java.time.Instant
import java.util.*

data class Policy(
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val description: String,
    val schemaVersion: Long,
    val lastUpdatedTime: Instant,
    val errorNotification: ErrorNotification?,
    val defaultState: String,
    val states: List<State>,
    val ismTemplate: List<ISMTemplate>? = null,
    val user: User? = null
) : ToXContentObject, Writeable {

    init {
        val distinctStateNames = states.map { it.name }.distinct()
        states.forEach { state ->
            state.transitions.forEach { transition ->
                require(distinctStateNames.contains(transition.stateName)) {
                    "Policy contains a transition in state=${state.name} pointing to a nonexistent state=${transition.stateName}"
                }
            }
        }
        require(distinctStateNames.size == states.size) { "Policy cannot have duplicate state names" }
        require(states.isNotEmpty()) { "Policy must contain at least one State" }
        requireNotNull(states.find { it.name == defaultState }) { "Policy must have a valid default state" }
    }

    fun toXContent(builder: XContentBuilder): XContentBuilder {
        return toXContent(builder, ToXContent.EMPTY_PARAMS)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(POLICY_TYPE)
        builder.field(POLICY_ID_FIELD, id)
            .field(DESCRIPTION_FIELD, "")
            .optionalTimeField(LAST_UPDATED_TIME_FIELD, lastUpdatedTime)
            .field(SCHEMA_VERSION_FIELD, schemaVersion)
            // .field(ERROR_NOTIFICATION_FIELD, errorNotification)
            .field(DEFAULT_STATE_FIELD, defaultState)
            .startArray(STATES_FIELD)
            .also { states.forEach { state -> state.toXContent(it, params) } }
            .endArray()
//        builder.startObject(STATES_FIELD)
//        for (state in states) {
//            builder.field(state.name, state)
//        }
//        builder.endObject()
        builder.optionalISMTemplateField(ISM_TEMPLATE, ismTemplate)
        if (params.paramAsBoolean(WITH_USER, true)) builder.optionalUserField(USER_FIELD, user)
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        return builder.endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        description = sin.readString(),
        schemaVersion = sin.readLong(),
        lastUpdatedTime = sin.readInstant(),
        errorNotification = sin.readOptionalWriteable(::ErrorNotification),
        defaultState = sin.readString(),
        states = sin.readList(::State),
        ismTemplate = if (sin.readBoolean()) {
            sin.readList(::ISMTemplate)
        } else null,
        user = if (sin.readBoolean()) {
            User(sin)
        } else null
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeString(description)
        out.writeLong(schemaVersion)
        out.writeInstant(lastUpdatedTime)
        out.writeOptionalWriteable(errorNotification)
        out.writeString(defaultState)
        out.writeList(states)
        if (ismTemplate != null) {
            out.writeBoolean(true)
            out.writeList(ismTemplate)
        } else {
            out.writeBoolean(false)
        }
        out.writeBoolean(user != null)
        user?.writeTo(out)
    }

    /**
     * Disallowed actions are ones that are not specified in the [ManagedIndexSettings.ALLOW_LIST] setting.
     */
    fun getDisallowedActions(allowList: List<String>): List<String> {
        val allowListSet = allowList.toSet()
        val disallowedActions = mutableListOf<String>()
        this.states.forEach { state ->
            state.actions.forEach { actionConfig ->
                if (!allowListSet.contains(actionConfig.type)) {
                    disallowedActions.add(actionConfig.type)
                }
            }
        }
        return disallowedActions.distinct()
    }

    fun getStateToExecute(managedIndexMetaData: ManagedIndexMetaData): State? {
        if (managedIndexMetaData.transitionTo != null) {
            return this.states.find { it.name == managedIndexMetaData.transitionTo }
        }
        return this.states.find {
            val stateMetaData = managedIndexMetaData.stateMetaData
            stateMetaData != null && it.name == stateMetaData.name
        }
    }

    companion object {
        const val POLICY_TYPE = "policy"
        const val POLICY_ID_FIELD = "policy_id"
        const val DESCRIPTION_FIELD = "description"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val SCHEMA_VERSION_FIELD = "schema_version"
        const val ERROR_NOTIFICATION_FIELD = "error_notification"
        const val DEFAULT_STATE_FIELD = "default_state"
        const val STATES_FIELD = "states"
        const val ISM_TEMPLATE = "ism_template"
        const val USER_FIELD = "user"

        const val PHASES_FIELD = "phases"

        const val HOT_PHASE = "hot"
        const val WARM_PHASE = "warm"
        const val COLD_PHASE = "cold"
        const val DELETE_PHASE = "delete"
        val ORDER_PHASES: List<String> = Arrays.asList(HOT_PHASE, WARM_PHASE, COLD_PHASE, DELETE_PHASE)


        @Suppress("ComplexMethod", "LongMethod", "NestedBlockDepth")
        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): Policy {
            var description: String? = null
            var defaultState: String? = null
            var errorNotification: ErrorNotification? = null
            var lastUpdatedTime: Instant? = null
            var schemaVersion: Long = IndexUtils.DEFAULT_SCHEMA_VERSION
            val states: MutableList<State> = mutableListOf()
            val ismTemplates: MutableList<ISMTemplate> = mutableListOf()
            var user: User? = null
            val mutableMap = sortStates(xcp)
            val builder = XContentFactory.jsonBuilder().map(mutableMap)
            val bytesReference = BytesReference.bytes(builder)
            val inputStream = bytesReference.streamInput()
            val xContentRegistry = NamedXContentRegistry(
                ClusterModule.getNamedXWriteables()
            )
            val parser = JsonXContent.jsonXContent.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                inputStream
            )
            parser.nextToken()

            ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser)
            while (parser.nextToken() != Token.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()

                when (fieldName) {
                    SCHEMA_VERSION_FIELD -> schemaVersion = parser.longValue()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = parser.instant()
                    POLICY_ID_FIELD -> { /* do nothing as this is an internal field */
                    }
                    DESCRIPTION_FIELD -> description = parser.text()
                    ERROR_NOTIFICATION_FIELD -> errorNotification =
                        if (parser.currentToken() == Token.VALUE_NULL) null else ErrorNotification.parse(parser)
                    DEFAULT_STATE_FIELD -> defaultState = parser.text()
                    STATES_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, parser.currentToken(), parser)
                        while (parser.nextToken() != Token.END_ARRAY) {
                            states.add(State.parse(parser))
                        }
                    }
                    ISM_TEMPLATE -> {
                        if (parser.currentToken() != Token.VALUE_NULL) {
                            when (parser.currentToken()) {
                                Token.START_ARRAY -> {
                                    while (parser.nextToken() != Token.END_ARRAY) {
                                        ismTemplates.add(ISMTemplate.parse(parser))
                                    }
                                }
                                Token.START_OBJECT -> {
                                    ismTemplates.add(ISMTemplate.parse(parser))
                                }
                                else -> ensureExpectedToken(Token.START_ARRAY, parser.currentToken(), parser)
                            }
                        }
                    }
                    USER_FIELD -> user = if (parser.currentToken() == Token.VALUE_NULL) null else User.parse(parser)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Policy.")
                }
            }

            return Policy(
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm,
                description = requireNotNull(description) { "$DESCRIPTION_FIELD is null" },
                schemaVersion = schemaVersion,
                lastUpdatedTime = lastUpdatedTime ?: Instant.now(),
                errorNotification = errorNotification,
                defaultState = requireNotNull(defaultState) { "$DEFAULT_STATE_FIELD is null" },
                states = states.toList(),
                ismTemplate = ismTemplates,
                user = user
            )
        }

        @Suppress("ComplexMethod", "LongMethod", "NestedBlockDepth")
        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse2(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): Policy {
            var description: String? = null
            var defaultState: String? = null
            var errorNotification: ErrorNotification? = null
            var lastUpdatedTime: Instant? = null
            var schemaVersion: Long = IndexUtils.DEFAULT_SCHEMA_VERSION
            val states: MutableList<State> = mutableListOf()
            val ismTemplates: MutableList<ISMTemplate> = mutableListOf()
            var user: User? = null

            val mutableMap = sortPhases(xcp)
            val builder = XContentFactory.jsonBuilder().map(mutableMap)
            val bytesReference = BytesReference.bytes(builder)
            val inputStream = bytesReference.streamInput()
            val xContentRegistry = NamedXContentRegistry(
                ClusterModule.getNamedXWriteables()
            )
            val parser = JsonXContent.jsonXContent.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                inputStream
            )
            parser.nextToken()
            ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser)
            while (parser.nextToken() != Token.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()

                when (fieldName) {
                    SCHEMA_VERSION_FIELD -> schemaVersion = parser.longValue()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = parser.instant()
                    POLICY_ID_FIELD -> { /* do nothing as this is an internal field */
                    }
                    // DESCRIPTION_FIELD -> description = xcp.text()
                    ERROR_NOTIFICATION_FIELD -> errorNotification =
                        if (parser.currentToken() == Token.VALUE_NULL) null else ErrorNotification.parse(parser)
                    DEFAULT_STATE_FIELD -> defaultState = parser.text()
                    STATES_FIELD, PHASES_FIELD -> {
                        ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser)
                        while (parser.nextToken() != Token.END_OBJECT) {
                            val stateName = parser.currentName()
                            parser.nextToken()
                            states.add(State.parse2(parser, stateName))
                            if (defaultState.isNullOrEmpty()) {
                                defaultState = stateName
                            }
                        }
                        val statesSize = states.size
                        if (statesSize >= 2) {
                            for (i in 1 until statesSize) {
                                if (states[i].minAge != null) {
                                    val cond = Conditions(states[i].minAge)
                                    val tr = Transition(
                                        stateName = states[i].name,
                                        conditions = cond
                                    )
                                    states[i - 1].transitions.clear()
                                    states[i - 1].transitions.add(tr)
                                }
                            }
                        }
                    }
//                    ISM_TEMPLATE -> {
//                        if (parser.currentToken() != Token.VALUE_NULL) {
//                            when (parser.currentToken()) {
//                                Token.START_ARRAY -> {
//                                    while (parser.nextToken() != Token.END_ARRAY) {
//                                        ismTemplates.add(ISMTemplate.parse(parser))
//                                    }
//                                }
//                                Token.START_OBJECT -> {
//                                    ismTemplates.add(ISMTemplate.parse(parser))
//                                }
//                                else -> ensureExpectedToken(Token.START_ARRAY, parser.currentToken(), parser)
//                            }
//                        }
//                    }
                    USER_FIELD -> user = if (parser.currentToken() == Token.VALUE_NULL) null else User.parse(parser)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Policy.")
                }
            }
            if (ismTemplates.isEmpty()) {
                val ismTemplate = ISMTemplate(
                    indexPatterns = mutableListOf(),
                    priority = 100,
                    lastUpdatedTime = lastUpdatedTime ?: Instant.now()
                )
                ismTemplates.add(ismTemplate)
            }
            return Policy(
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm,
                // description = requireNotNull(description) { "$DESCRIPTION_FIELD is null" },
                description = id,
                schemaVersion = schemaVersion,
                lastUpdatedTime = lastUpdatedTime ?: Instant.now(),
                errorNotification = errorNotification,
                defaultState = requireNotNull(defaultState) { "$DEFAULT_STATE_FIELD is null" },
                states = states.toList(),
                ismTemplate = ismTemplates,
                user = user
            )
        }

        fun sortPhases(xcp: XContentParser): MutableMap<String, Any> {
            val map: Map<String, Any> = xcp.map()
            val phases: Map<String, Any> = map["phases"] as Map<String, Any>
            val sortedPhases = phases.toList().sortedBy { (key, _) -> ORDER_PHASES.indexOf(key) }.toMap()
            val mutableMap = map.toMutableMap()
            mutableMap["phases"] = sortedPhases
            return mutableMap
        }

        fun sortStates(xcp: XContentParser): MutableMap<String, Any> {
            val order = arrayOf("hot", "warm", "cold", "delete")
            val map: Map<String, Any> = xcp.map()

            val states: List<Map<String, Any>> = map["states"] as List<Map<String, Any>>

            val sortedStates = states.sortedBy { state ->
                val stateName: String = state["name"] as String
                order.indexOf(stateName)
            }
            val mutableMap = map.toMutableMap()
            mutableMap["states"] = sortedStates
            return mutableMap
        }
    }
}
