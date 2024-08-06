/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement

import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.RandomCreateIndexGenerator.randomAlias
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.indexmanagement.indexstatemanagement.action.AliasAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.AllocationAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.CloseAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.ForceMergeAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.IndexPriorityAction
// import org.elasticsearch.indexmanagement.indexstatemanagement.action.NotificationAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.OpenAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.ReadOnlyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.ReadWriteAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.ReplicaCountAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.RollupAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.elasticsearch.indexmanagement.indexstatemanagement.action.SnapshotAction
import org.elasticsearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.elasticsearch.indexmanagement.indexstatemanagement.model.Conditions
import org.elasticsearch.indexmanagement.indexstatemanagement.model.ErrorNotification
import org.elasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.elasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.elasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.elasticsearch.indexmanagement.indexstatemanagement.model.State
import org.elasticsearch.indexmanagement.indexstatemanagement.model.StateFilter
import org.elasticsearch.indexmanagement.indexstatemanagement.model.Transition
import org.elasticsearch.indexmanagement.indexstatemanagement.model.coordinator.ClusterStateManagedIndexConfig
import org.elasticsearch.indexmanagement.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import org.elasticsearch.indexmanagement.common.model.notification.Channel
import org.elasticsearch.indexmanagement.indexstatemanagement.model.destination.Chime
import org.elasticsearch.indexmanagement.indexstatemanagement.model.destination.CustomWebhook
import org.elasticsearch.indexmanagement.indexstatemanagement.model.destination.Destination
import org.elasticsearch.indexmanagement.indexstatemanagement.model.destination.DestinationType
import org.elasticsearch.indexmanagement.indexstatemanagement.model.destination.Slack
import org.elasticsearch.indexmanagement.opensearchapi.string
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Action
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.jobscheduler.spi.schedule.CronSchedule
import org.elasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.elasticsearch.jobscheduler.spi.schedule.Schedule
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptType
import org.elasticsearch.test.ElasticSearchTestCase.randomAlphaOfLength
import org.elasticsearch.test.ElasticSearchTestCase.randomBoolean
import org.elasticsearch.test.ElasticSearchTestCase.randomDoubleBetween
import org.elasticsearch.test.ElasticSearchTestCase.randomInt
import org.elasticsearch.test.ElasticSearchTestCase.randomList
import org.elasticsearch.test.rest.ElasticSearchRestTestCase
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import kotlin.math.abs

fun randomPolicy(
    id: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    description: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    schemaVersion: Long = ElasticSearchRestTestCase.randomLong(),
    lastUpdatedTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    errorNotification: ErrorNotification? = randomErrorNotification(),
    states: List<State> = List(ElasticSearchRestTestCase.randomIntBetween(1, 10)) { randomState() },
    ismTemplate: List<ISMTemplate>? = null
): Policy {
    return Policy(
        id = id, schemaVersion = schemaVersion, lastUpdatedTime = lastUpdatedTime,
        errorNotification = errorNotification, defaultState = states[0].name, states = states, description = description, ismTemplate = ismTemplate
        // errorNotification = errorNotification, defaultState = states[0].name, states = states, ismTemplate = ismTemplate
    )
}

fun randomState(
    name: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    actions: List<Action> = listOf(),
    transitions: MutableList<Transition> = mutableListOf()
): State {
    return State(name = name, actions = actions, transitions = transitions)
}

fun randomTransition(
    stateName: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    conditions: Conditions? = randomConditions()
): Transition {
    return Transition(stateName = stateName, conditions = conditions)
}

/**
 * TODO: Excluded randomCronSchedule being included in randomConditions as two issues need to be resolved first:
 *   1. Job Scheduler needs to be published to maven central as there is an issue retrieving dependencies for SPI
 *   2. CronSchedule in Job Scheduler needs to implement equals/hash methods so assertEquals compares two CronSchedule
 *      objects properly when doing roundtrip parsing tests
 */
fun randomConditions(
    condition: Pair<String, Any>? =
            ElasticSearchRestTestCase.randomFrom(listOf(randomIndexAge(), randomDocCount(), randomSize(), randomRolloverAge(), null))
): Conditions? {

    if (condition == null) return null

    val type = condition.first
    val value = condition.second

    return when (type) {
        Conditions.MIN_INDEX_AGE_FIELD -> Conditions(indexAge = value as TimeValue)
        Conditions.MIN_DOC_COUNT_FIELD -> Conditions(docCount = value as Long)
        Conditions.MIN_SIZE_FIELD -> Conditions(size = value as ByteSizeValue)
//        Conditions.CRON_FIELD -> Conditions(cron = value as CronSchedule) // TODO: Uncomment after issues are fixed
        Conditions.MIN_ROLLOVER_AGE_FIELD -> Conditions(rolloverAge = value as TimeValue)
        else -> throw IllegalArgumentException("Invalid field: [$type] given for random Conditions.")
    }
}

fun nonNullRandomConditions(): Conditions =
    randomConditions(ElasticSearchRestTestCase.randomFrom(listOf(randomIndexAge(), randomDocCount(), randomSize())))!!

fun randomDeleteActionConfig(): DeleteAction {
    return DeleteAction(index = 0)
}

fun randomRolloverActionConfig(
    minSize: ByteSizeValue = randomByteSizeValue(),
    minDocs: Long = ElasticSearchRestTestCase.randomLongBetween(1, 1000),
    minAge: TimeValue = randomTimeValueObject(),
    minPrimaryShardSize: ByteSizeValue = randomByteSizeValue()
): RolloverAction {
    return RolloverAction(
        minSize = minSize,
        minDocs = minDocs,
        minAge = minAge,
        minPrimaryShardSize = minPrimaryShardSize,
        index = 0
    )
}

@Suppress("ReturnCount")
fun randomShrinkAction(
    numNewShards: Int? = null,
    maxShardSize: ByteSizeValue? = null,
    percentageOfSourceShards: Double? = null,
    targetIndexTemplate: Script? = if (randomBoolean()) randomTemplateScript(randomAlphaOfLength(10)) else null,
    aliases: List<Alias>? = if (randomBoolean()) randomList(10) { randomAlias() } else null,
    forceUnsafe: Boolean? = if (randomBoolean()) randomBoolean() else null
): ShrinkAction {
    if (numNewShards == null && maxShardSize == null && percentageOfSourceShards == null) {
        when (randomInt(2)) {
            0 -> return ShrinkAction(abs(randomInt()) + 1, null, null, targetIndexTemplate, aliases, forceUnsafe, 0)
            1 -> return ShrinkAction(null, randomByteSizeValue(), null, targetIndexTemplate, aliases, forceUnsafe, 0)
            2 -> return ShrinkAction(null, null, randomDoubleBetween(0.0, 1.0, true), targetIndexTemplate, aliases, forceUnsafe, 0)
        }
    }
    return ShrinkAction(numNewShards, maxShardSize, percentageOfSourceShards, targetIndexTemplate, aliases, forceUnsafe, 0)
}

fun randomReadOnlyActionConfig(): ReadOnlyAction {
    return ReadOnlyAction(index = 0)
}

fun randomReadWriteActionConfig(): ReadWriteAction {
    return ReadWriteAction(index = 0)
}

fun randomReplicaCountActionConfig(numOfReplicas: Int = ElasticSearchRestTestCase.randomIntBetween(0, 200)): ReplicaCountAction {
    return ReplicaCountAction(index = 0, numOfReplicas = numOfReplicas)
}

fun randomIndexPriorityActionConfig(indexPriority: Int = ElasticSearchRestTestCase.randomIntBetween(0, 100)): IndexPriorityAction {
    return IndexPriorityAction(index = 0, indexPriority = indexPriority)
}

fun randomForceMergeActionConfig(
    maxNumSegments: Int = ElasticSearchRestTestCase.randomIntBetween(1, 50)
): ForceMergeAction {
    return ForceMergeAction(maxNumSegments = maxNumSegments, index = 0)
}

//fun randomNotificationActionConfig(
//    destination: Destination = randomDestination(),
//    messageTemplate: Script = randomTemplateScript("random message"),
//    index: Int = 0
//): NotificationAction {
//    return NotificationAction(destination, null, messageTemplate, index)
//}

fun randomAllocationActionConfig(require: Map<String, String> = emptyMap(), exclude: Map<String, String> = emptyMap(), include: Map<String, String> = emptyMap()): AllocationAction {
    return AllocationAction(require, include, exclude, index = 0)
}

//fun randomRollupActionConfig(): RollupAction {
//    return RollupAction(ismRollup = randomISMRollup(), index = 0)
//}

fun randomCloseActionConfig(): CloseAction {
    return CloseAction(index = 0)
}

fun randomOpenActionConfig(): OpenAction {
    return OpenAction(index = 0)
}

fun randomAliasAction(includeIndices: Boolean = false): AliasAction {
    val actions = List(ElasticSearchRestTestCase.randomIntBetween(1, 10)) { if (includeIndices) randomAliasActionWithIndices() else randomAliasActions() }
    return AliasAction(actions = actions, index = 0)
}

fun randomAliasActions(): IndicesAliasesRequest.AliasActions {
    val types = listOf(IndicesAliasesRequest.AliasActions.Type.ADD, IndicesAliasesRequest.AliasActions.Type.REMOVE)
    return IndicesAliasesRequest.AliasActions(ElasticSearchRestTestCase.randomSubsetOf(1, types).first())
        .alias(ElasticSearchRestTestCase.randomAlphaOfLength(10))
}

fun randomAliasActionWithIndices(): IndicesAliasesRequest.AliasActions {
    val types = listOf(IndicesAliasesRequest.AliasActions.Type.ADD, IndicesAliasesRequest.AliasActions.Type.REMOVE)
    return IndicesAliasesRequest.AliasActions(ElasticSearchRestTestCase.randomSubsetOf(1, types).first())
        .alias(ElasticSearchRestTestCase.randomAlphaOfLength(10))
        .indices(ElasticSearchRestTestCase.randomAlphaOfLength(10))
}

fun randomDestination(type: DestinationType = randomDestinationType()): Destination {
    return Destination(
        type = type,
        chime = if (type == DestinationType.CHIME) randomChime() else null,
        slack = if (type == DestinationType.SLACK) randomSlack() else null,
        customWebhook = if (type == DestinationType.CUSTOM_WEBHOOK) randomCustomWebhook() else null
    )
}

fun randomDestinationType(): DestinationType {
    val types = listOf(DestinationType.SLACK, DestinationType.CHIME, DestinationType.CUSTOM_WEBHOOK)
    return ElasticSearchRestTestCase.randomSubsetOf(1, types).first()
}

fun randomChime(): Chime {
    return Chime("https://www.amazon.com")
}

fun randomSlack(): Slack {
    return Slack("https://www.amazon.com")
}

fun randomCustomWebhook(): CustomWebhook {
    return CustomWebhook(
        url = "https://www.amazon.com",
        scheme = null,
        host = null,
        port = -1,
        path = null,
        queryParams = emptyMap(),
        headerParams = emptyMap(),
        username = null,
        password = null
    )
}

fun randomTemplateScript(
    source: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    params: Map<String, String> = emptyMap(),
    scriptType: ScriptType = ScriptType.INLINE,
    lang: String = Script.DEFAULT_TEMPLATE_LANG
): Script = Script(scriptType, lang, source, params)

fun randomSnapshotActionConfig(repository: String = "repo", snapshot: String = "sp"): SnapshotAction {
    return SnapshotAction(repository, snapshot, index = 0)
}

/**
 * Helper functions for creating a random Conditions object
 */
fun randomIndexAge(indexAge: TimeValue = randomTimeValueObject()) = Conditions.MIN_INDEX_AGE_FIELD to indexAge

fun randomDocCount(docCount: Long = ElasticSearchRestTestCase.randomLongBetween(1, 1000)) = Conditions.MIN_DOC_COUNT_FIELD to docCount

fun randomSize(size: ByteSizeValue = randomByteSizeValue()) = Conditions.MIN_SIZE_FIELD to size

fun randomCronSchedule(cron: CronSchedule = CronSchedule("0 * * * *", ZoneId.of("UTC"))) =
    Conditions.CRON_FIELD to cron

fun randomRolloverAge(rolloverAge: TimeValue = randomTimeValueObject()) = Conditions.MIN_ROLLOVER_AGE_FIELD to rolloverAge

fun randomTimeValueObject(): TimeValue = TimeValue.parseTimeValue(ElasticSearchRestTestCase.randomPositiveTimeValue(), "")

fun randomByteSizeValue(): ByteSizeValue =
    ByteSizeValue.parseBytesSizeValue(
            ElasticSearchRestTestCase.randomIntBetween(1, 1000).toString() + ElasticSearchRestTestCase.randomFrom(listOf("b", "kb", "mb", "gb")),
        ""
    )
/**
 * End - Conditions helper functions
 */

fun randomChangePolicy(
    policyID: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    state: String? = if (ElasticSearchRestTestCase.randomBoolean()) ElasticSearchRestTestCase.randomAlphaOfLength(10) else null,
    include: List<StateFilter> = emptyList(),
    isSafe: Boolean = false
): ChangePolicy {
    return ChangePolicy(policyID, state, include, isSafe)
}

// will only return null since we dont want to send actual notifications during integ tests
@Suppress("FunctionOnlyReturningConstant")
fun randomErrorNotification(): ErrorNotification? = null

fun randomManagedIndexConfig(
    name: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    index: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    uuid: String = ElasticSearchRestTestCase.randomAlphaOfLength(20),
    enabled: Boolean = ElasticSearchRestTestCase.randomBoolean(),
    schedule: Schedule = IntervalSchedule(Instant.ofEpochMilli(Instant.now().toEpochMilli()), 5, ChronoUnit.MINUTES),
    lastUpdatedTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    policyID: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    policy: Policy? = randomPolicy(),
    changePolicy: ChangePolicy? = randomChangePolicy(),
    jitter: Double? = 0.0
): ManagedIndexConfig {
    return ManagedIndexConfig(
        jobName = name,
        index = index,
        indexUuid = uuid,
        enabled = enabled,
        jobSchedule = schedule,
        jobLastUpdatedTime = lastUpdatedTime,
        jobEnabledTime = enabledTime,
        policyID = policy?.id ?: policyID,
        policySeqNo = policy?.seqNo,
        policyPrimaryTerm = policy?.primaryTerm,
        policy = policy?.copy(seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM),
        changePolicy = changePolicy,
        jobJitter = jitter
    )
}

fun randomClusterStateManagedIndexConfig(
    index: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    uuid: String = ElasticSearchRestTestCase.randomAlphaOfLength(20),
    policyID: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
): ClusterStateManagedIndexConfig {
    return ClusterStateManagedIndexConfig(
        index = index,
        uuid = uuid,
        policyID = policyID,
        seqNo = seqNo,
        primaryTerm = primaryTerm
    )
}

fun randomSweptManagedIndexConfig(
    index: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    uuid: String = ElasticSearchRestTestCase.randomAlphaOfLength(20),
    policyID: String = ElasticSearchRestTestCase.randomAlphaOfLength(10),
    seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    changePolicy: ChangePolicy? = null,
    policy: Policy? = null
): SweptManagedIndexConfig {
    return SweptManagedIndexConfig(
        index = index,
        uuid = uuid,
        policyID = policyID,
        seqNo = seqNo,
        primaryTerm = primaryTerm,
        policy = policy,
        changePolicy = changePolicy
    )
}

fun randomISMTemplate(
    indexPatterns: List<String> = listOf(ElasticSearchRestTestCase.randomAlphaOfLength(10) + "*"),
    priority: Int = ElasticSearchRestTestCase.randomIntBetween(0, 100),
    lastUpdatedTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)
): ISMTemplate {
    return ISMTemplate(
        indexPatterns = indexPatterns,
        priority = priority,
        lastUpdatedTime = lastUpdatedTime
    )
}

fun randomChannel(id: String = ElasticSearchRestTestCase.randomAlphaOfLength(10)): Channel {
    return Channel(id = id)
}

fun Policy.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder).string()
}

fun State.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun Transition.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun Conditions.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun DeleteAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun RolloverAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ReadOnlyAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ReadWriteAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ShrinkAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ReplicaCountAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun IndexPriorityAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ForceMergeAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

//fun NotificationAction.toJsonString(): String {
//    val builder = XContentFactory.jsonBuilder()
//    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
//}

fun AllocationAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ChangePolicy.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ManagedIndexConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ManagedIndexMetaData.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder().startObject()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject().string()
}

fun SnapshotAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun RollupAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun CloseAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun OpenAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun AliasAction.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ISMTemplate.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun Channel.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

@Suppress("RethrowCaughtException")
fun <T> wait(
    timeout: Instant = Instant.ofEpochSecond(10),
    block: () -> T
) {
    val startTime = Instant.now().toEpochMilli()
    do {
        try {
            block()
            if ((Instant.now().toEpochMilli() - startTime) > timeout.toEpochMilli()) {
                return
            } else {
                Thread.sleep(100L)
            }
        } catch (e: Throwable) {
            throw e
        }
    } while (true)
}
