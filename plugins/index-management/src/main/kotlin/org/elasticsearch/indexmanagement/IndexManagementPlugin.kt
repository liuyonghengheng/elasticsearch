/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement

import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.support.ActionFilter
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.node.DiscoveryNodes
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.component.Lifecycle
import org.elasticsearch.common.component.LifecycleComponent
import org.elasticsearch.common.component.LifecycleListener
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.NamedWriteableRegistry
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.IndexScopedSettings
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.settings.SettingsFilter
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.indexmanagement.indexstatemanagement.DefaultIndexMetadataService
import org.elasticsearch.indexmanagement.indexstatemanagement.ExtensionStatusChecker
import org.elasticsearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.elasticsearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.elasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementHistory
import org.elasticsearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator
import org.elasticsearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.elasticsearch.indexmanagement.indexstatemanagement.MetadataService
import org.elasticsearch.indexmanagement.indexstatemanagement.PluginVersionSweepCoordinator
import org.elasticsearch.indexmanagement.indexstatemanagement.SkipExecution
import org.elasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.elasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.elasticsearch.indexmanagement.indexstatemanagement.resthandler.RestAddPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.resthandler.RestChangePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.resthandler.RestDeletePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.resthandler.RestExplainAction
import org.elasticsearch.indexmanagement.indexstatemanagement.resthandler.RestGetPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.resthandler.RestIndexPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.resthandler.RestRemovePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.resthandler.RestRetryFailedManagedIndexAction
import org.elasticsearch.indexmanagement.indexstatemanagement.settings.LegacyOpenDistroManagedIndexSettings
import org.elasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.TransportAddPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.TransportChangePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.TransportDeletePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.explain.TransportExplainAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.TransportGetPoliciesAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.TransportGetPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.TransportIndexPolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.TransportManagedIndexAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.TransportRemovePolicyAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.TransportRetryFailedManagedIndexAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.TransportUpdateManagedIndexMetaDataAction
import org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import org.elasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.elasticsearch.indexmanagement.indexstatemanagement.validation.ActionValidation
import org.elasticsearch.indexmanagement.indexstatemanagement.migration.ISMTemplateService
import org.elasticsearch.indexmanagement.refreshanalyzer.RefreshSearchAnalyzerAction
import org.elasticsearch.indexmanagement.refreshanalyzer.RestRefreshSearchAnalyzerAction
import org.elasticsearch.indexmanagement.refreshanalyzer.TransportRefreshSearchAnalyzerAction
import org.elasticsearch.indexmanagement.rollup.RollupIndexer
import org.elasticsearch.indexmanagement.rollup.RollupMapperService
import org.elasticsearch.indexmanagement.rollup.RollupMetadataService
import org.elasticsearch.indexmanagement.rollup.RollupRunner
import org.elasticsearch.indexmanagement.rollup.RollupSearchService
import org.elasticsearch.indexmanagement.rollup.action.delete.DeleteRollupAction
import org.elasticsearch.indexmanagement.rollup.action.delete.TransportDeleteRollupAction
import org.elasticsearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import org.elasticsearch.indexmanagement.rollup.action.explain.TransportExplainRollupAction
import org.elasticsearch.indexmanagement.rollup.action.get.GetRollupAction
import org.elasticsearch.indexmanagement.rollup.action.get.GetRollupsAction
import org.elasticsearch.indexmanagement.rollup.action.get.TransportGetRollupAction
import org.elasticsearch.indexmanagement.rollup.action.get.TransportGetRollupsAction
import org.elasticsearch.indexmanagement.rollup.action.index.IndexRollupAction
import org.elasticsearch.indexmanagement.rollup.action.index.TransportIndexRollupAction
import org.elasticsearch.indexmanagement.rollup.action.mapping.TransportUpdateRollupMappingAction
import org.elasticsearch.indexmanagement.rollup.action.mapping.UpdateRollupMappingAction
import org.elasticsearch.indexmanagement.rollup.action.start.StartRollupAction
import org.elasticsearch.indexmanagement.rollup.action.start.TransportStartRollupAction
import org.elasticsearch.indexmanagement.rollup.action.stop.StopRollupAction
import org.elasticsearch.indexmanagement.rollup.action.stop.TransportStopRollupAction
import org.elasticsearch.indexmanagement.rollup.actionfilter.FieldCapsFilter
import org.elasticsearch.indexmanagement.rollup.interceptor.RollupInterceptor
import org.elasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.indexmanagement.rollup.model.RollupMetadata
import org.elasticsearch.indexmanagement.rollup.resthandler.RestDeleteRollupAction
import org.elasticsearch.indexmanagement.rollup.resthandler.RestExplainRollupAction
import org.elasticsearch.indexmanagement.rollup.resthandler.RestGetRollupAction
import org.elasticsearch.indexmanagement.rollup.resthandler.RestIndexRollupAction
import org.elasticsearch.indexmanagement.rollup.resthandler.RestStartRollupAction
import org.elasticsearch.indexmanagement.rollup.resthandler.RestStopRollupAction
import org.elasticsearch.indexmanagement.rollup.settings.LegacyOpenDistroRollupSettings
import org.elasticsearch.indexmanagement.rollup.settings.RollupSettings
import org.elasticsearch.indexmanagement.rollup.util.RollupFieldValueExpressionResolver
import org.elasticsearch.indexmanagement.settings.IndexManagementSettings
import org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler.RestCreateSMPolicyHandler
import org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler.RestDeleteSMPolicyHandler
import org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler.RestExplainSMPolicyHandler
import org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler.RestGetSMPolicyHandler
import org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler.RestStartSMPolicyHandler
import org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler.RestStopSMPolicyHandler
import org.elasticsearch.indexmanagement.snapshotmanagement.api.resthandler.RestUpdateSMPolicyHandler
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.delete.TransportDeleteSMPolicyAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.explain.TransportExplainSMAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.get.TransportGetSMPoliciesAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.get.TransportGetSMPolicyAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.index.TransportIndexSMPolicyAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.start.TransportStartSMAction
import org.elasticsearch.indexmanagement.snapshotmanagement.api.transport.stop.TransportStopSMAction
import org.elasticsearch.indexmanagement.snapshotmanagement.SMRunner
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.elasticsearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.elasticsearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings
import org.elasticsearch.indexmanagement.spi.IndexManagementExtension
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.IndexMetadataService
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.StatusChecker
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.indexmanagement.transform.TransformRunner
import org.elasticsearch.indexmanagement.transform.action.delete.DeleteTransformsAction
import org.elasticsearch.indexmanagement.transform.action.delete.TransportDeleteTransformsAction
import org.elasticsearch.indexmanagement.transform.action.explain.ExplainTransformAction
import org.elasticsearch.indexmanagement.transform.action.explain.TransportExplainTransformAction
import org.elasticsearch.indexmanagement.transform.action.get.GetTransformAction
import org.elasticsearch.indexmanagement.transform.action.get.GetTransformsAction
import org.elasticsearch.indexmanagement.transform.action.get.TransportGetTransformAction
import org.elasticsearch.indexmanagement.transform.action.get.TransportGetTransformsAction
import org.elasticsearch.indexmanagement.transform.action.index.IndexTransformAction
import org.elasticsearch.indexmanagement.transform.action.index.TransportIndexTransformAction
import org.elasticsearch.indexmanagement.transform.action.preview.PreviewTransformAction
import org.elasticsearch.indexmanagement.transform.action.preview.TransportPreviewTransformAction
import org.elasticsearch.indexmanagement.transform.action.start.StartTransformAction
import org.elasticsearch.indexmanagement.transform.action.start.TransportStartTransformAction
import org.elasticsearch.indexmanagement.transform.action.stop.StopTransformAction
import org.elasticsearch.indexmanagement.transform.action.stop.TransportStopTransformAction
import org.elasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.indexmanagement.transform.model.TransformMetadata
import org.elasticsearch.indexmanagement.transform.resthandler.RestDeleteTransformAction
import org.elasticsearch.indexmanagement.transform.resthandler.RestExplainTransformAction
import org.elasticsearch.indexmanagement.transform.resthandler.RestGetTransformAction
import org.elasticsearch.indexmanagement.transform.resthandler.RestIndexTransformAction
import org.elasticsearch.indexmanagement.transform.resthandler.RestPreviewTransformAction
import org.elasticsearch.indexmanagement.transform.resthandler.RestStartTransformAction
import org.elasticsearch.indexmanagement.transform.resthandler.RestStopTransformAction
import org.elasticsearch.indexmanagement.transform.settings.TransformSettings
import org.elasticsearch.jobscheduler.spi.JobSchedulerExtension
import org.elasticsearch.jobscheduler.spi.ScheduledJobParser
import org.elasticsearch.jobscheduler.spi.ScheduledJobRunner
import org.elasticsearch.monitor.jvm.JvmService
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.ExtensiblePlugin
import org.elasticsearch.plugins.NetworkPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.repositories.RepositoriesService
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.RemoteClusterService
import org.elasticsearch.transport.TransportInterceptor
import org.elasticsearch.transport.TransportService
import org.elasticsearch.watcher.ResourceWatcherService
import java.util.function.Supplier

@Suppress("TooManyFunctions")
class IndexManagementPlugin : JobSchedulerExtension, NetworkPlugin, ActionPlugin, ExtensiblePlugin, Plugin() {

    private val logger = LogManager.getLogger(javaClass)
    lateinit var indexManagementIndices: IndexManagementIndices
    lateinit var actionValidation: ActionValidation
    lateinit var clusterService: ClusterService
    lateinit var indexNameExpressionResolver: IndexNameExpressionResolver
    lateinit var rollupInterceptor: RollupInterceptor
    lateinit var fieldCapsFilter: FieldCapsFilter
    lateinit var indexMetadataProvider: IndexMetadataProvider
    private val indexMetadataServices: MutableList<Map<String, IndexMetadataService>> = mutableListOf()
    private var customIndexUUIDSetting: String? = null
    private val extensions = mutableSetOf<String>()
    private val extensionCheckerMap = mutableMapOf<String, StatusChecker>()

    companion object {
        const val PLUGINS_BASE_URI = "/_plugins"
        const val ISM_BASE_URI = "$PLUGINS_BASE_URI/_ism"
        const val ROLLUP_BASE_URI = "$PLUGINS_BASE_URI/_rollup"
        const val TRANSFORM_BASE_URI = "$PLUGINS_BASE_URI/_transform"
        const val POLICY_BASE_URI = "$ISM_BASE_URI/policies"
        const val ROLLUP_JOBS_BASE_URI = "$ROLLUP_BASE_URI/jobs"
        const val INDEX_MANAGEMENT_INDEX = ".elasticsearch-ilm-config"
        const val INDEX_MANAGEMENT_JOB_TYPE = "elasticsearch-index-management"
        const val INDEX_STATE_MANAGEMENT_HISTORY_TYPE = "managed_index_meta_data"

        const val SM_BASE_URI = "/_slm"
        const val SM_POLICIES_URI = "$SM_BASE_URI/policies"

        const val OLD_PLUGIN_NAME = "opendistro-im"
        const val OPEN_DISTRO_BASE_URI = "/_opendistro"
        const val LEGACY_ISM_BASE_URI = "$OPEN_DISTRO_BASE_URI/_ism"
        const val LEGACY_ROLLUP_BASE_URI = "$OPEN_DISTRO_BASE_URI/_rollup"
        const val LEGACY_POLICY_BASE_URI = "$LEGACY_ISM_BASE_URI/policies"
        const val LEGACY_ROLLUP_JOBS_BASE_URI = "$LEGACY_ROLLUP_BASE_URI/jobs"

        const val ES_POLICY_BASE_URI = "_ilm/policy"

    }

    override fun getJobIndex(): String = INDEX_MANAGEMENT_INDEX

    override fun getJobType(): String = INDEX_MANAGEMENT_JOB_TYPE

    override fun getJobRunner(): ScheduledJobRunner = IndexManagementRunner

    override fun getGuiceServiceClasses(): Collection<Class<out LifecycleComponent?>> {
        return mutableListOf<Class<out LifecycleComponent?>>(GuiceHolder::class.java)
    }

    @Suppress("ComplexMethod")
    override fun getJobParser(): ScheduledJobParser {
        return ScheduledJobParser { xcp, id, jobDocVersion ->
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ManagedIndexConfig.MANAGED_INDEX_TYPE -> {
                        return@ScheduledJobParser ManagedIndexConfig.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    Policy.POLICY_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    Rollup.ROLLUP_TYPE -> {
                        return@ScheduledJobParser Rollup.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    RollupMetadata.ROLLUP_METADATA_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    Transform.TRANSFORM_TYPE -> {
                        return@ScheduledJobParser Transform.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    TransformMetadata.TRANSFORM_METADATA_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    ManagedIndexMetaData.MANAGED_INDEX_METADATA_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    SMPolicy.SM_TYPE -> {
                        return@ScheduledJobParser SMPolicy.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    SMMetadata.SM_METADATA_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    else -> {
                        logger.warn("Unsupported document was indexed in $INDEX_MANAGEMENT_INDEX with type: $fieldName")
                        xcp.skipChildren()
                    }
                }
            }
            return@ScheduledJobParser null
        }
    }

    override fun loadExtensions(loader: ExtensiblePlugin.ExtensionLoader) {
        val indexManagementExtensions = loader.loadExtensions(IndexManagementExtension::class.java)

        indexManagementExtensions.forEach { extension ->
            val extensionName = extension.getExtensionName()
            if (extensionName in extensions) {
                error("Multiple extensions of IndexManagement have same name $extensionName - not supported")
            }
            extension.getISMActionParsers().forEach { parser ->
                ISMActionsParser.instance.addParser(parser, extensionName)
            }
            indexMetadataServices.add(extension.getIndexMetadataService())
            extension.overrideClusterStateIndexUuidSetting()?.let {
                if (customIndexUUIDSetting != null) {
                    error(
                        "Multiple extensions of IndexManagement plugin overriding ClusterStateIndexUUIDSetting - not supported"
                    )
                }
                customIndexUUIDSetting = extension.overrideClusterStateIndexUuidSetting()
            }
            extensionCheckerMap[extensionName] = extension.statusChecker()
        }
    }

    override fun getRestHandlers(
        settings: Settings,
        restController: RestController,
        clusterSettings: ClusterSettings,
        indexScopedSettings: IndexScopedSettings,
        settingsFilter: SettingsFilter,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        nodesInCluster: Supplier<DiscoveryNodes>
    ): List<RestHandler> {
        return listOf(
            RestRefreshSearchAnalyzerAction(),
            RestIndexPolicyAction(settings, clusterService),
            RestGetPolicyAction(),
            RestDeletePolicyAction(),
            RestExplainAction(),
            RestRetryFailedManagedIndexAction(),
            RestAddPolicyAction(),
            RestRemovePolicyAction(),
            RestChangePolicyAction(),
            RestDeleteRollupAction(),
            RestGetRollupAction(),
            RestIndexRollupAction(),
            RestStartRollupAction(),
            RestStopRollupAction(),
            RestExplainRollupAction(),
            RestIndexTransformAction(),
            RestGetTransformAction(),
            RestPreviewTransformAction(),
            RestDeleteTransformAction(),
            RestExplainTransformAction(),
            RestStartTransformAction(),
            RestStopTransformAction(),
            RestGetSMPolicyHandler(),
            RestStartSMPolicyHandler(),
            RestStopSMPolicyHandler(),
            RestExplainSMPolicyHandler(),
            RestDeleteSMPolicyHandler(),
            RestCreateSMPolicyHandler(),
            RestUpdateSMPolicyHandler()
        )
    }

    @Suppress("LongMethod")
    override fun createComponents(
        client: Client,
        clusterService: ClusterService,
        threadPool: ThreadPool,
        resourceWatcherService: ResourceWatcherService,
        scriptService: ScriptService,
        xContentRegistry: NamedXContentRegistry,
        environment: Environment,
        nodeEnvironment: NodeEnvironment,
        namedWriteableRegistry: NamedWriteableRegistry,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        repositoriesServiceSupplier: Supplier<RepositoriesService>
    ): Collection<Any> {
        val settings = environment.settings()
        this.clusterService = clusterService
        rollupInterceptor = RollupInterceptor(clusterService, settings, indexNameExpressionResolver)
        val jvmService = JvmService(environment.settings())
        val transformRunner = TransformRunner.initialize(
            client,
            clusterService,
            xContentRegistry,
            settings,
            indexNameExpressionResolver,
            jvmService,
            threadPool
        )
        fieldCapsFilter = FieldCapsFilter(clusterService, settings, indexNameExpressionResolver)
        this.indexNameExpressionResolver = indexNameExpressionResolver

        val skipFlag = SkipExecution(client)
        RollupFieldValueExpressionResolver.registerServices(scriptService, clusterService)
        val rollupRunner = RollupRunner
            .registerClient(client)
            .registerClusterService(clusterService)
            .registerNamedXContentRegistry(xContentRegistry)
            .registerScriptService(scriptService)
            .registerSettings(settings)
            .registerThreadPool(threadPool)
            .registerMapperService(RollupMapperService(client, clusterService, indexNameExpressionResolver))
            .registerIndexer(RollupIndexer(settings, clusterService, client))
            .registerSearcher(RollupSearchService(settings, clusterService, client))
            .registerMetadataServices(RollupMetadataService(client, xContentRegistry))
            .registerConsumers()
            .registerClusterConfigurationProvider(skipFlag)
        indexManagementIndices = IndexManagementIndices(settings, client.admin().indices(), clusterService)
        actionValidation = ActionValidation(settings, clusterService, jvmService)
        val indexStateManagementHistory =
            IndexStateManagementHistory(
                settings,
                client,
                threadPool,
                clusterService,
                indexManagementIndices
            )

        indexMetadataProvider = IndexMetadataProvider(
            settings, client, clusterService,
            hashMapOf(
                DEFAULT_INDEX_TYPE to DefaultIndexMetadataService(customIndexUUIDSetting)
            )
        )
        indexMetadataServices.forEach { indexMetadataProvider.addMetadataServices(it) }

        val extensionChecker = ExtensionStatusChecker(extensionCheckerMap, clusterService)
        val managedIndexRunner = ManagedIndexRunner
            .registerClient(client)
            .registerClusterService(clusterService)
            .registerValidationService(actionValidation)
            .registerNamedXContentRegistry(xContentRegistry)
            .registerScriptService(scriptService)
            .registerSettings(settings)
            .registerConsumers() // registerConsumers must happen after registerSettings/clusterService
            .registerIMIndex(indexManagementIndices)
            .registerHistoryIndex(indexStateManagementHistory)
            .registerSkipFlag(skipFlag)
            .registerThreadPool(threadPool)
            .registerExtensionChecker(extensionChecker)
            .registerIndexMetadataProvider(indexMetadataProvider)

        val metadataService = MetadataService(client, clusterService, skipFlag, indexManagementIndices)
        val templateService = ISMTemplateService(client, clusterService, xContentRegistry, indexManagementIndices)

        val managedIndexCoordinator = ManagedIndexCoordinator(
            environment.settings(),
            client, clusterService, threadPool, indexManagementIndices, metadataService, templateService, indexMetadataProvider
        )

        val smRunner = SMRunner.init(client, threadPool, settings, indexManagementIndices, clusterService)

        val pluginVersionSweepCoordinator = PluginVersionSweepCoordinator(skipFlag, settings, threadPool, clusterService)

        return listOf(
            managedIndexRunner,
            rollupRunner,
            transformRunner,
            indexManagementIndices,
            actionValidation,
            managedIndexCoordinator,
            indexStateManagementHistory,
            indexMetadataProvider,
            smRunner,
            pluginVersionSweepCoordinator
        )
    }

    @Suppress("LongMethod")
    override fun getSettings(): List<Setting<*>> {
        return listOf(
            ManagedIndexSettings.HISTORY_ENABLED,
            ManagedIndexSettings.HISTORY_INDEX_MAX_AGE,
            ManagedIndexSettings.HISTORY_MAX_DOCS,
            ManagedIndexSettings.HISTORY_RETENTION_PERIOD,
            ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD,
            ManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS,
            ManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS,
            ManagedIndexSettings.POLICY_ID,
            ManagedIndexSettings.ROLLOVER_ALIAS,
            ManagedIndexSettings.ROLLOVER_SKIP,
            ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
            ManagedIndexSettings.ACTION_VALIDATION_ENABLED,
            ManagedIndexSettings.METADATA_SERVICE_ENABLED,
            ManagedIndexSettings.AUTO_MANAGE,
            ManagedIndexSettings.METADATA_SERVICE_STATUS,
            ManagedIndexSettings.TEMPLATE_MIGRATION_CONTROL,
            ManagedIndexSettings.JITTER,
            ManagedIndexSettings.JOB_INTERVAL,
            ManagedIndexSettings.SWEEP_PERIOD,
            ManagedIndexSettings.SWEEP_SKIP_PERIOD,
            ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
            ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS,
            ManagedIndexSettings.ALLOW_LIST,
            ManagedIndexSettings.SNAPSHOT_DENY_LIST,
            ManagedIndexSettings.RESTRICTED_INDEX_PATTERN,
            RollupSettings.ROLLUP_INGEST_BACKOFF_COUNT,
            RollupSettings.ROLLUP_INGEST_BACKOFF_MILLIS,
            RollupSettings.ROLLUP_SEARCH_BACKOFF_COUNT,
            RollupSettings.ROLLUP_SEARCH_BACKOFF_MILLIS,
            RollupSettings.ROLLUP_INDEX,
            RollupSettings.ROLLUP_ENABLED,
            RollupSettings.ROLLUP_SEARCH_ENABLED,
            RollupSettings.ROLLUP_DASHBOARDS,
            RollupSettings.ROLLUP_SEARCH_ALL_JOBS,
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_COUNT,
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_MILLIS,
            TransformSettings.TRANSFORM_JOB_SEARCH_BACKOFF_COUNT,
            TransformSettings.TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS,
            TransformSettings.TRANSFORM_CIRCUIT_BREAKER_ENABLED,
            TransformSettings.TRANSFORM_CIRCUIT_BREAKER_JVM_THRESHOLD,
            IndexManagementSettings.FILTER_BY_BACKEND_ROLES,
            LegacyOpenDistroManagedIndexSettings.HISTORY_ENABLED,
            LegacyOpenDistroManagedIndexSettings.HISTORY_INDEX_MAX_AGE,
            LegacyOpenDistroManagedIndexSettings.HISTORY_MAX_DOCS,
            LegacyOpenDistroManagedIndexSettings.HISTORY_RETENTION_PERIOD,
            LegacyOpenDistroManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD,
            LegacyOpenDistroManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS,
            LegacyOpenDistroManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS,
            LegacyOpenDistroManagedIndexSettings.POLICY_ID,
            LegacyOpenDistroManagedIndexSettings.ROLLOVER_ALIAS,
            LegacyOpenDistroManagedIndexSettings.ROLLOVER_SKIP,
            LegacyOpenDistroManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
            LegacyOpenDistroManagedIndexSettings.METADATA_SERVICE_ENABLED,
            LegacyOpenDistroManagedIndexSettings.JOB_INTERVAL,
            LegacyOpenDistroManagedIndexSettings.SWEEP_PERIOD,
            LegacyOpenDistroManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
            LegacyOpenDistroManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS,
            LegacyOpenDistroManagedIndexSettings.ALLOW_LIST,
            LegacyOpenDistroManagedIndexSettings.SNAPSHOT_DENY_LIST,
            LegacyOpenDistroManagedIndexSettings.AUTO_MANAGE,
            LegacyOpenDistroManagedIndexSettings.METADATA_SERVICE_STATUS,
            LegacyOpenDistroManagedIndexSettings.TEMPLATE_MIGRATION_CONTROL,
            LegacyOpenDistroManagedIndexSettings.RESTRICTED_INDEX_PATTERN,
            LegacyOpenDistroRollupSettings.ROLLUP_INGEST_BACKOFF_COUNT,
            LegacyOpenDistroRollupSettings.ROLLUP_INGEST_BACKOFF_MILLIS,
            LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_BACKOFF_COUNT,
            LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_BACKOFF_MILLIS,
            LegacyOpenDistroRollupSettings.ROLLUP_INDEX,
            LegacyOpenDistroRollupSettings.ROLLUP_ENABLED,
            LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_ENABLED,
            LegacyOpenDistroRollupSettings.ROLLUP_DASHBOARDS,
            SnapshotManagementSettings.FILTER_BY_BACKEND_ROLES
        )
    }

    override fun getActions(): List<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return listOf(
            ActionPlugin.ActionHandler(UpdateManagedIndexMetaDataAction.INSTANCE, TransportUpdateManagedIndexMetaDataAction::class.java),
            ActionPlugin.ActionHandler(RemovePolicyAction.INSTANCE, TransportRemovePolicyAction::class.java),
            ActionPlugin.ActionHandler(RefreshSearchAnalyzerAction.INSTANCE, TransportRefreshSearchAnalyzerAction::class.java),
            ActionPlugin.ActionHandler(AddPolicyAction.INSTANCE, TransportAddPolicyAction::class.java),
            ActionPlugin.ActionHandler(RetryFailedManagedIndexAction.INSTANCE, TransportRetryFailedManagedIndexAction::class.java),
            ActionPlugin.ActionHandler(ChangePolicyAction.INSTANCE, TransportChangePolicyAction::class.java),
            ActionPlugin.ActionHandler(IndexPolicyAction.INSTANCE, TransportIndexPolicyAction::class.java),
            ActionPlugin.ActionHandler(ExplainAction.INSTANCE, TransportExplainAction::class.java),
            ActionPlugin.ActionHandler(DeletePolicyAction.INSTANCE, TransportDeletePolicyAction::class.java),
            ActionPlugin.ActionHandler(GetPolicyAction.INSTANCE, TransportGetPolicyAction::class.java),
            ActionPlugin.ActionHandler(GetPoliciesAction.INSTANCE, TransportGetPoliciesAction::class.java),
            ActionPlugin.ActionHandler(DeleteRollupAction.INSTANCE, TransportDeleteRollupAction::class.java),
            ActionPlugin.ActionHandler(GetRollupAction.INSTANCE, TransportGetRollupAction::class.java),
            ActionPlugin.ActionHandler(GetRollupsAction.INSTANCE, TransportGetRollupsAction::class.java),
            ActionPlugin.ActionHandler(IndexRollupAction.INSTANCE, TransportIndexRollupAction::class.java),
            ActionPlugin.ActionHandler(StartRollupAction.INSTANCE, TransportStartRollupAction::class.java),
            ActionPlugin.ActionHandler(StopRollupAction.INSTANCE, TransportStopRollupAction::class.java),
            ActionPlugin.ActionHandler(ExplainRollupAction.INSTANCE, TransportExplainRollupAction::class.java),
            ActionPlugin.ActionHandler(UpdateRollupMappingAction.INSTANCE, TransportUpdateRollupMappingAction::class.java),
            ActionPlugin.ActionHandler(IndexTransformAction.INSTANCE, TransportIndexTransformAction::class.java),
            ActionPlugin.ActionHandler(GetTransformAction.INSTANCE, TransportGetTransformAction::class.java),
            ActionPlugin.ActionHandler(GetTransformsAction.INSTANCE, TransportGetTransformsAction::class.java),
            ActionPlugin.ActionHandler(PreviewTransformAction.INSTANCE, TransportPreviewTransformAction::class.java),
            ActionPlugin.ActionHandler(DeleteTransformsAction.INSTANCE, TransportDeleteTransformsAction::class.java),
            ActionPlugin.ActionHandler(ExplainTransformAction.INSTANCE, TransportExplainTransformAction::class.java),
            ActionPlugin.ActionHandler(StartTransformAction.INSTANCE, TransportStartTransformAction::class.java),
            ActionPlugin.ActionHandler(StopTransformAction.INSTANCE, TransportStopTransformAction::class.java),
            ActionPlugin.ActionHandler(ManagedIndexAction.INSTANCE, TransportManagedIndexAction::class.java),
            ActionPlugin.ActionHandler(SMActions.INDEX_SM_POLICY_ACTION_TYPE, TransportIndexSMPolicyAction::class.java),
            ActionPlugin.ActionHandler(SMActions.GET_SM_POLICY_ACTION_TYPE, TransportGetSMPolicyAction::class.java),
            ActionPlugin.ActionHandler(SMActions.DELETE_SM_POLICY_ACTION_TYPE, TransportDeleteSMPolicyAction::class.java),
            ActionPlugin.ActionHandler(SMActions.START_SM_POLICY_ACTION_TYPE, TransportStartSMAction::class.java),
            ActionPlugin.ActionHandler(SMActions.STOP_SM_POLICY_ACTION_TYPE, TransportStopSMAction::class.java),
            ActionPlugin.ActionHandler(SMActions.EXPLAIN_SM_POLICY_ACTION_TYPE, TransportExplainSMAction::class.java),
            ActionPlugin.ActionHandler(SMActions.GET_SM_POLICIES_ACTION_TYPE, TransportGetSMPoliciesAction::class.java)
        )
    }

    override fun getTransportInterceptors(namedWriteableRegistry: NamedWriteableRegistry, threadContext: ThreadContext): List<TransportInterceptor> {
        return listOf(rollupInterceptor)
    }

    override fun getActionFilters(): List<ActionFilter> {
        return listOf(fieldCapsFilter)
    }
}

class GuiceHolder @Inject constructor(
    remoteClusterService: TransportService
) : LifecycleComponent {
    override fun close() { /* do nothing */ }
    override fun lifecycleState(): Lifecycle.State? {
        return null
    }

    override fun addLifecycleListener(listener: LifecycleListener) { /* do nothing */ }
    override fun removeLifecycleListener(listener: LifecycleListener) { /* do nothing */ }
    override fun start() { /* do nothing */ }
    override fun stop() { /* do nothing */ }

    companion object {
        lateinit var remoteClusterService: RemoteClusterService
    }

    init {
        Companion.remoteClusterService = remoteClusterService.remoteClusterService
    }
}
