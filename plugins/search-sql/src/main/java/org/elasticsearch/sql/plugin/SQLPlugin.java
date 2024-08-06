
package org.elasticsearch.sql.plugin;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.sql.legacy.esdomain.LocalClusterState;
import org.elasticsearch.sql.legacy.executor.AsyncRestExecutor;
import org.elasticsearch.sql.legacy.metrics.Metrics;
import org.elasticsearch.sql.legacy.plugin.RestSqlAction;
import org.elasticsearch.sql.legacy.plugin.RestSqlStatsAction;
import org.elasticsearch.sql.search.setting.LegacyOpenDistroSettings;
import org.elasticsearch.sql.search.setting.SearchSettings;
import org.elasticsearch.sql.search.storage.script.ExpressionScriptEngine;
import org.elasticsearch.sql.search.storage.serialization.DefaultExpressionSerializer;
import org.elasticsearch.sql.plugin.rest.RestQuerySettingsAction;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

public class SQLPlugin extends Plugin implements ActionPlugin, ScriptPlugin {

  private ClusterService clusterService;

  /**
   * Settings should be inited when bootstrap the plugin.
   */
  private org.elasticsearch.sql.common.setting.Settings pluginSettings;

  public String name() {
    return "sql";
  }

  public String description() {
    return "Use sql to query Elasticsearch.";
  }

  @Override
  public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
                                           ClusterSettings clusterSettings,
                                           IndexScopedSettings indexScopedSettings,
                                           SettingsFilter settingsFilter,
                                           IndexNameExpressionResolver indexNameExpressionResolver,
                                           Supplier<DiscoveryNodes> nodesInCluster) {
    Objects.requireNonNull(clusterService, "Cluster service is required");
    Objects.requireNonNull(pluginSettings, "Cluster settings is required");

    LocalClusterState.state().setResolver(indexNameExpressionResolver);
    Metrics.getInstance().registerDefaultMetrics();

    return Arrays.asList(
       // new RestPPLQueryAction(restController, clusterService, pluginSettings, settings),
        new RestSqlAction(settings, clusterService, pluginSettings),
        new RestSqlStatsAction(settings, restController),
       // new RestPPLStatsAction(settings, restController),
        new RestQuerySettingsAction(settings, restController)
    );
  }

  @Override
  public Collection<Object> createComponents(Client client, ClusterService clusterService,
                                             ThreadPool threadPool,
                                             ResourceWatcherService resourceWatcherService,
                                             ScriptService scriptService,
                                             NamedXContentRegistry contentRegistry,
                                             Environment environment,
                                             NodeEnvironment nodeEnvironment,
                                             NamedWriteableRegistry namedWriteableRegistry,
                                             IndexNameExpressionResolver indexNameResolver,
                                             Supplier<RepositoriesService>
                                                       repositoriesServiceSupplier) {
    this.clusterService = clusterService;
    this.pluginSettings = new SearchSettings(clusterService.getClusterSettings());

    LocalClusterState.state().setClusterService(clusterService);
    LocalClusterState.state().setPluginSettings((SearchSettings) pluginSettings);

    return super
        .createComponents(client, clusterService, threadPool, resourceWatcherService, scriptService,
            contentRegistry, environment, nodeEnvironment, namedWriteableRegistry,
            indexNameResolver, repositoriesServiceSupplier);
  }

  @Override
  public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
    return Collections.singletonList(
        new FixedExecutorBuilder(
            settings,
            AsyncRestExecutor.SQL_WORKER_THREAD_POOL_NAME,
            EsExecutors.allocatedProcessors(settings),
            1000,
            null
        )
    );
  }

  @Override
  public List<Setting<?>> getSettings() {
    return new ImmutableList.Builder<Setting<?>>()
        .addAll(LegacyOpenDistroSettings.legacySettings())
        .addAll(SearchSettings.pluginSettings())
        .build();
  }

  @Override
  public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
    return new ExpressionScriptEngine(new DefaultExpressionSerializer());
  }

}
