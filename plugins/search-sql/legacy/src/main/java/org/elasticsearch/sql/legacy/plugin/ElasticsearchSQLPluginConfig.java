


package org.elasticsearch.sql.legacy.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.sql.common.setting.Settings;
import org.elasticsearch.sql.executor.ExecutionEngine;
import org.elasticsearch.sql.expression.config.ExpressionConfig;
import org.elasticsearch.sql.expression.function.BuiltinFunctionRepository;
import org.elasticsearch.sql.expression.function.ElasticsearchFunctions;
import org.elasticsearch.sql.monitor.ResourceMonitor;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.search.client.ElasticsearchNodeClient;
import org.elasticsearch.sql.search.executor.ElasticsearchExecutionEngine;
import org.elasticsearch.sql.search.executor.protector.ExecutionProtector;
import org.elasticsearch.sql.search.executor.protector.ElasticsearchExecutionProtector;
import org.elasticsearch.sql.search.monitor.ElasticsearchMemoryHealthy;
import org.elasticsearch.sql.search.monitor.ElasticsearchResourceMonitor;
import org.elasticsearch.sql.search.storage.ElasticsearchStorageEngine;
import org.elasticsearch.sql.storage.StorageEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Elasticsearch Plugin Config for SQL.
 */
@Configuration
@Import({ExpressionConfig.class})
public class ElasticsearchSQLPluginConfig {
  @Autowired
  private ClusterService clusterService;

  @Autowired
  private NodeClient nodeClient;

  @Autowired
  private Settings settings;

  @Autowired
  private BuiltinFunctionRepository functionRepository;

  @Bean
  public ElasticsearchClient client() {
    return new ElasticsearchNodeClient(clusterService, nodeClient);
  }

  @Bean
  public StorageEngine storageEngine() {
    return new ElasticsearchStorageEngine(client(), settings);
  }

  @Bean
  public ExecutionEngine executionEngine() {
    ElasticsearchFunctions.register(functionRepository);
    return new ElasticsearchExecutionEngine(client(), protector());
  }

  @Bean
  public ResourceMonitor resourceMonitor() {
    return new ElasticsearchResourceMonitor(settings, new ElasticsearchMemoryHealthy());
  }

  @Bean
  public ExecutionProtector protector() {
    return new ElasticsearchExecutionProtector(resourceMonitor());
  }
}
