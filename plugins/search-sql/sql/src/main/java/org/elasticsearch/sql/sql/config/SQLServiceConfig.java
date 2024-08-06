


package org.elasticsearch.sql.sql.config;

import org.elasticsearch.sql.analysis.Analyzer;
import org.elasticsearch.sql.analysis.ExpressionAnalyzer;
import org.elasticsearch.sql.executor.ExecutionEngine;
import org.elasticsearch.sql.expression.config.ExpressionConfig;
import org.elasticsearch.sql.expression.function.BuiltinFunctionRepository;
import org.elasticsearch.sql.sql.SQLService;
import org.elasticsearch.sql.sql.antlr.SQLSyntaxParser;
import org.elasticsearch.sql.storage.StorageEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * SQL service configuration for Spring container initialization.
 */
@Configuration
@Import({ExpressionConfig.class})
public class SQLServiceConfig {

  @Autowired
  private StorageEngine storageEngine;

  @Autowired
  private ExecutionEngine executionEngine;

  @Autowired
  private BuiltinFunctionRepository functionRepository;

  @Bean
  public Analyzer analyzer() {
    return new Analyzer(new ExpressionAnalyzer(functionRepository), storageEngine);
  }

  @Bean
  public SQLService sqlService() {
    return new SQLService(new SQLSyntaxParser(), analyzer(), storageEngine, executionEngine,
        functionRepository);
  }

}

