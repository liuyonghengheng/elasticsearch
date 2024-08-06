


package org.elasticsearch.sql.legacy.plugin;

import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.sql.executor.ExecutionEngine.QueryResponse;
import static org.elasticsearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.sql.common.antlr.SyntaxCheckException;
import org.elasticsearch.sql.common.response.ResponseListener;
import org.elasticsearch.sql.common.setting.Settings;
import org.elasticsearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.elasticsearch.sql.legacy.metrics.MetricName;
import org.elasticsearch.sql.legacy.metrics.Metrics;
import org.elasticsearch.sql.search.security.SecurityAccess;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.protocol.response.QueryResult;
import org.elasticsearch.sql.protocol.response.format.CsvResponseFormatter;
import org.elasticsearch.sql.protocol.response.format.Format;
import org.elasticsearch.sql.protocol.response.format.JdbcResponseFormatter;
import org.elasticsearch.sql.protocol.response.format.JsonResponseFormatter;
import org.elasticsearch.sql.protocol.response.format.RawResponseFormatter;
import org.elasticsearch.sql.protocol.response.format.ResponseFormatter;
import org.elasticsearch.sql.sql.SQLService;
import org.elasticsearch.sql.sql.config.SQLServiceConfig;
import org.elasticsearch.sql.sql.domain.SQLQueryRequest;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * New SQL REST action handler. This will not be registered to Elasticsearch unless:
 *  1) we want to test new SQL engine;
 *  2) all old functionalities migrated to new query engine and legacy REST handler removed.
 */
public class RestSQLQueryAction extends BaseRestHandler {

  private static final Logger LOG = LogManager.getLogger();

  public static final RestChannelConsumer NOT_SUPPORTED_YET = null;

  private final ClusterService clusterService;

  /**
   * Settings required by been initialization.
   */
  private final Settings pluginSettings;

  /**
   * Constructor of RestSQLQueryAction.
   */
  public RestSQLQueryAction(ClusterService clusterService, Settings pluginSettings) {
    super();
    this.clusterService = clusterService;
    this.pluginSettings = pluginSettings;
  }

  @Override
  public String getName() {
    return "sql_query_action";
  }

  @Override
  public List<Route> routes() {
    throw new UnsupportedOperationException("New SQL handler is not ready yet");
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient nodeClient) {
    throw new UnsupportedOperationException("New SQL handler is not ready yet");
  }

  /**
   * Prepare REST channel consumer for a SQL query request.
   * @param request     SQL request
   * @param nodeClient  node client
   * @return            channel consumer
   */
  public RestChannelConsumer prepareRequest(SQLQueryRequest request, NodeClient nodeClient) {
    if (!request.isSupported()) {
      return NOT_SUPPORTED_YET;
    }

    SQLService sqlService = createSQLService(nodeClient);
    PhysicalPlan plan;
    try {
      // For now analyzing and planning stage may throw syntax exception as well
      // which hints the fallback to legacy code is necessary here.
      plan = sqlService.plan(
                sqlService.analyze(
                    sqlService.parse(request.getQuery())));
    } catch (SyntaxCheckException e) {
      // When explain, print info log for what unsupported syntax is causing fallback to old engine
      if (request.isExplainRequest()) {
        LOG.info("Request is falling back to old SQL engine due to: " + e.getMessage());
      }
      return NOT_SUPPORTED_YET;
    }

    if (request.isExplainRequest()) {
      return channel -> sqlService.explain(plan, createExplainResponseListener(channel));
    }
    return channel -> sqlService.execute(plan, createQueryResponseListener(channel, request));
  }

  private SQLService createSQLService(NodeClient client) {
    return doPrivileged(() -> {
      AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
      context.registerBean(ClusterService.class, () -> clusterService);
      context.registerBean(NodeClient.class, () -> client);
      context.registerBean(Settings.class, () -> pluginSettings);
      context.register(ElasticsearchSQLPluginConfig.class);
      context.register(SQLServiceConfig.class);
      context.refresh();
      return context.getBean(SQLService.class);
    });
  }

  private ResponseListener<ExplainResponse> createExplainResponseListener(RestChannel channel) {
    return new ResponseListener<ExplainResponse>() {
      @Override
      public void onResponse(ExplainResponse response) {
        sendResponse(channel, OK, new JsonResponseFormatter<ExplainResponse>(PRETTY) {
          @Override
          protected Object buildJsonObject(ExplainResponse response) {
            return response;
          }
        }.format(response));
      }

      @Override
      public void onFailure(Exception e) {
        LOG.error("Error happened during explain", e);
        logAndPublishMetrics(e);
        sendResponse(channel, INTERNAL_SERVER_ERROR,
            "Failed to explain the query due to error: " + e.getMessage());
      }
    };
  }

  private ResponseListener<QueryResponse> createQueryResponseListener(RestChannel channel, SQLQueryRequest request) {
    Format format = request.format();
    ResponseFormatter<QueryResult> formatter;
    if (format.equals(Format.CSV)) {
      formatter = new CsvResponseFormatter(request.sanitize());
    } else if (format.equals(Format.RAW)) {
      formatter = new RawResponseFormatter();
    } else {
      formatter = new JdbcResponseFormatter(PRETTY);
    }
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
        sendResponse(channel, OK,
            formatter.format(new QueryResult(response.getSchema(), response.getResults())));
      }

      @Override
      public void onFailure(Exception e) {
        LOG.error("Error happened during query handling", e);
        logAndPublishMetrics(e);
        sendResponse(channel, INTERNAL_SERVER_ERROR, formatter.format(e));
      }
    };
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }

  private void sendResponse(RestChannel channel, RestStatus status, String content) {
    channel.sendResponse(new BytesRestResponse(
        status, "application/json; charset=UTF-8", content));
  }

  private static void logAndPublishMetrics(Exception e) {
    LOG.error("Server side error during query execution", e);
    Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
  }
}
