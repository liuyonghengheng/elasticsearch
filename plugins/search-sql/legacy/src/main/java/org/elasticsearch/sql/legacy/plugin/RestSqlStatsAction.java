


package org.elasticsearch.sql.legacy.plugin;

import static org.elasticsearch.rest.RestStatus.SERVICE_UNAVAILABLE;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.sql.legacy.executor.format.ErrorMessageFactory;
import org.elasticsearch.sql.legacy.metrics.Metrics;
import org.elasticsearch.sql.legacy.utils.LogUtils;

/**
 * Currently this interface is for node level.
 * Cluster level is coming up soon. https://github.com/opendistro-for-elasticsearch/sql/issues/41
 */
public class RestSqlStatsAction extends BaseRestHandler {
    private static final Logger LOG = LogManager.getLogger(RestSqlStatsAction.class);

    /**
     * API endpoint path
     */
    public static final String STATS_API_ENDPOINT = "/_sql/stats";
    //public static final String LEGACY_STATS_API_ENDPOINT = "/_opendistro/_sql/stats";

    public RestSqlStatsAction(Settings settings, RestController restController) {
        super();
    }

    @Override
    public String getName() {
        return "sql_stats_action";
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(RestRequest.Method.POST, STATS_API_ENDPOINT));
    }

//    @Override
//    public List<ReplacedRoute> replacedRoutes() {
//        return ImmutableList.of(
//            new ReplacedRoute(
//                RestRequest.Method.POST, STATS_API_ENDPOINT,
//                RestRequest.Method.POST, LEGACY_STATS_API_ENDPOINT),
//            new ReplacedRoute(
//                RestRequest.Method.GET, STATS_API_ENDPOINT,
//                RestRequest.Method.GET, LEGACY_STATS_API_ENDPOINT));
//    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {

        LogUtils.addRequestId();

        try {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK,
                    Metrics.getInstance().collectToJSON()));
        } catch (Exception e) {
            LOG.error("Failed during Query SQL STATS Action.", e);

            return channel -> channel.sendResponse(new BytesRestResponse(SERVICE_UNAVAILABLE,
                    ErrorMessageFactory.createErrorMessage(e, SERVICE_UNAVAILABLE.getStatus()).toString()));
        }
    }

    @Override
    protected Set<String> responseParams() {
        Set<String> responseParams = new HashSet<>(super.responseParams());
        responseParams.addAll(Arrays.asList("sql", "flat", "separator", "_score", "_type", "_id", "newLine", "format", "sanitize"));
        return responseParams;
    }

}
