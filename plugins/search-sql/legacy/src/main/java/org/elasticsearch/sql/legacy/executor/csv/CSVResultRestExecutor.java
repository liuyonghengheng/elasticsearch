


package org.elasticsearch.sql.legacy.executor.csv;

import com.google.common.base.Joiner;
import java.util.List;
import java.util.Map;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.sql.legacy.executor.QueryActionElasticExecutor;
import org.elasticsearch.sql.legacy.executor.RestExecutor;
import org.elasticsearch.sql.legacy.query.QueryAction;
import org.elasticsearch.sql.legacy.query.join.BackOffRetryStrategy;

/**
 * Created by Eliran on 26/12/2015.
 */
public class CSVResultRestExecutor implements RestExecutor {

    @Override
    public void execute(final Client client, final Map<String, String> params, final QueryAction queryAction,
                        final RestChannel channel) throws Exception {

        final String csvString = execute(client, params, queryAction);
        final BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, csvString);

        if (!BackOffRetryStrategy.isHealthy(2 * bytesRestResponse.content().length(), this)) {
            throw new IllegalStateException(
                    "[CSVResultRestExecutor] Memory could be insufficient when sendResponse().");
        }

        channel.sendResponse(bytesRestResponse);
    }

    @Override
    public String execute(final Client client, final Map<String, String> params, final QueryAction queryAction)
            throws Exception {

        final Object queryResult = QueryActionElasticExecutor.executeAnyAction(client, queryAction);

        final String separator = params.getOrDefault("separator", ",");
        final String newLine = params.getOrDefault("newLine", "\n");

        final boolean flat = getBooleanOrDefault(params, "flat", false);
        final boolean includeScore = getBooleanOrDefault(params, "_score", false);
        final boolean includeType = getBooleanOrDefault(params, "_type", false);
        final boolean includeId = getBooleanOrDefault(params, "_id", false);

        final List<String> fieldNames = queryAction.getFieldNames().orElse(null);
        final CSVResult result = new CSVResultsExtractor(includeScore, includeType, includeId)
                .extractResults(queryResult, flat, separator, fieldNames);

        return buildString(separator, result, newLine);
    }

    private boolean getBooleanOrDefault(Map<String, String> params, String param, boolean defaultValue) {
        boolean flat = defaultValue;
        if (params.containsKey(param)) {
            flat = Boolean.parseBoolean(params.get(param));
        }
        return flat;
    }

    private String buildString(String separator, CSVResult result, String newLine) {
        StringBuilder csv = new StringBuilder();
        csv.append(Joiner.on(separator).join(result.getHeaders()));
        csv.append(newLine);
        csv.append(Joiner.on(newLine).join(result.getLines()));
        return csv.toString();
    }

}
