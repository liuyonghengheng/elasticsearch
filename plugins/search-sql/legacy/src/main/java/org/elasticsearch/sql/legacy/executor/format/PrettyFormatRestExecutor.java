


package org.elasticsearch.sql.legacy.executor.format;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.sql.legacy.cursor.Cursor;
import org.elasticsearch.sql.legacy.cursor.DefaultCursor;
import org.elasticsearch.sql.legacy.exception.SqlParseException;
import org.elasticsearch.sql.legacy.executor.QueryActionElasticExecutor;
import org.elasticsearch.sql.legacy.executor.RestExecutor;
import org.elasticsearch.sql.legacy.query.DefaultQueryAction;
import org.elasticsearch.sql.legacy.query.QueryAction;
import org.elasticsearch.sql.legacy.query.join.BackOffRetryStrategy;

public class PrettyFormatRestExecutor implements RestExecutor {

    private static final Logger LOG = LogManager.getLogger();

    private final String format;

    public PrettyFormatRestExecutor(String format) {
        this.format = format.toLowerCase();
    }

    /**
     * Execute the QueryAction and return the REST response using the channel.
     */
    @Override
    public void execute(Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel) {
        String formattedResponse = execute(client, params, queryAction);
        BytesRestResponse bytesRestResponse;
        if (format.equals("jdbc")) {
            bytesRestResponse = new BytesRestResponse(RestStatus.OK,
                    "application/json; charset=UTF-8",
                    formattedResponse);
        } else {
            bytesRestResponse = new BytesRestResponse(RestStatus.OK, formattedResponse);
        }

        if (!BackOffRetryStrategy.isHealthy(2 * bytesRestResponse.content().length(), this)) {
            throw new IllegalStateException(
                    "[PrettyFormatRestExecutor] Memory could be insufficient when sendResponse().");
        }

        channel.sendResponse(bytesRestResponse);
    }

    @Override
    public String execute(Client client, Map<String, String> params, QueryAction queryAction) {
        Protocol protocol;

        try {
            if (queryAction instanceof DefaultQueryAction) {
                protocol = buildProtocolForDefaultQuery(client, (DefaultQueryAction) queryAction);
            } else {
                Object queryResult = QueryActionElasticExecutor.executeAnyAction(client, queryAction);
                protocol = new Protocol(client, queryAction, queryResult, format, Cursor.NULL_CURSOR);
            }
        } catch (Exception e) {
            if (e instanceof ElasticsearchException) {
                LOG.warn("An error occurred in Elasticsearch engine: "
                        + ((ElasticsearchException) e).getDetailedMessage(), e);
            } else {
                LOG.warn("Error happened in pretty formatter", e);
            }
            protocol = new Protocol(e);
        }

        return protocol.format();
    }

    /**
     * QueryActionElasticExecutor.executeAnyAction() returns SearchHits inside SearchResponse.
     * In order to get scroll ID if any, we need to execute DefaultQueryAction ourselves for SearchResponse.
     */
    private Protocol buildProtocolForDefaultQuery(Client client, DefaultQueryAction queryAction)
            throws SqlParseException {

        SearchResponse response = (SearchResponse) queryAction.explain().get();
        String scrollId = response.getScrollId();

        Protocol protocol;
        if (!Strings.isNullOrEmpty(scrollId)) {
            DefaultCursor defaultCursor = new DefaultCursor();
            defaultCursor.setScrollId(scrollId);
            defaultCursor.setLimit(queryAction.getSelect().getRowCount());
            defaultCursor.setFetchSize(queryAction.getSqlRequest().fetchSize());
            protocol = new Protocol(client, queryAction, response.getHits(), format, defaultCursor);
        } else {
            protocol = new Protocol(client, queryAction, response.getHits(), format, Cursor.NULL_CURSOR);
        }

        return protocol;
    }
}
