


package org.elasticsearch.sql.legacy.executor.cursor;

import static org.elasticsearch.rest.RestStatus.OK;

import java.util.Arrays;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.sql.common.setting.Settings;
import org.elasticsearch.sql.legacy.cursor.CursorType;
import org.elasticsearch.sql.legacy.cursor.DefaultCursor;
import org.elasticsearch.sql.legacy.esdomain.LocalClusterState;
import org.elasticsearch.sql.legacy.executor.Format;
import org.elasticsearch.sql.legacy.executor.format.Protocol;
import org.elasticsearch.sql.legacy.metrics.MetricName;
import org.elasticsearch.sql.legacy.metrics.Metrics;
import org.elasticsearch.sql.legacy.rewriter.matchtoterm.VerificationException;

public class CursorResultExecutor implements CursorRestExecutor {

    private String cursorId;
    private Format format;

    private static final Logger LOG = LogManager.getLogger(CursorResultExecutor.class);

    public CursorResultExecutor(String cursorId, Format format) {
        this.cursorId = cursorId;
        this.format = format;
    }

    public void execute(Client client, Map<String, String> params, RestChannel channel) throws Exception {
        try {
            String formattedResponse = execute(client, params);
            channel.sendResponse(new BytesRestResponse(OK, "application/json; charset=UTF-8", formattedResponse));
        } catch (IllegalArgumentException | JSONException e) {
            Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_CUS).increment();
            LOG.error("Error parsing the cursor", e);
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (ElasticsearchException e) {
            int status = (e.status().getStatus());
            if (status > 399 && status < 500) {
                Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_CUS).increment();
            } else if (status > 499) {
                Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
            }
            LOG.error("Error completing cursor request", e);
            channel.sendResponse(new BytesRestResponse(channel, e));
        }
    }

    public String execute(Client client, Map<String, String> params) throws Exception {
        /**
         * All cursor's are of the form <cursorType>:<base64 encoded cursor>
         * The serialized form before encoding is upto Cursor implementation
         */
        String[] splittedCursor = cursorId.split(":", 2);

        if (splittedCursor.length!=2) {
            throw new VerificationException("Not able to parse invalid cursor");
        }

        String type = splittedCursor[0];
        CursorType cursorType = CursorType.getById(type);

        switch(cursorType) {
            case DEFAULT:
                DefaultCursor defaultCursor = DefaultCursor.from(splittedCursor[1]);
                return handleDefaultCursorRequest(client, defaultCursor);
            case AGGREGATION:
            case JOIN:
            default: throw new VerificationException("Unsupported cursor type [" + type + "]");
        }
    }

    private String handleDefaultCursorRequest(Client client, DefaultCursor cursor) {
        String previousScrollId = cursor.getScrollId();
        LocalClusterState clusterState = LocalClusterState.state();
        TimeValue scrollTimeout = clusterState.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
        SearchResponse scrollResponse = client.prepareSearchScroll(previousScrollId).setScroll(scrollTimeout).get();
        SearchHits searchHits = scrollResponse.getHits();
        SearchHit[] searchHitArray = searchHits.getHits();
        String newScrollId = scrollResponse.getScrollId();

        int rowsLeft = (int) cursor.getRowsLeft();
        int fetch = cursor.getFetchSize();

        if (rowsLeft < fetch && rowsLeft < searchHitArray.length) {
            /**
             * This condition implies we are on the last page, and we might need to truncate the result from SearchHit[]
             * Avoid truncating in following two scenarios
             * 1. number of rows to be sent equals fetchSize
             * 2. size of SearchHit[] is already less that rows that needs to be sent
             *
             * Else truncate to desired number of rows
             */
            SearchHit[] newSearchHits = Arrays.copyOf(searchHitArray, rowsLeft);
            searchHits = new SearchHits(newSearchHits, searchHits.getTotalHits(), searchHits.getMaxScore());
        }

        rowsLeft = rowsLeft - fetch;

        if (rowsLeft <=0) {
            /** Clear the scroll context on last page */
            ClearScrollResponse clearScrollResponse = client.prepareClearScroll().addScrollId(newScrollId).get();
            if (!clearScrollResponse.isSucceeded()) {
                Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
                LOG.info("Error closing the cursor context {} ", newScrollId);
            }
        }

        cursor.setRowsLeft(rowsLeft);
        cursor.setScrollId(newScrollId);
        Protocol protocol = new Protocol(client, searchHits, format.name().toLowerCase(), cursor);
        return protocol.cursorFormat();
    }
}
