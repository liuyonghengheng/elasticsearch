


package org.elasticsearch.sql.legacy.executor.cursor;

import static org.elasticsearch.rest.RestStatus.OK;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.sql.legacy.cursor.CursorType;
import org.elasticsearch.sql.legacy.cursor.DefaultCursor;
import org.elasticsearch.sql.legacy.metrics.MetricName;
import org.elasticsearch.sql.legacy.metrics.Metrics;
import org.elasticsearch.sql.legacy.rewriter.matchtoterm.VerificationException;

public class CursorCloseExecutor implements CursorRestExecutor {

    private static final Logger LOG = LogManager.getLogger(CursorCloseExecutor.class);

    private static final String SUCCEEDED_TRUE = "{\"succeeded\":true}";
    private static final String SUCCEEDED_FALSE = "{\"succeeded\":false}";

    private String cursorId;

    public CursorCloseExecutor(String cursorId) {
        this.cursorId = cursorId;
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
        String[] splittedCursor = cursorId.split(":");

        if (splittedCursor.length!=2) {
            throw new VerificationException("Not able to parse invalid cursor");
        }

        String type = splittedCursor[0];
        CursorType cursorType = CursorType.getById(type);

        switch(cursorType) {
            case DEFAULT:
                DefaultCursor defaultCursor = DefaultCursor.from(splittedCursor[1]);
                return handleDefaultCursorCloseRequest(client, defaultCursor);
            case AGGREGATION:
            case JOIN:
            default: throw new VerificationException("Unsupported cursor type [" + type + "]");
        }

    }

    private String handleDefaultCursorCloseRequest(Client client, DefaultCursor cursor) {
        String scrollId = cursor.getScrollId();
        ClearScrollResponse clearScrollResponse = client.prepareClearScroll().addScrollId(scrollId).get();
        if (clearScrollResponse.isSucceeded()) {
            return SUCCEEDED_TRUE;
        } else {
            Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
            return SUCCEEDED_FALSE;
        }
    }
}
