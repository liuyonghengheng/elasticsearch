


package org.elasticsearch.sql.legacy.executor.cursor;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.sql.legacy.executor.Format;

public class CursorActionRequestRestExecutorFactory {

    public static CursorAsyncRestExecutor createExecutor(RestRequest request, String cursorId, Format format) {

        if (isCursorCloseRequest(request)) {
            return new CursorAsyncRestExecutor(new CursorCloseExecutor(cursorId));
        } else {
            return new CursorAsyncRestExecutor(new CursorResultExecutor(cursorId, format));
        }
    }

    private static boolean isCursorCloseRequest(final RestRequest request) {
        return request.path().endsWith("/_sql/close");
    }
}
