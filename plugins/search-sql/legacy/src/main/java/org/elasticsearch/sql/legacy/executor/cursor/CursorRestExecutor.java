



package org.elasticsearch.sql.legacy.executor.cursor;

import java.util.Map;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestChannel;

/**
 * Interface to execute cursor request.
 */
public interface CursorRestExecutor {

    void execute(Client client, Map<String, String> params, RestChannel channel)
            throws Exception;

    String execute(Client client, Map<String, String> params) throws Exception;
}
