


package org.elasticsearch.sql.legacy.executor;

import java.util.Map;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.sql.legacy.query.QueryAction;

/**
 * Created by Eliran on 26/12/2015.
 */
public interface RestExecutor {
    void execute(Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel)
            throws Exception;

    String execute(Client client, Map<String, String> params, QueryAction queryAction) throws Exception;
}
