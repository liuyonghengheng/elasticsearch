


package org.elasticsearch.sql.legacy.query.multi;

import org.elasticsearch.client.Client;
import org.elasticsearch.sql.legacy.exception.SqlParseException;
import org.elasticsearch.sql.legacy.query.QueryAction;

/**
 * Created by Eliran on 19/8/2016.
 */
public class ElasticsearchMultiQueryActionFactory {

    public static QueryAction createMultiQueryAction(Client client, MultiQuerySelect multiSelect)
            throws SqlParseException {
        switch (multiSelect.getOperation()) {
            case UNION_ALL:
            case UNION:
                return new MultiQueryAction(client, multiSelect);
            default:
                throw new SqlParseException("only supports union and union all");
        }
    }
}
