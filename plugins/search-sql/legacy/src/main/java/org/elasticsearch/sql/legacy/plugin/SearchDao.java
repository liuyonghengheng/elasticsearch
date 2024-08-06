


package org.elasticsearch.sql.legacy.plugin;

import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Set;
import org.elasticsearch.client.Client;
import org.elasticsearch.sql.legacy.domain.QueryActionRequest;
import org.elasticsearch.sql.legacy.exception.SQLFeatureDisabledException;
import org.elasticsearch.sql.legacy.exception.SqlParseException;
import org.elasticsearch.sql.legacy.query.ElasticsearchActionFactory;
import org.elasticsearch.sql.legacy.query.QueryAction;


public class SearchDao {

    private static final Set<String> END_TABLE_MAP = new HashSet<>();

    static {
        END_TABLE_MAP.add("limit");
        END_TABLE_MAP.add("order");
        END_TABLE_MAP.add("where");
        END_TABLE_MAP.add("group");

    }

    private Client client = null;

    public SearchDao(Client client) {
        this.client = client;
    }

    public Client getClient() {
        return client;
    }

    /**
     * Prepare action And transform sql
     * into Elasticsearch ActionRequest
     *
     * @param queryActionRequest SQL query action request to execute.
     * @return Elasticsearch request
     * @throws SqlParseException
     */
    public QueryAction explain(QueryActionRequest queryActionRequest)
        throws SqlParseException, SQLFeatureNotSupportedException, SQLFeatureDisabledException {
        return ElasticsearchActionFactory.create(client, queryActionRequest);
    }
}
