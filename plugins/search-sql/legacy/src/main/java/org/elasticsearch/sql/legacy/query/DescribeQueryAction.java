


package org.elasticsearch.sql.legacy.query;

import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.sql.legacy.domain.IndexStatement;
import org.elasticsearch.sql.legacy.domain.QueryStatement;
import org.elasticsearch.sql.legacy.utils.Util;

public class DescribeQueryAction extends QueryAction {

    private final IndexStatement statement;

    public DescribeQueryAction(Client client, IndexStatement statement) {
        super(client, null);
        this.statement = statement;
    }

    @Override
    public QueryStatement getQueryStatement() {
        return statement;
    }

    @Override
    public SqlElasticsearchRequestBuilder explain() {
        final GetIndexRequestBuilder indexRequestBuilder = Util.prepareIndexRequestBuilder(client, statement);

        return new SqlElasticsearchRequestBuilder(indexRequestBuilder);
    }
}
