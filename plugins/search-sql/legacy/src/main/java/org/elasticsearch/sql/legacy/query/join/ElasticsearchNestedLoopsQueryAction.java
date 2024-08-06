


package org.elasticsearch.sql.legacy.query.join;

import org.elasticsearch.client.Client;
import org.elasticsearch.sql.legacy.domain.JoinSelect;
import org.elasticsearch.sql.legacy.domain.Where;
import org.elasticsearch.sql.legacy.domain.hints.Hint;
import org.elasticsearch.sql.legacy.domain.hints.HintType;
import org.elasticsearch.sql.legacy.exception.SqlParseException;

/**
 * Created by Eliran on 15/9/2015.
 */
public class ElasticsearchNestedLoopsQueryAction extends ElasticsearchJoinQueryAction {

    public ElasticsearchNestedLoopsQueryAction(Client client, JoinSelect joinSelect) {
        super(client, joinSelect);
    }

    @Override
    protected void fillSpecificRequestBuilder(JoinRequestBuilder requestBuilder) throws SqlParseException {
        NestedLoopsElasticRequestBuilder nestedBuilder = (NestedLoopsElasticRequestBuilder) requestBuilder;
        Where where = joinSelect.getConnectedWhere();
        nestedBuilder.setConnectedWhere(where);

    }

    @Override
    protected JoinRequestBuilder createSpecificBuilder() {
        return new NestedLoopsElasticRequestBuilder();
    }

    @Override
    protected void updateRequestWithHints(JoinRequestBuilder requestBuilder) {
        super.updateRequestWithHints(requestBuilder);
        for (Hint hint : this.joinSelect.getHints()) {
            if (hint.getType() == HintType.NL_MULTISEARCH_SIZE) {
                Integer multiSearchMaxSize = (Integer) hint.getParams()[0];
                ((NestedLoopsElasticRequestBuilder) requestBuilder).setMultiSearchMaxSize(multiSearchMaxSize);
            }
        }
    }

    private String removeAlias(String field) {
        String alias = joinSelect.getFirstTable().getAlias();
        if (!field.startsWith(alias + ".")) {
            alias = joinSelect.getSecondTable().getAlias();
        }
        return field.replace(alias + ".", "");
    }

}
