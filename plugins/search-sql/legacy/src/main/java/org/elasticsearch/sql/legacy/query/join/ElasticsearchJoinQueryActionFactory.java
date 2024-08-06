


package org.elasticsearch.sql.legacy.query.join;

import java.util.List;
import org.elasticsearch.client.Client;
import org.elasticsearch.sql.legacy.domain.Condition;
import org.elasticsearch.sql.legacy.domain.JoinSelect;
import org.elasticsearch.sql.legacy.domain.hints.Hint;
import org.elasticsearch.sql.legacy.domain.hints.HintType;
import org.elasticsearch.sql.legacy.query.QueryAction;

/**
 * Created by Eliran on 15/9/2015.
 */
public class ElasticsearchJoinQueryActionFactory {
    public static QueryAction createJoinAction(Client client, JoinSelect joinSelect) {
        List<Condition> connectedConditions = joinSelect.getConnectedConditions();
        boolean allEqual = true;
        for (Condition condition : connectedConditions) {
            if (condition.getOPERATOR() != Condition.OPERATOR.EQ) {
                allEqual = false;
                break;
            }

        }
        if (!allEqual) {
            return new ElasticsearchNestedLoopsQueryAction(client, joinSelect);
        }

        boolean useNestedLoopsHintExist = false;
        for (Hint hint : joinSelect.getHints()) {
            if (hint.getType() == HintType.USE_NESTED_LOOPS) {
                useNestedLoopsHintExist = true;
                break;
            }
        }
        if (useNestedLoopsHintExist) {
            return new ElasticsearchNestedLoopsQueryAction(client, joinSelect);
        }

        return new ElasticsearchHashJoinQueryAction(client, joinSelect);

    }
}
