


package org.elasticsearch.sql.legacy.executor.multi;

import com.alibaba.druid.sql.ast.statement.SQLUnionOperator;
import org.elasticsearch.client.Client;
import org.elasticsearch.sql.legacy.antlr.semantic.SemanticAnalysisException;
import org.elasticsearch.sql.legacy.executor.ElasticHitsExecutor;
import org.elasticsearch.sql.legacy.query.multi.MultiQueryRequestBuilder;

/**
 * Created by Eliran on 21/8/2016.
 */
public class MultiRequestExecutorFactory {
    public static ElasticHitsExecutor createExecutor(Client client, MultiQueryRequestBuilder builder) {
        SQLUnionOperator relation = builder.getRelation();
        switch (relation) {
            case UNION_ALL:
            case UNION:
                return new UnionExecutor(client, builder);
            case MINUS:
                return new MinusExecutor(client, builder);
            default:
                throw new SemanticAnalysisException("Unsupported operator: " + relation);
        }
    }
}
