


package org.elasticsearch.sql.legacy.expression.core.builder;

import java.util.List;
import org.elasticsearch.sql.legacy.expression.core.Expression;

/**
 * The definition of the {@link Expression} builder.
 */
public interface ExpressionBuilder {
    Expression build(List<Expression> expressionList);
}
