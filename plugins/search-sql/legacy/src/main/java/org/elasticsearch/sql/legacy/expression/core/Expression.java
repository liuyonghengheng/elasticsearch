


package org.elasticsearch.sql.legacy.expression.core;


import org.elasticsearch.sql.legacy.expression.domain.BindingTuple;
import org.elasticsearch.sql.legacy.expression.model.ExprValue;

/**
 * The definition of the Expression.
 */
public interface Expression {
    /**
     * Evaluate the result on the BindingTuple context.
     * @param tuple BindingTuple
     * @return ExprValue
     */
    ExprValue valueOf(BindingTuple tuple);
}
