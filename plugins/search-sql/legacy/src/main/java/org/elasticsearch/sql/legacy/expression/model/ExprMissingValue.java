


package org.elasticsearch.sql.legacy.expression.model;

/**
 * The definition of the missing value.
 */
public class ExprMissingValue implements ExprValue {
    @Override
    public ExprValueKind kind() {
        return ExprValueKind.MISSING_VALUE;
    }
}
