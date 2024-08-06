


package org.elasticsearch.sql.legacy.expression.core.operator;

import java.util.List;
import org.elasticsearch.sql.legacy.expression.model.ExprValue;

/**
 * Scalar Operator is a function has one or more arguments and return a single value.
 */
public interface ScalarOperator {
    /**
     * Apply the operator to the input arguments.
     * @param valueList argument list.
     * @return result.
     */
    ExprValue apply(List<ExprValue> valueList);

    /**
     * The name of the operator.
     * @return name.
     */
    String name();
}
