


package org.elasticsearch.sql.legacy.expression.model;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode
@RequiredArgsConstructor
public class ExprDoubleValue implements ExprValue {
    private final Double value;

    @Override
    public Object value() {
        return value;
    }

    @Override
    public ExprValueKind kind() {
        return ExprValueKind.DOUBLE_VALUE;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
