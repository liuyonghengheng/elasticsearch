


package org.elasticsearch.sql.legacy.expression.model;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode
@RequiredArgsConstructor
public class ExprIntegerValue implements ExprValue {
    private final Integer value;

    @Override
    public Object value() {
        return value;
    }

    @Override
    public ExprValueKind kind() {
        return ExprValueKind.INTEGER_VALUE;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
