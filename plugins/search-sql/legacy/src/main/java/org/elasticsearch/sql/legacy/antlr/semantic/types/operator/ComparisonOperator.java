


package org.elasticsearch.sql.legacy.antlr.semantic.types.operator;

import static org.elasticsearch.sql.legacy.antlr.semantic.types.base.ElasticsearchDataType.BOOLEAN;
import static org.elasticsearch.sql.legacy.antlr.semantic.types.base.ElasticsearchDataType.TYPE_ERROR;

import java.util.List;
import org.elasticsearch.sql.legacy.antlr.semantic.types.Type;

/**
 * Type for comparison operator
 */
public enum ComparisonOperator implements Type {

    EQUAL("="),
    NOT_EQUAL("<>"),
    NOT_EQUAL2("!="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL_TO(">="),
    SMALLER_THAN("<"),
    SMALLER_THAN_OR_EQUAL_TO("<="),
    IS("IS");

    /** Actual name representing the operator */
    private final String name;

    ComparisonOperator(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Type construct(List<Type> actualArgs) {
        if (actualArgs.size() != 2) {
            return TYPE_ERROR;
        }

        Type leftType = actualArgs.get(0);
        Type rightType = actualArgs.get(1);
        if (leftType.isCompatible(rightType) || rightType.isCompatible(leftType)) {
            return BOOLEAN;
        }
        return TYPE_ERROR;
    }

    @Override
    public String usage() {
        return "Please use compatible types from each side.";
    }

    @Override
    public String toString() {
        return "Operator [" + getName() + "]";
    }
}
