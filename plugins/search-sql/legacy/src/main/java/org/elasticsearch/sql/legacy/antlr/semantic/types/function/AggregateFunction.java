


package org.elasticsearch.sql.legacy.antlr.semantic.types.function;

import static org.elasticsearch.sql.legacy.antlr.semantic.types.base.ElasticsearchDataType.DOUBLE;
import static org.elasticsearch.sql.legacy.antlr.semantic.types.base.ElasticsearchDataType.INTEGER;
import static org.elasticsearch.sql.legacy.antlr.semantic.types.base.ElasticsearchDataType.NUMBER;
import static org.elasticsearch.sql.legacy.antlr.semantic.types.base.ElasticsearchDataType.EASYSEARCH_TYPE;
import static org.elasticsearch.sql.legacy.antlr.semantic.types.special.Generic.T;

import org.elasticsearch.sql.legacy.antlr.semantic.types.Type;
import org.elasticsearch.sql.legacy.antlr.semantic.types.TypeExpression;

/**
 * Aggregate function
 */
public enum AggregateFunction implements TypeExpression {
    COUNT(
        func().to(INTEGER), // COUNT(*)
        func(EASYSEARCH_TYPE).to(INTEGER)
    ),
    MAX(func(T(NUMBER)).to(T)),
    MIN(func(T(NUMBER)).to(T)),
    AVG(func(T(NUMBER)).to(DOUBLE)),
    SUM(func(T(NUMBER)).to(T));

    private TypeExpressionSpec[] specifications;

    AggregateFunction(TypeExpressionSpec... specifications) {
        this.specifications = specifications;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public TypeExpressionSpec[] specifications() {
        return specifications;
    }

    private static TypeExpressionSpec func(Type... argTypes) {
        return new TypeExpressionSpec().map(argTypes);
    }

    @Override
    public String toString() {
        return "Function [" + name() + "]";
    }
}
