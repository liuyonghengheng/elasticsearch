/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

/**
 * Common base for trigonometric functions.
 */
abstract class AbstractTrigonometricFunction extends UnaryScalarFunction implements Mappable {
    AbstractTrigonometricFunction(Source source, Expression field) {
        super(source, field);
    }

    protected abstract EvalOperator.ExpressionEvaluator doubleEvaluator(EvalOperator.ExpressionEvaluator field);

    @Override
    public final Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> fieldEval = Cast.cast(field().dataType(), DataTypes.DOUBLE, toEvaluator.apply(field()));
        return () -> doubleEvaluator(fieldEval.get());
    }

    @Override
    public final Object fold() {
        return Mappable.super.fold();
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isNumeric(field, sourceText(), DEFAULT);
    }

    @Override
    public final DataType dataType() {
        return DataTypes.DOUBLE;
    }
}
