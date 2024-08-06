

package org.elasticsearch.sql.expression.operator.predicate;

import static org.elasticsearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.elasticsearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.elasticsearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.elasticsearch.sql.expression.function.FunctionDSL.impl;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.elasticsearch.sql.data.model.ExprBooleanValue;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.expression.function.BuiltinFunctionName;
import org.elasticsearch.sql.expression.function.BuiltinFunctionRepository;
import org.elasticsearch.sql.expression.function.FunctionBuilder;
import org.elasticsearch.sql.expression.function.FunctionDSL;
import org.elasticsearch.sql.expression.function.FunctionName;
import org.elasticsearch.sql.expression.function.FunctionResolver;
import org.elasticsearch.sql.expression.function.FunctionSignature;
import org.elasticsearch.sql.expression.function.SerializableFunction;

/**
 * The definition of unary predicate function
 * not, Accepts one Boolean value and produces a Boolean.
 */
@UtilityClass
public class UnaryPredicateOperator {
  /**
   * Register Unary Predicate Function.
   */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(not());
    repository.register(isNotNull());
    repository.register(ifNull());
    repository.register(nullIf());
    repository.register(isNull(BuiltinFunctionName.IS_NULL));
    repository.register(isNull(BuiltinFunctionName.ISNULL));
    repository.register(ifFunction());
  }

  private static FunctionResolver not() {
    return FunctionDSL.define(BuiltinFunctionName.NOT.getName(), FunctionDSL
        .impl(UnaryPredicateOperator::not, BOOLEAN, BOOLEAN));
  }

  /**
   * The not logic.
   * A       NOT A
   * TRUE    FALSE
   * FALSE   TRUE
   * NULL    NULL
   * MISSING MISSING
   */
  public ExprValue not(ExprValue v) {
    if (v.isMissing() || v.isNull()) {
      return v;
    } else {
      return ExprBooleanValue.of(!v.booleanValue());
    }
  }

  private static FunctionResolver isNull(BuiltinFunctionName funcName) {
    return FunctionDSL
        .define(funcName.getName(), Arrays.stream(ExprCoreType.values())
            .map(type -> FunctionDSL
                .impl((v) -> ExprBooleanValue.of(v.isNull()), BOOLEAN, type))
            .collect(
                Collectors.toList()));
  }

  private static FunctionResolver isNotNull() {
    return FunctionDSL
        .define(BuiltinFunctionName.IS_NOT_NULL.getName(), Arrays.stream(ExprCoreType.values())
            .map(type -> FunctionDSL
                .impl((v) -> ExprBooleanValue.of(!v.isNull()), BOOLEAN, type))
            .collect(
                Collectors.toList()));
  }

  private static FunctionResolver ifFunction() {
    FunctionName functionName = BuiltinFunctionName.IF.getName();
    List<ExprCoreType> typeList = ExprCoreType.coreTypes();

    List<SerializableFunction<FunctionName, org.apache.commons.lang3.tuple.Pair<FunctionSignature,
            FunctionBuilder>>> functionsOne = typeList.stream().map(v ->
            impl((UnaryPredicateOperator::exprIf), v, BOOLEAN, v, v))
            .collect(Collectors.toList());

    FunctionResolver functionResolver = FunctionDSL.define(functionName, functionsOne);
    return functionResolver;
  }

  private static FunctionResolver ifNull() {
    FunctionName functionName = BuiltinFunctionName.IFNULL.getName();
    List<ExprCoreType> typeList = ExprCoreType.coreTypes();

    List<SerializableFunction<FunctionName, org.apache.commons.lang3.tuple.Pair<FunctionSignature,
            FunctionBuilder>>> functionsOne = typeList.stream().map(v ->
            impl((UnaryPredicateOperator::exprIfNull), v, v, v))
            .collect(Collectors.toList());

    FunctionResolver functionResolver = FunctionDSL.define(functionName, functionsOne);
    return functionResolver;
  }

  private static FunctionResolver nullIf() {
    FunctionName functionName = BuiltinFunctionName.NULLIF.getName();
    List<ExprCoreType> typeList = ExprCoreType.coreTypes();

    FunctionResolver functionResolver =
        FunctionDSL.define(functionName,
            typeList.stream().map(v ->
              impl((UnaryPredicateOperator::exprNullIf), v, v, v))
              .collect(Collectors.toList()));
    return functionResolver;
  }

  /** v2 if v1 is null.
   * @param v1 varable 1
   * @param v2 varable 2
   * @return v2 if v1 is null
   */
  public static ExprValue exprIfNull(ExprValue v1, ExprValue v2) {
    return (v1.isNull() || v1.isMissing()) ? v2 : v1;
  }

  /** return null if v1 equls to v2.
   * @param v1 varable 1
   * @param v2 varable 2
   * @return null if v1 equls to v2
   */
  public static ExprValue exprNullIf(ExprValue v1, ExprValue v2) {
    return v1.equals(v2) ? LITERAL_NULL : v1;
  }

  public static ExprValue exprIf(ExprValue v1, ExprValue v2, ExprValue v3) {
    return !v1.isNull() && !v1.isMissing() && LITERAL_TRUE.equals(v1) ? v2 : v3;
  }

}
