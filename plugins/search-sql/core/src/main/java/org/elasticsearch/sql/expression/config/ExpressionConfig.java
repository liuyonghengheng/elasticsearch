

package org.elasticsearch.sql.expression.config;

import java.util.HashMap;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.aggregation.AggregatorFunction;
import org.elasticsearch.sql.expression.datetime.DateTimeFunction;
import org.elasticsearch.sql.expression.datetime.IntervalClause;
import org.elasticsearch.sql.expression.function.BuiltinFunctionRepository;
import org.elasticsearch.sql.expression.function.ElasticsearchFunctions;
import org.elasticsearch.sql.expression.operator.arthmetic.ArithmeticFunction;
import org.elasticsearch.sql.expression.operator.arthmetic.MathematicalFunction;
import org.elasticsearch.sql.expression.operator.convert.TypeCastOperator;
import org.elasticsearch.sql.expression.operator.predicate.BinaryPredicateOperator;
import org.elasticsearch.sql.expression.operator.predicate.UnaryPredicateOperator;
import org.elasticsearch.sql.expression.text.TextFunction;
import org.elasticsearch.sql.expression.window.WindowFunctions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Expression Config for Spring IoC.
 */
@Configuration
public class ExpressionConfig {
  /**
   * BuiltinFunctionRepository constructor.
   */
  @Bean
  public BuiltinFunctionRepository functionRepository() {
    BuiltinFunctionRepository builtinFunctionRepository =
        new BuiltinFunctionRepository(new HashMap<>());
    ArithmeticFunction.register(builtinFunctionRepository);
    BinaryPredicateOperator.register(builtinFunctionRepository);
    MathematicalFunction.register(builtinFunctionRepository);
    UnaryPredicateOperator.register(builtinFunctionRepository);
    AggregatorFunction.register(builtinFunctionRepository);
    DateTimeFunction.register(builtinFunctionRepository);
    IntervalClause.register(builtinFunctionRepository);
    WindowFunctions.register(builtinFunctionRepository);
    TextFunction.register(builtinFunctionRepository);
    TypeCastOperator.register(builtinFunctionRepository);
    ElasticsearchFunctions.register(builtinFunctionRepository);
    return builtinFunctionRepository;
  }

  @Bean
  public DSL dsl(BuiltinFunctionRepository repository) {
    return new DSL(repository);
  }
}
