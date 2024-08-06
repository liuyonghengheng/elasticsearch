

package org.elasticsearch.sql.expression.window;

import static java.util.Collections.emptyList;

import com.google.common.collect.ImmutableMap;
import java.util.function.Supplier;
import lombok.experimental.UtilityClass;
import org.elasticsearch.sql.expression.function.BuiltinFunctionName;
import org.elasticsearch.sql.expression.function.BuiltinFunctionRepository;
import org.elasticsearch.sql.expression.function.FunctionBuilder;
import org.elasticsearch.sql.expression.function.FunctionName;
import org.elasticsearch.sql.expression.function.FunctionResolver;
import org.elasticsearch.sql.expression.function.FunctionSignature;
import org.elasticsearch.sql.expression.window.ranking.DenseRankFunction;
import org.elasticsearch.sql.expression.window.ranking.RankFunction;
import org.elasticsearch.sql.expression.window.ranking.RankingWindowFunction;
import org.elasticsearch.sql.expression.window.ranking.RowNumberFunction;

/**
 * Window functions that register all window functions in function repository.
 */
@UtilityClass
public class WindowFunctions {

  /**
   * Register all window functions to function repository.
   * @param repository  function repository
   */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(rowNumber());
    repository.register(rank());
    repository.register(denseRank());
  }

  private FunctionResolver rowNumber() {
    return rankingFunction(BuiltinFunctionName.ROW_NUMBER.getName(), RowNumberFunction::new);
  }

  private FunctionResolver rank() {
    return rankingFunction(BuiltinFunctionName.RANK.getName(), RankFunction::new);
  }

  private FunctionResolver denseRank() {
    return rankingFunction(BuiltinFunctionName.DENSE_RANK.getName(), DenseRankFunction::new);
  }

  private FunctionResolver rankingFunction(FunctionName functionName,
                                           Supplier<RankingWindowFunction> constructor) {
    FunctionSignature functionSignature = new FunctionSignature(functionName, emptyList());
    FunctionBuilder functionBuilder = arguments -> constructor.get();
    return new FunctionResolver(functionName, ImmutableMap.of(functionSignature, functionBuilder));
  }

}
