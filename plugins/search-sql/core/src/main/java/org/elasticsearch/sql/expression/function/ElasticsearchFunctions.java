
package org.elasticsearch.sql.expression.function;

import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.stream.Collectors;
import lombok.ToString;
import lombok.experimental.UtilityClass;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.data.type.ExprCoreType;
import org.elasticsearch.sql.data.type.ExprType;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.FunctionExpression;
import org.elasticsearch.sql.expression.NamedArgumentExpression;
import org.elasticsearch.sql.expression.env.Environment;

@UtilityClass
public class ElasticsearchFunctions {
  public void register(BuiltinFunctionRepository repository) {
    repository.register(match());
  }

  private static FunctionResolver match() {
    FunctionName funcName = BuiltinFunctionName.MATCH.getName();
    return new FunctionResolver(funcName,
        ImmutableMap.<FunctionSignature, FunctionBuilder>builder()
            .put(new FunctionSignature(funcName, ImmutableList.of(STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList.of(STRING, STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList.of(STRING, STRING, STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING, STRING)),
                args -> new ElasticsearchFunction(funcName, args))
            .build());
  }

  private static class ElasticsearchFunction extends FunctionExpression {
    private final FunctionName functionName;
    private final List<Expression> arguments;

    public ElasticsearchFunction(FunctionName functionName, List<Expression> arguments) {
      super(functionName, arguments);
      this.functionName = functionName;
      this.arguments = arguments;
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      throw new UnsupportedOperationException(String.format(
          "Elasticsearch defined function [%s] is only supported in WHERE and HAVING clause.",
          functionName));
    }

    @Override
    public ExprType type() {
      return ExprCoreType.BOOLEAN;
    }

    @Override
    public String toString() {
      List<String> args = arguments.stream()
          .map(arg -> String.format("%s=%s", ((NamedArgumentExpression) arg)
              .getArgName(), ((NamedArgumentExpression) arg).getValue().toString()))
          .collect(Collectors.toList());
      return String.format("%s(%s)", functionName, String.join(", ", args));
    }
  }
}
