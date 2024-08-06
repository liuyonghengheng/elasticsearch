

package org.elasticsearch.sql.search.storage.script.filter;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.script.Script.DEFAULT_SCRIPT_TYPE;
import static org.elasticsearch.sql.search.storage.script.ExpressionScriptEngine.EXPRESSION_LANG_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.expression.ExpressionNodeVisitor;
import org.elasticsearch.sql.expression.FunctionExpression;
import org.elasticsearch.sql.expression.function.BuiltinFunctionName;
import org.elasticsearch.sql.expression.function.FunctionName;
import org.elasticsearch.sql.search.storage.script.filter.lucene.LuceneQuery;
import org.elasticsearch.sql.search.storage.script.filter.lucene.RangeQuery;
import org.elasticsearch.sql.search.storage.script.filter.lucene.RangeQuery.Comparison;
import org.elasticsearch.sql.search.storage.script.filter.lucene.TermQuery;
import org.elasticsearch.sql.search.storage.script.filter.lucene.WildcardQuery;
import org.elasticsearch.sql.search.storage.script.filter.lucene.relevance.MatchQuery;
import org.elasticsearch.sql.search.storage.serialization.ExpressionSerializer;

@RequiredArgsConstructor
public class FilterQueryBuilder extends ExpressionNodeVisitor<QueryBuilder, Object> {

  /**
   * Serializer that serializes expression for build DSL query.
   */
  private final ExpressionSerializer serializer;

  /**
   * Mapping from function name to lucene query builder.
   */
  private final Map<FunctionName, LuceneQuery> luceneQueries =
      ImmutableMap.<FunctionName, LuceneQuery>builder()
          .put(BuiltinFunctionName.EQUAL.getName(), new TermQuery())
          .put(BuiltinFunctionName.LESS.getName(), new RangeQuery(Comparison.LT))
          .put(BuiltinFunctionName.GREATER.getName(), new RangeQuery(Comparison.GT))
          .put(BuiltinFunctionName.LTE.getName(), new RangeQuery(Comparison.LTE))
          .put(BuiltinFunctionName.GTE.getName(), new RangeQuery(Comparison.GTE))
          .put(BuiltinFunctionName.LIKE.getName(), new WildcardQuery())
          .put(BuiltinFunctionName.MATCH.getName(), new MatchQuery())
          .put(BuiltinFunctionName.QUERY.getName(), new MatchQuery())
          .put(BuiltinFunctionName.MATCH_QUERY.getName(), new MatchQuery())
          .put(BuiltinFunctionName.MATCHQUERY.getName(), new MatchQuery())
          .build();

  /**
   * Build Elasticsearch filter query from expression.
   * @param expr  expression
   * @return      query
   */
  public QueryBuilder build(Expression expr) {
    return expr.accept(this, null);
  }

  @Override
  public QueryBuilder visitFunction(FunctionExpression func, Object context) {
    FunctionName name = func.getFunctionName();
    switch (name.getFunctionName()) {
      case "and":
        return buildBoolQuery(func, context, BoolQueryBuilder::filter);
      case "or":
        return buildBoolQuery(func, context, BoolQueryBuilder::should);
      case "not":
        return buildBoolQuery(func, context, BoolQueryBuilder::mustNot);
      default: {
        LuceneQuery query = luceneQueries.get(name);
        if (query != null && query.canSupport(func)) {
          return query.build(func);
        }
        return buildScriptQuery(func);
      }
    }
  }

  private BoolQueryBuilder buildBoolQuery(FunctionExpression node,
                                          Object context,
                                          BiFunction<BoolQueryBuilder, QueryBuilder,
                                              QueryBuilder> accumulator) {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    for (Expression arg : node.getArguments()) {
      accumulator.apply(boolQuery, arg.accept(this, context));
    }
    return boolQuery;
  }

  private ScriptQueryBuilder buildScriptQuery(FunctionExpression node) {
    return new ScriptQueryBuilder(new Script(
        DEFAULT_SCRIPT_TYPE, EXPRESSION_LANG_NAME, serializer.serialize(node), emptyMap()));
  }

}
