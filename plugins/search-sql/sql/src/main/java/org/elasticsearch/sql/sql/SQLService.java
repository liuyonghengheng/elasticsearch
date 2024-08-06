


package org.elasticsearch.sql.sql;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.sql.analysis.AnalysisContext;
import org.elasticsearch.sql.analysis.Analyzer;
import org.elasticsearch.sql.ast.tree.UnresolvedPlan;
import org.elasticsearch.sql.common.response.ResponseListener;
import org.elasticsearch.sql.executor.ExecutionEngine;
import org.elasticsearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.elasticsearch.sql.executor.ExecutionEngine.QueryResponse;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.function.BuiltinFunctionRepository;
import org.elasticsearch.sql.planner.Planner;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.sql.antlr.SQLSyntaxParser;
import org.elasticsearch.sql.sql.domain.SQLQueryRequest;
import org.elasticsearch.sql.sql.parser.AstBuilder;
import org.elasticsearch.sql.storage.StorageEngine;

/**
 * SQL service.
 */
@RequiredArgsConstructor
public class SQLService {

  private final SQLSyntaxParser parser;

  private final Analyzer analyzer;

  private final StorageEngine storageEngine;

  private final ExecutionEngine executionEngine;

  private final BuiltinFunctionRepository repository;

  /**
   * Parse, analyze, plan and execute the query.
   * @param request       SQL query request
   * @param listener      callback listener
   */
  public void execute(SQLQueryRequest request, ResponseListener<QueryResponse> listener) {
    try {
      executionEngine.execute(
                        plan(
                            analyze(
                                parse(request.getQuery()))), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Given physical plan, execute it and listen on response.
   * @param plan        physical plan
   * @param listener    callback listener
   */
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    try {
      executionEngine.execute(plan, listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Given physical plan, explain it.
   * @param plan        physical plan
   * @param listener    callback listener
   */
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    try {
      executionEngine.explain(plan, listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Parse query and convert parse tree (CST) to abstract syntax tree (AST).
   */
  public UnresolvedPlan parse(String query) {
    ParseTree cst = parser.parse(query);
    return cst.accept(new AstBuilder(query));
  }

  /**
   * Analyze abstract syntax to generate logical plan.
   */
  public LogicalPlan analyze(UnresolvedPlan ast) {
    return analyzer.analyze(ast, new AnalysisContext());
  }

  /**
   * Generate optimal physical plan from logical plan.
   */
  public PhysicalPlan plan(LogicalPlan logicalPlan) {
    return new Planner(storageEngine, LogicalPlanOptimizer.create(new DSL(repository)))
        .plan(logicalPlan);
  }

}
