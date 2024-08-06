


package org.elasticsearch.sql.sql.parser;

import static java.util.Collections.emptyList;
import static org.elasticsearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.FromClauseContext;
import static org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.HavingClauseContext;
import static org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.SelectClauseContext;
import static org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.SelectElementContext;
import static org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.SubqueryAsRelationContext;
import static org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.TableAsRelationContext;
import static org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.WhereClauseContext;
import static org.elasticsearch.sql.sql.parser.ParserUtils.getTextInQuery;
import static org.elasticsearch.sql.utils.SystemIndexUtils.TABLE_INFO;
import static org.elasticsearch.sql.utils.SystemIndexUtils.mappingTable;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.sql.ast.expression.Alias;
import org.elasticsearch.sql.ast.expression.AllFields;
import org.elasticsearch.sql.ast.expression.Function;
import org.elasticsearch.sql.ast.expression.UnresolvedExpression;
import org.elasticsearch.sql.ast.tree.Filter;
import org.elasticsearch.sql.ast.tree.Limit;
import org.elasticsearch.sql.ast.tree.Project;
import org.elasticsearch.sql.ast.tree.Relation;
import org.elasticsearch.sql.ast.tree.RelationSubquery;
import org.elasticsearch.sql.ast.tree.UnresolvedPlan;
import org.elasticsearch.sql.ast.tree.Values;
import org.elasticsearch.sql.common.antlr.SyntaxCheckException;
import org.elasticsearch.sql.common.utils.StringUtils;
import org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser;
import org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.QuerySpecificationContext;
import org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParserBaseVisitor;
import org.elasticsearch.sql.sql.parser.context.ParsingContext;

/**
 * Abstract syntax tree (AST) builder.
 */
@RequiredArgsConstructor
public class AstBuilder extends ElasticsearchSQLParserBaseVisitor<UnresolvedPlan> {

  private final AstExpressionBuilder expressionBuilder = new AstExpressionBuilder();

  /**
   * Parsing context stack that contains context for current query parsing.
   */
  private final ParsingContext context = new ParsingContext();

  /**
   * SQL query to get original token text. This is necessary because token.getText() returns
   * text without whitespaces or other characters discarded by lexer.
   */
  private final String query;

  @Override
  public UnresolvedPlan visitShowStatement(ElasticsearchSQLParser.ShowStatementContext ctx) {
    final UnresolvedExpression tableFilter = visitAstExpression(ctx.tableFilter());
    return new Project(Collections.singletonList(AllFields.of()))
        .attach(new Filter(tableFilter).attach(new Relation(qualifiedName(TABLE_INFO))));
  }

  @Override
  public UnresolvedPlan visitDescribeStatement(ElasticsearchSQLParser.DescribeStatementContext ctx) {
    final Function tableFilter = (Function) visitAstExpression(ctx.tableFilter());
    final String tableName = tableFilter.getFuncArgs().get(1).toString();
    final Relation table = new Relation(qualifiedName(mappingTable(tableName.toString())));
    if (ctx.columnFilter() == null) {
      return new Project(Collections.singletonList(AllFields.of())).attach(table);
    } else {
      return new Project(Collections.singletonList(AllFields.of()))
          .attach(new Filter(visitAstExpression(ctx.columnFilter())).attach(table));
    }
  }

  @Override
  public UnresolvedPlan visitQuerySpecification(QuerySpecificationContext queryContext) {
    context.push();
    context.peek().collect(queryContext, query);

    Project project = (Project) visit(queryContext.selectClause());

    if (queryContext.fromClause() == null) {
      Optional<UnresolvedExpression> allFields =
          project.getProjectList().stream().filter(node -> node instanceof AllFields)
              .findFirst();
      if (allFields.isPresent()) {
        throw new SyntaxCheckException("No FROM clause found for select all");
      }
      // Attach an Values operator with only a empty row inside so that
      // Project operator can have a chance to evaluate its expression
      // though the evaluation doesn't have any dependency on what's in Values.
      Values emptyValue = new Values(ImmutableList.of(emptyList()));
      return project.attach(emptyValue);
    }

    // If limit (and offset) keyword exists:
    // Add Limit node, plan structure becomes:
    // Project -> Limit -> visit(fromClause)
    // Else:
    // Project -> visit(fromClause)
    UnresolvedPlan from = visit(queryContext.fromClause());
    if (queryContext.limitClause() != null) {
      from = visit(queryContext.limitClause()).attach(from);
    }
    UnresolvedPlan result = project.attach(from);
    context.pop();
    return result;
  }

  @Override
  public UnresolvedPlan visitSelectClause(SelectClauseContext ctx) {
    ImmutableList.Builder<UnresolvedExpression> builder =
        new ImmutableList.Builder<>();
    if (ctx.selectElements().star != null) { //TODO: project operator should be required?
      builder.add(AllFields.of());
    }
    ctx.selectElements().selectElement().forEach(field -> builder.add(visitSelectItem(field)));
    return new Project(builder.build());
  }

  @Override
  public UnresolvedPlan visitLimitClause(ElasticsearchSQLParser.LimitClauseContext ctx) {
    return new Limit(
        Integer.parseInt(ctx.limit.getText()),
        ctx.offset == null ? 0 : Integer.parseInt(ctx.offset.getText())
    );
  }

  @Override
  public UnresolvedPlan visitFromClause(FromClauseContext ctx) {
    UnresolvedPlan result = visit(ctx.relation());

    if (ctx.whereClause() != null) {
      result = visit(ctx.whereClause()).attach(result);
    }

    // Because aggregation maybe implicit, this has to be handled here instead of visitGroupByClause
    AstAggregationBuilder aggBuilder = new AstAggregationBuilder(context.peek());
    UnresolvedPlan aggregation = aggBuilder.visit(ctx.groupByClause());
    if (aggregation != null) {
      result = aggregation.attach(result);
    }

    if (ctx.havingClause() != null) {
      result = visit(ctx.havingClause()).attach(result);
    }

    if (ctx.orderByClause() != null) {
      AstSortBuilder sortBuilder = new AstSortBuilder(context.peek());
      result = sortBuilder.visit(ctx.orderByClause()).attach(result);
    }
    return result;
  }

  @Override
  public UnresolvedPlan visitTableAsRelation(TableAsRelationContext ctx) {
    String tableAlias = (ctx.alias() == null) ? null
        : StringUtils.unquoteIdentifier(ctx.alias().getText());
    return new Relation(visitAstExpression(ctx.tableName()), tableAlias);
  }

  @Override
  public UnresolvedPlan visitSubqueryAsRelation(SubqueryAsRelationContext ctx) {
    return new RelationSubquery(visit(ctx.subquery), ctx.alias().getText());
  }

  @Override
  public UnresolvedPlan visitWhereClause(WhereClauseContext ctx) {
    return new Filter(visitAstExpression(ctx.expression()));
  }

  @Override
  public UnresolvedPlan visitHavingClause(HavingClauseContext ctx) {
    AstHavingFilterBuilder builder = new AstHavingFilterBuilder(context.peek());
    return new Filter(builder.visit(ctx.expression()));
  }

  @Override
  protected UnresolvedPlan aggregateResult(UnresolvedPlan aggregate, UnresolvedPlan nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }

  private UnresolvedExpression visitAstExpression(ParseTree tree) {
    return expressionBuilder.visit(tree);
  }

  private UnresolvedExpression visitSelectItem(SelectElementContext ctx) {
    String name = StringUtils.unquoteIdentifier(getTextInQuery(ctx.expression(), query));
    UnresolvedExpression expr = visitAstExpression(ctx.expression());

    if (ctx.alias() == null) {
      return new Alias(name, expr);
    } else {
      String alias = StringUtils.unquoteIdentifier(ctx.alias().getText());
      return new Alias(name, expr, alias);
    }
  }

}
