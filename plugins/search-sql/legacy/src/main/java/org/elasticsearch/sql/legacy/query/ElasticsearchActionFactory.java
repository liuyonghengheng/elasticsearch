


package org.elasticsearch.sql.legacy.query;

import static org.elasticsearch.sql.legacy.domain.IndexStatement.StatementType;
import static org.elasticsearch.sql.legacy.utils.Util.toSqlExpr;

import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.sql.common.setting.Settings;
import org.elasticsearch.sql.legacy.domain.ColumnTypeProvider;
import org.elasticsearch.sql.legacy.domain.Delete;
import org.elasticsearch.sql.legacy.domain.IndexStatement;
import org.elasticsearch.sql.legacy.domain.JoinSelect;
import org.elasticsearch.sql.legacy.domain.QueryActionRequest;
import org.elasticsearch.sql.legacy.domain.Select;
import org.elasticsearch.sql.legacy.esdomain.LocalClusterState;
import org.elasticsearch.sql.legacy.exception.SQLFeatureDisabledException;
import org.elasticsearch.sql.legacy.exception.SqlParseException;
import org.elasticsearch.sql.legacy.executor.ElasticResultHandler;
import org.elasticsearch.sql.legacy.executor.Format;
import org.elasticsearch.sql.legacy.executor.QueryActionElasticExecutor;
import org.elasticsearch.sql.legacy.executor.adapter.QueryPlanQueryAction;
import org.elasticsearch.sql.legacy.executor.adapter.QueryPlanRequestBuilder;
import org.elasticsearch.sql.legacy.parser.ElasticLexer;
import org.elasticsearch.sql.legacy.parser.SqlParser;
import org.elasticsearch.sql.legacy.parser.SubQueryExpression;
import org.elasticsearch.sql.legacy.query.join.ElasticsearchJoinQueryActionFactory;
import org.elasticsearch.sql.legacy.query.multi.MultiQueryAction;
import org.elasticsearch.sql.legacy.query.multi.MultiQuerySelect;
import org.elasticsearch.sql.legacy.query.planner.core.BindingTupleQueryPlanner;
import org.elasticsearch.sql.legacy.rewriter.RewriteRuleExecutor;
import org.elasticsearch.sql.legacy.rewriter.alias.TableAliasPrefixRemoveRule;
import org.elasticsearch.sql.legacy.rewriter.identifier.UnquoteIdentifierRule;
import org.elasticsearch.sql.legacy.rewriter.join.JoinRewriteRule;
import org.elasticsearch.sql.legacy.rewriter.matchtoterm.TermFieldRewriter;
import org.elasticsearch.sql.legacy.rewriter.matchtoterm.TermFieldRewriter.TermRewriterFilter;
import org.elasticsearch.sql.legacy.rewriter.nestedfield.NestedFieldRewriter;
import org.elasticsearch.sql.legacy.rewriter.ordinal.OrdinalRewriterRule;
import org.elasticsearch.sql.legacy.rewriter.parent.SQLExprParentSetterRule;
import org.elasticsearch.sql.legacy.rewriter.subquery.SubQueryRewriteRule;
import org.elasticsearch.sql.legacy.utils.StringUtils;

public class ElasticsearchActionFactory {

    public static QueryAction create(Client client, String sql)
        throws SqlParseException, SQLFeatureNotSupportedException, SQLFeatureDisabledException {
        return create(client, new QueryActionRequest(sql, new ColumnTypeProvider(), Format.JSON));
    }

    /**
     * Create the compatible Query object
     * based on the SQL query.
     *
     * @param request The SQL query.
     * @return Query object.
     */
    public static QueryAction create(Client client, QueryActionRequest request)
        throws SqlParseException, SQLFeatureNotSupportedException, SQLFeatureDisabledException {
        String sql = request.getSql();
        // Remove line breaker anywhere and semicolon at the end
        sql = sql.replaceAll("\\R", " ").trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        switch (getFirstWord(sql)) {
            case "SELECT":
                SQLQueryExpr sqlExpr = (SQLQueryExpr) toSqlExpr(sql);

                RewriteRuleExecutor<SQLQueryExpr> ruleExecutor = RewriteRuleExecutor.builder()
                        .withRule(new SQLExprParentSetterRule())
                        .withRule(new OrdinalRewriterRule(sql))
                        .withRule(new UnquoteIdentifierRule())
                        .withRule(new TableAliasPrefixRemoveRule())
                        .withRule(new SubQueryRewriteRule())
                        .build();
                ruleExecutor.executeOn(sqlExpr);
                sqlExpr.accept(new NestedFieldRewriter());

                if (isMulti(sqlExpr)) {
                    sqlExpr.accept(new TermFieldRewriter(TermRewriterFilter.MULTI_QUERY));
                    MultiQuerySelect multiSelect =
                            new SqlParser().parseMultiSelect((SQLUnionQuery) sqlExpr.getSubQuery().getQuery());
                    return new MultiQueryAction(client, multiSelect);
                } else if (isJoin(sqlExpr, sql)) {
                    new JoinRewriteRule(LocalClusterState.state()).rewrite(sqlExpr);
                    sqlExpr.accept(new TermFieldRewriter(TermRewriterFilter.JOIN));
                    JoinSelect joinSelect = new SqlParser().parseJoinSelect(sqlExpr);
                    return ElasticsearchJoinQueryActionFactory.createJoinAction(client, joinSelect);
                } else {
                    sqlExpr.accept(new TermFieldRewriter());
                    // migrate aggregation to query planner framework.
                    if (shouldMigrateToQueryPlan(sqlExpr, request.getFormat())) {
                        return new QueryPlanQueryAction(new QueryPlanRequestBuilder(
                                new BindingTupleQueryPlanner(client, sqlExpr, request.getTypeProvider())));
                    }
                    Select select = new SqlParser().parseSelect(sqlExpr);
                    return handleSelect(client, select);
                }
            case "DELETE":
                if (isSQLDeleteEnabled()) {
                    SQLStatementParser parser = createSqlStatementParser(sql);
                    SQLDeleteStatement deleteStatement = parser.parseDeleteStatement();
                    Delete delete = new SqlParser().parseDelete(deleteStatement);
                    return new DeleteQueryAction(client, delete);
                } else {
                    throw new SQLFeatureDisabledException(
                        StringUtils.format("DELETE clause is disabled by default and will be "
                                + "deprecated. Using the %s setting to enable it",
                            Settings.Key.SQL_DELETE_ENABLED.getKeyValue()));
                }
            case "SHOW":
                IndexStatement showStatement = new IndexStatement(StatementType.SHOW, sql);
                return new ShowQueryAction(client, showStatement);
            case "DESCRIBE":
                IndexStatement describeStatement = new IndexStatement(StatementType.DESCRIBE, sql);
                return new DescribeQueryAction(client, describeStatement);
            default:
                throw new SQLFeatureNotSupportedException(
                        String.format("Query must start with SELECT, DELETE, SHOW or DESCRIBE: %s", sql));
        }
    }

    private static boolean isSQLDeleteEnabled() {
        return LocalClusterState.state().getSettingValue(Settings.Key.SQL_DELETE_ENABLED);
    }

    private static String getFirstWord(String sql) {
        int endOfFirstWord = sql.indexOf(' ');
        return sql.substring(0, endOfFirstWord > 0 ? endOfFirstWord : sql.length()).toUpperCase();
    }

    private static boolean isMulti(SQLQueryExpr sqlExpr) {
        return sqlExpr.getSubQuery().getQuery() instanceof SQLUnionQuery;
    }

    private static void executeAndFillSubQuery(Client client,
                                               SubQueryExpression subQueryExpression,
                                               QueryAction queryAction) throws SqlParseException {
        List<Object> values = new ArrayList<>();
        Object queryResult;
        try {
            queryResult = QueryActionElasticExecutor.executeAnyAction(client, queryAction);
        } catch (Exception e) {
            throw new SqlParseException("could not execute SubQuery: " + e.getMessage());
        }

        String returnField = subQueryExpression.getReturnField();
        if (queryResult instanceof SearchHits) {
            SearchHits hits = (SearchHits) queryResult;
            for (SearchHit hit : hits) {
                values.add(ElasticResultHandler.getFieldValue(hit, returnField));
            }
        } else {
            throw new SqlParseException("on sub queries only support queries that return Hits and not aggregations");
        }
        subQueryExpression.setValues(values.toArray());
    }

    private static QueryAction handleSelect(Client client, Select select) {
        if (select.isAggregate) {
            return new AggregationQueryAction(client, select);
        } else {
            return new DefaultQueryAction(client, select);
        }
    }

    private static SQLStatementParser createSqlStatementParser(String sql) {
        ElasticLexer lexer = new ElasticLexer(sql);
        lexer.nextToken();
        return new MySqlStatementParser(lexer);
    }

    private static boolean isJoin(SQLQueryExpr sqlExpr, String sql) {
        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) sqlExpr.getSubQuery().getQuery();
        return query.getFrom() instanceof SQLJoinTableSource
               && ((SQLJoinTableSource) query.getFrom()).getJoinType() != SQLJoinTableSource.JoinType.COMMA;
    }

    @VisibleForTesting
    public static boolean shouldMigrateToQueryPlan(SQLQueryExpr expr, Format format) {
        // The JSON format will return the Elasticsearch aggregation result, which is not supported by the QueryPlanner.
        if (format == Format.JSON) {
            return false;
        }
        QueryPlannerScopeDecider decider = new QueryPlannerScopeDecider();
        return decider.isInScope(expr);
    }

    private static class QueryPlannerScopeDecider extends MySqlASTVisitorAdapter {
        private boolean hasAggregationFunc = false;
        private boolean hasNestedFunction = false;
        private boolean hasGroupBy = false;
        private boolean hasAllColumnExpr = false;

        public boolean isInScope(SQLQueryExpr expr) {
            expr.accept(this);
            return !hasAllColumnExpr && !hasNestedFunction && (hasGroupBy || hasAggregationFunc);
        }

        @Override
        public boolean visit(SQLSelectItem expr) {
            if (expr.getExpr() instanceof SQLAllColumnExpr) {
                hasAllColumnExpr = true;
            }
            return super.visit(expr);
        }

        @Override
        public boolean visit(SQLSelectGroupByClause expr) {
            hasGroupBy = true;
            return super.visit(expr);
        }

        @Override
        public boolean visit(SQLAggregateExpr expr) {
            hasAggregationFunc = true;
            return super.visit(expr);
        }

        @Override
        public boolean visit(SQLMethodInvokeExpr expr) {
            if (expr.getMethodName().equalsIgnoreCase("nested")) {
                hasNestedFunction = true;
            }
            return super.visit(expr);
        }
    }
}
