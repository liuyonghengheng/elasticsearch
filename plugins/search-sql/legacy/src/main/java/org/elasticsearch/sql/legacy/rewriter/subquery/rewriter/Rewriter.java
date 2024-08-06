


package org.elasticsearch.sql.legacy.rewriter.subquery.rewriter;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

/**
 * Interface of SQL Rewriter
 */
public interface Rewriter {

    /**
     * Whether the Rewriter can rewrite the SQL?
     */
    boolean canRewrite();

    /**
     * Rewrite the SQL.
     */
    void rewrite();

    default SQLBinaryOpExpr and(SQLBinaryOpExpr left, SQLBinaryOpExpr right) {
        return new SQLBinaryOpExpr(left, SQLBinaryOperator.BooleanAnd, right);
    }
}
