


package org.elasticsearch.sql.legacy.rewriter;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Query Optimize Rule
 */
public interface RewriteRule<T extends SQLQueryExpr> {

    /**
     * Checking whether the rule match the query?
     *
     * @return true if the rule match to the query.
     * @throws SQLFeatureNotSupportedException
     */
    boolean match(T expr) throws SQLFeatureNotSupportedException;

    /**
     * Optimize the query.
     *
     * @throws SQLFeatureNotSupportedException
     */
    void rewrite(T expr) throws SQLFeatureNotSupportedException;
}
