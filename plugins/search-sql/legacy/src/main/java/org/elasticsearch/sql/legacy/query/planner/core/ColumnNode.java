


package org.elasticsearch.sql.legacy.query.planner.core;

import com.google.common.base.Strings;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.elasticsearch.sql.legacy.executor.format.Schema;
import org.elasticsearch.sql.legacy.expression.core.Expression;

/**
 * The definition of column node.
 */
@Builder
@Setter
@Getter
@ToString
public class ColumnNode {
    private String name;
    private String alias;
    private Schema.Type type;
    private Expression expr;

    public String columnName() {
        return Strings.isNullOrEmpty(alias) ? name : alias;
    }
}
