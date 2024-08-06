


package org.elasticsearch.sql.legacy.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.legacy.executor.Format;

/**
 * The definition of QueryActionRequest.
 */
@Getter
@RequiredArgsConstructor
public class QueryActionRequest {
    private final String sql;
    private final ColumnTypeProvider typeProvider;
    private final Format format;
}
