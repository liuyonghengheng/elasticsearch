


package org.elasticsearch.sql.legacy.executor;

import java.io.IOException;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.sql.legacy.exception.SqlParseException;

/**
 * Created by Eliran on 21/8/2016.
 */
public interface ElasticHitsExecutor {
    void run() throws IOException, SqlParseException;

    SearchHits getHits();
}
