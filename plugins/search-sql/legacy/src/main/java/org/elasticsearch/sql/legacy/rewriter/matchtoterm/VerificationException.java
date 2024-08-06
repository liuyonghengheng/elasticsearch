


package org.elasticsearch.sql.legacy.rewriter.matchtoterm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

public class VerificationException extends ElasticsearchException {

    public VerificationException(String message) {
        super(message);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
