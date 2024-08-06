package com.infinilabs.security.auth;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

public class AuthenticationFailedExceptions {

    public static ElasticsearchSecurityException createSecurityException(String msg, final Object... args) {
        ElasticsearchSecurityException e = new ElasticsearchSecurityException(msg, RestStatus.UNAUTHORIZED, null, args);
        e.addHeader("WWW-Authenticate", "Basic realm=\"Security\" charset=\"UTF-8\"");
        return e;
    }
}
