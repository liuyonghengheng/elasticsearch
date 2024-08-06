

package org.elasticsearch.commons.authuser;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.commons.ConfigConstants;

public class AuthUserRequestBuilder {
    private final String auth;

    public AuthUserRequestBuilder(String auth) {
        if (Strings.isNullOrEmpty(auth)) {
            throw new IllegalArgumentException("Authorization token cannot be null");
        }
        this.auth = auth;
    }

    public Request build() {
        Request request = new Request("GET", "/_opendistro/_security/authinfo");
        request
            .setOptions(
                RequestOptions.DEFAULT
                    .toBuilder()
                    .addHeader(ConfigConstants.CONTENT_TYPE, ConfigConstants.CONTENT_TYPE_DEFAULT)
                    .addHeader(ConfigConstants.AUTHORIZATION, auth)
            );
        return request;
    }
}
