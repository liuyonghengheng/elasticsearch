


package org.elasticsearch.jdbc.protocol.http;

import org.elasticsearch.jdbc.protocol.exceptions.ResponseException;
import org.apache.http.HttpResponse;

public interface HttpResponseHandler<T> {

    T handleResponse(HttpResponse response) throws ResponseException;
}
