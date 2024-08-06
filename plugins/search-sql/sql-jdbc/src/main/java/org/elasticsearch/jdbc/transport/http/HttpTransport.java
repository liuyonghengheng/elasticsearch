


package org.elasticsearch.jdbc.transport.http;

import org.elasticsearch.jdbc.transport.Transport;
import org.elasticsearch.jdbc.transport.TransportException;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;

public interface HttpTransport extends Transport {

    CloseableHttpResponse doGet(String path, Header[] headers, HttpParam[] params, int timeout)
            throws TransportException;

    CloseableHttpResponse doPost(String path, Header[] headers, HttpParam[] params, String body, int timeout)
            throws TransportException;
}
