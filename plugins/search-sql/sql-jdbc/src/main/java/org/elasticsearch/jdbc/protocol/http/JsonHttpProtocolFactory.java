


package org.elasticsearch.jdbc.protocol.http;

import org.elasticsearch.jdbc.config.ConnectionConfig;
import org.elasticsearch.jdbc.protocol.ProtocolFactory;
import org.elasticsearch.jdbc.transport.http.HttpTransport;


public class JsonHttpProtocolFactory implements ProtocolFactory<JsonHttpProtocol, HttpTransport> {

    public static JsonHttpProtocolFactory INSTANCE = new JsonHttpProtocolFactory();

    private JsonHttpProtocolFactory() {

    }

    @Override
    public JsonHttpProtocol getProtocol(ConnectionConfig connectionConfig, HttpTransport transport) {
        return new JsonHttpProtocol(transport);
    }
}
