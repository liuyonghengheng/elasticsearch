


package org.elasticsearch.jdbc.protocol.http;

import org.elasticsearch.jdbc.config.ConnectionConfig;
import org.elasticsearch.jdbc.protocol.ProtocolFactory;
import org.elasticsearch.jdbc.transport.http.HttpTransport;

/**
 * Factory to create JsonCursorHttpProtocol objects
 *
 *  @author abbas hussain
 *  @since 07.05.20
 */
public class JsonCursorHttpProtocolFactory implements ProtocolFactory<JsonCursorHttpProtocol, HttpTransport> {

    public static JsonCursorHttpProtocolFactory INSTANCE = new JsonCursorHttpProtocolFactory();

    private JsonCursorHttpProtocolFactory() {

    }

    @Override
    public JsonCursorHttpProtocol getProtocol(ConnectionConfig connectionConfig, HttpTransport transport) {
        return new JsonCursorHttpProtocol(transport);
    }
}
