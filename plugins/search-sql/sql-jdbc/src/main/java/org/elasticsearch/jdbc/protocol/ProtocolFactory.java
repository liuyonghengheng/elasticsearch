


package org.elasticsearch.jdbc.protocol;

import org.elasticsearch.jdbc.config.ConnectionConfig;
import org.elasticsearch.jdbc.transport.Transport;

public interface ProtocolFactory<P extends Protocol, T extends Transport> {
    P getProtocol(ConnectionConfig config, T transport);
}
