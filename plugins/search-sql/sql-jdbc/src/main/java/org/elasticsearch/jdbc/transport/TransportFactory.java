


package org.elasticsearch.jdbc.transport;

import org.elasticsearch.jdbc.config.ConnectionConfig;
import org.elasticsearch.jdbc.logging.Logger;

public interface TransportFactory<T extends Transport> {

    T getTransport(ConnectionConfig config, Logger log, String userAgent) throws TransportException;
}
