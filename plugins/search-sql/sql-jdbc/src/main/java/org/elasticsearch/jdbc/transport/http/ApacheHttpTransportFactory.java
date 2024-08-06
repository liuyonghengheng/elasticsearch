


package org.elasticsearch.jdbc.transport.http;

import org.elasticsearch.jdbc.config.ConnectionConfig;
import org.elasticsearch.jdbc.logging.Logger;
import org.elasticsearch.jdbc.transport.TransportException;
import org.elasticsearch.jdbc.transport.TransportFactory;

public class ApacheHttpTransportFactory implements TransportFactory<ApacheHttpTransport> {

    public static ApacheHttpTransportFactory INSTANCE = new ApacheHttpTransportFactory();

    private ApacheHttpTransportFactory() {

    }

    @Override
    public ApacheHttpTransport getTransport(ConnectionConfig config, Logger log, String userAgent) throws TransportException {
        return new ApacheHttpTransport(config, log, userAgent);
    }
}
