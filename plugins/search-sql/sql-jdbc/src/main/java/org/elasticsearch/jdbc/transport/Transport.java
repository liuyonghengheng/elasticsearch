


package org.elasticsearch.jdbc.transport;

public interface Transport {

    void close() throws TransportException;

    void setReadTimeout(int timeout);

}
