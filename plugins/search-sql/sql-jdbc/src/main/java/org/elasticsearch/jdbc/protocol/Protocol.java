


package org.elasticsearch.jdbc.protocol;

import org.elasticsearch.jdbc.protocol.exceptions.ResponseException;

import java.io.IOException;

public interface Protocol extends AutoCloseable {

    ConnectionResponse connect(int timeout) throws ResponseException, IOException;

    QueryResponse execute(QueryRequest request) throws ResponseException, IOException;

    void close() throws IOException;
}
