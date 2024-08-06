


package org.elasticsearch.jdbc.transport;

import java.io.IOException;


public class TransportException extends IOException {

    public TransportException() {
        super();
    }

    public TransportException(String message) {
        super(message);
    }

    public TransportException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransportException(Throwable cause) {
        super(cause);
    }
}
