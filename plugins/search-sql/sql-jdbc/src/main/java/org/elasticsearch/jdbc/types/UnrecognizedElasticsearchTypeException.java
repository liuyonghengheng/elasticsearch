


package org.elasticsearch.jdbc.types;

public class UnrecognizedElasticsearchTypeException extends IllegalArgumentException {

    public UnrecognizedElasticsearchTypeException() {
    }

    public UnrecognizedElasticsearchTypeException(String s) {
        super(s);
    }

    public UnrecognizedElasticsearchTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnrecognizedElasticsearchTypeException(Throwable cause) {
        super(cause);
    }
}
