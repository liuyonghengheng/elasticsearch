


package org.elasticsearch.jdbc.config;

public class RequestCompressionConnectionProperty extends BoolConnectionProperty {

    public static final String KEY = "requestCompression";

    public RequestCompressionConnectionProperty() {
        super(KEY);
    }
}
