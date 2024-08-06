


package org.elasticsearch.jdbc.config;

public class HostConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "host";

    public HostConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return "localhost";
    }

}
