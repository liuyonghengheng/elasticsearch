


package org.elasticsearch.jdbc.config;

public class TrustStorePasswordConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "trustStorePassword";

    public TrustStorePasswordConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return null;
    }

}
