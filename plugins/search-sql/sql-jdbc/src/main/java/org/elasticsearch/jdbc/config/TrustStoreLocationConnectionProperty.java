


package org.elasticsearch.jdbc.config;

public class TrustStoreLocationConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "trustStoreLocation";

    public TrustStoreLocationConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return null;
    }

}
