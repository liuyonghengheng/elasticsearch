


package org.elasticsearch.jdbc.config;

public class TrustStoreTypeConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "trustStoreType";

    public TrustStoreTypeConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return "JKS";
    }

}
