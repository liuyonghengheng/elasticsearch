


package org.elasticsearch.jdbc.config;

public class KeyStoreTypeConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "keyStoreType";

    public KeyStoreTypeConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return "JKS";
    }

}
