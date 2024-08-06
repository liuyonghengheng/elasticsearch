


package org.elasticsearch.jdbc.config;

public class KeyStoreLocationConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "keyStoreLocation";

    public KeyStoreLocationConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return null;
    }

}
