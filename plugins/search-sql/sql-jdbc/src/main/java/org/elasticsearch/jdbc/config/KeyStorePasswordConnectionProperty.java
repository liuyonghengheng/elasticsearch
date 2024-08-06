


package org.elasticsearch.jdbc.config;

public class KeyStorePasswordConnectionProperty extends StringConnectionProperty {
    public static final String KEY = "keyStorePassword";

    public KeyStorePasswordConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return null;
    }

}
