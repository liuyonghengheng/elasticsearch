


package org.elasticsearch.jdbc.config;

public class RegionConnectionProperty extends StringConnectionProperty {

    public static final String KEY = "region";

    public RegionConnectionProperty() {
        super(KEY);
    }

    public String getDefault() {
        return null;
    }
}
