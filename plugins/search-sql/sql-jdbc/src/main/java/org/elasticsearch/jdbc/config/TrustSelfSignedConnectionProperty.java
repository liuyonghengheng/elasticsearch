


package org.elasticsearch.jdbc.config;

public class TrustSelfSignedConnectionProperty extends BoolConnectionProperty {

    public static final String KEY = "trustSelfSigned";

    public TrustSelfSignedConnectionProperty() {
        super(KEY);
    }
}
