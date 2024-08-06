


package org.elasticsearch.jdbc.config;

public class HostnameVerificationConnectionProperty extends BoolConnectionProperty {

    public static final String KEY = "hostnameVerification";

    public HostnameVerificationConnectionProperty() {
        super(KEY);
    }

    @Override
    public Boolean getDefault() {
        return true;
    }
}
