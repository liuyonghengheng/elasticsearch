


package org.elasticsearch.jdbc.protocol;

public class JdbcQueryParam implements Parameter {
    private Object value;

    private String type;

    public JdbcQueryParam(String type, Object value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public String getType() {
        return type;
    }
}
