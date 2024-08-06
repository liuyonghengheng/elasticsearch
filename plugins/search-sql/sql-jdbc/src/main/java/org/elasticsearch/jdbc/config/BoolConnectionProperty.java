


package org.elasticsearch.jdbc.config;

public class BoolConnectionProperty extends ConnectionProperty<Boolean> {

    public BoolConnectionProperty(String key) {
        super(key);
    }

    @Override
    protected Boolean parseValue(Object value) throws ConnectionPropertyException {

        if (value == null) {
            return getDefault();
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }

        throw new ConnectionPropertyException(getKey(),
                String.format("Property %s requires a valid boolean. Invalid property value of type %s. ",
                        getKey(), value.getClass().getName()));
    }

    @Override
    public Boolean getDefault() {
        return false;
    }
}
