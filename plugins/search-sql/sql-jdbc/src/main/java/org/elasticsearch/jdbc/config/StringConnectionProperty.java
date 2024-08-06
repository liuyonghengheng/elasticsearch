


package org.elasticsearch.jdbc.config;

public class StringConnectionProperty extends ConnectionProperty<String> {

    public StringConnectionProperty(String key) {
        super(key);
    }

    @Override
    protected String parseValue(Object value) throws ConnectionPropertyException {

        if (value == null) {
            return getDefault();
        } else if (value instanceof String) {
            return (String) value;
        }

        throw new ConnectionPropertyException(getKey(),
                String.format("Property %s requires a valid string. " +
                        "Invalid value of type: %s specified.", getKey(), value.getClass().getName()));

    }

    @Override
    public String getDefault() {
        return null;
    }
}
