


package org.elasticsearch.jdbc.transport.http;

public class HttpParam {

    private String name;

    private String value;

    public HttpParam(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }
}
