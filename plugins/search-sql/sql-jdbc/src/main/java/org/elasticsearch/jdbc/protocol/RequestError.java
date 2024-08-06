


package org.elasticsearch.jdbc.protocol;

public interface RequestError {
    String getReason();

    String getDetails();

    String getType();
}
