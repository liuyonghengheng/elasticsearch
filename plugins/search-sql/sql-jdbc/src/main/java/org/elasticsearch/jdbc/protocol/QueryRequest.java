


package org.elasticsearch.jdbc.protocol;

import java.util.List;

public interface QueryRequest {

    String getQuery();

    List<? extends Parameter> getParameters();

    public int getFetchSize();

}
