


package org.elasticsearch.jdbc;

import java.sql.SQLException;

public interface ElasticsearchConnection extends java.sql.Connection {

    String getClusterName() throws SQLException;

    String getClusterUUID() throws SQLException;

}
