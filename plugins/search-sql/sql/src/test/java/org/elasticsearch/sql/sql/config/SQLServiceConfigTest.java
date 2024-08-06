


package org.elasticsearch.sql.sql.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class SQLServiceConfigTest {

  @Test
  public void shouldReturnSQLService() {
    SQLServiceConfig config = new SQLServiceConfig();
    assertNotNull(config.sqlService());
  }

}
